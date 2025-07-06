import os
import logging
import json
from datetime import date
from datetime import datetime
from typing import Dict, List, Any, Optional
import pytz
import requests
from google.cloud import bigquery

from llm_core import LLMCore
from claim_manager.claim_generator import (
    ClaimGenerator,
    generar_reclamacion_eps,
    generar_reclamacion_supersalud,
    generar_tutela,
    generar_desacato
)

# Configuración de logging
target = os.getenv('LOG_TARGET', 'stdout')
if target == 'stdout':
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PatientModule:
    def __init__(self):
        self.bq = bigquery.Client()
        self.project = os.getenv('PROJECT_ID')
        self.dataset = os.getenv('DATASET_ID')
        self.table = os.getenv('TABLE_ID')
        self.api_url = os.getenv('API_RECEPCIONISTA_URL')

    def check_and_send_followups(self, today: date = None) -> None:
        """
        Envía mensajes de seguimiento usando session_id.
        El escalamiento automático se delega completamente al ClaimManager.
        """
        tz_colombia = pytz.timezone('America/Bogota')
        today = datetime.now(tz_colombia).date()
        logger.info(f"🔍 Buscando reclamaciones pendientes para {today.isoformat()}")

        sql = f"""
        SELECT 
            t.paciente_clave,
            pres.user_id AS user_id,
            pres.id_session AS session_id  --
        FROM `{self.project}.{self.dataset}.{self.table}` AS t,
             UNNEST(t.prescripciones) AS pres,
             UNNEST(t.reclamaciones) AS rec
        WHERE rec.fecha_revision = '{today.isoformat()}'
          AND rec.estado_reclamacion != 'resuelto'
        """
        logger.info(f"📝 SQL ejecutado: {sql}")

        for row in self.bq.query(sql).result():
            user_id = row.user_id
            patient_key = row.paciente_clave
            session_id = row.session_id 
            
            try:
                self.send_message(
                    user_id, session_id, 
                    "Hola, ¿ya le entregaron los medicamentos relacionados con su solicitud?",
                    buttons=[
                        {"text": "✅ Sí", "callback_data": f"followup_yes_{session_id}"},   # ✅ USAR SESSION_ID
                        {"text": "❌ No", "callback_data": f"followup_no_{session_id}"},    # ✅ USAR SESSION_ID
                    ]
                )
                
                # Determinar el identificador de usuario para logging
                if session_id.startswith("WA_"):
                    phone_number = self._extract_phone_from_whatsapp_session(session_id)
                    user_identifier = f"WhatsApp:{phone_number}" if phone_number else f"WhatsApp:unknown"
                else:
                    user_identifier = f"Telegram:{user_id}"
                
                logger.info(f"Mensaje enviado a {user_identifier} para session {session_id} (paciente {patient_key})")
            except Exception as e:
                logger.error(f"Error enviando mensaje: {e}")

    def send_message(self, user_id: str, session_id: str, text: str, buttons: list = None) -> None:
        """Envía mensaje via API recepcionista."""
        # Determinar el canal basado en el session_id
        channel_prefix = self._get_channel_from_session(session_id)
        
        if channel_prefix == "WA":
            # Para WhatsApp, extraer el número de teléfono desde el session_id
            # Formato session_id: WA_573146748777_20250704_184334
            phone_number = self._extract_phone_from_whatsapp_session(session_id)
            if phone_number:
                formatted_user_id = f"WA_{phone_number}"
            else:
                logger.error(f"No se pudo extraer número de teléfono de session_id: {session_id}")
                return
        else:
            # Para Telegram, usar el formato tradicional
            formatted_user_id = f"TL_{user_id}"
        
        payload = {
            "user_id": formatted_user_id,
            "session_id": session_id,  # ✅ USAR SESSION_ID REAL
            "message": text
        }
        if buttons:
            payload["buttons"] = buttons

        logger.info(f"📤 Enviando payload a recepcionista: {json.dumps(payload)}")
        logger.info(f"📤 URL destino: {self.api_url}/send_message")

        try:
            resp = requests.post(f"{self.api_url}/send_message", json=payload)

            # 🔍 LOG DE RESPUESTA
            logger.info(f"📥 Respuesta de recepcionista: {resp.status_code} - {resp.text}")

            if resp.status_code != 200:
                logger.error(f"❌ Error enviando mensaje: {resp.status_code} - {resp.text}")
        except Exception as e:
            logger.exception(f"❌ Excepción durante envío de mensaje: {e}")    


    def _get_channel_from_session(self, session_id: str) -> str:
        """
        Determina el canal (TL/WA) basándose en el session_id.
        Busca en Firestore la información de la sesión para obtener el canal.
        """
        try:
            # Consultar BigQuery para obtener información del canal desde prescripciones
            sql = f"""
            SELECT 
                canal_contacto,
                user_id
            FROM `{self.project}.{self.dataset}.{self.table}` AS t,
                 UNNEST(t.prescripciones) AS pres
            WHERE pres.id_session = '{session_id}'
            LIMIT 1
            """
            
            results = self.bq.query(sql).result()
            for row in results:
                canal_contacto = row.canal_contacto
                user_id = row.user_id
                
                # Determinar canal basado en canal_contacto o user_id
                if canal_contacto == "WA" or (user_id and user_id.startswith("WA_")):
                    return "WA"
                else:
                    return "TL"
            
            # Si no se encuentra en prescripciones, buscar en reclamaciones
            sql_rec = f"""
            SELECT 
                t.prescripciones[SAFE_OFFSET(0)].canal_contacto as canal_contacto,
                t.prescripciones[SAFE_OFFSET(0)].user_id as user_id
            FROM `{self.project}.{self.dataset}.{self.table}` AS t,
                 UNNEST(t.reclamaciones) AS rec
            WHERE rec.id_session = '{session_id}'
            LIMIT 1
            """
            
            results_rec = self.bq.query(sql_rec).result()
            for row in results_rec:
                canal_contacto = row.canal_contacto
                user_id = row.user_id
                
                if canal_contacto == "WA" or (user_id and user_id.startswith("WA_")):
                    return "WA"
                else:
                    return "TL"
            
            # Por defecto, asumir Telegram
            logger.warning(f"No se pudo determinar canal para session_id {session_id}, asumiendo Telegram")
            return "TL"
            
        except Exception as e:
            logger.error(f"Error determinando canal para session_id {session_id}: {e}")
            return "TL"  # Fallback a Telegram

    def _extract_phone_from_whatsapp_session(self, session_id: str) -> Optional[str]:
        """
        Extrae el número de teléfono de un session_id de WhatsApp.
        Formato esperado: WA_573146748777_20250704_184334
        
        Args:
            session_id: ID de sesión de WhatsApp
            
        Returns:
            Número de teléfono o None si no se puede extraer
        """
        try:
            if not session_id.startswith("WA_"):
                return None
            
            # Remover prefijo WA_ y dividir por _
            parts = session_id[3:].split("_")
            
            if len(parts) >= 3:
                # El primer elemento después de WA_ debería ser el número de teléfono
                phone_number = parts[0]
                
                # Validar que sea un número válido
                if phone_number.isdigit() and len(phone_number) >= 10:
                    logger.info(f"Número de teléfono extraído de {session_id}: {phone_number}")
                    return phone_number
                else:
                    logger.error(f"Número de teléfono inválido extraído: {phone_number}")
                    return None
            else:
                logger.error(f"Formato de session_id inesperado: {session_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error extrayendo número de teléfono de session_id {session_id}: {e}")
            return None

    def update_reclamation_status(self, session_id: str, new_status: str) -> bool:
        """
        Actualiza estado de reclamación usando session_id.
        Si es resuelto, TODAS las reclamaciones van a resuelto.
        """
        try:
            patient_key = self._get_patient_key_by_session_id(session_id)
            if not patient_key:
                logger.error(f"No se encontró patient_key para session_id: {session_id}")
                return False
            
            logger.info(f"Session {session_id} corresponde a patient_key: {patient_key}")
            
            if new_status == "resuelto":
                # TODAS las reclamaciones a resuelto
                sql = f"""
                UPDATE `{self.project}.{self.dataset}.{self.table}` AS t
                SET reclamaciones = ARRAY(
                    SELECT AS STRUCT
                        r.med_no_entregados,
                        r.tipo_accion,
                        r.texto_reclamacion,
                        'resuelto' AS estado_reclamacion,
                        r.nivel_escalamiento,
                        r.url_documento,
                        r.numero_radicado,
                        r.fecha_radicacion,
                        r.fecha_revision,
                        r.id_session
                    FROM UNNEST(t.reclamaciones) AS r
                )
                WHERE paciente_clave = '{patient_key}'
                """
            else:
                # Solo las de la sesión específica
                sql = f"""
                UPDATE `{self.project}.{self.dataset}.{self.table}` AS t
                SET reclamaciones = ARRAY(
                    SELECT AS STRUCT
                        r.med_no_entregados,
                        r.tipo_accion,
                        r.texto_reclamacion,
                        CASE 
                            WHEN r.id_session = '{session_id}' THEN '{new_status}'
                            ELSE r.estado_reclamacion
                        END AS estado_reclamacion,
                        r.nivel_escalamiento,
                        r.url_documento,
                        r.numero_radicado,
                        r.fecha_radicacion,
                        r.fecha_revision,
                        r.id_session
                    FROM UNNEST(t.reclamaciones) AS r
                )
                WHERE paciente_clave = '{patient_key}'
                """
            
            self.bq.query(sql).result()
            logger.info(f"Estado actualizado a '{new_status}' para paciente {patient_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error actualizando estado para session {session_id}: {e}")
            return False

    def _get_patient_key_by_session_id(self, session_id: str) -> Optional[str]:
        """
        NUEVA FUNCIÓN: Busca el patient_key usando el session_id
        
        Args:
            session_id: ID de la sesión
            
        Returns:
            patient_key si se encuentra, None si no existe
        """
        try:
            # Buscar en prescripciones
            sql = f"""
            SELECT 
                paciente_clave
            FROM `{self.project}.{self.dataset}.{self.table}` AS t,
                 UNNEST(t.prescripciones) AS pres
            WHERE pres.id_session = '{session_id}'
            LIMIT 1
            """
            
            results = self.bq.query(sql).result()
            for row in results:
                return row.paciente_clave
            
            # Si no se encuentra en prescripciones, buscar en reclamaciones
            sql_reclamaciones = f"""
            SELECT 
                paciente_clave
            FROM `{self.project}.{self.dataset}.{self.table}` AS t,
                 UNNEST(t.reclamaciones) AS rec
            WHERE rec.id_session = '{session_id}'
            LIMIT 1
            """
            
            results_rec = self.bq.query(sql_reclamaciones).result()
            for row in results_rec:
                return row.paciente_clave
            
            return None
            
        except Exception as e:
            logger.error(f"Error buscando patient_key para session_id {session_id}: {e}")
            return None