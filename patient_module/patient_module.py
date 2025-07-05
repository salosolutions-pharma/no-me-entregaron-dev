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
                logger.info(f"Mensaje enviado a {user_id} para session {session_id} (paciente {patient_key})")
            except Exception as e:
                logger.error(f"Error enviando mensaje: {e}")

    def send_message(self, user_id: str, session_id: str, text: str, buttons: list = None) -> None:
        """Envía mensaje via API recepcionista."""
        payload = {
            "user_id": f"TL_{user_id}",
            "session_id": session_id,  # ✅ USAR SESSION_ID REAL
            "message": text
        }
        if buttons:
            payload["buttons"] = buttons

        resp = requests.post(f"{self.api_url}/send_message", json=payload)
        if resp.status_code != 200:
            logger.error(f"Error enviando mensaje: {resp.text}")

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