import os
import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
import pytz
import requests
from google.cloud import bigquery
from flask import Request
import functions_framework
from utils.logger_config import setup_structured_logging  # 👈 AGREGAR

# Configuración de logging
setup_structured_logging()  # 👈 CAMBIAR ESTA LÍNEA
logger = logging.getLogger(__name__)

class RadicadoSender:
    def __init__(self):
        self.bq = bigquery.Client()
        self.project = os.getenv('PROJECT_ID')
        self.dataset = os.getenv('DATASET_ID') 
        self.table = os.getenv('TABLE_ID')
        self.api_url = os.getenv('API_RECEPCIONISTA_URL')
        
        if not all([self.project, self.dataset, self.table, self.api_url]):
            raise ValueError("Variables de entorno faltantes: PROJECT_ID, DATASET_ID, TABLE_ID, API_RECEPCIONISTA_URL")

    def _get_channel_from_session(self, session_id: str) -> str:
        """Extrae el prefijo del canal del session_id."""
        if session_id.startswith("WA_"):
            return "WA"
        elif session_id.startswith("TL_"):
            return "TL"
        else:
            logger.warning(f"Session ID sin prefijo reconocido: {session_id}")
            return "UNKNOWN"

    def _extract_phone_from_whatsapp_session(self, session_id: str) -> str:
        """Extrae el número de teléfono del session_id de WhatsApp."""
        try:
            # Formato esperado: WA_573001234567_timestamp
            parts = session_id.split("_")
            if len(parts) >= 2 and parts[0] == "WA":
                return parts[1]
            return None
        except Exception as e:
            logger.error(f"Error extrayendo teléfono de {session_id}: {e}")
            return None

    def send_message(self, user_id: str, session_id: str, text: str) -> bool:
        """
        Envía mensaje via API recepcionista.
        Adaptado del patient_module pero sin botones.
        """
        channel_prefix = self._get_channel_from_session(session_id)
        
        if channel_prefix == "WA":
            phone_number = self._extract_phone_from_whatsapp_session(session_id)
            if phone_number:
                formatted_user_id = f"WA_{phone_number}"
            else:
                logger.error(f"No se pudo extraer número de teléfono de session_id: {session_id}")
                return False
        else:
            formatted_user_id = f"TL_{user_id}"
        
        payload = {
            "user_id": formatted_user_id,
            "session_id": session_id,
            "message": text
        }

        logger.info(f"📤 Enviando payload a recepcionista: {json.dumps(payload)}")
        logger.info(f"📤 URL destino: {self.api_url}/send_message")

        try:
            resp = requests.post(f"{self.api_url}/send_message", json=payload)
            logger.info(f"📥 Respuesta de recepcionista: {resp.status_code} - {resp.text}")

            if resp.status_code == 200:
                return True
            else:
                logger.error(f"❌ Error enviando mensaje: {resp.status_code} - {resp.text}")
                return False
        except Exception as e:
            logger.exception(f"❌ Excepción durante envío de mensaje: {e}")
            return False

    def update_reclamation_status(self, paciente_clave: str, nivel_escalamiento: int, new_status: str) -> bool:
        """
        Actualiza el estado de una reclamación específica y la fecha_radicacion.
        """
        try:
            # Obtener fecha actual en UTC-5 (Colombia)
            tz_colombia = pytz.timezone('America/Bogota')
            fecha_hoy = datetime.now(tz_colombia).date()

            sql = f"""
            UPDATE `{self.project}.{self.dataset}.{self.table}`
            SET reclamaciones = ARRAY(
                SELECT AS STRUCT 
                    rec.med_no_entregados,
                    rec.tipo_accion,
                    rec.texto_reclamacion,
                    '{new_status}' AS estado_reclamacion,
                    rec.nivel_escalamiento,
                    rec.url_documento,
                    rec.numero_radicado,
                    DATE('{fecha_hoy.isoformat()}') AS fecha_radicacion,
                    rec.fecha_revision,
                    rec.id_session
                FROM UNNEST(reclamaciones) AS rec
            )
            WHERE paciente_clave = '{paciente_clave}'
            """
            
            job = self.bq.query(sql)
            job.result()  # Esperar a que termine
            logger.info(f"✅ Estado actualizado para paciente {paciente_clave}, nivel {nivel_escalamiento} - Fecha radicación: {fecha_hoy}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error actualizando estado: {e}")
            return False

    def process_radicado_notifications(self) -> Dict[str, Any]:
        """
        Busca reclamaciones con estado 'enviar_radicado' y numero_radicado no nulo,
        y envía los mensajes correspondientes.
        """
        logger.info("🔍 Buscando reclamaciones pendientes de envío de radicado...")
        
        sql = f"""
        SELECT 
            t.paciente_clave,
            pres.user_id AS user_id,
            pres.id_session AS session_id,
            t.canal_contacto,
            rec.nivel_escalamiento,
            rec.tipo_accion,
            rec.numero_radicado,
            rec.estado_reclamacion
        FROM `{self.project}.{self.dataset}.{self.table}` AS t,
             UNNEST(t.prescripciones) AS pres,
             UNNEST(t.reclamaciones) AS rec
        WHERE rec.estado_reclamacion = 'enviar_radicado'
          AND rec.numero_radicado IS NOT NULL
          AND rec.numero_radicado != ''
          AND rec.id_session = pres.id_session
        """
        
        logger.info(f"📝 SQL ejecutado: {sql}")
        
        results = {
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "details": []
        }
        
        try:
            for row in self.bq.query(sql).result():
                results["processed"] += 1
                
                user_id = row.user_id
                patient_key = row.paciente_clave
                session_id = row.session_id
                numero_radicado = row.numero_radicado
                nivel_escalamiento = row.nivel_escalamiento
                canal_detectado = getattr(row, 'canal_contacto', None)
                
                # Detectar canal si no está presente
                if not canal_detectado:
                    if session_id.startswith("TL_"):
                        canal_detectado = "TL"
                    elif session_id.startswith("WA_"):
                        canal_detectado = "WA"
                    else:
                        logger.warning(f"❓ Canal no detectado para session {session_id}")
                        canal_detectado = "UNKNOWN"
                
                # Crear mensaje personalizado
                mensaje = f"🎉 ¡Buenas noticias! Tu reclamación ha sido radicada.\n\n📋 Número de radicado: {numero_radicado}\n\n 📞 Te contactaremos cuando se cumpla el plazo para verificar si recibiste tus medicamentos. POR FAVOR NO RESPONDER ESTE MENSAJE"
                
                logger.info(f"📧 Procesando {canal_detectado}: {patient_key} (session: {session_id}) - Radicado: {numero_radicado}")
                
                # Enviar mensaje
                success = self.send_message(user_id, session_id, mensaje)
                
                if success:
                    # Actualizar estado a 'radicado' 
                    update_success = self.update_reclamation_status(
                        patient_key, 
                        nivel_escalamiento, 
                        'radicado'
                    )
                    
                    if update_success:
                        results["successful"] += 1
                        logger.info(f"✅ Radicado enviado y estado actualizado para {patient_key}")
                        results["details"].append({
                            "patient_key": patient_key,
                            "session_id": session_id,
                            "canal": canal_detectado,
                            "radicado": numero_radicado,
                            "status": "success"
                        })
                    else:
                        results["failed"] += 1
                        logger.error(f"❌ Mensaje enviado pero falló actualización de estado para {patient_key}")
                        results["details"].append({
                            "patient_key": patient_key,
                            "session_id": session_id,
                            "canal": canal_detectado,
                            "radicado": numero_radicado,
                            "status": "message_sent_update_failed"
                        })
                else:
                    results["failed"] += 1
                    logger.error(f"❌ Falló envío de mensaje para {patient_key}")
                    results["details"].append({
                        "patient_key": patient_key,
                        "session_id": session_id,
                        "canal": canal_detectado,
                        "radicado": numero_radicado,
                        "status": "send_failed"
                    })
                    
        except Exception as e:
            logger.error(f"❌ Error general procesando notificaciones: {e}")
            results["error"] = str(e)
        
        logger.info(f"📊 Resumen: {results['processed']} procesados, {results['successful']} exitosos, {results['failed']} fallidos")
        return results


@functions_framework.http
def send_radicado_notifications(request: Request):
    """
    Cloud Function HTTP para enviar notificaciones de números de radicado.
    
    Puede ser llamada manualmente o programada con Cloud Scheduler.
    """
    try:
        logger.info("🚀 Iniciando proceso de envío de números de radicado")
        
        # Verificar método HTTP (opcional)
        if request.method not in ['GET', 'POST']:
            return {"error": "Método no permitido"}, 405
        
        # Inicializar sender
        sender = RadicadoSender()
        
        # Procesar notificaciones
        results = sender.process_radicado_notifications()
        
        # Preparar respuesta
        response = {
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
            "results": results
        }
        
        logger.info(f"✅ Proceso completado: {json.dumps(response)}")
        return response, 200
        
    except Exception as e:
        logger.error(f"❌ Error en Cloud Function: {e}", exc_info=True)
        return {
            "status": "error", 
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, 500


# Para testing local
# if __name__ == "__main__":
#     # Configurar variables de entorno para testing local
#     os.environ.setdefault('PROJECT_ID', 'tu-proyecto')
#     os.environ.setdefault('DATASET_ID', 'tu-dataset') 
#     os.environ.setdefault('TABLE_ID', 'tu-tabla')
#     os.environ.setdefault('API_RECEPCIONISTA_URL', 'https://tu-api.com')
    
#     sender = RadicadoSender()
#     results = sender.process_radicado_notifications()
#     print(f"Resultados: {json.dumps(results, indent=2)}")