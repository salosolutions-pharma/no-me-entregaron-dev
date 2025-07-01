import os
import logging
import json
from datetime import date
from typing import Dict

import requests
from google.cloud import bigquery

from llm_core import LLMCore
from claim_manager.claim_generator import ClaimGenerator

# Configuración de logging
target = os.getenv('LOG_TARGET', 'stdout')
if target == 'stdout':
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PatientModuleError(Exception):
    pass

class PatientModule:
    """
    Módulo encargado del seguimiento diario y del análisis de respuesta del paciente.
    """
    def __init__(self):
        self.bq = bigquery.Client()
        self.project = os.getenv('PROJECT_ID')
        self.dataset = os.getenv('DATASET_ID')
        self.table = os.getenv('TABLE_ID')
        self.api_url = os.getenv('API_RECEPCIONISTA_URL')
        self.llm_core = LLMCore()
        self.claimgen = ClaimGenerator()

    def check_and_send_followups(self, today: date = None) -> None:
        """
        Envia el primer mensaje de seguimiento a todos los pacientes con revisión pendiente.
        """
        today = today or date.today()
        sql = f"""
        SELECT pres.user_id AS user_id, pres.id_session AS session_id
        FROM `{self.project}.{self.dataset}.{self.table}` AS t,
             UNNEST(t.prescripciones) AS pres,
             UNNEST(t.reclamaciones) AS rec
        WHERE rec.fecha_revision = '{today.isoformat()}'
          AND rec.estado_reclamacion != 'resuelto'
        """
        logger.info(f"Buscando reclamaciones pendientes para {today}")
        for row in self.bq.query(sql).result():
            user_id = row.user_id
            session_id = row.session_id
            try:
                self.send_message(
                    user_id, session_id,
                    "Hola, ¿ya le entregaron los medicamentos relacionados con su solicitud?",
                    buttons=[
                        {"label": "✅ Sí", "action": "followup_yes"},
                        {"label": "❌ No", "action": "followup_no"},
                    ]
                )
                logger.info(f"Mensaje inicial enviado a sesión {user_id}")
            except Exception as e:
                logger.error(f"Error al enviar mensaje a {user_id}: {e}")

    def handle_user_reply(self, user_id: str, user_reply: str) -> None:
        """
        Procesa la respuesta del paciente usando LLM y actúa en consecuencia.
        """
        prompt = f"""El paciente ha respondido: "{user_reply}"

        Actúas como un asistente virtual de salud encargado de hacer seguimiento a una solicitud médica.

        Tu objetivo es extraer esta información desde la respuesta:

        1. Si el paciente indica que ya le entregaron los medicamentos:
        {{
        "formula_entregada": true,
        "desea_escalar": false
        }}

        2. Si el paciente dice que NO le han entregado los medicamentos, y además desea escalar:
        {{
        "formula_entregada": false,
        "desea_escalar": true
        }}

        3. Si el paciente dice que NO le han entregado los medicamentos, pero NO desea escalar:
        {{
        "formula_entregada": false,
        "desea_escalar": false
        }}

        **Reglas:**
        - Devuelve solo el JSON.
        - No incluyas ningún texto adicional.
        """
        llm_response = self.llm_core.ask_text(prompt)

        logger.warning(f"Respuesta del LLM para sesión {user_id}: '{llm_response}'")

        if not llm_response.strip():
            raise PatientModuleError("Respuesta vacía del LLM")

        try:
            data = json.loads(llm_response)
            entregada = bool(data.get("formula_entregada", False))
            escalacion = bool(data.get("desea_escalar", False))
        except Exception as e:
            raise PatientModuleError(f"Respuesta de IA inválida: {e}")

        if entregada:
            self._mark_as_resolved(user_id)
            self.send_message(user_id, "¡Excelente! Marcamos tu caso como resuelto.")
        elif escalacion:
            self._escalation_protocol(user_id)
        else:
            self.send_message(user_id, "Entendido. Quedamos atentos a cualquier novedad.")

    def send_message(self, user_id: str, session_id:str, text: str, buttons: list = None) -> None:
        """
        Envía un mensaje a través de la API recepcionista.
        Si recibe la lista `buttons`, la incluye en el payload.
        """
        payload = {
            "user_id": f"TL_{user_id}",
            "session_id": f"{session_id}",
            "message": text
        }
        if buttons:
            payload["buttons"] = buttons

        resp = requests.post(f"{self.api_url}/send_message", json=payload)
        if resp.status_code != 200:
            logger.error(f"Error al enviar mensaje a {user_id}: {resp.text}")


    def _mark_as_resolved(self, user_id: str) -> None:
        """
        Actualiza BigQuery, marcando la reclamación como resuelta.
        """
        table_ref = f"{self.project}.{self.dataset}.{self.table}"
        sql = f"""
        UPDATE `{table_ref}`
        
        SET reclamaciones = ARRAY(
            SELECT IF(r.id_user = '{user_id}',
                      REPLACE(r, estado_reclamacion='resuelto'), r)
            FROM UNNEST(reclamaciones) AS r
        )
        WHERE EXISTS(
            SELECT 1 FROM UNNEST(prescripciones) AS p
            WHERE p.id_user = '{user_id}'
        )
        """
        self.bq.query(sql).result()
        logger.info(f"Sesión {user_id}: reclamación marcada como resuelta.")

    def update_reclamation_status(self, session_id: str, new_status: str) -> bool:
            
        try:
            table_ref = f"{self.project}.{self.dataset}.{self.table}"
            sql = f"""
            UPDATE `{table_ref}` AS t
            SET reclamaciones = ARRAY(
                SELECT
                    IF(r.id_session = '{session_id}',
                        STRUCT(
                            r.med_no_entregados,
                            r.tipo_accion,
                            r.texto_reclamacion,
                            '{new_status}' AS estado_reclamacion,
                            r.nivel_escalamiento,
                            r.url_documento,
                            r.numero_radicado,
                            r.fecha_radicacion,
                            r.fecha_revision,
                            r.id_session
                        ),
                        r
                    )
                FROM UNNEST(t.reclamaciones) AS r
            )
            WHERE EXISTS (
                SELECT 1 FROM UNNEST(t.reclamaciones) AS r
                WHERE r.id_session = '{session_id}'
            )
            """
            self.bq.query(sql).result()
            logger.info(f"Actualizado estado de reclamación para {session_id} a {new_status}")
            return True
        except Exception as e:
            logger.error(f"Error actualizando reclamación {session_id}: {e}")
            return False    

    def _escalation_protocol(self, session_id: str) -> None:
        """
        Aplica el protocolo de escalamiento según categoría de riesgo y nivel.
        """
        data = self._get_patient_data(session_id)
        riesgo = data['categoria_riesgo']
        nivel = data['nivel_escalamiento']
        if riesgo == 'simple':
            self.claimgen.generar_reclamacion_supersalud(data)
        elif riesgo == 'priorizado':
            if nivel < 4:
                self.claimgen.generar_reclamacion_supersalud(data)
            else:
                self.claimgen.generar_tutela(data)
        else:
            if nivel == 1:
                self.claimgen.generar_reclamacion_supersalud(data)
            else:
                self.claimgen.generar_tutela(data)
        self.send_message(session_id, "He iniciado el proceso de escalamiento según tu categoría de riesgo.")

    def _get_patient_data(self, session_id: str) -> Dict:
        """
        Recupera categoría y nivel de escalamiento desde BigQuery.
        """
        table_ref = f"{self.project}.{self.dataset}.{self.table}"
        sql = f"""
        SELECT rec.categoria_riesgo, rec.nivel_escalamiento
        FROM `{table_ref}` AS t,
             UNNEST(t.prescripciones) AS pres,
             UNNEST(t.reclamaciones) AS rec
        WHERE pres.id_session = '{session_id}'
        LIMIT 1
        """
        rows = list(self.bq.query(sql).result())
        if not rows:
            raise PatientModuleError(f"No existe paciente para sesión {session_id}")
        return dict(rows[0])
