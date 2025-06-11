import logging
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from manual_instrucciones.prompt_manager import prompt_manager
from processor_image_prescription.bigquery_pip import (
    get_bigquery_client,
    insert_or_update_patient_data,
    _build_prescription_struct_sql,
    _convert_bq_row_to_dict_recursive,
    PROJECT_ID,  # Importar directamente si se usa globalmente
    DATASET_ID,
    TABLE_ID,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _debug_informante_schema(patient_key: str):
    """
    Función de depuración para inspeccionar el esquema y los datos actuales del campo 'informante'.
    """
    try:
        client = get_bigquery_client()
        table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        # 1. Verificar esquema
        try:
            table = client.get_table(table_reference)
            logger.info("🔧 DEBUG: Esquema completo de la tabla:")
            for field in table.schema:
                if field.name == "informante":
                    logger.info(f"🔧 Campo informante: {field}")
                    if field.fields:
                        for subfield in field.fields:
                            logger.info(f"🔧   - {subfield.name}: {subfield.field_type} ({subfield.mode})")
        except Exception as e:
            logger.error(f"Error obteniendo esquema: {e}")

        # 2. Verificar datos actuales
        try:
            query = f"""
                SELECT informante
                FROM `{table_reference}`
                WHERE paciente_clave = @patient_key
                LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
            )
            results = client.query(query, job_config=job_config).result()

            for row in results:
                logger.info(f"🔧 DEBUG: Datos actuales de informante: {row.informante}")

        except Exception as e:
            logger.error(f"Error obteniendo datos actuales: {e}")

    except Exception as e:
        logger.error(f"Error en _debug_informante_schema: {e}")


class ClaimManagerError(Exception):
    """Excepción para errores específicos del ClaimManager."""


class ClaimManager:
    """
    Gestiona la recopilación de información adicional para generar una reclamación,
    consultando y actualizando la tabla de pacientes en BigQuery.
    """

    # Constantes para las claves de los prompts y campos requeridos
    """_PROMPT_KEYWORD_MAP = {
        "fecha_nacimiento": "fecha de nacimiento",
        "correo": "correo electrónico para mantenerte informado",
        "canal_contacto": "canal prefieres que te contactemos",
        "operador_logistico": "operador logístico o la farmacia",
        "sede_farmacia": "farmacia o punto de entrega",
        "informante": "quién está haciendo esta solicitud",
        "informante_familiar_details": "nombre completo del familiar, su identificación y parentesco",
        "claim_completion": "He recopilado toda la información necesaria",
        "patient_not_found": "paciente no encontrado para reclamación",
        "generic_field_prompt_template": "Por favor, proporciona el dato para {field_name}",
        "telegram_error_get_data": "Error al obtener datos del paciente",
        "telegram_error_unexpected_get_data": "Error inesperado al obtener datos del paciente",
        "telegram_error_checking_fields": "Ocurrió un error al verificar los campos",
        "telegram_error_unexpected_claim": "Ocurrió un error inesperado al continuar con la reclamación",
        "telegram_error_saving_undelivered_meds": "Hubo un problema al guardar los medicamentos no entregados",
        "telegram_error_unexpected_undelivered_meds": "Ocurrió un error inesperado al procesar tu selección",
        "validation_date_invalid": "fecha que proporcionaste no es válida",
        "validation_email_invalid": "email que proporcionaste no es válido",
        "validation_phone_invalid": "número de teléfono no es válido",
        "validation_generic_invalid": "El formato no es válido",
    }"""

    def __init__(self):
        try:
            self.bq_client = get_bigquery_client()
            self.table_reference = f"{self.bq_client.project}.{DATASET_ID}.{TABLE_ID}"
            logger.info("ClaimManager inicializado con conexión a BigQuery.")
        except Exception as e:
            logger.critical("Error fatal al inicializar ClaimManager: %s", e)
            raise ClaimManagerError("No se pudo inicializar ClaimManager correctamente.")


    def _get_patient_data(self, patient_key: str) -> Optional[Dict[str, Any]]:
        """ Obtiene los datos del paciente desde BigQuery a partir de su clave.
        """
        try:
            query = f"""
                SELECT *
                FROM `{self.table_reference}`
                WHERE paciente_clave = @patient_key
                LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
                ]
            )
            results = self.bq_client.query(query, job_config=job_config).result()

            for row in results:
                return _convert_bq_row_to_dict_recursive(row)

        except Exception as e:
            logger.exception(f"Error al obtener datos del paciente '{patient_key}': {e}")
            return None
        
    def get_next_missing_field_prompt(self, patient_key: str) -> Dict[str, Optional[str]]:
        """
        Genera dinámicamente un prompt para el siguiente campo faltante, basado en datos ya confirmados.
        """
        try:
            patient_record = self._get_patient_data(patient_key)
            if not patient_record:
                return {
                    "field_name": None,
                    "prompt_text": f"⚠️ No se encontró ningún paciente con la clave '{patient_key}'. ¿Podrías verificar y enviarla nuevamente?"
                }

            # Lista de campos que se esperan (puedes ajustarla)
            required_fields_order = [
                "fecha_nacimiento",
                "correo",
                "canal_contacto",
                "operador_logistico",
                "sede_farmacia",
                "informante",
            ]

            datos_confirmados = []
            for field in required_fields_order:
                value = patient_record.get(field)

                if field == "informante":
                    if isinstance(value, list) and value:
                        informante_data = value[0]
                        if all(informante_data.get(k) for k in ["nombre", "identificacion", "parentesco"]):
                            datos_confirmados.append(f"- Informante: {informante_data['nombre']} ({informante_data['parentesco']})")
                            continue
                    # Si llega aquí, es porque falta completar informante
                    campo_faltante = "informante"
                    break
                elif value:
                    if isinstance(value, list):
                        datos_confirmados.append(f"- {field.replace('_', ' ').capitalize()}: {', '.join(map(str, value))}")
                    else:
                        datos_confirmados.append(f"- {field.replace('_', ' ').capitalize()}: {value}")
                else:
                    campo_faltante = field
                    break
            else:
                return {
                    "field_name": None,
                    "prompt_text": "✅ Ya hemos recopilado toda la información necesaria para tu reclamación. ¡Gracias por tu colaboración!"
                }

            datos_confirmados_str = "\n".join(datos_confirmados)

            prompt = f"""
            Eres un asistente virtual amigable y empático, especializado en ayudar a los pacientes a completar información para generar una reclamación médica ante una EPS.

            Actualmente se ha procesado y confirmado la siguiente información del paciente:

            {datos_confirmados_str}

            Sin embargo, falta el siguiente dato necesario para avanzar en la reclamación:
            "{campo_faltante}"

            Por favor, formula una pregunta clara, cortés y directa al paciente para que proporcione ese dato faltante.

            Además, si el paciente responde con dudas o preguntas fuera del tema, responde con empatía y vuelve a guiarlo para que responda la pregunta sobre "{campo_faltante}".

            No termines la conversación hasta haber obtenido este dato.

            Incluye emojis para hacer la conversación más cercana y amigable.

            Ejemplo de cómo iniciar la pregunta:  
            "Para continuar con tu reclamo necesito que me indiques tu {campo_faltante}. ¿Me lo puedes compartir, por favor?"

            Haz la pregunta ahora:
            """.strip()

            return {"field_name": campo_faltante, "prompt_text": prompt}

        except Exception as e:
            logger.exception(f"Error inesperado en generación de prompt para '{patient_key}': {e}")
            return {
                "field_name": None,
                "prompt_text": "😓 Ocurrió un error inesperado mientras generábamos tu pregunta. Por favor, inténtalo nuevamente."
            }
    


    def _informante_completo(self, informante_data: dict) -> bool:
        """
        Retorna True si el informante familiar tiene todos los datos (nombre, identificacion, parentesco).
        """
        return all(informante_data.get(campo) for campo in ["nombre", "identificacion", "parentesco"])

    def update_patient_field(self, patient_key: str, field_name: str, value: Any) -> bool:
        """
        Actualiza un campo específico del paciente en BigQuery.
        """
        try:
            if field_name == "informante":
                logger.info(f"🔧 DEBUG: Iniciando debug para campo informante, paciente: {patient_key}")
                _debug_informante_schema(patient_key)
                logger.info(f"🔧 DEBUG: Valor a insertar: {value}")

            normalized_value = self._normalize_field_value(field_name, value)

            if field_name == "informante":
                logger.info(f"🔧 DEBUG: Valor normalizado: {normalized_value}")

            update_data = {field_name: normalized_value}

            insert_or_update_patient_data(
                patient_data={"paciente_clave": patient_key},  # Solo la clave
                fields_to_update=update_data,  # Solo el campo específico
            )

            logger.info(f"✅ Campo '{field_name}' del paciente '{patient_key}' actualizado a '{normalized_value}'.")
            return True

        except GoogleAPIError as e:
            logger.error(f"Error de BigQuery al actualizar campo '{field_name}' para paciente '{patient_key}': {e}")
            raise ClaimManagerError(f"Error al actualizar campo en BigQuery: {e}") from e
        except Exception as e:
            logger.exception(f"Error inesperado al actualizar campo '{field_name}' para paciente '{patient_key}'.")
            raise ClaimManagerError(f"Error inesperado al actualizar campo: {e}") from e

    def update_undelivered_medicines(
        self, patient_key: str, session_id: str, undelivered_med_names: List[str]
    ) -> bool:
        """
        Actualiza el estado 'entregado' de los medicamentos seleccionados a 'no entregado'
        para una prescripción específica (identificada por session_id) en el registro del paciente.
        """
        try:
            patient_record = self._get_patient_data(patient_key)
            if not patient_record:
                logger.warning(f"No se encontró el paciente '{patient_key}' para actualizar medicamentos.")
                return False

            updated_prescriptions = []
            prescription_found = False

            for presc in patient_record.get("prescripciones", []):
                if presc.get("id_session") == session_id:
                    prescription_found = True
                    updated_meds = []
                    for med in presc.get("medicamentos", []):
                        if med.get("nombre") in undelivered_med_names:
                            med["entregado"] = "no entregado"
                            logger.info(
                                f"Medicamento '{med.get('nombre')}' marcado como 'no entregado' en sesión {session_id}."
                            )
                        elif "entregado" not in med:  # Asegurarse de que los no seleccionados sigan como 'pendiente'
                            med["entregado"] = "pendiente"
                        updated_meds.append(med)

                    presc["medicamentos"] = updated_meds
                    updated_prescriptions.append(presc)
                else:
                    updated_prescriptions.append(presc)

            if not prescription_found:
                logger.warning(f"No se encontró prescripción para session_id '{session_id}' en paciente '{patient_key}'.")
                return False

            # Construir la parte SQL para actualizar el array de prescripciones
            updated_prescriptions_sql_list = []
            for presc_dict in updated_prescriptions:
                updated_prescriptions_sql_list.append(_build_prescription_struct_sql(presc_dict))

            update_sql = f"""
                UPDATE `{self.table_reference}`
                SET prescripciones = [
                    {", ".join(updated_prescriptions_sql_list)}
                ]
                WHERE paciente_clave = @patient_key
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
            )

            job = self.bq_client.query(update_sql, job_config=job_config)
            job.result()

            if job.errors:
                logger.error(
                    f"Errores en BigQuery UPDATE para medicamentos de paciente '{patient_key}': {job.errors}"
                )
                return False

            logger.info(
                f"Estado de medicamentos 'entregado' actualizado para paciente '{patient_key}' en sesión '{session_id}'."
            )
            return True

        except GoogleAPIError as e:
            logger.error(
                f"Error de BigQuery al actualizar medicamentos no entregados para paciente '{patient_key}': {e}",
                exc_info=True,
            )
            error_msg = prompt_manager.get_prompt_by_keyword(
                "Telegram", self._PROMPT_KEYWORD_MAP["telegram_error_saving_undelivered_meds"]
            )
            if not error_msg:
                error_msg = f"Error de BigQuery al actualizar medicamentos: {e}"
            raise ClaimManagerError(error_msg) from e
        except Exception as e:
            logger.exception(
                f"Error inesperado al actualizar medicamentos no entregados para paciente '{patient_key}': {e}"
            )
            error_msg = prompt_manager.get_prompt_by_keyword(
                "Telegram", self._PROMPT_KEYWORD_MAP["telegram_error_unexpected_undelivered_meds"]
            )
            if not error_msg:
                error_msg = f"Error inesperado al actualizar medicamentos: {e}"
            raise ClaimManagerError(error_msg) from e

    def _normalize_field_value(self, field_name: str, value: Any) -> Any:
        """
        Normaliza el valor de un campo según su tipo para BigQuery.
        """
        if field_name == "fecha_nacimiento":
            return value

        if field_name == "correo":
            if isinstance(value, str):
                return [email.strip() for email in value.split(",") if email.strip()]
            if not isinstance(value, list):
                return []
            return value

        if field_name == "telefono_contacto":
            if isinstance(value, str):
                return [value.strip()]
            if not isinstance(value, list):
                return []
            return value

        if field_name == "informante":
            if isinstance(value, str):
                informante_type = self._standardize_informante_response(value)
                if informante_type == "paciente":
                    return [{"nombre": "Paciente", "parentesco": "Mismo", "identificacion": ""}]
                if informante_type == "familiar":
                    return [{"nombre": "", "parentesco": "familiar", "identificacion": ""}]
                return [{"nombre": "", "parentesco": "", "identificacion": ""}]

            if isinstance(value, dict):
                clean_value = {
                    "nombre": value.get("nombre", ""),
                    "parentesco": value.get("parentesco", ""),
                    "identificacion": value.get("identificacion", ""),
                }
                return [clean_value]
            if not isinstance(value, list):
                return []
            return value

        if field_name == "canal_contacto":
            return self._standardize_channel_response(value)

        if field_name == "operador_logistico":
            return self._standardize_operator_response(value)

        if field_name == "sede_farmacia":
            return self._standardize_pharmacy_response(value)

        return value

    def _standardize_informante_response(self, user_response: str) -> str:
        """Estandariza la respuesta del informante a opciones predefinidas."""
        response_lower = user_response.lower().strip()

        if any(word in response_lower for word in ["paciente", "yo soy", "soy el paciente"]):
            return "paciente"
        if any(word in response_lower for word in ["familiar", "familia", "pariente"]):
            return "familiar"
        if any(word in response_lower for word in ["tutor", "acudiente", "representante", "legal"]):
            return "tutor"
        return user_response

    def _standardize_channel_response(self, user_response: str) -> str:
        """Estandariza la respuesta del canal de contacto."""
        response_lower = user_response.lower().strip()

        if any(word in response_lower for word in ["whatsapp", "whats", "wa"]):
            return "WhatsApp"
        if any(word in response_lower for word in ["telegram", "tg"]):
            return "Telegram"
        if any(word in response_lower for word in ["correo", "email", "mail"]):
            return "Correo electrónico"
        if any(word in response_lower for word in ["llamada", "teléfono", "telefono", "call"]):
            return "Llamada telefónica"
        return user_response

    def _standardize_operator_response(self, user_response: str) -> str:
        """Estandariza la respuesta del operador logístico."""
        response_lower = user_response.lower().strip()

        operators_map = {
            "audifarma": "Audifarma",
            "cruz verde": "Cruz Verde",
            "copidrogas": "Copidrogas",
            "colsubsidio": "Colsubsidio",
            "farmacia de la eps": "Farmacia de la EPS",
            "no sé": "No especificado",
            "no se": "No especificado",
            "mi eps": "EPS directamente",
            "eps directamente": "EPS directamente",
        }

        for key, value in operators_map.items():
            if key in response_lower:
                return value

        return user_response

    def _standardize_pharmacy_response(self, user_response: str) -> str:
        """Estandariza la respuesta de la sede de farmacia."""
        response_lower = user_response.lower().strip()

        if any(phrase in response_lower for phrase in ["no sé", "no se", "donde me diga"]):
            return "Según indicación de EPS"
        if any(phrase in response_lower for phrase in ["casa", "domicilio", "entregan"]):
            return "Entrega a domicilio"
        return user_response

    def get_field_validation_message(self, field_name: str) -> str:
        """
        Obtiene mensajes de validación específicos desde manual_instrucciones.
        """
        keyword = self._PROMPT_KEYWORD_MAP.get(f"validation_{field_name}")
        if keyword:
            error_msg = prompt_manager.get_prompt_by_keyword("Validaciones", keyword)
            if error_msg:
                return error_msg

        # Fallback genérico de validación si el específico no se encuentra o el campo no está mapeado
        return (
            prompt_manager.get_prompt_by_keyword(
                "Validaciones", self._PROMPT_KEYWORD_MAP["validation_generic_invalid"]
            )
            or "El formato no es válido. Por favor, verifica tu respuesta."
        )


try:
    claim_manager = ClaimManager()
except ClaimManagerError as e:
    logger.critical(f"Error fatal al inicializar ClaimManager: {e}. El bot no podrá generar reclamaciones.")
    claim_manager = None