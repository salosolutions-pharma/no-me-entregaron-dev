import logging
import re
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from manual_instrucciones.prompt_manager import prompt_manager
from llm_core import LLMCore
from processor_image_prescription.bigquery_pip import (
    get_bigquery_client,
    insert_or_update_patient_data,
    _convert_bq_row_to_dict_recursive,
    BigQueryServiceError,
    update_patient_medications_no_buffer,
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID,
)

logger = logging.getLogger(__name__)


class ClaimManagerError(Exception):
    """Excepción para errores específicos del ClaimManager."""


class ClaimManager:
    """Gestiona la recopilación de información adicional para generar una reclamación."""

    REQUIRED_FIELDS_ORDER = [
        "tipo_documento",
        "numero_documento", 
        "nombre_paciente",
        "fecha_nacimiento",
        "correo",
        "telefono_contacto",
        "regimen",
        "ciudad",
        "direccion",
        "eps_estandarizada",
        "farmacia",
        "sede_farmacia",
        "informante",
    ]

    FIELD_DISPLAY_NAMES = {
        "tipo_documento": "tipo de documento",
        "numero_documento": "número de documento",
        "nombre_paciente": "nombre completo",
        "fecha_nacimiento": "fecha de nacimiento",
        "correo": "correo electrónico",
        "telefono_contacto": "número de teléfono",
        "regimen": "régimen de salud (Contributivo o Subsidiado)",
        "ciudad": "ciudad de residencia",
        "direccion": "dirección de residencia",
        "eps_estandarizada": "EPS",
        "eps_confirmacion": "confirmación de tu EPS",
        "farmacia": "farmacia donde recoges medicamentos",
        "sede_farmacia": "sede o punto de entrega específico",
        "informante": "información sobre quién está haciendo esta solicitud",
    }

    PHARMACY_STANDARDIZATION_MAP = {
        "no sé": "Según indicación de EPS",
        "no se": "Según indicación de EPS",
        "donde me diga": "Según indicación de EPS",
        "casa": "Entrega a domicilio",
        "domicilio": "Entrega a domicilio",
        "entregan": "Entrega a domicilio",
    }

    def __init__(self):
        try:
            self.bq_client = get_bigquery_client()
            if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
                raise ValueError("Variables de entorno de BigQuery no configuradas.")
            self.table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            self.llm_core = LLMCore()
            logger.info("ClaimManager inicializado con conexión a BigQuery y LLM Core.")
        except Exception as e:
            logger.critical("Error fatal al inicializar ClaimManager: %s", e)
            raise ClaimManagerError("No se pudo inicializar ClaimManager correctamente.") from e

    def _get_patient_data(self, patient_key: str) -> Optional[Dict[str, Any]]:
        """Obtiene los datos del paciente desde BigQuery a partir de su clave."""
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
            return None
        except GoogleAPIError as e:
            logger.error(f"Error de BigQuery al obtener datos del paciente '{patient_key}': {e}")
            raise ClaimManagerError(f"Error al obtener datos del paciente en BigQuery: {e}") from e
        except Exception as e:
            logger.exception(f"Error inesperado al obtener datos del paciente '{patient_key}'.")
            raise ClaimManagerError(f"Error inesperado al obtener datos del paciente: {e}") from e

    def get_next_missing_field_prompt(self, patient_key: str) -> Dict[str, Optional[str]]:
        """Genera dinámicamente un prompt para el siguiente campo faltante usando el LLM Core."""
        try:
            patient_record = self._get_patient_data(patient_key)
            if not patient_record:
                return {
                    "field_name": None,
                    "prompt_text": f"⚠️ No se encontró ningún paciente con la clave '{patient_key}'. ¿Podrías verificar y enviarla nuevamente?",
                }

            datos_confirmados = []
            campo_faltante = None

            for field in self.REQUIRED_FIELDS_ORDER:
                value = patient_record.get(field)

                if field == "informante":
                    if (isinstance(value, list) and value and 
                        value[0].get("nombre") and value[0].get("parentesco")):
                        parentesco = value[0]["parentesco"]
                        display_text = ("Paciente" if parentesco == "Mismo paciente" 
                                      else f"{value[0]['nombre']} ({parentesco})")
                        datos_confirmados.append(f"- Informante: {display_text}")
                    else:
                        campo_faltante = "informante"
                        break
                elif field in ["correo", "telefono_contacto"]:
                    if isinstance(value, list) and any(v and str(v).strip() for v in value):
                        datos_confirmados.append(
                            f"- {self._get_field_display_name(field)}: {', '.join(str(v) for v in value if v and str(v).strip())}"
                        )
                    else:
                        campo_faltante = field
                        break
                elif field == "eps_estandarizada":
                    if value and str(value).strip():
                        datos_confirmados.append(f"- EPS: {value}")
                    else:
                        eps_cruda = patient_record.get("eps_cruda")
                        if eps_cruda and str(eps_cruda).strip():
                            campo_faltante = "eps_confirmacion"
                        else:
                            campo_faltante = "eps_estandarizada"
                        break
                elif value and str(value).strip():
                    datos_confirmados.append(f"- {self._get_field_display_name(field)}: {value}")
                else:
                    campo_faltante = field
                    break
            else:
                return {
                    "field_name": None,
                    "prompt_text": "✅ Ya hemos recopilado toda la información necesaria para tu reclamación. ¡Gracias por tu colaboración!",
                }

            claim_prompt_template = prompt_manager.get_prompt_by_keyword("CLAIM")
            if not claim_prompt_template:
                logger.error("Prompt 'CLAIM' no encontrado en manual_instrucciones.")
                return {
                    "field_name": campo_faltante,
                    "prompt_text": f"Para continuar, necesito que me indiques tu {self._get_field_display_name(campo_faltante)}. ¿Me lo puedes compartir, por favor?",
                }

            datos_confirmados_str = "\n".join(datos_confirmados) if datos_confirmados else "Ninguno confirmado aún."
            full_prompt = claim_prompt_template.format(
                datos_confirmados_str=datos_confirmados_str,
                campo_faltante=self._get_field_display_name(campo_faltante),
            )

            try:
                generated_question = self.llm_core.ask_text(full_prompt)
                logger.info(f"Pregunta generada por LLM para campo '{campo_faltante}'.")
                return {"field_name": campo_faltante, "prompt_text": generated_question}
            except Exception as llm_error:
                logger.error(f"Error al generar pregunta con LLM para '{campo_faltante}': {llm_error}")
                return {
                    "field_name": campo_faltante,
                    "prompt_text": f"Para continuar, necesito que me indiques tu {self._get_field_display_name(campo_faltante)}. ¿Me lo puedes compartir, por favor? 😊",
                }

        except Exception as e:
            logger.exception(f"Error inesperado en get_next_missing_field_prompt para '{patient_key}'.")
            return {
                "field_name": None,
                "prompt_text": "😓 Ocurrió un error inesperado al generar tu pregunta. Por favor, inténtalo nuevamente.",
            }

    def _get_field_display_name(self, field_name: str) -> str:
        """Convierte nombres de campos técnicos a nombres amigables para mostrar al usuario."""
        return self.FIELD_DISPLAY_NAMES.get(field_name, field_name.replace("_", " "))

    def update_patient_field(self, patient_key: str, field_name: str, value: Any) -> bool:
        """Actualiza un campo específico del paciente en BigQuery."""
        try:
            normalized_value = self._normalize_field_value(field_name, value)
            update_data = {field_name: normalized_value}

            insert_or_update_patient_data(
                patient_data={"paciente_clave": patient_key},
                fields_to_update=update_data,
            )
            logger.info(f"Campo '{field_name}' del paciente '{patient_key}' actualizado.")
            return True
        except GoogleAPIError as e:
            logger.error(f"Error de BigQuery al actualizar campo '{field_name}' para paciente '{patient_key}': {e}")
            raise ClaimManagerError(f"Error de BigQuery al actualizar campo: {e}") from e
        except Exception as e:
            logger.exception(f"Error inesperado al actualizar campo '{field_name}' para paciente '{patient_key}'.")
            raise ClaimManagerError(f"Error inesperado al actualizar campo: {e}") from e

    def update_undelivered_medicines(self, patient_key: str, session_id: str, 
                                   undelivered_med_names: List[str]) -> bool:
        """Actualiza el estado 'entregado' de los medicamentos en la estructura de prescripciones."""
        try:
            success = update_patient_medications_no_buffer(patient_key, session_id, undelivered_med_names)
            if success:
                logger.info(f"Medicamentos actualizados para paciente '{patient_key}' en sesión '{session_id}'.")
            return success
        except Exception as e:
            logger.error(f"Error al delegar la actualización de medicamentos: {e}", exc_info=True)
            return False

    def _normalize_date(self, date_input: Any) -> Optional[str]:
        """Normaliza fechas usando IA para interpretar el formato del usuario."""
        if not date_input:
            return None

        date_str = str(date_input).strip()

        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        date_normalization_prompt = f"""
Necesito que normalices esta fecha de nacimiento a formato YYYY-MM-DD para una base de datos.

Fecha proporcionada por el usuario: "{date_str}"

Instrucciones:
- Si la fecha está en formato DD/MM/YYYY o DD-MM-YYYY, conviértela a YYYY-MM-DD
- Si está en formato MM/DD/YYYY, usa el contexto para determinar el formato correcto (en Colombia se usa DD/MM/YYYY)
- Si el año tiene 2 dígitos, asume que años 00-30 son 2000-2030, y 31-99 son 1931-1999
- Si la fecha no es válida o no se puede interpretar, responde "INVALID"
- Responde SOLO con la fecha en formato YYYY-MM-DD o "INVALID"

Ejemplos:
- "28/01/2003" → "2003-01-28"
- "15-12-1995" → "1995-12-15"
- "03/05/85" → "1985-05-03"
- "5 de abril de 1990" → "1990-04-05"

Fecha a normalizar: "{date_str}"
Respuesta:"""

        try:
            ai_response = self.llm_core.ask_text(date_normalization_prompt)
            normalized_date = ai_response.strip().strip('"\'')

            if normalized_date == "INVALID":
                logger.error(f"IA no pudo normalizar la fecha: '{date_str}'")
                return None

            if re.match(r'^\d{4}-\d{2}-\d{2}$', normalized_date):
                try:
                    from datetime import datetime
                    datetime.strptime(normalized_date, '%Y-%m-%d')
                    logger.info(f"Fecha normalizada por IA: '{date_str}' → '{normalized_date}'")
                    return normalized_date
                except ValueError:
                    logger.error(f"IA generó fecha inválida: '{normalized_date}' para entrada '{date_str}'")
                    return None
            else:
                logger.error(f"IA no devolvió formato correcto: '{normalized_date}' para entrada '{date_str}'")
                return None

        except Exception as e:
            logger.error(f"Error usando IA para normalizar fecha '{date_str}': {e}")
            return self._fallback_date_normalization(date_str)

    def _fallback_date_normalization(self, date_str: str) -> Optional[str]:
        """Normalización manual básica como fallback si la IA falla."""
        from datetime import datetime
        
        date_patterns = [
            (r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$', '%d/%m/%Y'),
            (r'^(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})$', '%Y/%m/%d'),
            (r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{2})$', '%d/%m/%y'),
        ]

        for pattern, format_str in date_patterns:
            match = re.match(pattern, date_str)
            if match:
                try:
                    if format_str in ['%d/%m/%Y', '%d/%m/%y']:
                        if format_str == '%d/%m/%Y':
                            day, month, year = match.groups()
                        else:
                            day, month, year_short = match.groups()
                            year = f"20{year_short}" if int(year_short) < 50 else f"19{year_short}"

                        parsed_date = datetime(int(year), int(month), int(day))
                        return parsed_date.strftime('%Y-%m-%d')

                    else:
                        parsed_date = datetime.strptime(date_str, format_str)
                        return parsed_date.strftime('%Y-%m-%d')

                except ValueError as e:
                    logger.warning(f"Error parseando fecha '{date_str}' con formato '{format_str}': {e}")
                    continue

        logger.error(f"No se pudo normalizar la fecha: '{date_str}'.")
        return None

    def _normalize_field_value(self, field_name: str, value: Any) -> Any:
        """Normaliza el valor de un campo según su tipo y el contexto del bot."""
        if value is None:
            return None

        if field_name == "fecha_nacimiento":
            return self._normalize_date(value)

        simple_string_fields = [
            "tipo_documento", "numero_documento", "nombre_paciente",
            "regimen", "ciudad", "direccion", "eps_estandarizada",
            "canal_contacto", "farmacia",
        ]
        if field_name in simple_string_fields:
            return str(value).strip()

        if field_name in ["correo", "telefono_contacto"]:
            if isinstance(value, str):
                return [item.strip() for item in value.split(",") if item.strip()]
            if isinstance(value, list):
                return [str(item).strip() for item in value if str(item).strip()]
            return []

        if field_name == "informante":
            if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                return value
            else:
                logger.warning(f"Formato inesperado para el campo 'informante': {value}.")
                return []

        if field_name in ["farmacia", "sede_farmacia"]:
            return self._standardize_response(value, self.PHARMACY_STANDARDIZATION_MAP)

        return str(value).strip()

    def _standardize_response(self, user_response: str, mapping: Dict[str, str]) -> str:
        """Aplica un mapeo de estandarización a la respuesta del usuario."""
        response_lower = str(user_response).lower().strip()
        for key, standard_value in mapping.items():
            if key in response_lower:
                return standard_value
        return user_response

    def update_informante_with_merge(self, patient_key: str, 
                                   new_informante: List[Dict[str, Any]]) -> bool:
        """Actualiza el campo 'informante' (REPEATED RECORD) usando MERGE"""
        if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
            raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

        client = get_bigquery_client()
        table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        try:
            get_query = f"""
                SELECT * FROM `{table_reference}`
                WHERE paciente_clave = @patient_key
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
            )
            results = client.query(get_query, job_config=job_config).result()
            current_data = None
            for row in results:
                current_data = _convert_bq_row_to_dict_recursive(row)
                break

            if not current_data:
                logger.error(f"Paciente {patient_key} no encontrado para actualizar informante.")
                return False

            current_data["informante"] = new_informante

            struct_sql = []
            for item in new_informante:
                import json
                nombre = json.dumps(item["nombre"])
                parentesco = json.dumps(item["parentesco"])
                identificacion = json.dumps(item["identificacion"])
                struct_sql.append(
                    f"STRUCT({nombre} AS nombre, "
                    f"{parentesco} AS parentesco, "
                    f"{identificacion} AS identificacion)"
                )
            array_sql = "[" + ", ".join(struct_sql) + "]"

            merge_query = f"""
            MERGE `{table_reference}` T
            USING (
            SELECT
                @patient_key AS paciente_clave,
                {array_sql} AS informante
            ) S
            ON T.paciente_clave = S.paciente_clave
            WHEN MATCHED THEN
                UPDATE SET informante = S.informante
            WHEN NOT MATCHED THEN
                INSERT (paciente_clave, informante)
                VALUES (S.paciente_clave, S.informante)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
                ]
            )
            client.query(merge_query, job_config=job_config).result()
            logger.info(f"Informante MERGE actualizado para paciente {patient_key}")
            return True

        except Exception as e:
            logger.error(f"Error actualizando informante: {e}")
            return False


try:
    claim_manager = ClaimManager()
    logger.info("ClaimManager instanciado correctamente.")
except ClaimManagerError as e:
    logger.critical(f"Error fatal al inicializar ClaimManager: {e}.")
    claim_manager = None