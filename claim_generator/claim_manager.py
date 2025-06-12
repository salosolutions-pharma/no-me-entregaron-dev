import logging
import re
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from google.cloud import firestore, bigquery
from google.api_core.exceptions import GoogleAPIError

from manual_instrucciones.prompt_manager import prompt_manager
from llm_core import LLMCore
from processor_image_prescription.bigquery_pip import (
    get_bigquery_client,
    insert_or_update_patient_data,
    _convert_bq_row_to_dict_recursive,
    load_table_from_json_direct,
    BigQueryServiceError,
    update_patient_medications_no_buffer, # Asegurarse de que esta función exista y se use
    PROJECT_ID, # Utilizado para construir la referencia de la tabla
    DATASET_ID, # Utilizado para construir la referencia de la tabla
    TABLE_ID, # Utilizado para construir la referencia de la tabla
)

# Configuración de Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ClaimManagerError(Exception):
    """Excepción para errores específicos del ClaimManager."""


class ClaimManager:
    """
    Gestiona la recopilación de información adicional para generar una reclamación,
    consultando y actualizando la tabla de pacientes en BigQuery usando prompts dinámicos.
    """

    # Definición de campos requeridos y su orden de prioridad
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
        "operador_logistico",
        "sede_farmacia",
        "informante",
    ]

    # Mapeo de nombres de campos técnicos a nombres amigables para el usuario
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
        "operador_logistico": "operador logístico o farmacia donde recoges medicamentos",
        "sede_farmacia": "sede o punto de entrega específico",
        "informante": "información sobre quién está haciendo esta solicitud",
    }

    OPERATOR_STANDARDIZATION_MAP = {
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
                raise ValueError("Variables de entorno de BigQuery (PROJECT_ID, DATASET_ID, TABLE_ID) no configuradas.")
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
        """
        Genera dinámicamente un prompt para el siguiente campo faltante usando el LLM Core
        y el prompt CLAIM de manual_instrucciones.
        """
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
                    # El campo 'informante' en BigQuery es una lista de structs
                    if isinstance(value, list) and value and value[0].get("nombre") and value[0].get("parentesco"):
                        parentesco = value[0]["parentesco"]
                        display_text = "Paciente" if parentesco == "Mismo paciente" else f"{value[0]['nombre']} ({parentesco})"
                        datos_confirmados.append(f"- Informante: {display_text}")
                    else:
                        campo_faltante = "informante"
                        break
                elif field in ["correo", "telefono_contacto"]:
                    # Campos que son arrays de strings
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
                            campo_faltante = "eps_confirmacion"  # Preguntar al usuario si esta es su EPS
                        else:
                            campo_faltante = "eps_estandarizada"  # Pedir directamente la EPS
                        break
                elif value and str(value).strip():
                    # Campo tiene valor válido
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
        """
        Actualiza un campo específico del paciente en BigQuery.
        Utiliza `insert_or_update_patient_data` que maneja el upsert (DELETE + INSERT) completo.
        """
        try:
            # Normaliza el valor antes de enviarlo a BigQuery
            normalized_value = self._normalize_field_value(field_name, value)
            update_data = {field_name: normalized_value}

            # La función insert_or_update_patient_data manejará la lógica de upsert
            insert_or_update_patient_data(
                patient_data={"paciente_clave": patient_key}, # Se necesita paciente_clave para el lookup
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

    def update_undelivered_medicines(self, patient_key: str, session_id: str, undelivered_med_names: List[str]) -> bool:
        """
        Actualiza el estado 'entregado' de los medicamentos en la estructura de prescripciones
        delegando la lógica a `bigquery_pip.update_patient_medications_no_buffer`.
        """
        try:
            success = update_patient_medications_no_buffer(patient_key, session_id, undelivered_med_names)
            if success:
                logger.info(f"Medicamentos actualizados para paciente '{patient_key}' en sesión '{session_id}' a través de `bigquery_pip`.")
            return success
        except Exception as e:
            logger.error(f"Error al delegar la actualización de medicamentos a `bigquery_pip`: {e}", exc_info=True)
            return False

    def _normalize_date(self, date_input: Any) -> Optional[str]:
        """
        Normaliza fechas usando IA para interpretar el formato del usuario.
        Convierte cualquier formato de fecha a YYYY-MM-DD para BigQuery.
        """
        if not date_input:
            return None

        date_str = str(date_input).strip()

        # Si ya está en formato correcto YYYY-MM-DD, devolverlo directamente
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        # Usar IA para interpretar la fecha
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

            # Validar que la respuesta de la IA sea correcta
            if normalized_date == "INVALID":
                logger.error(f"IA no pudo normalizar la fecha: '{date_str}'")
                return None

            # Verificar que el formato sea correcto
            if re.match(r'^\d{4}-\d{2}-\d{2}$', normalized_date):
                # Validar que sea una fecha real
                try:
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
            # Fallback a normalización manual básica
            return self._fallback_date_normalization(date_str)

    def _fallback_date_normalization(self, date_str: str) -> Optional[str]:
        """
        Normalización manual básica como fallback si la IA falla.
        """
        # Intentar parsear diferentes formatos comunes
        date_patterns = [
            (r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$', '%d/%m/%Y'),  # DD/MM/YYYY o DD-MM-YYYY
            (r'^(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})$', '%Y/%m/%d'),  # YYYY/MM/DD o YYYY-MM-DD
            (r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{2})$', '%d/%m/%y'),  # DD/MM/YY o DD-MM-YY
        ]

        for pattern, format_str in date_patterns:
            match = re.match(pattern, date_str)
            if match:
                try:
                    if format_str in ['%d/%m/%Y', '%d/%m/%y']:
                        # Para DD/MM/YYYY, reorganizar a YYYY-MM-DD
                        if format_str == '%d/%m/%Y':
                            day, month, year = match.groups()
                        else:  # %d/%m/%y
                            day, month, year_short = match.groups()
                            year = f"20{year_short}" if int(year_short) < 50 else f"19{year_short}"

                        # Validar que la fecha sea válida
                        parsed_date = datetime(int(year), int(month), int(day))
                        return parsed_date.strftime('%Y-%m-%d')

                    else:
                        # Para otros formatos
                        parsed_date = datetime.strptime(date_str, format_str)
                        return parsed_date.strftime('%Y-%m-%d')

                except ValueError as e:
                    logger.warning(f"Error parseando fecha '{date_str}' con formato '{format_str}': {e}")
                    continue

        # Si no se pudo parsear
        logger.error(f"No se pudo normalizar la fecha: '{date_str}'. Formatos aceptados: DD/MM/YYYY, YYYY-MM-DD")
        return None

    def _normalize_field_value(self, field_name: str, value: Any) -> Any:
        """Normaliza el valor de un campo según su tipo y el contexto del bot."""

        if value is None:
            return None

        # Manejo especial para fechas con IA
        if field_name == "fecha_nacimiento":
            return self._normalize_date(value)

        # Campos de texto simple que siempre deben ser string
        simple_string_fields = [
            "tipo_documento",
            "numero_documento",
            "nombre_paciente",
            "regimen",
            "ciudad",
            "direccion",
            "eps_estandarizada",
            "canal_contacto",
        ]
        if field_name in simple_string_fields:
            return str(value).strip()

        # Campos que son arrays de strings (ej. correo, telefono)
        if field_name in ["correo", "telefono_contacto"]:
            if isinstance(value, str):
                return [item.strip() for item in value.split(",") if item.strip()]
            if isinstance(value, list):
                return [str(item).strip() for item in value if str(item).strip()]
            return []

        # Campo informante: se espera una lista de diccionarios (struct)
        if field_name == "informante":
            if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                return value
            else:
                logger.warning(f"Formato inesperado para el campo 'informante': {value}. Se esperaba List[Dict].")
                return []

        # Normalización específica para operador_logistico
        if field_name == "operador_logistico":
            return self._standardize_response(value, self.OPERATOR_STANDARDIZATION_MAP)

        # Normalización específica para sede_farmacia
        if field_name == "sede_farmacia":
            return self._standardize_response(value, self.PHARMACY_STANDARDIZATION_MAP)

        # Si no hay una normalización específica, devolver como string
        return str(value).strip()

    def _standardize_response(self, user_response: str, mapping: Dict[str, str]) -> str:
        """Aplica un mapeo de estandarización a la respuesta del usuario."""
        response_lower = str(user_response).lower().strip()
        for key, standard_value in mapping.items():
            if key in response_lower:
                return standard_value
        return user_response
    
    def update_informante_with_merge(self,
        patient_key: str,
        new_informante: List[Dict[str, Any]]
    ) -> bool:
        """
        Actualiza el campo 'informante' (REPEATED RECORD) usando MERGE
        """
        if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
            raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

        client = get_bigquery_client()
        table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        try:
            # Obtener el registro completo actual
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

            # Reemplazar (o extender) la lista de informantes
            current_data["informante"] = new_informante

            # Montar lista de literales STRUCT(...)
            struct_sql = []
            for item in new_informante:
                nombre = json.dumps(item["nombre"])
                parentesco = json.dumps(item["parentesco"])
                identificacion = json.dumps(item["identificacion"])
                struct_sql.append(
                    f"STRUCT({nombre} AS nombre, "
                    f"{parentesco} AS parentesco, "
                    f"{identificacion} AS identificacion)"
                )
            # Unirlos en un ARRAY[…]
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
            logger.info(f"🔄 Informante MERGE actualizado para paciente {patient_key}")
            return True



        except Exception as e:
            logger.error(f"❌ Error actualizando informante SIN BÚFER: {e}")
            return False



try:
    claim_manager = ClaimManager()
    logger.info("ClaimManager instanciado correctamente.")
except ClaimManagerError as e:
    logger.critical(f"Error fatal al inicializar ClaimManager: {e}. El bot no podrá generar reclamaciones.")
    claim_manager = None