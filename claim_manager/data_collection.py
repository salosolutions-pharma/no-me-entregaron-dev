import logging
import re
from typing import Any, Dict, List, Optional
from datetime import datetime

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
    load_table_from_json_direct,
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
        """✅ VERSIÓN OPTIMIZADA con cache de prompts estáticos."""
        try:
            patient_record = self._get_patient_data(patient_key)
            if not patient_record:
                return {
                    "field_name": None,
                    "prompt_text": f"⚠️ No se encontró paciente {patient_key}. Verifica la clave.",
                }

            datos_confirmados = []
            campo_faltante = None

            # ✅ MISMA LÓGICA de verificación de campos
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
                # ✅ TODOS LOS CAMPOS COMPLETOS
                return {
                    "field_name": None,
                    "prompt_text": "✅ Ya hemos recopilado toda la información necesaria para tu reclamación. ¡Gracias por tu colaboración!",
                }

            # ✅ USAR PROMPTS ESTÁTICOS (sin LLM) para campos comunes
            STATIC_PROMPTS = {
                "correo": "📧 Para continuar con tu reclamación, necesito tu **correo electrónico**. ¿Me lo puedes compartir, por favor?",
                "telefono_contacto": "📱 Para continuar, necesito tu **número de teléfono** de contacto. ¿Me lo puedes compartir?",
                "ciudad": "🏙️ ¿En qué **ciudad** resides actualmente?",
                "direccion": "🏠 ¿Cuál es tu **dirección** de residencia completa?",
                "regimen": "🏥 ¿Cuál es tu régimen de salud: Contributivo o Subsidiado?",
                "farmacia": "🏥 ¿En qué **farmacia** recoges habitualmente tus medicamentos? (Ej: Cruz Verde, Copidrogas, Audifarma)",
                "sede_farmacia": "📍 ¿Cuál es la **sede específica** de la farmacia donde recoges tus medicamentos?",
                "nombre_paciente": "👤 Para continuar, necesito tu **nombre completo**. ¿Me lo puedes compartir?",
                "tipo_documento": "🆔 ¿Cuál es tu **tipo de documento**? (CC, TI, CE, PP)",
                "numero_documento": "🔢 ¿Cuál es tu **número de documento**?",
                "fecha_nacimiento": "📅 ¿Podrías indicarme tu **fecha de nacimiento** en formato DD/MM/AAAA? (Ej: 15/03/1990)",
            }
            
            # ✅ SI es un campo común, usar prompt estático (MUY RÁPIDO)
            if campo_faltante in STATIC_PROMPTS:
                logger.info(f"⚡ Usando prompt estático para campo '{campo_faltante}' (sin LLM)")
                return {
                    "field_name": campo_faltante,
                    "prompt_text": STATIC_PROMPTS[campo_faltante]
                }

            # ✅ SOLO para campos especiales, usar LLM
            try:
                claim_prompt_template = prompt_manager.get_prompt_by_module_and_function("DATA", "recoleccion_campos")
                if not claim_prompt_template:
                    logger.warning("Prompt DATA.recoleccion_campos no encontrado, usando estático")
                    return {
                        "field_name": campo_faltante,
                        "prompt_text": f"Para continuar, necesito que me indiques tu {self._get_field_display_name(campo_faltante)}. ¿Me lo puedes compartir, por favor? 😊",
                    }

                datos_confirmados_str = "\n".join(datos_confirmados) if datos_confirmados else "Ninguno confirmado aún."
                full_prompt = claim_prompt_template.format(
                    datos_confirmados_str=datos_confirmados_str,
                    campo_faltante=self._get_field_display_name(campo_faltante),
                )

                logger.info(f"🤖 Usando LLM para campo especial '{campo_faltante}'")
                generated_question = self.llm_core.ask_text(full_prompt)
                return {"field_name": campo_faltante, "prompt_text": generated_question}
                
            except Exception as llm_error:
                logger.error(f"Error LLM para '{campo_faltante}': {llm_error}")
                return {
                    "field_name": campo_faltante,
                    "prompt_text": f"Para continuar, necesito que me indiques tu {self._get_field_display_name(campo_faltante)}. ¿Me lo puedes compartir, por favor? 😊",
                }

        except Exception as e:
            logger.exception(f"Error en get_next_missing_field_prompt para '{patient_key}'.")
            return {
                "field_name": None,
                "prompt_text": "😓 Ocurrió un error. Por favor, inténtalo nuevamente.",
            }


    def get_next_missing_tutela_field_prompt(self, patient_key: str, 
                                    datos_tutela_actuales: Dict[str, Any] = None,
                                    tutela_id: str = None) -> Dict[str, Optional[str]]:
        """
        Genera prompts para recolectar datos específicos de tutela para desacato.
        Solo pide los 5 campos esenciales que necesita el usuario proporcionar.
        
        Args:
            patient_key: Clave del paciente
            datos_tutela_actuales: Datos de tutela ya recolectados
            tutela_id: ID específico de la tutela (OBLIGATORIO)
            
        Returns:
            Dict con field_name y prompt_text para el siguiente campo faltante
        """
        try:
            if not tutela_id or not tutela_id.strip():
                logger.error(f"tutela_id es obligatorio para recolección de campos de tutela")
                return {
                    "field_name": None,
                    "prompt_text": "😓 Error técnico: ID de tutela requerido. Por favor, inténtalo nuevamente."
                }
            
            # Campos mínimos necesarios para desacato
            REQUIRED_TUTELA_FIELDS = [
                "numero_sentencia",
                "fecha_sentencia", 
                "fecha_radicacion_tutela", 
                "juzgado",
                "ciudad"
            ]
            
            # Prompts específicos para cada campo
            TUTELA_FIELD_PROMPTS = {
                "numero_sentencia": (
                    "📋 Para generar el desacato necesito el *número exacto de tu acción de tutela*.\n\n"
                    "Este número aparece en la sentencia de tutela (ejemplos: '078', 'T-05001-31-05-001-2025-10098-00').\n\n"
                    "¿Puedes proporcionarme el número completo tal como aparece en tu sentencia?"
                ),
                
                "fecha_sentencia": (
                    "📅 ¿En qué fecha te *concedieron la tutela*?\n\n"
                    "Necesito la fecha exacta de la sentencia en formato DD/MM/AAAA (ejemplo: 28/05/2025).\n\n"
                    "Esta fecha aparece en el encabezado de tu sentencia de tutela."
                ),
                
                "fecha_radicacion_tutela": (
                    "📋 ¿En qué fecha *radicaste tu acción de tutela*?\n\n"
                    "Formato DD/MM/AAAA (ejemplo: 15/05/2025).\n\n"
                    "Esta es la fecha en que presentaste inicialmente la tutela ante el juzgado."
                ),
                
                "juzgado": (
                    "⚖️ Necesito el *nombre completo del juzgado* que te concedió la tutela.\n\n"
                    "Ejemplos:\n"
                    "• 'Juzgado Primero Laboral del Circuito de Medellín'\n"
                    "• 'Juzgado Tercero Civil Municipal de Bogotá'\n\n"
                    "¿Puedes indicarme el nombre exacto como aparece en tu sentencia?"
                ),
                
                "ciudad": (
                    "🏙️ ¿En qué *ciudad* se encuentra el juzgado que concedió tu tutela?\n\n"
                    "Ejemplos: Medellín, Bogotá, Cali, Barranquilla, etc.\n\n"
                    "Necesito la ciudad donde está ubicado físicamente el juzgado."
                )
            }
            
            if not datos_tutela_actuales:
                datos_tutela_actuales = {}
            
            # Buscar primer campo faltante
            for field in REQUIRED_TUTELA_FIELDS:
                value = datos_tutela_actuales.get(field)
                if not value or not str(value).strip():
                    logger.info(f"🔍 Campo faltante para tutela_id {tutela_id}: {field}")
                    return {
                        "field_name": field,
                        "prompt_text": TUTELA_FIELD_PROMPTS[field]
                    }
            
            # Todos los campos están completos
            logger.info(f"✅ Todos los campos de tutela completos para tutela_id: {tutela_id}")
            return {
                "field_name": None,
                "prompt_text": "✅ Tengo todos los datos de tu tutela. Generando el incidente de desacato..."
            }
            
        except Exception as e:
            logger.error(f"Error en get_next_missing_tutela_field_prompt para '{patient_key}' (tutela_id: {tutela_id}): {e}")
            return {
                "field_name": None,
                "prompt_text": "😓 Ocurrió un error. Por favor, inténtalo nuevamente."
            }
        
    def get_existing_tutela_data(self, patient_key: str, tutela_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene datos existentes de tutela para un paciente desde la tabla tutelas.
        
        Args:
            patient_key: Clave del paciente
            
        Returns:
            Dict con datos de tutela si existen, None si no hay datos
        """
        try:
            from google.cloud import bigquery
            
            query = f"""
            SELECT 
                numero_sentencia,
                fecha_sentencia,
                fecha_radicacion_tutela,
                juzgado,
                ciudad,
                created_at
            FROM `{PROJECT_ID}.{DATASET_ID}.tutelas`
            WHERE paciente_clave = @patient_key AND tutela_id = @tutela_id
            ORDER BY created_at DESC
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
                    bigquery.ScalarQueryParameter("tutela_id", "STRING", tutela_id),
                ]
            )
            
            results = self.bq_client.query(query, job_config=job_config).result()
            
            for row in results:
                return {
                    "numero_sentencia": row.numero_sentencia,
                    "fecha_sentencia": row.fecha_sentencia.strftime("%d/%m/%Y") if row.fecha_sentencia else "",
                    "fecha_radicacion_tutela": row.fecha_radicacion_tutela.strftime("%d/%m/%Y") if row.fecha_radicacion_tutela else "",
                    "juzgado": row.juzgado,
                    "ciudad": row.ciudad
                }
            
            logger.info(f"No se encontraron datos de tutela para paciente: {patient_key}")
            return None
            
        except Exception as e:
            logger.error(f"Error obteniendo datos de tutela existentes para {patient_key}: {e}")
            return None

    def save_tutela_data_simple(self, patient_key: str, tutela_id: str, tutela_data: Dict[str, Any]) -> bool:
        """
        Guarda datos mínimos de tutela en la tabla tutelas simplificada.
        
        Args:
            patient_key: Clave del paciente
            tutela_id: ID específico de la tutela (OBLIGATORIO)
            tutela_data: Datos de tutela recolectados del usuario
            
        Returns:
            bool: True si se guardó correctamente
        """
        try:
            if not tutela_id or not tutela_id.strip():
                logger.error(f"tutela_id es obligatorio para guardar datos de tutela")
                return False
            
            # Preparar registro simplificado
            tutela_record = {
                "paciente_clave": patient_key,
                "tutela_id": tutela_id,
                "numero_sentencia": str(tutela_data.get("numero_sentencia", "")).strip(),
                "fecha_sentencia": tutela_data.get("fecha_sentencia"),  # Ya en formato YYYY-MM-DD
                "fecha_radicacion_tutela": tutela_data.get("fecha_radicacion_tutela"),  # Ya en formato YYYY-MM-DD
                "juzgado": str(tutela_data.get("juzgado", "")).strip(),
                "ciudad": str(tutela_data.get("ciudad", "")).strip(),
                "created_at": datetime.now().isoformat()
            }
            
            # Validar datos mínimos
            if not tutela_record["numero_sentencia"] or not tutela_record["juzgado"]:
                logger.error(f"Datos de tutela incompletos para {patient_key} (tutela_id: {tutela_id}): faltan número o juzgado")
                return False
            
            # Guardar en tabla tutelas
            table_reference = f"{PROJECT_ID}.{DATASET_ID}.tutelas"
            load_table_from_json_direct([tutela_record], table_reference)
            
            logger.info(f"✅ Datos de tutela guardados exitosamente para paciente {patient_key} (tutela_id: {tutela_id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos de tutela simples para {patient_key} (tutela_id: {tutela_id}): {e}")
            return False
        
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
        """
        ✅ VERSIÓN OPTIMIZADA que usa la función segura ya implementada.
        Reemplaza el MERGE complejo por UPDATE simple.
        """
        try:
            from processor_image_prescription.bigquery_pip import update_single_field_safe
            
            logger.info(f"🔄 Actualizando informante para paciente {patient_key}")
            
            # ✅ USAR función segura ya implementada (es mucho más rápida)
            success = update_single_field_safe(patient_key, "informante", new_informante)
            
            if success:
                logger.info(f"✅ Informante actualizado para paciente {patient_key}")
            else:
                logger.error(f"❌ Error actualizando informante para {patient_key}")
                
            return success
            
        except Exception as e:
            logger.error(f"❌ Error en update_informante_with_merge: {e}")
            return False

    def save_tutela_data_to_bigquery(self, patient_key: str, tutela_id: str, tutela_data: Dict[str, Any]) -> bool:
        """
        Guarda los datos básicos de tutela en la tabla tutelas simplificada.
        MODIFICADO: Ahora usa estructura simplificada con solo campos esenciales.
        
        Args:
            patient_key: Clave del paciente
            tutela_id: ID específico de la tutela (OBLIGATORIO)
            tutela_data: Datos de la tutela recolectados del usuario
            
        Returns:
            bool: True si se guardó correctamente
        """
        try:
            if not tutela_id or not tutela_id.strip():
                logger.error(f"tutela_id es obligatorio para guardar datos de tutela en BigQuery")
                return False
            
            # ✅ NUEVA ESTRUCTURA SIMPLIFICADA - Solo campos esenciales
            tutela_record = {
                "paciente_clave": patient_key,
                "tutela_id": tutela_id,
                "numero_sentencia": str(tutela_data.get("numero_sentencia", "")).strip(),
                "fecha_sentencia": tutela_data.get("fecha_sentencia"),  # Formato YYYY-MM-DD
                "fecha_radicacion_tutela": tutela_data.get("fecha_radicacion_tutela"),  # Formato YYYY-MM-DD  
                "juzgado": str(tutela_data.get("juzgado", "")).strip(),
                "ciudad": str(tutela_data.get("ciudad", "")).strip(),
                "created_at": datetime.now().isoformat()
            }
            
            # ✅ VALIDACIÓN MÍNIMA - Solo campos críticos
            if not tutela_record["numero_sentencia"] or not tutela_record["juzgado"]:
                logger.error(f"Datos de tutela incompletos para {patient_key} (tutela_id: {tutela_id}): faltan número o juzgado")
                return False
            
            # ✅ GUARDAR en tabla simplificada
            table_reference = f"{PROJECT_ID}.{DATASET_ID}.tutelas"
            load_table_from_json_direct([tutela_record], table_reference)
            
            logger.info(f"✅ Datos de tutela guardados en estructura simplificada para paciente {patient_key} (tutela_id: {tutela_id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos de tutela simplificados para paciente {patient_key} (tutela_id: {tutela_id}): {e}")
            return False
        
try:
    claim_manager = ClaimManager()
    logger.info("ClaimManager instanciado correctamente.")
except ClaimManagerError as e:
    logger.critical(f"Error fatal al inicializar ClaimManager: {e}.")
    claim_manager = None