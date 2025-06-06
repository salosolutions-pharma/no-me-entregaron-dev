import logging
import os
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union, Final
from motor_eps.parser import EPSParser, EPSParserError

# Importaciones de módulos locales (asumiendo que están en la misma estructura de proyecto)
from .cloud_storage_pip import upload_image_to_bucket, CloudStorageServiceError
from .bigquery_pip import insert_or_update_patient_data
from llm_core import LLMCore # Asumiendo que llm_core es una clase importable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Prefijo (carpeta) para todas las prescripciones en Cloud Storage
_PRESCRIPTION_PREFIX: Final[str] = "" 

class PIPProcessorError(RuntimeError):
    """Excepción base para errores de PIPProcessor."""
    pass

# --- Funciones auxiliares de limpieza y validación ---

def _clean_text_encoding(text: str) -> str:
    """Limpia la codificación del texto y elimina caracteres problemáticos."""
    if not isinstance(text, str):
        return text
    try:
        # Intenta decodificar de latin-1 a utf-8 para manejar caracteres especiales
        cleaned_text = text.encode("latin-1", errors="ignore").decode("utf-8")
    except UnicodeDecodeError:
        # Si ya está en UTF-8 o no se puede decodificar, lo mantiene
        cleaned_text = text
    return cleaned_text.replace("�", "").replace("\n", " ")

_CODE_FENCE_REGEX = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.S)

def _extract_json_from_code_fence(text: str) -> str:
    """Extrae el contenido JSON de una valla de código Markdown."""
    match = _CODE_FENCE_REGEX.search(text)
    if match:
        return match.group(1)
    logger.warning("No se encontró JSON dentro de vallas de código. Retornando texto original.")
    return text.strip()

def _format_date_string(date_str: str | None) -> str | None:
    """
    Convierte una cadena de fecha a formato 'YYYY-MM-DD'.
    Soporta 'DD/MM/YYYY' o asume 'YYYY-MM-DD' si contiene guiones.
    """
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        if "/" in date_str:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        if "-" in date_str:
            # Asume que ya está en formato YYYY-MM-DD si contiene guiones
            # Podría añadirse una validación más estricta si es necesario
            return date_str 
    except ValueError:
        logger.debug("Formato de fecha no reconocido: %s", date_str)
    return None

def _recursive_clean_and_format_data(data: Any) -> Any:
    """
    Limpia recursivamente strings (codificación) y formatea fechas
    en estructuras de datos anidadas (diccionarios y listas).
    """
    if isinstance(data, dict):
        cleaned_dict: Dict[str, Any] = {}
        for key, value in data.items():
            if key == "fecha_atencion": # Asumiendo este es el único campo de fecha a formatear recursivamente
                cleaned_dict[key] = _format_date_string(value)
            else:
                cleaned_dict[key] = _recursive_clean_and_format_data(value)
        return cleaned_dict
    if isinstance(data, list):
        return [_recursive_clean_and_format_data(item) for item in data]
    if isinstance(data, str):
        return _clean_text_encoding(data)
    return data

def _validate_patient_data(data: Dict[str, Any]) -> bool:
    """
    Valida que los datos extraídos contengan los campos mínimos requeridos
    para identificar a un paciente.
    """
    return bool(data.get("tipo_documento") and data.get("numero_documento"))

class PIPProcessor:
    """
    Clase para procesar imágenes de prescripciones:
    1. Extrae información usando un modelo de lenguaje (LLM).
    2. Sube la imagen procesada a Google Cloud Storage.
    3. Guarda los datos extraídos y la URL de la imagen en BigQuery.
    """

    def __init__(self,
                 bucket_name: str | None = None,
                 prompt_path: str | Path = ""):
        """
        Inicializa el procesador.
        
        Args:
            bucket_name: Nombre del bucket de GCS para almacenar imágenes.
                         Si es None, se intentará cargar de la variable de entorno BUCKET_PRESCRIPCIONES.
            prompt_path: Ruta al archivo de prompt para el LLM.
                         Si es una cadena vacía, se intentará cargar de PIP_PROMPT_PATH o usará 'prompt_PIP.txt'.
        
        Raises:
            PIPProcessorError: Si BUCKET_PRESCRIPCIONES no está configurado.
        """
        self.bucket_name: str = bucket_name or os.getenv("BUCKET_PRESCRIPCIONES", "")
        self.prompt_path: Path = Path(prompt_path) if prompt_path else Path(os.getenv("PIP_PROMPT_PATH", "prompt_PIP.txt"))
        self.llm_core_instance = LLMCore() 

        try:
            self.eps_parser = EPSParser()  
            logger.info("EPSParser inicializado exitosamente en PIPProcessor.")
        except EPSParserError as e:
            logger.critical(f"Error fatal al inicializar EPSParser: {e}. El procesador de PIP podría no funcionar correctamente para EPS.")
            

        if not self.bucket_name:
            logger.critical("Variable BUCKET_PRESCRIPCIONES no configurada. El procesador no puede operar.")
            raise PIPProcessorError("BUCKET_PRESCRIPCIONES no configurado.")
        
        if not self.prompt_path.is_file():
            logger.warning(f"Archivo de prompt no encontrado en {self.prompt_path}. Asegúrate de que la ruta es correcta.")
            # Dependiendo de la criticidad, aquí podrías decidir lanzar una excepción si el prompt es indispensable
            # raise PIPProcessorError(f"Archivo de prompt no encontrado: {self.prompt_path}")

        logger.info(f"PIPProcessor inicializado con bucket='{self.bucket_name}' y prompt='{self.prompt_path}'.")

    def process_image(self, image_path: str | Path, session_id: str) -> Union[str, Dict[str, Any]]:
        """
        Orquesta el procesamiento de una imagen de fórmula médica.
        
        Args:
            image_path: Ruta local de la imagen a procesar.
            session_id: ID de la sesión asociada al procesamiento.
            
        Returns:
            Dict[str, Any]: Un diccionario con los datos extraídos y la URL de la imagen si el
                            procesamiento fue exitoso.
            str: Un mensaje de error descriptivo si el procesamiento falló en alguna etapa.
        """
        try:
            logger.info(f"🚀 Iniciando procesamiento de imagen '{image_path}' para sesión '{session_id}'.")

            # 1. Cargar prompt y extraer datos con LLM
            prompt_content = self.prompt_path.read_text(encoding="utf-8")
            logger.info("📝 Solicitando extracción de datos a LLM...")
            llm_response = self.llm_core_instance.ask_image(prompt_content, image_path)
            logger.debug(f"🧠 Respuesta completa del LLM:\n{llm_response}")

            if "fórmula médica válida" in llm_response.lower() or "no contiene una fórmula" in llm_response.lower():
                logger.warning("⚠️ La imagen no contiene una prescripción médica reconocible según el LLM.")
                return "La imagen no contiene una fórmula médica válida o legible."

            # 2. Parsear y limpiar datos del LLM
            try:
                json_text = _extract_json_from_code_fence(llm_response)
                raw_extracted_data = json.loads(json_text)["datos"]
            except (json.JSONDecodeError, KeyError) as exc:
                logger.error(f"Error al parsear la respuesta JSON del LLM. Respuesta LLM: {llm_response[:500]}", exc_info=True)
                return "No se pudo extraer información válida de la imagen (formato JSON inválido)."
            except Exception as exc:
                logger.error(f"Error inesperado al procesar la respuesta del LLM: {exc}", exc_info=True)
                return "Ocurrió un error al interpretar la respuesta del modelo de inteligencia artificial."

            cleaned_data: Dict[str, Any] = _recursive_clean_and_format_data(raw_extracted_data)

            if not _validate_patient_data(cleaned_data):
                logger.warning("❌ Los datos extraídos no contienen campos mínimos de paciente (tipo_documento o numero_documento).")
                return "La imagen no contiene los datos mínimos requeridos de un paciente."

            # 3. Subir imagen a Cloud Storage
            patient_key = f"CO{cleaned_data['tipo_documento']}{cleaned_data['numero_documento']}"
            logger.info("☁️ Subiendo imagen a Cloud Storage...")
            image_url = upload_image_to_bucket(self.bucket_name, image_path, patient_key, _PRESCRIPTION_PREFIX)
            cleaned_data["url_prescripcion_subida"] = image_url # Añadir URL a los datos de retorno

            # 4. Construir y guardar registro en BigQuery
            patient_record = self._build_patient_record(cleaned_data, image_url, session_id, self.eps_parser)
            logger.info("💾 Guardando datos de prescripción en BigQuery...")
            insert_or_update_patient_data(patient_record)

            logger.info("✅ Procesamiento de prescripción completado exitosamente.")
            return cleaned_data # Devuelve el diccionario completo con la URL

        except CloudStorageServiceError as exc:
            logger.error(f"Error de Cloud Storage durante el procesamiento: {exc}", exc_info=True)
            return f"Error al subir la imagen al almacenamiento en la nube: {exc}"
        except PIPProcessorError as exc:
            logger.error(f"Error específico de PIPProcessor: {exc}", exc_info=True)
            return str(exc)
        except Exception as exc:
            logger.exception("❌ Error inesperado y no manejado en PIPProcessor.process_image.")
            return "Ocurrió un error inesperado al procesar la imagen. Por favor, inténtalo de nuevo."

    @staticmethod
    def _build_patient_record(data: Dict[str, Any],
                              image_url: str,
                              session_id: str,
                              eps_parser_instance: Any = None) -> Dict[str, Any]:
        """
        Construye el diccionario de registro de paciente para BigQuery a partir de los datos extraídos.
        Asume que `data['medicamentos']` ya puede incluir el campo 'entregado' si ha sido modificado.
        """
        patient_unique_id = f"CO{data['tipo_documento']}{data['numero_documento']}"
        
        eps_cruda = data.get("eps")
        eps_estandarizada = None

        # Si hay una EPS cruda y se proporcionó una instancia del parser
        if eps_cruda and eps_parser_instance:
            try:
                parse_result = eps_parser_instance.parse_eps_name(eps_cruda) # 
                eps_estandarizada = parse_result.get("standardized_entity") # 
                logger.info(f"EPS '{eps_cruda}' estandarizada a '{eps_estandarizada}'. Método: {parse_result.get('method_used')}")
            except Exception as e:
                logger.error(f"Error al estandarizar EPS '{eps_cruda}': {e}")

        prescription_data = {
            "id_session": session_id,
            "url_prescripcion": image_url,
            "categoria_riesgo": data.get("categoria_riesgo"),
            "fecha_atencion": data.get("fecha_atencion"),
            "diagnostico": data.get("diagnostico"),
            "IPS": data.get("ips"),
            "medicamentos": data.get("medicamentos", []), # Puede contener 'entregado'
        }

        # Estructura del registro del paciente para BigQuery
        return {
            "paciente_clave": patient_unique_id,
            "pais": "CO",
            "tipo_documento": data.get("tipo_documento"),
            "numero_documento": data.get("numero_documento"),
            "nombre_paciente": data.get("paciente"),
            "telefono_contacto": data.get("telefono", []),
            "regimen": data.get("regimen"),
            "ciudad": data.get("ciudad"),
            "direccion": data.get("direccion"),
            "eps_cruda": eps_cruda,
            # Campos que no se extraen directamente de la prescripción inicial
            "fecha_nacimiento": None,
            "correo": [],
            "canal_contacto": None,
            "operador_logistico": None,
            "sede_farmacia": None,
            "eps_estandarizada": eps_estandarizada,
            "informante": [],
            "sesiones": [],
            "prescripciones": [prescription_data], # La prescripción actual como un array
            "reclamaciones": [],
        }