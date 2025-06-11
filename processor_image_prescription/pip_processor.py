import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Union, Optional
from contextlib import suppress
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from google.api_core.exceptions import GoogleAPIError

from motor_eps.parser import EPSParser, EPSParserError
from llm_core import LLMCore
from manual_instrucciones.prompt_manager import prompt_manager
from .cloud_storage_pip import upload_image_to_bucket, CloudStorageServiceError
from .bigquery_pip import insert_or_update_patient_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PIPProcessorError(RuntimeError):
    """Excepción base para errores de PIPProcessor."""


class PIPProcessor:
    """
    Procesa imágenes de fórmulas médicas:
    - Extrae datos usando LLM y prompts dinámicos.
    - Sube imágenes a Cloud Storage.
    - Prepara datos para BigQuery.
    """

    def __init__(self, bucket_name: Optional[str] = None):
        self.bucket_name = bucket_name or os.getenv("BUCKET_PRESCRIPCIONES", "")
        if not self.bucket_name:
            logger.critical("La variable de entorno BUCKET_PRESCRIPCIONES no está configurada.")
            raise PIPProcessorError("BUCKET_PRESCRIPCIONES no configurado.")

        self.llm_core = LLMCore()

        try:
            self.eps_parser = EPSParser()
            logger.info("EPSParser inicializado correctamente.")
        except EPSParserError as e:
            logger.warning(f"EPSParser no pudo inicializarse: {e}")
            self.eps_parser = None

        if prompt_manager is None:
            logger.critical("PromptManager no disponible en PIPProcessor.")
            raise PIPProcessorError("PromptManager no disponible.")

    def process_image(self, image_path: Union[str, Path], session_id: str) -> Union[str, Dict[str, Any]]:
        try:
            prompt_content = prompt_manager.get_prompt_by_keyword(
                "PIP"
            )
            if not prompt_content:
                error_msg = "No se encontró el prompt para procesar la fórmula médica."
                logger.error(error_msg)
                return error_msg

            logger.info(f"Procesando imagen {image_path} para sesión {session_id}...")
            llm_response = self.llm_core.ask_image(prompt_content, image_path)
            logger.debug(f"Respuesta LLM: {llm_response[:300]}")

            # Validar respuesta que indica fórmula inválida
            invalid_formula_msg = "La imagen que enviaste no contiene una fórmula médica válida"
            if invalid_formula_msg and invalid_formula_msg in llm_response:
                return invalid_formula_msg

            # Extraer JSON del LLM
            json_text = self._extract_json_from_response(llm_response)
            parsed_response = json.loads(json_text)

            if "error" in parsed_response:
                error_msg = "No pude extraer la información de tu fórmula correctamente"
                logger.error(f"Error JSON LLM: {parsed_response['error']}")
                return error_msg

            raw_data = parsed_response.get("datos")
            if not raw_data:
                error_msg = "Error: datos no encontrados en la respuesta."
                logger.error("Campo 'datos' no encontrado en respuesta LLM.")
                return error_msg

            cleaned_data = self._clean_and_format_data(raw_data)

            if not self._validate_patient_data(cleaned_data):
                return invalid_formula_msg or "La fórmula médica no contiene datos válidos."

            # Sube imagen a Cloud Storage
            patient_key = self._generate_patient_key(cleaned_data, session_id)
            image_url = upload_image_to_bucket(self.bucket_name, image_path, patient_key, prefix="prescripciones")
            cleaned_data["url_prescripcion_subida"] = image_url

            # Manejo de campos faltantes (opcional, para flujo interactivo)
            missing_fields = self._detect_missing_fields(cleaned_data)
            if missing_fields:
                cleaned_data["_requires_completion"] = True
                cleaned_data["_missing_fields"] = missing_fields
            else:
                cleaned_data["_requires_completion"] = False

            logger.info("Procesamiento de fórmula médica completado con éxito.")
            return cleaned_data

        except CloudStorageServiceError as e:
            msg =  "Error subiendo imagen."
            logger.error(f"Error subida a la nube: {e}")
            return msg
        except json.JSONDecodeError as e:
            msg = "No pude extraer la información de tu fórmula correctamente"
            logger.error(f"JSON inválido: {e}")
            return msg
        except Exception as e:
            msg = "Error inesperado procesando la fórmula."
            logger.error(f"Error inesperado: {e}")
            return msg
        finally:
            if isinstance(image_path, Path) and image_path.exists():
                with suppress(OSError):
                    image_path.unlink()

    def _extract_json_from_response(self, text: str) -> str:
        import re

        code_fence_patterns = [
            re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.S),
            re.compile(r"```(\{.*?\})```", re.S),
            re.compile(r"`(\{.*?\})`", re.S),
        ]

        for pattern in code_fence_patterns:
            match = pattern.search(text)
            if match:
                return match.group(1)

        # Buscar JSON plano
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start:end + 1]

        # Fallback genérico
        logger.warning("No se encontró JSON válido en respuesta LLM.")
        return "{}"

    def _clean_and_format_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        def clean_text(value: Any) -> Any:
            if isinstance(value, str):
                return value.strip()
            if isinstance(value, dict):
                return {k: clean_text(v) for k, v in value.items()}
            if isinstance(value, list):
                return [clean_text(i) for i in value]
            return value

        return clean_text(data)

    def _validate_patient_data(self, data: Dict[str, Any]) -> bool:
        return bool(
            (data.get("tipo_documento") and data.get("numero_documento")) or
            data.get("paciente")
        )

    def _generate_patient_key(self, data: Dict[str, Any], session_id: str) -> str:
        tipo = data.get("tipo_documento", "")
        numero = data.get("numero_documento", "")
        if tipo and numero:
            return f"CO{tipo}{numero}"
        return f"UNKN_{session_id}"

    def _detect_missing_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Implementa si quieres detección de campos faltantes y prompts para completar
        return {}

    def get_confirmation_message(self, extracted_data: Dict[str, Any]) -> str:
        template = "He extraído la siguiente información"
        if template:
            try:
                summary = json.dumps(extracted_data, ensure_ascii=False, indent=2)
                return template.format(datos_extraidos=summary)
            except Exception:
                pass
        # Mensaje genérico si no hay template
        return "He extraído la información de tu fórmula médica. Por favor confirma que es correcta."