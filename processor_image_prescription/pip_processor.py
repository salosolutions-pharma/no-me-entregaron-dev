import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Union, Optional
from contextlib import suppress

from dotenv import load_dotenv
load_dotenv()

from motor_eps.parser import EPSParser, EPSParserError
from llm_core import LLMCore
from manual_instrucciones.prompt_manager import prompt_manager
from .cloud_storage_pip import upload_image_to_bucket, CloudStorageServiceError
from .bigquery_pip import insert_or_update_patient_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PIPProcessorError(RuntimeError):
    """Excepción base para errores en PIPProcessor."""


class PIPProcessor:
    """
    Procesa imágenes de recetas médicas utilizando prompts dinámicos de BigQuery:
    - Extrae datos utilizando LLM y prompts dinámicos de manual_instrucciones.
    - Sube imágenes a Cloud Storage.
    - Prepara y almacena datos en la tabla de pacientes de BigQuery.
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
            logger.warning(f"No se pudo inicializar EPSParser: {e}")
            self.eps_parser = None

        if prompt_manager is None:
            logger.critical("PromptManager no disponible en PIPProcessor.")
            raise PIPProcessorError("PromptManager no disponible.")

    def process_image(self, image_path: Union[str, Path], session_id: str) -> Union[str, Dict[str, Any]]:
        """
        Procesa una imagen de receta médica utilizando prompts dinámicos de BigQuery.

        Args:
            image_path (Union[str, Path]): Ruta al archivo de imagen.
            session_id (str): El ID de la sesión de usuario actual.

        Returns:
            Union[str, Dict[str, Any]]: Un diccionario con los datos extraídos y procesados
                                        o un mensaje de error en cadena.
        """
        try:
            prompt_content = prompt_manager.get_prompt_by_keyword("PIP")
            if not prompt_content:
                return self._get_error_message("pip_prompt_not_found")

            logger.info(f"Procesando imagen {image_path} para la sesión {session_id}...")
            llm_response = self.llm_core.ask_image(prompt_content, image_path)
            logger.debug(f"Respuesta del LLM: {llm_response[:300]}")

            if self._is_invalid_prescription(llm_response):
                return self._get_error_message("invalid_prescription")

            json_text = self._extract_json_from_response(llm_response)
            if not json_text or json_text == "{}":
                logger.error("No se pudo extraer JSON válido de la respuesta del LLM.")
                return self._get_error_message("json_extraction_failed")

            try:
                parsed_response = json.loads(json_text)
            except json.JSONDecodeError as e:
                logger.error(f"Fallo al parsear JSON: {e}")
                return self._get_error_message("json_parse_failed")

            if "error" in parsed_response:
                logger.error(f"Error de JSON del LLM: {parsed_response['error']}")
                return self._get_error_message("extraction_error")

            raw_data = parsed_response.get("datos")
            if not raw_data:
                logger.error("El campo 'datos' no se encontró en la respuesta del LLM.")
                return self._get_error_message("no_data_extracted")

            cleaned_data = self._clean_and_format_data(raw_data)

            if not self._validate_patient_data(cleaned_data):
                return self._get_error_message("invalid_patient_data")

            patient_key = self._generate_patient_key(cleaned_data, session_id)
            try:
                image_url = upload_image_to_bucket(self.bucket_name, image_path, patient_key, prefix="prescripciones")
                cleaned_data["url_prescripcion_subida"] = image_url
            except CloudStorageServiceError as e:
                logger.error(f"Error al subir a la nube: {e}")
                return self._get_error_message("cloud_upload_error")

            if self.eps_parser and cleaned_data.get("eps"):
                eps_result = self.eps_parser.parse_eps_name(cleaned_data["eps"])
                cleaned_data["eps_cruda"] = eps_result["original_name"]
                cleaned_data["eps_estandarizada"] = eps_result["standardized_entity"]

            bigquery_data = self._prepare_data_for_bigquery(cleaned_data, session_id, patient_key)

            try:
                insert_or_update_patient_data(bigquery_data)
                logger.info(f"Datos del paciente guardados correctamente para la clave: {patient_key}")
            except Exception as e:
                logger.error(f"Error al guardar en BigQuery: {e}")
                return self._get_error_message("bigquery_save_error")

            missing_fields = self._detect_missing_fields(cleaned_data)
            cleaned_data["_requires_completion"] = bool(missing_fields)
            cleaned_data["_missing_fields"] = missing_fields

            logger.info("Procesamiento de receta médica completado correctamente.")
            return cleaned_data

        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            return self._get_error_message("unexpected_error")
        finally:
            if isinstance(image_path, Path) and image_path.exists():
                with suppress(OSError):
                    image_path.unlink()

    def _get_error_message(self, error_type: str) -> str:
        """
        Obtiene mensajes de error utilizando fallbacks predefinidos.
        """
        error_prompts = {
            "pip_prompt_not_found": "No se encontró el prompt para procesar la fórmula médica.",
            "invalid_prescription": "Por favor, envía una foto de una fórmula médica válida y legible para poder procesarla correctamente.",
            "json_extraction_failed": "No pude extraer la información de tu fórmula correctamente. ¿Podrías enviar una foto más clara?",
            "json_parse_failed": "No pude extraer la información de tu fórmula correctamente. ¿Podrías enviar una foto más clara?",
            "extraction_error": "No pude extraer la información de tu fórmula correctamente. ¿Podrías enviar una foto más clara?",
            "no_data_extracted": "No pude extraer la información de tu fórmula correctamente. ¿Podrías enviar una foto más clara?",
            "invalid_patient_data": "La fórmula médica no contiene datos válidos del paciente. Por favor verifica que sea una receta médica válida.",
            "cloud_upload_error": "Error al subir la imagen. Por favor, inténtalo de nuevo.",
            "bigquery_save_error": "Error al guardar la información. Por favor, inténtalo de nuevo.",
            "unexpected_error": "Error inesperado procesando la fórmula. Por favor, inténtalo de nuevo."
        }
        return error_prompts.get(error_type, "Error procesando la fórmula médica.")

    def _is_invalid_prescription(self, llm_response: str) -> bool:
        """
        Verifica si la respuesta del LLM indica que no es una receta médica válida.
        """
        invalid_indicators = [
            "no contiene una fórmula médica válida", "no es una fórmula médica",
            "imagen no válida", "no se puede procesar", "error", "invalid", "not a prescription"
        ]
        return any(indicator in llm_response.lower() for indicator in invalid_indicators)

    def _extract_json_from_response(self, text: str) -> str:
        """
        Extrae una cadena JSON de un texto dado, buscando patrones comunes.
        """
        code_fence_patterns = [
            re.compile(r"```json\s*(\{.*?\})\s*```", re.S | re.I),
            re.compile(r"```(?:\w+)?\s*(\{.*?\})\s*```", re.S),
            re.compile(r"`(\{.*?\})`", re.S),
        ]

        for pattern in code_fence_patterns:
            match = pattern.search(text)
            if match:
                return match.group(1).strip()

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start:end + 1].strip()

        logger.warning("No se encontró JSON válido en la respuesta del LLM.")
        return ""

    def _clean_and_format_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Limpia y formatea recursivamente los valores de cadena en un diccionario."""
        def clean_text(value: Any) -> Any:
            if isinstance(value, str):
                return value.strip()
            if isinstance(value, dict):
                return {k: clean_text(v) for k, v in value.items()}
            if isinstance(value, list):
                return [clean_text(item) for item in value]
            return value
        return clean_text(data)

    def _validate_patient_data(self, data: Dict[str, Any]) -> bool:
        """Valida si los datos esenciales del paciente están presentes."""
        has_document = bool(data.get("tipo_documento") and data.get("numero_documento"))
        has_patient_name = bool(data.get("paciente") or data.get("nombre_paciente"))
        has_medications = bool(data.get("medicamentos") and len(data.get("medicamentos", [])) > 0)
        return has_document and has_patient_name and has_medications

    def _generate_patient_key(self, data: Dict[str, Any], session_id: str) -> str:
        """Genera una clave única para el paciente basándose en los datos disponibles."""
        tipo = data.get("tipo_documento", "").upper()
        numero = str(data.get("numero_documento", "")).strip()

        if tipo and numero:
            tipo_clean = tipo.replace(".", "").replace(" ", "")
            if tipo_clean in ["CC", "CEDULA", "CÉDULA"]:
                tipo_clean = "CC"
            elif tipo_clean in ["TI", "TARJETAIDENTIDAD"]:
                tipo_clean = "TI"
            elif tipo_clean in ["CE", "CEDULAEXTRANJERIA"]:
                tipo_clean = "CE"
            elif tipo_clean in ["PP", "PASAPORTE"]:
                tipo_clean = "PP"
            return f"CO{tipo_clean}{numero}"
        return f"UNKN_{session_id}"

    def _prepare_data_for_bigquery(self, data: Dict[str, Any], session_id: str, patient_key: str) -> Dict[str, Any]:
        """
        Prepara los datos en el formato correcto para la tabla de pacientes de BigQuery.
        """
        medications = []
        for med in data.get("medicamentos", []):
            if isinstance(med, dict):
                medications.append({
                    "nombre": str(med.get("nombre", "")).strip(),
                    "dosis": str(med.get("dosis", "")).strip(),
                    "cantidad": str(med.get("cantidad", "")).strip(),
                    "entregado": "pendiente"
                })
            elif isinstance(med, str):
                medications.append({
                    "nombre": med.strip(),
                    "dosis": "",
                    "cantidad": "",
                    "entregado": "pendiente"
                })

        prescription = {
            "id_session": session_id,
            "url_prescripcion": data.get("url_prescripcion_subida", ""),
            "categoria_riesgo": self._classify_risk(data),
            "fecha_atencion": data.get("fecha_atencion", ""),
            "diagnostico": data.get("diagnostico", ""),
            "IPS": data.get("ips", ""),
            "medicamentos": medications
        }

        return {
            "paciente_clave": patient_key,
            "pais": "CO",
            "tipo_documento": data.get("tipo_documento", ""),
            "numero_documento": str(data.get("numero_documento", "")),
            "nombre_paciente": data.get("paciente") or data.get("nombre_paciente", ""),
            "eps_cruda": data.get("eps_cruda", data.get("eps", "")),
            "eps_estandarizada": data.get("eps_estandarizada"),
            "prescripciones": [prescription]
        }

    def _classify_risk(self, data: Dict[str, Any]) -> str:
        """
        Classifica el riesgo basándose en el diagnóstico y los medicamentos, utilizando primero la clasificación del prompt PIP.
        """
        existing_risk = data.get("categoria_riesgo")
        if existing_risk:
            if "vital" in existing_risk.lower():
                return "Riesgo Vital"
            elif "priorizado" in existing_risk.lower():
                return "Riesgo Priorizado"
            elif "simple" in existing_risk.lower():
                return "Riesgo Simple"
            return existing_risk

        diagnostico = data.get("diagnostico", "").lower()
        medicamentos_text = " ".join([
            med.get("nombre", "") if isinstance(med, dict) else str(med)
            for med in data.get("medicamentos", [])
        ]).lower()

        vital_keywords = [
            "cancer", "tumor", "oncolog", "quimio", "radio", "metasta",
            "infarto", "cardiaco", "coronar", "angin", "arritmi",
            "diabetes", "diabetic", "insulin", "glucos",
            "hipertens", "presion", "antihipertens",
            "renal", "dialisi", "trasplant", "riñon",
            "hepatic", "higado", "cirros", "hepatit",
            "respirator", "asma", "epoc", "pulmon", "bronc",
            "neurologic", "epilep", "convuls", "parkins", "alzheim",
            "psiquiatr", "antidepres", "antipsicoticos", "litio"
        ]

        priority_keywords = [
            "pediatr", "niño", "infant", "adolescent",
            "embaraz", "gestant", "matern", "prenatal",
            "adult mayor", "geriatr", "ancian",
            "cronic", "chronic", "permanente", "vida",
            "dolor", "analges", "morfin", "opioi",
            "antibiot", "infeccion", "sepsi", "bacteri"
        ]

        combined_text = f"{diagnostico} {medicamentos_text}"

        if any(keyword in combined_text for keyword in vital_keywords):
            return "Riesgo Vital"
        elif any(keyword in combined_text for keyword in priority_keywords):
            return "Riesgo Priorizado"
        return "Riesgo Simple"

    def _detect_missing_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detecta campos faltantes que podrían necesitar completarse interactivamente.
        """
        missing_fields = {}
        optional_fields = [
            "fecha_nacimiento", "correo", "telefono_contacto",
            "canal_contacto", "regimen", "ciudad", "direccion",
            "operador_logistico", "sede_farmacia"
        ]
        for field in optional_fields:
            if not data.get(field):
                missing_fields[field] = True
        return missing_fields

    def get_confirmation_message(self, extracted_data: Dict[str, Any]) -> str:
        """
        Genera un mensaje de confirmación para los datos extraídos.
        """
        patient_name = extracted_data.get("paciente") or extracted_data.get("nombre_paciente", "")
        document_info = f"{extracted_data.get('tipo_documento', '')} {extracted_data.get('numero_documento', '')}"
        
        # Obtener la lista de medicamentos
        medicamentos_list = extracted_data.get("medicamentos", [])
        
        # Formatear los medicamentos para la salida
        if medicamentos_list:
            # Crea una lista de cadenas, una para cada medicamento, incluyendo dosis si está disponible
            formatted_meds = []
            for med in medicamentos_list:
                if isinstance(med, dict):
                    med_name = med.get('nombre', 'Desconocido').strip()
                    med_dosis = med.get('dosis', '').strip()
                    if med_dosis:
                        formatted_meds.append(f"- {med_name} ({med_dosis})")
                    else:
                        formatted_meds.append(f"- {med_name}")
                elif isinstance(med, str):
                    formatted_meds.append(f"- {med.strip()}")
            
            medicamentos_display = "\n" + "\n".join(formatted_meds)
        else:
            medicamentos_display = "Ninguno"

        return f"""✅ He extraído la información de tu fórmula médica:

👤 **Paciente:** {patient_name}
🆔 **Documento:** {document_info}
🏥 **EPS:** {extracted_data.get("eps", "No especificada")}
💊 **Medicamentos:**{medicamentos_display}
🏥 **Diagnóstico:** {extracted_data.get("diagnostico", "No especificado")}
⚡ **Categoría de riesgo:** {extracted_data.get("categoria_riesgo", "No clasificado")}

¿Es correcta esta información? Si es así, podemos continuar con tu reclamación."""