import json
import logging
import os
import re
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List, Union, Optional
from utils.logger_config import setup_structured_logging 
from motor_eps.parser import EPSParser, EPSParserError
from llm_core import LLMCore
from manual_instrucciones.prompt_manager import prompt_manager
from .cloud_storage_pip import upload_image_to_bucket, CloudStorageServiceError
from .bigquery_pip import insert_or_update_patient_data

if not logging.getLogger().hasHandlers():  # 👈 AGREGAR
    setup_structured_logging()            # 👈 AGREGAR

logger = logging.getLogger(__name__)

class PIPProcessorError(RuntimeError):
    """Excepción base para errores en PIPProcessor."""


class PIPProcessor:
    """Procesa imágenes de recetas médicas utilizando LLMs, sube imágenes a Cloud Storage y almacena los datos en BigQuery."""

    ERROR_MESSAGES = {
        "pip_prompt_not_found": "No se encontró el prompt para procesar la fórmula médica. Por favor, contacta con soporte.",
        "invalid_prescription": "Por favor, envía una foto de una fórmula médica válida y legible para poder procesarla correctamente.",
        "json_extraction_failed": "No pude extraer la información de tu fórmula correctamente. ¿Podrías enviar una foto más clara?",
        "json_parse_failed": "Hubo un problema al entender la información de tu fórmula. ¿Podrías enviarla de nuevo?",
        "extraction_error": "No pude extraer la información de tu fórmula. Asegúrate de que sea una imagen clara y legible.",
        "no_data_extracted": "No pude encontrar los datos principales en tu fórmula. ¿Es una receta médica válida?",
        "invalid_patient_data": "La fórmula médica no contiene datos esenciales del paciente. Por favor verifica que sea una receta válida.",
        "cloud_upload_error": "No se pudo guardar la imagen en la nube. Por favor, inténtalo de nuevo más tarde.",
        "bigquery_save_error": "No se pudo guardar la información en nuestra base de datos. Inténtalo de nuevo.",
        "unexpected_error": "Ocurrió un error inesperado al procesar tu fórmula. Por favor, inténtalo de nuevo.",
    }

    RISK_KEYWORDS = {
        "vital": [
            "cancer", "tumor", "oncolog", "quimio", "radio", "metasta",
            "infarto", "cardiaco", "coronar", "angin", "arritmi",
            "diabetes", "diabetic", "insulin", "glucos",
            "hipertens", "presion", "antihipertens",
            "renal", "dialisi", "trasplant", "riñon",
            "hepatic", "higado", "cirros", "hepatit",
            "respirator", "asma", "epoc", "pulmon", "bronc",
            "neurologic", "epilep", "convuls", "parkins", "alzheim",
            "psiquiatr", "antidepres", "antipsicoticos", "litio",
        ],
        "priorizado": [
            "pediatr", "niño", "infant", "adolescent",
            "embaraz", "gestant", "matern", "prenatal",
            "adult mayor", "geriatr", "ancian",
            "cronic", "chronic", "permanente", "vida",
            "dolor", "analges", "morfin", "opioi",
            "antibiot", "infeccion", "sepsi", "bacteri",
        ],
    }

    DOCUMENT_TYPES_MAP = {
        "cc": "CC", "cedula": "CC", "cédula": "CC",
        "ti": "TI", "tarjetaidentidad": "TI",
        "ce": "CE", "cedulaextranjeria": "CE",
        "pp": "PP", "pasaporte": "PP",
    }

    def __init__(self, bucket_name: Optional[str] = None):
        self.bucket_name = bucket_name or os.getenv("BUCKET_PRESCRIPCIONES", "")
        if not self.bucket_name:
            logger.critical("La variable de entorno BUCKET_PRESCRIPCIONES no está configurada.")
            raise PIPProcessorError("BUCKET_PRESCRIPCIONES no configurado.")

        self.llm_core = LLMCore()
        self.eps_parser: Optional[EPSParser] = None
        try:
            self.eps_parser = EPSParser()
            logger.info("EPSParser inicializado correctamente.")
        except EPSParserError as e:
            logger.warning(f"No se pudo inicializar EPSParser: {e}. La estandarización de EPS no estará disponible.")

        if prompt_manager is None:
            logger.critical("PromptManager no disponible en PIPProcessor.")
            raise PIPProcessorError("PromptManager no disponible.")

    def process_image(self, image_path: Union[str, Path], session_id: str, telegram_user_id: str = None) -> Union[str, Dict[str, Any]]:
        """Procesa una imagen de receta médica de principio a fin."""
        temp_image_path: Path = Path(image_path) if isinstance(image_path, str) else image_path
        try:
            prompt_content = prompt_manager.get_prompt_by_module_and_function("PIP", "extraccion_data")
            if not prompt_content:
                return self._get_error_message("pip_prompt_not_found")

            logger.info(f"Procesando imagen {temp_image_path.name} para la sesión {session_id}...")
            llm_response = self.llm_core.ask_image(prompt_content, str(temp_image_path))
            logger.debug(f"Respuesta inicial del LLM (primeros 300 chars): {llm_response[:300]}")

            if self._is_invalid_prescription(llm_response):
                return self._get_error_message("invalid_prescription")

            parsed_data = self._parse_llm_response(llm_response)
            if isinstance(parsed_data, str):
                return parsed_data

            cleaned_data = self._clean_and_format_data(parsed_data.get("datos", {}))

            if not self._validate_patient_data(cleaned_data):
                return self._get_error_message("invalid_patient_data")

            patient_key = self._generate_patient_key(cleaned_data, session_id)
            cleaned_data["patient_key"] = patient_key
            cleaned_data["session_id"] = session_id

            try:
                image_url = upload_image_to_bucket(self.bucket_name, temp_image_path, patient_key, prefix="prescripciones")
                cleaned_data["url_prescripcion_subida"] = image_url
            except CloudStorageServiceError as e:
                logger.error(f"Error al subir imagen a Cloud Storage: {e}")
                return self._get_error_message("cloud_upload_error")

            self._process_eps(cleaned_data)
            cleaned_data["categoria_riesgo"] = self._classify_risk(cleaned_data)
            cleaned_data["canal_contacto"] = cleaned_data.get("canal_contacto") or self._detect_channel_from_session_id(session_id)
            
            bigquery_data = self._prepare_data_for_bigquery(cleaned_data, session_id, patient_key, telegram_user_id)
            try:
                insert_or_update_patient_data(bigquery_data)
                logger.info(f"Datos del paciente guardados/actualizados en BigQuery para clave: {patient_key}")
            except Exception as e:
                logger.error(f"Error al guardar datos en BigQuery: {e}")
                return self._get_error_message("bigquery_save_error")

            cleaned_data["_requires_medication_selection"] = bool(cleaned_data.get("medicamentos"))
            cleaned_data["_missing_fields"] = self._detect_missing_fields(cleaned_data)
            cleaned_data["_requires_completion"] = bool(cleaned_data["_missing_fields"])

            logger.info("Procesamiento de receta médica completado correctamente.")
            return cleaned_data

        except Exception as e:
            logger.exception(f"Error inesperado durante el procesamiento de la imagen: {e}")
            return self._get_error_message("unexpected_error")
        finally:
            if temp_image_path and temp_image_path.exists():
                with suppress(OSError):
                    temp_image_path.unlink()

    def _get_error_message(self, error_type: str) -> str:
        """Retorna un mensaje de error estandarizado para el usuario."""
        return self.ERROR_MESSAGES.get(error_type, self.ERROR_MESSAGES["unexpected_error"])

    def _is_invalid_prescription(self, llm_response: str) -> bool:
        """Verifica si la respuesta del LLM sugiere que la imagen no es una receta válida."""
        invalid_indicators = [
            "no contiene una fórmula médica válida", "no es una fórmula médica",
            "imagen no válida", "no se puede procesar", "error", "invalid", 
            "not a prescription", "no se encontraron datos"
        ]
        return any(indicator in llm_response.lower() for indicator in invalid_indicators)

    def _extract_json_from_response(self, text: str) -> str:
        """Extrae una cadena JSON de un texto dado, buscando patrones comunes."""
        patterns = [
            re.compile(r"```json\s*(\{.*?\})\s*```", re.S | re.I),
            re.compile(r"```(?:\w+)?\s*(\{.*?\})\s*```", re.S),
            re.compile(r"`(\{.*?\})`", re.S),
        ]
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                return match.group(1).strip()

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start : end + 1].strip()

        logger.warning("No se encontró JSON válido en la respuesta del LLM.")
        return ""

    def _parse_llm_response(self, llm_response: str) -> Union[Dict[str, Any], str]:
        """Extrae y parsea el JSON de la respuesta del LLM."""
        json_text = self._extract_json_from_response(llm_response)
        if not json_text:
            return self._get_error_message("json_extraction_failed")

        try:
            parsed_response = json.loads(json_text)
        except json.JSONDecodeError as e:
            logger.error(f"Fallo al parsear JSON de la respuesta del LLM: {e}. Texto JSON: {json_text[:200]}...")
            return self._get_error_message("json_parse_failed")

        if "error" in parsed_response:
            logger.error(f"El LLM reportó un error: {parsed_response['error']}")
            return self._get_error_message("extraction_error")

        if not parsed_response.get("datos"):
            logger.warning("El campo 'datos' no se encontró o está vacío en la respuesta parseada del LLM.")
            return self._get_error_message("no_data_extracted")

        return parsed_response

    def _clean_and_format_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Limpia y formatea recursivamente los valores de cadena en un diccionario."""
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, str):
                cleaned[key] = value.strip()
            elif isinstance(value, dict):
                cleaned[key] = self._clean_and_format_data(value)
            elif isinstance(value, list):
                cleaned[key] = [
                    self._clean_and_format_data(item) if isinstance(item, dict) 
                    else (item.strip() if isinstance(item, str) else item)
                    for item in value
                ]
            else:
                cleaned[key] = value
        return cleaned

    def _validate_patient_data(self, data: Dict[str, Any]) -> bool:
        """Valida si los datos esenciales del paciente están presentes."""
        has_document = bool(data.get("tipo_documento") and data.get("numero_documento"))
        has_patient_name = bool(data.get("paciente") or data.get("nombre_paciente"))
        has_medications = bool(data.get("medicamentos") and len(data.get("medicamentos", [])) > 0)
        return has_document and has_patient_name and has_medications

    def _generate_patient_key(self, data: Dict[str, Any], session_id: str) -> str:
        """Genera una clave única para el paciente."""
        doc_type = data.get("tipo_documento", "").upper().replace(".", "").replace(" ", "")
        doc_number = str(data.get("numero_documento", "")).strip()

        standardized_doc_type = self.DOCUMENT_TYPES_MAP.get(doc_type, doc_type)

        if standardized_doc_type and doc_number:
            return f"CO{standardized_doc_type}{doc_number}"
        return f"UNKN_{session_id}"

    def _process_eps(self, data: Dict[str, Any]) -> None:
        """Procesa y estandariza la EPS si el parser está disponible."""
        if self.eps_parser and data.get("eps"):
            try:
                eps_result = self.eps_parser.parse_eps_name(data["eps"])
                data["eps_cruda"] = eps_result["original_name"]
                data["eps_estandarizada"] = eps_result["standardized_entity"]
            except EPSParserError as e:
                logger.warning(f"Error al parsear EPS '{data['eps']}': {e}. Se guardará la EPS cruda.")
                data["eps_cruda"] = data["eps"]
                data["eps_estandarizada"] = None
        elif data.get("eps"):
            data["eps_cruda"] = data["eps"]
            data["eps_estandarizada"] = None
        else:
            data["eps_cruda"] = None
            data["eps_estandarizada"] = None
        
    def _prepare_data_for_bigquery(self, data: Dict[str, Any], session_id: str, patient_key: str, telegram_user_id: str = None) -> Dict[str, Any]:
        """Prepara los datos en el formato correcto para la tabla de pacientes de BigQuery."""
        
        medications_bq = []
        for med_raw in data.get("medicamentos", []):
            if isinstance(med_raw, dict):
                medications_bq.append({
                    "nombre": str(med_raw.get("nombre", "")).strip(),
                    "dosis": str(med_raw.get("dosis", "")).strip(),
                    "cantidad": str(med_raw.get("cantidad", "")).strip(),
                    "entregado": "pendiente",
                })
            elif isinstance(med_raw, str):
                medications_bq.append({
                    "nombre": med_raw.strip(),
                    "dosis": "",
                    "cantidad": "",
                    "entregado": "pendiente",
                })

        prescription_bq = {
            "id_session": session_id,
            "user_id": str(telegram_user_id) if telegram_user_id else "unknown",
            "url_prescripcion": data.get("url_prescripcion_subida", ""),
            "categoria_riesgo": data.get("categoria_riesgo", "No clasificado"),
            "justificacion_riesgo": data.get("justificacion_riesgo", ""),
            "fecha_atencion": data.get("fecha_atencion", ""),
            "diagnostico": data.get("diagnostico", ""),
            "IPS": data.get("ips", ""),
            "medicamentos": medications_bq,
        }

        correo = data.get("correo")
        telefono_contacto = data.get("telefono_contacto")
        if isinstance(correo, str):
            correo = [c.strip() for c in correo.split(',') if c.strip()]
        if isinstance(telefono_contacto, str):
            telefono_contacto = [t.strip() for t in telefono_contacto.split(',') if t.strip()]

        return {
            "paciente_clave": patient_key,
            "pais": "CO",
            "tipo_documento": data.get("tipo_documento", ""),
            "numero_documento": str(data.get("numero_documento", "")),
            "nombre_paciente": data.get("paciente") or data.get("nombre_paciente", ""),
            "fecha_nacimiento": data.get("fecha_nacimiento", ""),
            "correo": correo if isinstance(correo, list) else [],
            "telefono_contacto": telefono_contacto if isinstance(telefono_contacto, list) else [],
            "canal_contacto": data.get("canal_contacto", ""),
            "regimen": data.get("regimen", ""),
            "ciudad": data.get("ciudad", ""),
            "direccion": data.get("direccion", ""),
            "eps_cruda": data.get("eps_cruda", ""),
            "eps_estandarizada": data.get("eps_estandarizada", ""),
            "farmacia": data.get("farmacia", ""),
            "sede_farmacia": data.get("sede_farmacia", ""),
            "medicamentos_no_entregados": [],
            "informante": [],
            "prescripciones": [prescription_bq],
        }

    def _classify_risk(self, data: Dict[str, Any]) -> str:
        """Clasifica el riesgo basándose en el diagnóstico y los medicamentos."""
        existing_risk = data.get("categoria_riesgo")
        if existing_risk:
            if "vital" in existing_risk.lower():
                return "vital"
            if "priorizado" in existing_risk.lower():
                return "priorizado"
            if "simple" in existing_risk.lower():
                return "simple"
            return existing_risk

        diagnostico = data.get("diagnostico", "").lower()
        medicamentos_text = " ".join([
            med.get("nombre", "") if isinstance(med, dict) else str(med)
            for med in data.get("medicamentos", [])
        ]).lower()

        combined_text = f"{diagnostico} {medicamentos_text}"

        if any(keyword in combined_text for keyword in self.RISK_KEYWORDS["vital"]):
            return "vital"
        if any(keyword in combined_text for keyword in self.RISK_KEYWORDS["priorizado"]):
            return "priorizado"
        return "simple"
    
    def _detect_channel_from_session_id(self, session_id: str) -> str:
        """
        Detecta el canal desde session_id. Reutiliza la lógica que ya existe en PatientModule.
        """
        if session_id.startswith("WA_"):
            return "WA"
        elif session_id.startswith("TL_"):
            return "TL"
        else:
            logger.warning(f"Session ID sin prefijo reconocido: {session_id}")
            return "TL"

    def _detect_missing_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Detecta campos que podrían necesitar completarse interactivamente."""
        missing_fields = {}
        optional_fields_to_check = [
            "fecha_nacimiento", "correo", "telefono_contacto",
            "regimen", "ciudad", "direccion", "farmacia", "sede_farmacia"
        ]
        for field in optional_fields_to_check:
            value = data.get(field)
            if isinstance(value, list):
                if not any(v and str(v).strip() for v in value):
                    missing_fields[field] = True
            elif not value or (isinstance(value, str) and not value.strip()):
                missing_fields[field] = True
        return missing_fields

    def get_medication_selection_message(self, extracted_data: Dict[str, Any]) -> str:
        """Genera el mensaje completo de confirmación con todos los datos extraídos."""
        patient_name = extracted_data.get("paciente") or extracted_data.get("nombre_paciente", "")
        tipo_doc = extracted_data.get("tipo_documento", "")
        num_doc = extracted_data.get("numero_documento", "")
        document_info = f"{tipo_doc} {num_doc}" if tipo_doc and num_doc else "No especificado"

        eps = extracted_data.get("eps") or extracted_data.get("eps_cruda") or "No especificada"
        diagnostico = extracted_data.get("diagnostico", "No especificado")
        categoria_riesgo = extracted_data.get("categoria_riesgo", "No clasificado")

        medicamentos_list = extracted_data.get("medicamentos", [])

        if not medicamentos_list:
            medicamentos_display = "No se encontraron medicamentos"
        else:
            formatted_meds = []
            for i, med in enumerate(medicamentos_list):
                if isinstance(med, dict):
                    med_name = med.get("nombre", "Desconocido").strip()
                    med_dosis = med.get("dosis", "").strip()
                    med_cantidad = med.get("cantidad", "").strip()

                    display_line = f"{i+1}. {med_name}"
                    if med_dosis:
                        display_line += f" ({med_dosis})"
                    if med_cantidad:
                        display_line += f" - Cantidad: {med_cantidad}"
                    formatted_meds.append(display_line)
                elif isinstance(med, str):
                    formatted_meds.append(f"{i+1}. {med.strip()}")

            medicamentos_display = "\n".join(formatted_meds)

        return f"""✅ **Fórmula procesada correctamente**

👤 **Paciente:** {patient_name}
🆔 **Documento:** {document_info}
🏥 **EPS:** {eps}
🩺 **Diagnóstico:** {diagnostico}
⚡ **Categoría de riesgo:** {categoria_riesgo}

💊 **Medicamentos encontrados:**
{medicamentos_display}
"""