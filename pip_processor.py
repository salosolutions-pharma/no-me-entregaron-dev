from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union

from bigquery_services import insert_or_update_patient_data
from cloud_storage_services import upload_image_to_bucket
from openai_service import extract_data_from_prescription, read_prompt_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PIPProcessorError(RuntimeError):
    """Excepción base para errores de PIPProcessor."""


# --------------------------------------------------------------------------- #
#  Auxiliares de limpieza / validación
# --------------------------------------------------------------------------- #
def _clean_text_encoding(text: str) -> str:
    """Solución rápida a mojibake y caracteres sueltos."""
    if not isinstance(text, str):
        return text
    with_charmap = text
    try:
        with_charmap = text.encode("latin-1").decode("utf-8")
    except UnicodeDecodeError:  # no era mojibake
        pass
    return with_charmap.replace("�", "")


def _convert_date(date_str: str | None) -> str | None:
    """DD/MM/YYYY → YYYY-MM-DD (o None)."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        if "/" in date_str:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        if "-" in date_str:
            return date_str  # ya correcto
    except ValueError:
        logger.debug("Formato de fecha no reconocido: %s", date_str)
    return None


def _recursive_clean(data: Any) -> Any:
    """Aplica _clean_text_encoding y _convert_date de forma recursiva."""
    if isinstance(data, dict):
        cleaned: Dict[str, Any] = {}
        for k, v in data.items():
            cleaned[k] = _convert_date(v) if k == "fecha_atencion" else _recursive_clean(v)
        return cleaned
    if isinstance(data, list):
        return [_recursive_clean(i) for i in data]
    if isinstance(data, str):
        return _clean_text_encoding(data)
    return data


def _validate_minimum_fields(data: Dict[str, Any]) -> bool:
    """Comprueba que existan los campos necesarios para identificar al paciente."""
    return bool(data.get("tipo_documento") and data.get("numero_documento"))


# --------------------------------------------------------------------------- #
#  PIPProcessor
# --------------------------------------------------------------------------- #
class PIPProcessor:
    """Pipeline completo de procesamiento de fórmula médica."""

    def __init__(self,
                 bucket_name: str | None = None,
                 prompt_path: str | os.PathLike[str] = "prompt_PIP.txt"):
        self.bucket_name: str | None = bucket_name or os.getenv("BUCKET_PRESCRIPCIONES")
        self.prompt_path = Path(prompt_path)

        if not self.bucket_name:
            raise PIPProcessorError("Variable BUCKET_PRESCRIPCIONES no configurada.")

        if not self.prompt_path.is_file():
            raise PIPProcessorError(f"Prompt no encontrado en {self.prompt_path}")

        logger.debug("PIPProcessor inicializado con bucket='%s' y prompt='%s'",
                     self.bucket_name, self.prompt_path)

    # ------------------------------ API pública ----------------------------- #
    def process_image(self, image_path: str | os.PathLike[str], session_id: str) -> Union[str, Dict[str, Any]]:
        """
        Orquesta todo el flujo. Devuelve datos limpios o string con el error.
        """
        try:
            logger.info("🚀 Procesando imagen '%s' (session=%s)", image_path, session_id)

            # 1. Leer prompt
            prompt = read_prompt_file(self.prompt_path)

            # 2. Llamar a OpenAI
            logger.info("📝 Extrayendo datos con OpenAI…")
            llm_response = extract_data_from_prescription(image_path, prompt)

            # 3. Validar mensaje “no fórmula válida”
            if isinstance(llm_response, str) and "fórmula médica válida" in llm_response:
                logger.warning("Imagen no contiene prescripción reconocible.")
                return llm_response

            # 4. Parsear y limpiar
            try:
                raw = json.loads(llm_response)["datos"]
            except (json.JSONDecodeError, KeyError) as exc:
                raise PIPProcessorError("La respuesta del modelo no es JSON válido.") from exc

            clean_data: Dict[str, Any] = _recursive_clean(raw)
            if not _validate_minimum_fields(clean_data):
                raise PIPProcessorError("Datos insuficientes para identificar al paciente.")

            # 5. Generar paciente_clave y subir imagen
            paciente_clave = f"CO{clean_data['tipo_documento']}{clean_data['numero_documento']}"
            logger.info("☁️ Subiendo imagen a Cloud Storage…")
            image_url = upload_image_to_bucket(self.bucket_name, image_path, paciente_clave)

            # 6. Crear registro
            patient_record = self._build_patient_record(clean_data, image_url, session_id)

            # 7. Persistir en BigQuery
            logger.info("💾 Guardando datos en BigQuery…")
            insert_or_update_patient_data(patient_record)

            logger.info("✅ Procesamiento completado.")
            return clean_data

        except PIPProcessorError as exc:
            logger.error("❌ %s", exc)
            return str(exc)
        except Exception as exc:  # pragma: no cover
            logger.exception("❌ Error inesperado en process_image")
            return "Error inesperado en el procesamiento."

    # --------------------------- Métodos internos --------------------------- #
    @staticmethod
    def _build_patient_record(data: Dict[str, Any],
                              image_url: str,
                              session_id: str) -> Dict[str, Any]:
        """Devuelve el dict listo para BigQuery."""
        clave = f"CO{data['tipo_documento']}{data['numero_documento']}"
        prescripcion = {
            "id_session": session_id,
            "url_prescripcion": image_url,
            "categoria_riesgo": None,
            "fecha_atencion": data.get("fecha_atencion"),
            "diagnostico": data.get("diagnostico"),
            "IPS": data.get("ips"),
            "medicamentos": data.get("medicamentos", []),
        }

        return {
            "paciente_clave": clave,
            "pais": "CO",
            "tipo_documento": data.get("tipo_documento"),
            "numero_documento": data.get("numero_documento"),
            "nombre_paciente": data.get("paciente"),
            "telefono_contacto": data.get("telefono", []),
            "regimen": data.get("regimen"),
            "ciudad": data.get("ciudad"),
            "direccion": data.get("direccion"),
            "eps_cruda": data.get("eps"),
            "prescripciones": [prescripcion],
        }
