from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union

from bigquery_pip import insert_or_update_patient_data
from cloud_storage_pip import upload_image_to_bucket
from llm_core.openai_service import encode_image_to_base64

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PIPProcessorError(RuntimeError):
    """Excepción base para errores de PIPProcessor."""


# --------------------------------------------------------------------------- #
#  Auxiliares de limpieza / validación
# --------------------------------------------------------------------------- #
def _clean_text_encoding(text: str) -> str:
    if not isinstance(text, str):
        return text
    with_charmap = text
    try:
        with_charmap = text.encode("latin-1").decode("utf-8")
    except UnicodeDecodeError:
        pass
    return with_charmap.replace("�", "")


def _convert_date(date_str: str | None) -> str | None:
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        if "/" in date_str:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        if "-" in date_str:
            return date_str
    except ValueError:
        logger.debug("Formato de fecha no reconocido: %s", date_str)
    return None


def _recursive_clean(data: Any) -> Any:
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
    return bool(data.get("tipo_documento") and data.get("numero_documento"))


class PIPProcessor:
    def __init__(self,
                 bucket_name: str | None = None,
                 prompt_path: str | os.PathLike[str] = r"G:\Mi unidad\No me entregaron\Repositorios\no-me-entregaron-dev\processor_image_prescription\prompt_PIP.txt"):
        self.bucket_name: str | None = bucket_name or os.getenv("BUCKET_PRESCRIPCIONES")
        self.prompt_path = Path(prompt_path)

        if not self.bucket_name:
            raise PIPProcessorError("Variable BUCKET_PRESCRIPCIONES no configurada.")

        if not self.prompt_path.is_file():
            raise PIPProcessorError(f"Prompt no encontrado en {self.prompt_path}")

        logger.debug("PIPProcessor inicializado con bucket='%s' y prompt='%s'",
                     self.bucket_name, self.prompt_path)

    def process_image(self, image_path: str | os.PathLike[str], session_id: str) -> Union[str, Dict[str, Any]]:
        try:
            logger.info("🚀 Procesando imagen '%s' (session=%s)", image_path, session_id)

            prompt = self.prompt_path.read_text(encoding="utf-8")
            logger.info("📝 Extrayendo datos con LLM…")
            llm_response = _extract_prescription_with_openai(image_path, prompt)

            if isinstance(llm_response, str) and "fórmula médica válida" in llm_response:
                logger.warning("Imagen no contiene prescripción reconocible.")
                return llm_response

            try:
                raw = json.loads(llm_response)["datos"]
            except (json.JSONDecodeError, KeyError) as exc:
                raise PIPProcessorError("La respuesta del modelo no es JSON válido.") from exc

            clean_data: Dict[str, Any] = _recursive_clean(raw)
            if not _validate_minimum_fields(clean_data):
                raise PIPProcessorError("Datos insuficientes para identificar al paciente.")

            paciente_clave = f"CO{clean_data['tipo_documento']}{clean_data['numero_documento']}"
            logger.info("☁️ Subiendo imagen a Cloud Storage…")
            image_url = upload_image_to_bucket(self.bucket_name, image_path, paciente_clave)

            patient_record = self._build_patient_record(clean_data, image_url, session_id)

            logger.info("💾 Guardando datos en BigQuery…")
            insert_or_update_patient_data(patient_record)

            logger.info("✅ Procesamiento completado.")
            return clean_data

        except PIPProcessorError as exc:
            logger.error("❌ %s", exc)
            return str(exc)
        except Exception as exc:
            logger.exception("❌ Error inesperado en process_image")
            return "Error inesperado en el procesamiento."

    @staticmethod
    def _build_patient_record(data: Dict[str, Any],
                              image_url: str,
                              session_id: str) -> Dict[str, Any]:
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


def _extract_prescription_with_openai(
    image_path: str | os.PathLike[str],
    prompt: str,
    *,
    model: str = "gpt-4.1-mini",
    max_tokens: int = 1500,
    temperature: float = 0.0,
    timeout: int = 60,
) -> str:
    import requests
    import os

    headers = {
        "Authorization": f"Bearer {os.getenv('OPENAI_API_KEY')}",
        "Content-Type": "application/json; charset=utf-8",
    }

    image_b64 = encode_image_to_base64(image_path)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"},
                    }
                ],
            },
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        logger.exception("❌ Error de red al llamar a OpenAI")
        raise PIPProcessorError("Fallo de red al acceder a OpenAI") from exc

    if resp.status_code != 200:
        err = f"Error OpenAI {resp.status_code} – {resp.text}"
        logger.error(err)
        raise PIPProcessorError(err)

    try:
        data = resp.json()
        content: str = data["choices"][0]["message"]["content"]
        logger.info("✅ Respuesta de OpenAI procesada")
        return content
    except (KeyError, IndexError, ValueError) as exc:
        logger.exception("❌ Formato inesperado en la respuesta de OpenAI")
        raise PIPProcessorError("Formato de respuesta inesperado") from exc

