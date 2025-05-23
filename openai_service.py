from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Final, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------- #
#  Constantes y configuración
# --------------------------------------------------------------------------- #
OPENAI_API_KEY: Final[str | None] = os.getenv("OPENAI_API_KEY")
OPENAI_API_URL: Final[str] = "https://api.openai.com/v1/chat/completions"
DEFAULT_MODEL: Final[str] = "gpt-4.1-mini"   
DEFAULT_TIMEOUT: Final[int] = 60            

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY no se encuentra en variables de entorno.")


class OpenAIServiceError(RuntimeError):
    """Excepción base para fallos de la integración con OpenAI."""


# --------------------------------------------------------------------------- #
#  Utilidades
# --------------------------------------------------------------------------- #
def encode_image_to_base64(image_path: str | os.PathLike[str]) -> str:
    """
    Devuelve la imagen codificada en base64 (sin encabezado data:*).

    Parameters
    ----------
    image_path : str | Path
        Ruta de la imagen (JPG, PNG…).

    Returns
    -------
    str
        Cadena base64.

    Raises
    ------
    OpenAIServiceError
        Si el archivo no puede leerse.
    """
    try:
        with Path(image_path).expanduser().open("rb") as fh:
            return base64.b64encode(fh.read()).decode("utf-8")
    except Exception as exc:  # pragma: no cover
        logger.exception("❌ Error codificando imagen a base64")
        raise OpenAIServiceError("No se pudo codificar la imagen") from exc


def read_prompt_file(prompt_path: str | os.PathLike[str]) -> str:
    """
    Lee un archivo de texto con el prompt del sistema.

    Returns
    -------
    str
        Contenido del archivo.

    Raises
    ------
    OpenAIServiceError
        Si el archivo no existe o no se puede leer.
    """
    try:
        return Path(prompt_path).expanduser().read_text(encoding="utf-8")
    except Exception as exc:  # pragma: no cover
        logger.exception("❌ Error leyendo el prompt")
        raise OpenAIServiceError("No se pudo leer el prompt") from exc


# --------------------------------------------------------------------------- #
#  Operación principal
# --------------------------------------------------------------------------- #
def extract_data_from_prescription(
    image_path: str | os.PathLike[str],
    prompt: str,
    *,
    model: str = DEFAULT_MODEL,
    max_tokens: int = 1500,
    temperature: float = 0.0,
    timeout: int = DEFAULT_TIMEOUT,
) -> str:
    """
    Envía la imagen + prompt a la API de OpenAI y devuelve la respuesta en texto.

    Parámetros con * kwargs permiten personalizar el modelo / temperatura
    sin cambiar la firma principal.

    Returns
    -------
    str
        Contenido devuelto por el modelo o mensaje de error legible.

    Raises
    ------
    OpenAIServiceError
        Para errores de red, time-outs o formatos inesperados de respuesta.
    """
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
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
            OPENAI_API_URL,
            headers=headers,
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:  # pragma: no cover
        logger.exception("❌ Error de red al llamar a OpenAI")
        raise OpenAIServiceError("Fallo de red al acceder a OpenAI") from exc

    if resp.status_code != 200:
        err = f"Error OpenAI {resp.status_code} – {resp.text}"
        logger.error(err)
        raise OpenAIServiceError(err)

    try:
        data = resp.json()
        content: str = data["choices"][0]["message"]["content"]
        logger.info("✅ Respuesta de OpenAI procesada")
        return content
    except (KeyError, IndexError, ValueError) as exc:  # pragma: no cover
        logger.exception("❌ Formato inesperado en la respuesta de OpenAI")
        raise OpenAIServiceError("Formato de respuesta inesperado") from exc