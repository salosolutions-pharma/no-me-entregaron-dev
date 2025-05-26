from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
from typing import Final

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
    except Exception as exc:
        logger.exception("❌ Error codificando imagen a base64")
        raise OpenAIServiceError("No se pudo codificar la imagen") from exc
