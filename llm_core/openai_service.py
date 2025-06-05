import os
import logging
import requests
from pathlib import Path
from base64 import b64encode
from typing import Any, Final, Optional
from dotenv import load_dotenv

# Configura el logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Cargar variables de entorno
load_dotenv()


class OpenAIServiceError(RuntimeError):
    """Excepción personalizada para errores específicos de OpenAI."""
    pass


class OpenAIClient:
    _API_KEY: Final[str | None] = os.getenv("OPENAI_API_KEY")
    _DEFAULT_MODEL: Final[str] = os.getenv("OPENAI_DEFAULT_MODEL")
    _DEFAULT_TIMEOUT: Final[int] = int(os.getenv("OPENAI_TIMEOUT"))
    _API_URL: Final[str] = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions")

    def __init__(self):
        if not self._API_KEY:
            raise RuntimeError("OPENAI_API_KEY no se encuentra en las variables de entorno.")
        logger.info("OpenAIClient inicializado con modelo por defecto: %s", self._DEFAULT_MODEL)

    def _encode_image_to_b64(self, image_path: str | Path) -> tuple[str, str]:
        path = Path(image_path)
        ext = path.suffix.lower()
        mime = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
        }.get(ext)

        if not mime:
            raise OpenAIServiceError(f"Formato de imagen no soportado: {ext}")

        encoded = b64encode(path.read_bytes()).decode()
        return encoded, mime

    def _post(self, body: dict[str, Any], timeout: int) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._API_KEY}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(self._API_URL, headers=headers, json=body, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.exception("❌ Error en la solicitud a OpenAI API")
            raise OpenAIServiceError(f"OpenAI API falló: {e}")

    def ask_openai(self, prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        current_model = model or self._DEFAULT_MODEL
        timeout = timeout or self._DEFAULT_TIMEOUT

        body = {
            "model": current_model,
            "messages": [{"role": "user", "content": prompt}],
        }

        logger.info("📤 Enviando solicitud de texto a OpenAI (modelo: %s)", current_model)
        data = self._post(body, timeout)

        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            logger.exception("❌ Formato inesperado en la respuesta de OpenAI")
            raise OpenAIServiceError("Respuesta inesperada de OpenAI") from e

    def ask_openai_image(self, prompt: str, image_path: str | Path, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        current_model = model or self._DEFAULT_MODEL
        timeout = timeout or self._DEFAULT_TIMEOUT

        image_b64, mime = self._encode_image_to_b64(image_path)

        body = {
            "model": current_model,
            "messages": [
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime};base64,{image_b64}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 1500,
            "temperature": 0.3,
        }

        logger.info("📤 Enviando imagen a OpenAI Vision (modelo: %s)", current_model)
        data = self._post(body, timeout)

        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            logger.exception("❌ Formato inesperado en la respuesta de OpenAI Vision")
            raise OpenAIServiceError("Respuesta inesperada de OpenAI Vision") from e


# Instancia global exportable
openai_client = OpenAIClient()

def ask_openai(prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
    return openai_client.ask_openai(prompt, model, timeout)

def ask_openai_image(prompt: str, image_path: str | Path, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
    return openai_client.ask_openai_image(prompt, image_path, model, timeout)