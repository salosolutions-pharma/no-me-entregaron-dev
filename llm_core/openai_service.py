import os
import logging
from base64 import b64encode
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class OpenAIServiceError(RuntimeError):
    """Excepción personalizada para errores específicos de OpenAI."""


class OpenAIClient:
    def __init__(self):
        self._api_key = os.getenv("OPENAI_API_KEY")
        self._default_model = os.getenv("OPENAI_DEFAULT_MODEL")
        self._default_timeout = int(os.getenv("OPENAI_TIMEOUT", "60"))
        self._api_url = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions")
        
        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY no se encuentra en las variables de entorno.")
        logger.info("OpenAIClient inicializado con modelo: %s", self._default_model)

    def _encode_image_to_b64(self, image_path: str | Path) -> tuple[str, str]:
        """Codifica imagen a base64 y determina tipo MIME."""
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

    def _post(self, body: dict, timeout: int) -> dict:
        """Realiza petición POST a la API de OpenAI."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(self._api_url, headers=headers, json=body, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.exception("Error en la solicitud a OpenAI API")
            raise OpenAIServiceError(f"OpenAI API falló: {e}")

    def ask_openai(self, prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta de texto a OpenAI."""
        current_model = model or self._default_model
        timeout = timeout or self._default_timeout

        body = {
            "model": current_model,
            "messages": [{"role": "user", "content": prompt}],
        }

        logger.info("Enviando solicitud de texto a OpenAI (modelo: %s)", current_model)
        data = self._post(body, timeout)

        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            logger.exception("Formato inesperado en la respuesta de OpenAI")
            raise OpenAIServiceError("Respuesta inesperada de OpenAI") from e

    def ask_openai_image(self, prompt: str, image_path: str | Path, 
                        model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta con imagen a OpenAI Vision."""
        current_model = model or self._default_model
        timeout = timeout or self._default_timeout

        image_b64, mime = self._encode_image_to_b64(image_path)

        body = {
            "model": current_model,
            "messages": [
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": [{
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{image_b64}"}
                    }]
                }
            ],
            "max_tokens": 1500,
            "temperature": 0.3,
        }

        logger.info("Enviando imagen a OpenAI Vision (modelo: %s)", current_model)
        data = self._post(body, timeout)

        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            logger.exception("Formato inesperado en la respuesta de OpenAI Vision")
            raise OpenAIServiceError("Respuesta inesperada de OpenAI Vision") from e


openai_client = OpenAIClient()

def ask_openai(prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
    return openai_client.ask_openai(prompt, model, timeout)

def ask_openai_image(prompt: str, image_path: str | Path, 
                    model: Optional[str] = None, timeout: Optional[int] = None) -> str:
    return openai_client.ask_openai_image(prompt, image_path, model, timeout)