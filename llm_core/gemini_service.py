import os
import logging
from base64 import b64encode
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class GeminiServiceError(RuntimeError):
    """Excepción personalizada para errores específicos de Gemini."""


class GeminiClient:
    def __init__(self):
        self._api_key = os.getenv("GEMINI_API_KEY")
        self._default_model = os.getenv("GEMINI_DEFAULT_MODEL")
        self._default_timeout = int(os.getenv("GEMINI_TIMEOUT", "60"))
        self._base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        
        if not self._api_key:
            raise RuntimeError("GEMINI_API_KEY no se encuentra en las variables de entorno.")
        logger.info("GeminiClient inicializado con modelo: %s", self._default_model)

    def _encode_image_to_b64(self, image_path: str | Path) -> tuple[str, str]:
        """Codifica la imagen como base64 y devuelve su tipo MIME."""
        path = Path(image_path)
        ext = path.suffix.lower()
        mime = {
            ".png": "image/png",
            ".jpg": "image/jpg", 
            ".jpeg": "image/jpeg",
        }.get(ext, "image/jpeg")
        encoded = b64encode(path.read_bytes()).decode()
        return encoded, mime

    def _post(self, body: dict, url: str, timeout: int) -> dict:
        """Realiza petición POST a la API de Gemini."""
        headers = {"Content-Type": "application/json"}
        params = {"key": self._api_key}

        try:
            response = requests.post(url, headers=headers, params=params, json=body, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.exception("Error en la solicitud a Gemini API")
            raise GeminiServiceError(f"Gemini API falló: {e}")

    def ask_gemini(self, prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta de solo texto a Gemini."""
        model_name = model or self._default_model
        url = f"{self._base_url}/{model_name}:generateContent"

        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.7,
                "topK": 1,
                "topP": 1,
                "maxOutputTokens": 2048,
            },
        }

        data = self._post(body, url, timeout or self._default_timeout)
        try:
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError) as e:
            logger.exception("Formato inesperado en la respuesta de Gemini")
            raise GeminiServiceError("Respuesta inesperada de Gemini") from e

    def ask_gemini_image(self, prompt: str, image_path: str | Path, 
                        model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta que incluye imagen (Gemini Vision)."""
        model_name = model or self._default_model
        url = f"{self._base_url}/{model_name}:generateContent"

        image_b64, mime_type = self._encode_image_to_b64(image_path)

        body = {
            "contents": [{
                "role": "user",
                "parts": [
                    {"inlineData": {"mimeType": mime_type, "data": image_b64}},
                    {"text": prompt}
                ]
            }],
            "generationConfig": {"temperature": 0, "maxOutputTokens": 2048}
        }

        logger.info("Enviando imagen a Gemini Vision (modelo: %s)", model_name)
        data = self._post(body, url, timeout or self._default_timeout)
        try:
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError) as e:
            logger.exception("Formato inesperado en la respuesta de Gemini Vision")
            raise GeminiServiceError("Respuesta inesperada de Gemini Vision") from e


gemini_client = GeminiClient()

def ask_gemini(prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
    return gemini_client.ask_gemini(prompt, model, timeout)

def ask_gemini_image(prompt: str, image_path: str | Path, 
                    model: Optional[str] = None, timeout: Optional[int] = None) -> str:
    return gemini_client.ask_gemini_image(prompt, image_path, model, timeout)