import os
import logging
import requests
from pathlib import Path
from base64 import b64encode
from typing import Any, Final, Optional
from dotenv import load_dotenv

# Configurar logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Cargar variables de entorno
load_dotenv()


class ClaudeServiceError(RuntimeError):
    """Excepción personalizada para errores específicos de Claude."""
    pass


class ClaudeClient:
    _API_KEY: Final[str | None] = os.getenv("ANTHROPIC_API_KEY")
    _DEFAULT_MODEL: Final[str] = os.getenv("ANTHROPIC_DEFAULT_MODEL")
    _DEFAULT_TIMEOUT: Final[int] = int(os.getenv("ANTHROPIC_TIMEOUT"))
    _BASE_URL: Final[str] = "https://api.anthropic.com/v1/messages"

    def __init__(self):
        if not self._API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY no está configurada.")
        logger.info("ClaudeClient inicializado con modelo por defecto: %s", self._DEFAULT_MODEL)

    def _encode_image_to_b64(self, image_path: str | Path) -> tuple[str, str]:
        """Codifica la imagen a base64 y retorna con tipo MIME."""
        path = Path(image_path)
        mime = "image/jpeg"
        encoded = b64encode(path.read_bytes()).decode()
        return encoded, mime

    def _post(self, body: dict[str, Any], timeout: int) -> dict[str, Any]:
        headers = {
            "x-api-key": self._API_KEY,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01",
        }
        try:
            response = requests.post(self._BASE_URL, headers=headers, json=body, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.exception("❌ Error en la solicitud a Claude API")
            raise ClaudeServiceError(f"Claude API falló: {e}")

    def ask_claude(self, prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta de solo texto a Claude."""
        body = {
            "model": model or self._DEFAULT_MODEL,
            "max_tokens": 1000,
            "messages": [{"role": "user", "content": prompt}],
        }
        data = self._post(body, timeout or self._DEFAULT_TIMEOUT)
        try:
            return data["content"][0]["text"]
        except (KeyError, IndexError) as e:
            logger.exception("❌ Formato inesperado en la respuesta de Claude")
            raise ClaudeServiceError("Respuesta inesperada de Claude") from e
        

    def ask_claude_image(self, prompt: str, image_path: str | Path, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta que incluye imagen."""
        image_b64, mime_type = self._encode_image_to_b64(image_path)
        data_url = f"data:{mime_type};base64,{image_b64}"
        body = {
            "model": model or self._DEFAULT_MODEL,
            "max_tokens": 1000,
            "temperature": 0,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": mime_type,
                                "data": image_b64
                            }
                        }
                    ]
                }
            ]
        }

        logger.info("📤 Enviando imagen a Claude (modelo: %s)", body["model"])
        data = self._post(body, timeout or self._DEFAULT_TIMEOUT)
        return "".join(block.get("text", "") for block in data.get("content", []))


# Instancia global del cliente
claude_client = ClaudeClient()

def ask_claude(prompt: str, model: str = None, timeout: int = None) -> str:
    return claude_client.ask_claude(prompt, model, timeout)

def ask_claude_image(prompt: str, image_path: str | Path, model: str = None, timeout: int = 60) -> str:
    return claude_client.ask_claude_image(prompt, image_path, model, timeout)
'''

# llm_core/claude_service.py

import os
import requests
import logging
from pathlib import Path
from base64 import b64encode
from typing import Any, Final, Optional
from dotenv import load_dotenv

# Configura logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Cargar variables de entorno
load_dotenv()


class ClaudeServiceError(RuntimeError):
    """Excepción personalizada para errores específicos de Claude."""
    pass


class ClaudeClient:
    _API_KEY: Final[str | None] = os.getenv("ANTHROPIC_API_KEY")
    _DEFAULT_MODEL: Final[str] = os.getenv("ANTHROPIC_DEFAULT_MODEL", "claude-3-5-sonnet-20241022")
    _DEFAULT_TIMEOUT: Final[int] = int(os.getenv("ANTHROPIC_TIMEOUT", "60"))
    _BASE_URL: Final[str] = "https://api.anthropic.com/v1/messages"

    def __init__(self):
        if not self._API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY no está configurada.")
        logger.info("ClaudeClient inicializado con modelo por defecto: %s", self._DEFAULT_MODEL)

    def _encode_image_to_b64(self, image_path: str | Path) -> tuple[str, str]:
        """Codifica la imagen a base64 y retorna con tipo MIME."""
        path = Path(image_path)
        ext = path.suffix.lower()
        mime = mime = "image/jpeg"
        encoded = b64encode(path.read_bytes()).decode()
        return encoded, mime

    def _post(self, body: dict[str, Any], timeout: int) -> dict[str, Any]:
        headers = {
            "x-api-key": self._API_KEY,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01",
        }
        try:
            response = requests.post(self._BASE_URL, headers=headers, json=body, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.exception("❌ Error en la solicitud a Claude API")
            raise ClaudeServiceError(f"Claude API falló: {e}")

    def ask_claude(self, prompt: str, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta de solo texto."""
        body = {
            "model": model or self._DEFAULT_MODEL,
            "max_tokens": 1000,
            "messages": [{"role": "user", "content": prompt}],
        }
        data = self._post(body, timeout or self._DEFAULT_TIMEOUT)
        return data["content"][0]["text"]

    def ask_claude_image(self, prompt: str, image_path: str | Path, model: Optional[str] = None, timeout: Optional[int] = None) -> str:
        """Consulta que incluye imagen."""
        image_b64, mime_type = self._encode_image_to_b64(image_path)
        body = {
            "model": model or self._DEFAULT_MODEL,
            "max_tokens": 1000,
            "temperature": 0,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": mime_type,
                                "data": image_b64
                            }
                        }
                    ]
                }
            ]
        }

        logger.info("📤 Enviando imagen a Claude (modelo: %s)", body["model"])
        data = self._post(body, timeout or self._DEFAULT_TIMEOUT)
        return "".join(block.get("text", "") for block in data.get("content", []))


# Instancia global exportable
claude_client = ClaudeClient()

def ask_claude(prompt: str, model: str = None, timeout: int = None) -> str:
    return claude_client.ask_claude(prompt, model, timeout)

def ask_claude_image(prompt: str, image_path: str | Path, model: str = None, timeout: int = 60) -> str:
    return claude_client.ask_claude_image(prompt, image_path, model, timeout)
'''