from __future__ import annotations

import logging
import os
from typing import Any, Final

import requests
from dotenv import load_dotenv

# Configuración inicial del logger para el módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Carga las variables de entorno al inicio del módulo
load_dotenv()

class OpenAIServiceError(RuntimeError):
    """Excepción personalizada para errores específicos de la integración con la API de OpenAI."""
    pass

class OpenAIClient:
    """
    Cliente para interactuar con la API de OpenAI.

    Este cliente gestiona la configuración de la API, la construcción de solicitudes
    y el manejo de respuestas y errores.
    """

    # Constantes de configuración que se cargan desde las variables de entorno
    # Se usan 'Final' para indicar que son constantes y no deben ser reasignadas.
    _API_KEY: Final[str | None] = os.getenv("OPENAI_API_KEY")
    _API_URL: Final[str] = os.getenv(
        "OPENAI_API_URL",
        "https://api.openai.com/v1/chat/completions"
    )
    _DEFAULT_MODEL: Final[str] = os.getenv("OPENAI_DEFAULT_MODEL", "gpt-4.1-mini")
    _DEFAULT_TIMEOUT: Final[int] = int(os.getenv("OPENAI_TIMEOUT", "60"))

    def __init__(self):
        """
        Inicializa el cliente de OpenAI.

        Verifica que la clave de API esté configurada.
        """
        if not self._API_KEY:
            raise RuntimeError("OPENAI_API_KEY no se encuentra en las variables de entorno.")
        
        self.logger = logger
        self.logger.info("OpenAIClient inicializado con modelo por defecto: %s", self._DEFAULT_MODEL)

    def _build_payload(self, prompt: str, model: str) -> dict[str, Any]:
        """
        Construye el cuerpo (payload) de la solicitud para la API de OpenAI.

        Args:
            prompt: El texto de entrada para el modelo.
            model: El nombre del modelo a usar.

        Returns:
            Un diccionario que representa el payload JSON para la API de OpenAI.
        """
        return {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            # Puedes añadir más parámetros de configuración aquí si son comunes a todas las llamadas
            # Por ejemplo: "temperature": 0.7, "max_tokens": 2048, etc.
        }

    def _parse_response(self, data: dict[str, Any]) -> str:
        """
        Parsea la respuesta JSON de la API de OpenAI para extraer el texto generado.

        Args:
            data: El diccionario JSON de la respuesta de la API.

        Returns:
            El texto generado por el modelo.

        Raises:
            OpenAIServiceError: Si la respuesta no contiene el formato esperado.
        """
        # Verifica la estructura esperada de la respuesta de OpenAI
        # Formato esperado: { choices: [ { message: { content: ... } } ] }
        if "choices" in data and len(data["choices"]) > 0:
            choice = data["choices"][0]
            if "message" in choice and "content" in choice["message"]:
                return choice["message"]["content"]
        
        # Si no se encuentra el formato esperado, registra un error y lanza una excepción
        self.logger.error("❌ Respuesta de OpenAI sin contenido esperado: %s", data)
        raise OpenAIServiceError("Respuesta de OpenAI sin contenido válido")

    def ask_openai(
        self,
        prompt: str,
        model: str | None = None,
        timeout: int | None = None,
    ) -> str:
        """
        Realiza una llamada a la API de OpenAI para generar texto.

        Args:
            prompt: El texto de entrada para el modelo.
            model: (Opcional) El nombre del modelo a usar (ej. "gpt-4.1-mini").
                   Si no se especifica, se usará el modelo por defecto configurado.
            timeout: (Opcional) El tiempo máximo de espera para la respuesta en segundos.
                     Si no se especifica, se usará el timeout por defecto.

        Returns:
            El texto generado por el modelo de OpenAI.

        Raises:
            OpenAIServiceError: Si ocurre un error durante la conexión, la solicitud
                                o el procesamiento de la respuesta de la API de OpenAI.
        """
        # Usa el modelo y timeout pasados como argumento, o los valores por defecto de la clase
        current_model = model if model is not None else self._DEFAULT_MODEL
        current_timeout = timeout if timeout is not None else self._DEFAULT_TIMEOUT

        headers = {
            "Authorization": f"Bearer {self._API_KEY}",
            "Content-Type": "application/json",
        }
        payload = self._build_payload(prompt, current_model) # Usa el método privado para construir el payload

        try:
            self.logger.info("Enviando solicitud a OpenAI con modelo: %s", current_model)
            resp = requests.post(
                self._API_URL,
                json=payload,
                headers=headers,
                timeout=current_timeout,
            )
        except requests.RequestException as exc:
            self.logger.exception("❌ Error en la conexión con OpenAI")
            raise OpenAIServiceError("No se pudo conectar a OpenAI") from exc

        # Verifica si la respuesta HTTP fue exitosa
        if not resp.ok:
            self.logger.error(
                "❌ OpenAI API devolvió status %s: %s",
                resp.status_code,
                resp.text,
            )
            raise OpenAIServiceError(f"Status {resp.status_code}: {resp.text}")

        try:
            data = resp.json()
            return self._parse_response(data) # Usa el método privado para parsear la respuesta
        except (ValueError, KeyError, IndexError) as exc: # Se añadió IndexError para mayor robustez
            self.logger.exception("❌ Error decodificando JSON de OpenAI o formato inesperado")
            raise OpenAIServiceError("Respuesta JSON inválida o formato inesperado de OpenAI") from exc

# Instancia global del cliente OpenAI para facilitar su uso en otros módulos
# Esto sigue el patrón singleton, donde se crea una única instancia del cliente.
openai_client = OpenAIClient()

# Exporta la función ask_openai para compatibilidad con el código existente
# Ahora, ask_openai simplemente llama al método de la instancia global.
def ask_openai(
    prompt: str,
    model: str | None = None,
    timeout: int | None = None,
) -> str:
    """
    Función de conveniencia para llamar a la API de OpenAI a través de la instancia global del cliente.
    """
    return openai_client.ask_openai(prompt, model, timeout)