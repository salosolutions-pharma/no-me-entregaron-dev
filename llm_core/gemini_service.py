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

class GeminiServiceError(RuntimeError):
    """Excepción personalizada para errores específicos de la integración con la API de Gemini."""
    pass

class GeminiClient:
    """
    Cliente para interactuar con la API de Google Gemini.

    Este cliente gestiona la configuración de la API, la construcción de solicitudes
    y el manejo de respuestas y errores.
    """

    # Constantes de configuración que se cargan desde las variables de entorno
    # Se usan 'Final' para indicar que son constantes y no deben ser reasignadas.
    _API_KEY: Final[str | None] = os.getenv("GEMINI_API_KEY")
    _DEFAULT_MODEL: Final[str] = os.getenv("GEMINI_DEFAULT_MODEL", "gemini-2.5-flash")
    _DEFAULT_TIMEOUT: Final[int] = int(os.getenv("GEMINI_TIMEOUT", "60"))
    _BASE_URL: Final[str] = "https://generativelanguage.googleapis.com/v1beta/models"

    def __init__(self):
        """
        Inicializa el cliente de Gemini.

        Verifica que la clave de API esté configurada.
        """
        if not self._API_KEY:
            raise RuntimeError("GEMINI_API_KEY no se encuentra en las variables de entorno.")
        
        # Opcional: Configurar un logger específico para la instancia si se desea,
        # pero el logger global del módulo es suficiente para este caso.
        self.logger = logger 
        self.logger.info("GeminiClient inicializado con modelo por defecto: %s", self._DEFAULT_MODEL)

    def _build_payload(self, prompt: str) -> dict[str, Any]:
        """
        Construye el cuerpo (payload) de la solicitud para la API de Gemini.

        Args:
            prompt: El texto de entrada para el modelo.

        Returns:
            Un diccionario que representa el payload JSON para la API de Gemini.
        """
        return {
            "contents": [
                {
                    "parts": [
                        {"text": prompt}
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.7,
                "topK": 1,
                "topP": 1,
                "maxOutputTokens": 2048,
            }
        }

    def _parse_response(self, data: dict[str, Any]) -> str:
        """
        Parsea la respuesta JSON de la API de Gemini para extraer el texto generado.

        Args:
            data: El diccionario JSON de la respuesta de la API.

        Returns:
            El texto generado por el modelo.

        Raises:
            GeminiServiceError: Si la respuesta no contiene el formato esperado.
        """
        # Verifica la estructura esperada de la respuesta de Gemini
        if "candidates" in data and len(data["candidates"]) > 0:
            candidate = data["candidates"][0]
            if "content" in candidate and "parts" in candidate["content"]:
                return candidate["content"]["parts"][0]["text"]
        
        # Si no se encuentra el formato esperado, registra un error y lanza una excepción
        self.logger.error("❌ Respuesta de Gemini sin contenido esperado: %s", data)
        raise GeminiServiceError("Respuesta de Gemini sin contenido válido")

    def ask_gemini(
        self,
        prompt: str,
        model: str | None = None,
        timeout: int | None = None,
    ) -> str:
        """
        Realiza una llamada a la API de Gemini para generar texto.

        Args:
            prompt: El texto de entrada para el modelo.
            model: (Opcional) El nombre del modelo a usar (ej. "gemini-1.5-flash").
                   Si no se especifica, se usará el modelo por defecto configurado.
            timeout: (Opcional) El tiempo máximo de espera para la respuesta en segundos.
                     Si no se especifica, se usará el timeout por defecto.

        Returns:
            El texto generado por el modelo de Gemini.

        Raises:
            GeminiServiceError: Si ocurre un error durante la conexión, la solicitud
                                o el procesamiento de la respuesta de la API de Gemini.
        """
        # Usa el modelo y timeout pasados como argumento, o los valores por defecto de la clase
        current_model = model if model is not None else self._DEFAULT_MODEL
        current_timeout = timeout if timeout is not None else self._DEFAULT_TIMEOUT

        # Construye la URL completa para la solicitud
        url = f"{self._BASE_URL}/{current_model}:generateContent"
        
        headers = {"Content-Type": "application/json"}
        params = {"key": self._API_KEY}
        payload = self._build_payload(prompt) # Usa el método privado para construir el payload

        try:
            self.logger.info("Enviando solicitud a Gemini con modelo: %s", current_model)
            resp = requests.post(url, params=params, json=payload, headers=headers, timeout=current_timeout)
        except requests.RequestException as exc:
            self.logger.exception("❌ Error en la conexión con Gemini")
            raise GeminiServiceError("No se pudo conectar a Gemini") from exc
        
        # Verifica si la respuesta HTTP fue exitosa
        if not resp.ok:
            self.logger.error("❌ Gemini API devolvió status %s: %s", resp.status_code, resp.text)
            raise GeminiServiceError(f"Status {resp.status_code}: {resp.text}")
        
        try:
            data = resp.json()
            return self._parse_response(data) # Usa el método privado para parsear la respuesta
            
        except (ValueError, KeyError, IndexError) as exc:
            self.logger.exception("❌ Error decodificando JSON de Gemini o formato inesperado")
            raise GeminiServiceError("Respuesta JSON inválida o formato inesperado de Gemini") from exc

# Instancia global del cliente Gemini para facilitar su uso en otros módulos
# Esto sigue el patrón singleton, donde se crea una única instancia del cliente.
gemini_client = GeminiClient()

# Exporta la función ask_gemini para compatibilidad con el código existente
# Ahora, ask_gemini simplemente llama al método de la instancia global.
def ask_gemini(
    prompt: str,
    model: str | None = None,
    timeout: int | None = None,
) -> str:
    """
    Función de conveniencia para llamar a la API de Gemini a través de la instancia global del cliente.
    """
    return gemini_client.ask_gemini(prompt, model, timeout)