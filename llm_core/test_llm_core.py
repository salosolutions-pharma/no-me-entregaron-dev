from __future__ import annotations

import logging
import os
import sys
# from typing import TYPE_CHECKING # Ya no es necesario para este enfoque simplificado

# Asegúrate de que el directorio padre esté en el PYTHONPATH para las importaciones relativas
# Esto es crucial si 'llm_core' no es un paquete instalado.
sys.path.append(os.path.dirname(__file__))

# Importa las funciones de conveniencia directamente para asegurar su disponibilidad en tiempo de ejecución.
# Estas funciones ya llaman a las instancias globales de sus respectivos clientes (OpenAIClient, GeminiClient).
from openai_service import ask_openai, OpenAIServiceError
from gemini_service import ask_gemini, GeminiServiceError


# Configuración del logger para el módulo principal de testing
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")

class LLMTextGenerator:
    """
    Clase para gestionar la generación de texto utilizando diferentes modelos LLM,
    con una lógica de fallback entre servicios.
    """

    def __init__(self):
        """
        Inicializa el generador de texto LLM.
        
        Aquí, las funciones ask_openai y ask_gemini se importan directamente
        del módulo, ya que son las funciones de conveniencia que exponen
        la funcionalidad de los clientes refactorizados.
        """
        self.openai_ask_func = ask_openai
        self.gemini_ask_func = ask_gemini
        self.logger = logger # Usa el logger del módulo

    def generate_text(self, prompt: str) -> str:
        """
        Genera texto utilizando el servicio de OpenAI, con fallback a Gemini si falla.

        Args:
            prompt: El texto de entrada para el modelo.

        Returns:
            El texto generado por el LLM.

        Raises:
            RuntimeError: Si ambos servicios de LLM fallan.
        """
        self.logger.info("Prompt: %r", prompt)
        try:
            # Intenta con OpenAI primero
            reply = self.openai_ask_func(prompt)
            self.logger.info("→ Respuesta obtenida de OpenAI")
            return reply
        except OpenAIServiceError as e:
            self.logger.warning("OpenAIServiceError: %s. Fallback a Gemini...", e)
            try:
                # Si OpenAI falla, intenta con Gemini
                reply = self.gemini_ask_func(prompt)
                self.logger.info("→ Respuesta obtenida de Gemini")
                return reply
            except GeminiServiceError as e2:
                # Si ambos fallan, registra el error y lanza una excepción final
                self.logger.error("GeminiServiceError: %s. No hay más modelos activos.", e2)
                raise RuntimeError("Todos los servicios de LLM fallaron") from e2

def main():
    """
    Función principal para ejecutar la prueba de generación de texto.
    """
    # Instancia el generador de texto
    text_generator = LLMTextGenerator()

    prompt = "Escribe un poema breve en español sobre un atardecer en la montaña."
    try:
        # Llama al método generate_text de la instancia
        respuesta = text_generator.generate_text(prompt)
        print("\n=== RESPUESTA ===\n")
        print(respuesta)
    except Exception as exc:
        # Captura cualquier excepción general y la registra
        logger.exception("Error al generar texto: %s", exc)
        sys.exit(1)

if __name__ == "__main__":
    main()