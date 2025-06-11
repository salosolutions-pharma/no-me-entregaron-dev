import logging
from typing import Any, Dict

from manual_instrucciones.prompt_manager import prompt_manager
from session_manager.session_manager import SessionManager
from llm_core import LLMCore

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ConsentManager:
    """
    Gestiona el flujo de consentimiento y bienvenida usando prompts dinámicos.
    """

    def __init__(self):
        self.session_manager = SessionManager()
        self.llm_core = LLMCore()
        logger.info("ConsentManager inicializado con LLM Core.")

    def get_bot_response(self, user_message: str = "", session_context: Dict[str, Any] = None) -> str:
        """
        Genera la respuesta del bot usando el prompt BYC.
        """
        try:
            byc_prompt = prompt_manager.get_prompt_by_keyword("BYC")
            if not byc_prompt:
                logger.error("Prompt BYC no encontrado.")
                return "Lo siento, hay un problema técnico. Por favor intenta más tarde."

            context_info = self._build_session_context(session_context or {})

            farewell_keywords = ["hasta luego", "adiós", "chao", "bye", "gracias", "no necesito nada más", "ya no necesito ayuda"]
            is_farewell = any(keyword in user_message.lower() for keyword in farewell_keywords)

            full_prompt = f"""
{byc_prompt}

=== CONTEXTO ACTUAL DE LA SESIÓN ===
{context_info}

=== ÚLTIMO MENSAJE DEL USUARIO ===
"{user_message}"

=== INSTRUCCIONES ADICIONALES ===
- Si el usuario dice palabras de despedida como "hasta luego", "adiós", "gracias", "no necesito nada más":
  * Responde cordialmente despidiéndote.
  * Indica que la sesión se cerrará.
  * Menciona que pueden regresar cuando necesiten ayuda.
- Sigue el flujo: 1) Teléfono → 2) Consentimiento → 3) Fórmula médica
- Mantén un tono amigable y profesional con emojis.
- NO menciones que estás usando un prompt o que eres un LLM.

¿Es este un mensaje de despedida?: {"SÍ" if is_farewell else "NO"}

Responde ahora como el asistente "No Me Entregaron":
"""

            response = self.llm_core.ask_text(full_prompt)
            return response.strip()

        except Exception as e:
            logger.error(f"Error generando respuesta con prompt BYC: {e}")
            return "Disculpa, hubo un error técnico. Por favor intenta nuevamente."

    def _build_session_context(self, session_context: Dict[str, Any]) -> str:
        """
        Construye una descripción del contexto de la sesión actual para el LLM.
        """
        context_lines = []

        if session_context.get("phone_shared"):
            context_lines.append("✅ El usuario YA compartió su número de teléfono")
            if session_context.get("phone"):
                context_lines.append(f"   📞 Teléfono: {session_context['phone']}")
        else:
            context_lines.append("❌ El usuario NO ha compartido su número de teléfono")

        if session_context.get("consent_given"):
            context_lines.append("✅ El usuario YA otorgó su consentimiento para tratamiento de datos")
        elif session_context.get("consent_asked"):
            context_lines.append("⏳ Se solicitó consentimiento, esperando respuesta del usuario")
        else:
            context_lines.append("❌ NO se ha solicitado consentimiento aún")

        if session_context.get("prescription_uploaded"):
            context_lines.append("✅ El usuario YA subió su fórmula médica")
        else:
            context_lines.append("❌ El usuario NO ha subido su fórmula médica")

        if session_context.get("session_id"):
            context_lines.append(f"🔑 ID de Sesión: {session_context['session_id']}")

        return "\n".join(context_lines)

    def handle_consent_response(
        self, user_telegram_id: int, user_identifier_for_session: str, consent_status: str, session_id: str
    ) -> bool:
        """
        Procesa la respuesta de consentimiento.
        """
        try:
            success = self.session_manager.update_consent_for_session(session_id, consent_status)
            if not success:
                logger.warning(f"No se pudo actualizar el consentimiento en la sesión '{session_id}'.")
            return success
        except Exception as e:
            logger.error(f"Error al actualizar el consentimiento: {e}", exc_info=True)
            return False

    def get_consent_response_message(self, consent_granted: bool, session_context: Dict[str, Any] = None) -> str:
        """
        Genera el mensaje de respuesta de consentimiento usando el prompt BYC.
        """
        context = session_context or {}
        context.update({
            "phone_shared": True,
            "consent_given": consent_granted,
            "consent_asked": True,
            "prescription_uploaded": False
        })

        user_message = "Sí, autorizo el tratamiento de mis datos" if consent_granted else "No autorizo el tratamiento de mis datos"
        return self.get_bot_response(user_message, context)

    def should_close_session(self, user_message: str, session_context: Dict[str, Any] = None) -> bool:
        """
        Determina si la sesión debe cerrarse basándose en el mensaje del usuario.
        """
        farewell_keywords = [
            "hasta luego", "adiós", "chao", "bye", "gracias",
            "no necesito nada más", "ya no necesito ayuda",
            "eso es todo", "hasta la vista", "nos vemos"
        ]

        message_lower = user_message.lower().strip()
        return any(keyword in message_lower for keyword in farewell_keywords)