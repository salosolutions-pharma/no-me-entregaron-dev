import logging
from typing import Any, Dict

from manual_instrucciones.prompt_manager import prompt_manager
from session_manager.session_manager import SessionManager
from llm_core import LLMCore

logger = logging.getLogger(__name__)


class ConsentManager:
    """Gestiona el flujo de consentimiento y bienvenida usando prompts dinámicos."""

    def __init__(self):
        self.session_manager = SessionManager()
        self.llm_core = LLMCore()
        logger.info("ConsentManager inicializado con LLM Core.")

    def _convert_to_telegram_format(self, text: str) -> str:
        """Convierte formato markdown estándar a formato Telegram."""
        import re
        text = re.sub(r'\*\*(.*?)\*\*', r'*\1*', text)
        return text

    def get_bot_response(self, user_message: str = "", 
                        session_context: Dict[str, Any] = None) -> str:
        """Genera la respuesta del bot usando el prompt BYC."""
        try:
            byc_prompt = prompt_manager.get_prompt_by_module_and_function("BYC", "consentimiento")
            if not byc_prompt:
                logger.error("Prompt BYC no encontrado.")
                return "Lo siento, hay un problema técnico. Por favor intenta más tarde."

            context_info = self._build_session_context(session_context or {})
            canal = self._get_channel_from_context(session_context or {})
            logger.info(f"🔍 Canal detectado para prompt: {canal}")

            farewell_keywords = [
                "hasta luego", "adiós", "chao", "bye", "gracias", 
                "no necesito nada más", "ya no necesito ayuda"
            ]
            is_farewell = any(keyword in user_message.lower() for keyword in farewell_keywords)

            # ✅ MODIFICADO: Formatear el prompt con la variable {canal}
            try:
                formatted_prompt = byc_prompt.format(canal=canal)
                logger.info(f"✅ Prompt formateado correctamente con canal: {canal}")
            except KeyError as e:
                logger.warning(f"⚠️ Variable faltante en prompt: {e}. Usando prompt sin formatear.")
                formatted_prompt = byc_prompt
            except Exception as e:
                logger.error(f"❌ Error formateando prompt: {e}")
                formatted_prompt = byc_prompt

            full_prompt = f"""
{formatted_prompt}

=== CONTEXTO ACTUAL DE LA SESIÓN ===
{context_info}

=== ÚLTIMO MENSAJE DEL USUARIO ===
"{user_message}"

=== INSTRUCCIONES ADICIONALES ===
- Si el usuario dice palabras de despedida como "hasta luego", "adiós", "gracias", "no necesito nada más":
  * Responde cordialmente despidiéndote.
  * Indica que la sesión se cerrará.
  * Menciona que pueden regresar cuando necesiten ayuda.
- Sigue el flujo: 1) Teléfono (SOLO si canal=TL) → 2) Consentimiento → 3) Fórmula médica
- Mantén un tono amigable y profesional con emojis.
- NO menciones que estás usando un prompt o que eres un LLM.
- ✅ IMPORTANTE: Usa formato Telegram para negritas: *texto* en lugar de **texto**
- ✅ CANAL ACTUAL: {canal}

¿Es este un mensaje de despedida?: {"SÍ" if is_farewell else "NO"}

Responde ahora como el asistente "No Me Entregaron":
"""

            response = self.llm_core.ask_text(full_prompt)
            return self._convert_to_telegram_format(response.strip())

        except Exception as e:
            logger.error(f"Error generando respuesta con prompt BYC: {e}")
            return "Disculpa, hubo un error técnico. Por favor intenta nuevamente."

    def _get_channel_from_context(self, session_context: Dict[str, Any]) -> str:
        """
        ✅ NUEVA FUNCIÓN: Detecta el canal desde el contexto de sesión.
        Prioriza diferentes fuentes de información del canal.
        """
        # Intentar diferentes campos que pueden contener la información del canal
        possible_fields = [
            "detected_channel",  # Telegram
            "canal",            # WhatsApp 
            "channel",          # Genérico
            "canal_contacto"    # BigQuery
        ]
        
        for field in possible_fields:
            channel_value = session_context.get(field)
            if channel_value and channel_value in ["TL", "WA"]:
                logger.info(f"📍 Canal detectado desde campo '{field}': {channel_value}")
                return channel_value
        
        # Si no se encuentra, intentar deducir desde session_id
        session_id = session_context.get("session_id", "")
        if session_id.startswith("TL_"):
            logger.info("📍 Canal detectado desde session_id prefix: TL")
            return "TL"
        elif session_id.startswith("WA_"):
            logger.info("📍 Canal detectado desde session_id prefix: WA") 
            return "WA"
        
        # Fallback por defecto
        logger.warning("⚠️ No se pudo detectar canal, usando fallback: TL")
        return "TL"

    def _build_session_context(self, session_context: Dict[str, Any]) -> str:
        """Construye una descripción del contexto de la sesión actual para el LLM."""
        context_lines = []
        
        # ✅ MODIFICADO: Usar la nueva función para detectar canal
        canal = self._get_channel_from_context(session_context)
        context_lines.append(f"🌐 Canal de comunicación: {canal}")

        if session_context.get("phone_shared"):
            context_lines.append("✅ El usuario YA compartió su número de teléfono")
            if session_context.get("phone"):
                context_lines.append(f"   📞 Teléfono: {session_context['phone']}")
        else:
            if canal == "TL":
                context_lines.append("❌ El usuario NO ha compartido su número de teléfono")
            else:
                context_lines.append("ℹ️ Canal WhatsApp - Número ya disponible implícitamente")

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

    def handle_consent_response(self, user_telegram_id: int, user_identifier_for_session: str, 
                               consent_status: str, session_id: str) -> bool:
        """Procesa la respuesta de consentimiento."""
        try:
            success = self.session_manager.update_consent_for_session(session_id, consent_status)
            if not success:
                logger.warning(f"No se pudo actualizar el consentimiento en la sesión '{session_id}'.")
            return success
        except Exception as e:
            logger.error(f"Error al actualizar el consentimiento: {e}", exc_info=True)
            return False

    def get_consent_response_message(self, consent_granted: bool, 
                                   session_context: Dict[str, Any] = None) -> str:
        """Genera el mensaje de respuesta de consentimiento usando el prompt BYC."""
        context = session_context or {}
        context.update({
            "phone_shared": True,
            "consent_given": consent_granted,
            "consent_asked": True,
            "prescription_uploaded": False
        })

        user_message = ("Sí, autorizo el tratamiento de mis datos" if consent_granted 
                       else "No autorizo el tratamiento de mis datos")
        return self.get_bot_response(user_message, context)

    def should_close_session(self, user_message: str, session_context: Dict[str, Any] = None) -> bool:
        """
        Determina si la sesión debe cerrarse basándose en el mensaje del usuario.
        ✅ RESTAURADO: Funcionalidad de cierre por despedida.
        """
        farewell_keywords = [
            "hasta luego", "adiós", "chao", "bye", "gracias",
            "no necesito nada más", "ya no necesito ayuda",
            "eso es todo", "hasta la vista", "nos vemos"
        ]

        message_lower = user_message.lower().strip()
        return any(keyword in message_lower for keyword in farewell_keywords)

    