import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ApplicationBuilder,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from BYC.consentimiento import ConsentManager
from processor_image_prescription.pip_processor import PIPProcessor
from claim_generator.claim_manager import ClaimManager

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

TELEGRAM_API_TOKEN = os.getenv("TELEGRAM_API_TOKEN", "")
SESSION_EXPIRATION_SECONDS = int(os.getenv("SESSION_EXPIRATION_SECONDS", 3600))

if not TELEGRAM_API_TOKEN:
    logger.critical("TELEGRAM_API_TOKEN no configurado. Abortando.")
    sys.exit(1)

consent_manager: Optional[ConsentManager] = None
pip_processor_instance: Optional[PIPProcessor] = None
claim_manager: Optional[ClaimManager] = None

try:
    consent_manager = ConsentManager()
    pip_processor_instance = PIPProcessor()
    claim_manager = ClaimManager()
    logger.info("Todos los componentes inicializados correctamente.")
except Exception as e:
    logger.critical(f"Error al inicializar componentes: {e}")
    sys.exit(1)

active_sessions: Dict[str, datetime] = {}


def create_consent_keyboard() -> InlineKeyboardMarkup:
    """Crea el teclado para la respuesta de consentimiento."""
    buttons = [
        [InlineKeyboardButton("✅ Sí, autorizo", callback_data="consent_yes")],
        [InlineKeyboardButton("❌ No autorizo", callback_data="consent_no")],
    ]
    return InlineKeyboardMarkup(buttons)


def create_contact_keyboard() -> ReplyKeyboardMarkup:
    """Crea el teclado para compartir contacto."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton(text="📱 Enviar mi número", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_session_context(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    """Recupera el contexto de la sesión actual."""
    return {
        "phone_shared": context.user_data.get("phone") is not None,
        "phone": context.user_data.get("phone"),
        "consent_given": context.user_data.get("consent_given", False),
        "consent_asked": context.user_data.get("consent_asked", False),
        "prescription_uploaded": context.user_data.get("prescription_uploaded", False),
        "session_id": context.user_data.get("session_id")
    }


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detecta mensajes de despedida y cierra la sesión automáticamente."""
    chat_id = update.effective_chat.id
    user_message = update.message.text if update.message else ""

    logger.info(f"Mensaje recibido de {chat_id}: '{user_message}'")

    try:
        session_context = get_session_context(context)

        if consent_manager.should_close_session(user_message, session_context):
            session_id = session_context.get("session_id")
            if session_id:
                response = consent_manager.get_bot_response(user_message, session_context)
                await context.bot.send_message(chat_id=chat_id, text=response)

                consent_manager.session_manager.close_session(session_id, reason="user_farewell")
                active_sessions.pop(session_id, None)
                context.user_data.clear()
                logger.info(f"Sesión {session_id} cerrada por despedida del usuario.")
            return

        response = consent_manager.get_bot_response(user_message, session_context)
        keyboard = None

        if any(word in response.lower() for word in ["autorización", "autorizas", "consentimiento"]) and not session_context.get("consent_asked"):
            keyboard = create_consent_keyboard()
            context.user_data["consent_asked"] = True
            logger.info("Teclado de consentimiento añadido.")

        elif any(word in response.lower() for word in ["teléfono", "número", "contacto"]) and not session_context.get("phone_shared"):
            keyboard = create_contact_keyboard()
            logger.info("Teclado de contacto añadido.")

        await context.bot.send_message(
            chat_id=chat_id,
            text=response,
            reply_markup=keyboard
        )

        if session_context.get("session_id"):
            consent_manager.session_manager.add_message_to_session(
                session_context["session_id"], user_message, "user", "conversation"
            )
            consent_manager.session_manager.add_message_to_session(
                session_context["session_id"], response, "bot", "conversation"
            )
        logger.info(f"Respuesta enviada a {chat_id}.")

    except Exception as e:
        logger.error(f"Error en handle_message: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="Disculpa, hubo un error técnico. Por favor intenta nuevamente."
        )


async def process_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa el contacto compartido y actualiza el contexto."""
    contact = update.message.contact
    phone = contact.phone_number if contact else None

    logger.info(f"Contacto recibido: {phone}")

    if not phone:
        await update.message.reply_text("No pude obtener tu número. Por favor, inténtalo de nuevo.")
        return

    try:
        session_info = consent_manager.session_manager.create_session_with_history_check(phone, channel="TL")
        context.user_data["session_id"] = session_info["new_session_id"]
        context.user_data["phone"] = phone
        context.user_data["phone_shared"] = True
        active_sessions[session_info["new_session_id"]] = datetime.now()

        logger.info(f"Sesión creada: {session_info['new_session_id']}")

        await update.message.reply_text("¡Perfecto! Gracias por compartir tu número. 📱", reply_markup=ReplyKeyboardRemove())

        session_context = get_session_context(context)
        response = consent_manager.get_bot_response("He compartido mi número de teléfono", session_context)

        consent_manager.session_manager.add_message_to_session(
            session_info["new_session_id"], f"Teléfono compartido: {phone}", "user", "contact_shared"
        )
        await update.message.reply_text(response, reply_markup=create_consent_keyboard())

    except Exception as e:
        logger.error(f"Error al crear la sesión para el contacto: {e}")
        await context.bot.send_message(f"Ocurrió un problema al crear tu sesión. Por favor, inténtalo de nuevo.")


async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja las respuestas de los botones."""
    query = update.callback_query
    await query.answer()
    data = query.data
    session_id = context.user_data.get("session_id")

    logger.info(f"Callback recibido: {data}")

    if not session_id:
        await query.edit_message_text("Tu sesión ha expirado. Por favor, inicia una nueva conversación.")
        return

    if data == "consent_yes":
        await handle_consent_granted(query, context, session_id)
    elif data == "consent_no":
        await handle_consent_denied(query, context, session_id)
    else:
        await query.edit_message_text("Opción no reconocida.")


async def handle_consent_granted(query, context, session_id: str) -> None:
    """Maneja el consentimiento otorgado usando la sesión existente."""
    user_id = query.from_user.id
    phone = context.user_data.get("phone")

    logger.info(f"Consentimiento otorgado por el usuario {user_id}")

    if phone and consent_manager:
        success = consent_manager.handle_consent_response(
            user_id, phone, "autorizado", session_id=session_id
        )

        if success:
            context.user_data["consent_given"] = True
            session_context = get_session_context(context)
            response = consent_manager.get_consent_response_message(True, session_context)
            await query.edit_message_text(response)
            consent_manager.session_manager.add_message_to_session(
                session_id, "Consentimiento otorgado", "user", "consent_granted"
            )
        else:
            await query.edit_message_text("Hubo un problema al guardar tu consentimiento.")


async def handle_consent_denied(query, context, session_id: str) -> None:
    """Maneja el consentimiento denegado."""
    user_id = query.from_user.id
    phone = context.user_data.get("phone")

    logger.info(f"Consentimiento denegado por el usuario {user_id}")

    if phone and consent_manager:
        consent_manager.handle_consent_response(user_id, phone, "no autorizado", session_id)

    session_context = get_session_context(context)
    response = consent_manager.get_consent_response_message(False, session_context)

    await query.edit_message_text(response)

    if consent_manager:
        consent_manager.session_manager.close_session(session_id, reason="no_consent")

    active_sessions.pop(session_id, None)
    context.user_data.clear()


async def process_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa imágenes de recetas médicas usando el procesador PIP."""

    session_context = get_session_context(context)

    if not session_context.get("consent_given"):
        await update.message.reply_text(
            "Primero necesito tu autorización para procesar tus datos.",
            reply_markup=create_consent_keyboard()
        )
        return

    session_id = session_context.get("session_id")
    if not session_id:
        await update.message.reply_text(
            "No hay una sesión activa. Por favor, reinicia la conversación."
        )
        return

    try:
        processing_msg = await update.message.reply_text(
            "📸 Imagen recibida. Analizando tu fórmula médica, por favor espera..."
        )

        photo = update.message.photo[-1]
        photo_file = await photo.get_file()

        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
            temp_image_path = Path(temp_file.name)

        await photo_file.download_to_drive(temp_image_path)

        result = pip_processor_instance.process_image(temp_image_path, session_id)
        await processing_msg.delete()

        if isinstance(result, str):
            await update.message.reply_text(result)
            return

        if isinstance(result, dict):
            context.user_data["prescription_uploaded"] = True

            consent_manager.session_manager.add_message_to_session(
                session_id, "Fórmula médica procesada exitosamente", "system", "prescription_processed"
            )

            confirmation_msg = pip_processor_instance.get_confirmation_message(result)
            await update.message.reply_text(confirmation_msg)

            if result.get("_requires_completion"):
                await update.message.reply_text(
                    "📝 Para completar tu reclamación, necesito algunos datos adicionales. "
                    "Te haré algunas preguntas para completar tu información."
                )
            else:
                await update.message.reply_text(
                    "✅ Tu información está completa. ¿Deseas proceder con la reclamación?"
                )

        else:
            await update.message.reply_text(
                "Hubo un problema procesando tu fórmula. Por favor, inténtalo de nuevo."
            )

    except Exception as e:
        logger.error(f"Error procesando imagen: {e}")
        await update.message.reply_text(
            "Ocurrió un error procesando tu imagen. Por favor, inténtalo de nuevo."
        )

    finally:
        if 'temp_image_path' in locals() and temp_image_path.exists():
            temp_image_path.unlink()


def setup_handlers(application: Application) -> None:
    """Configura los manejadores de mensajes."""
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.CONTACT, process_contact))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.PHOTO, process_photo))
    logger.info("Manejadores configurados.")


async def check_expired_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Verifica y expira sesiones antiguas."""
    logger.info("Buscando sesiones expiradas...")
    expired_count = 0

    for session_id in list(active_sessions.keys()):
        try:
            if consent_manager and consent_manager.session_manager:
                expired = consent_manager.session_manager.check_and_expire_session(session_id, SESSION_EXPIRATION_SECONDS)
                if expired:
                    active_sessions.pop(session_id, None)
                    expired_count += 1
        except Exception as e:
            logger.error(f"Error verificando la expiración de la sesión {session_id}: {e}")

    if expired_count > 0:
        logger.info(f"{expired_count} sesiones expiradas.")


def setup_job_queue(application: Application) -> None:
    """Configura los trabajos programados."""
    job_queue = application.job_queue
    job_queue.run_repeating(check_expired_sessions, interval=60, first=10)
    logger.info("Cola de trabajos configurada para la verificación de expiración de sesiones.")


def main() -> None:
    """Función principal."""
    logger.info("Iniciando Bot de Telegram con prompts dinámicos...")

    if not consent_manager:
        logger.critical("Componentes críticos no inicializados. Abortando.")
        sys.exit(1)

    application = ApplicationBuilder().token(TELEGRAM_API_TOKEN).build()
    setup_job_queue(application)
    setup_handlers(application)

    logger.info("Bot iniciado y escuchando mensajes.")
    application.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()