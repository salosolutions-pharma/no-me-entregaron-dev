import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

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
from telegram.constants import ChatAction
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from processor_image_prescription.pip_processor import PIPProcessor
#from io import BytesIO
import tempfile
from pathlib import Path
# Ajustar path para imports locales si es necesario
def _setup_project_path() -> None:
    path_parts = os.path.abspath(__file__).split(os.sep)
    try:
        repo_root_index = path_parts.index("no-me-entregaron-dev")
        project_root = os.sep.join(path_parts[: repo_root_index + 1])
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
    except ValueError:
        fallback = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
        if fallback not in sys.path:
            sys.path.insert(0, fallback)

_setup_project_path()

from BYC.consentimiento import ConsentManager
from processor_image_prescription.pip_processor import PIPProcessor
from claim_generator.claim_manager import ClaimManager
from manual_instrucciones.prompt_manager import prompt_manager  # Instancia global

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
except Exception as e:
    logger.critical(f"Error inicializando componentes: {e}")
    sys.exit(1)


active_sessions: Dict[str, datetime] = {}


def create_phone_keyboard() -> ReplyKeyboardMarkup:
    text = "Por favor, comparte tu número de teléfono"
    keyboard = [[KeyboardButton(text, request_contact=True)]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)


def create_consent_keyboard() -> InlineKeyboardMarkup:
    yes = "✅ Sí, autorizo"
    no = "❌ No autorizo"
    buttons = [
        [InlineKeyboardButton(yes, callback_data="consent_yes")],
        [InlineKeyboardButton(no, callback_data="consent_no")],
    ]
    return InlineKeyboardMarkup(buttons)


def create_pip_confirmation_keyboard() -> InlineKeyboardMarkup:
    yes = "✅ Sí, es correcto"
    modify = "✏️ Necesito modificar algo"
    buttons = [
        [InlineKeyboardButton(yes, callback_data="pip_confirm_ok")],
        [InlineKeyboardButton(modify, callback_data="pip_confirm_modify")],
    ]
    return InlineKeyboardMarkup(buttons)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra mensaje de bienvenida y solicita el número de contacto."""
    chat_id = update.effective_chat.id

    # Mostrar mensaje de bienvenida
    welcome_text = consent_manager.get_welcome_message()
    await context.bot.send_message(chat_id=chat_id, text=welcome_text)

    # Pedir número de contacto con botón de Telegram
    contact_keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton(text="📱 Enviar mi número", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

    await context.bot.send_message(
        chat_id=chat_id,
        text="Por favor, comparte tu número de contacto para continuar con el proceso de reclamación.",
        reply_markup=contact_keyboard
    )

async def process_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa el contacto y crea la sesión."""
    contact = update.message.contact if update.message else None
    phone = contact.phone_number if contact else None

    if not phone:
        await update.message.reply_text("No pude obtener tu número. Por favor, intenta nuevamente.")
        return

    try:
        session_info = consent_manager.session_manager.create_session_with_history_check(phone, channel="TL")
        context.user_data["session_id"] = session_info["new_session_id"]
        context.user_data["phone"] = phone
        active_sessions[session_info["new_session_id"]] = datetime.now()

        await update.message.reply_text("¡Perfecto! Gracias por compartir tu número. 📱", reply_markup=ReplyKeyboardRemove())

        consent_prompt = "📝 Para procesar tu fórmula médica, necesito tu autorización. ¿Deseas continuar?"
        await update.message.reply_text(consent_prompt, reply_markup=create_consent_keyboard())

    except Exception as e:
        logger.error(f"❌ Error creando sesión para contacto: {e}")
        await update.message.reply_text("Ocurrió un problema al crear tu sesión. Por favor, intenta de nuevo.")

pip_processor = PIPProcessor()  

async def process_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa la imagen de una prescripción enviada por el usuario."""
    user = update.effective_user
    chat_id = update.effective_chat.id
    session_id = context.user_data.get("session_id")
    phone = context.user_data.get("phone")

    if not session_id or not phone:
        await update.message.reply_text("Primero necesito que me compartas tu número de contacto para continuar 📱.")
        return

    if not update.message.photo:
        await update.message.reply_text("No recibí una imagen. Por favor, envía una foto de la prescripción 📸.")
        return

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    photo_file = await update.message.photo[-1].get_file()
    photo_bytes = await photo_file.download_as_bytearray()

    # Escribir imagen en un archivo temporal
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_img_file:
            temp_img_file.write(photo_bytes)
            temp_img_path = Path(temp_img_file.name)

        resultado = pip_processor.process_image(temp_img_path, session_id=session_id)

        if isinstance(resultado, dict) and resultado.get("success") is not False:
            await update.message.reply_text("✅ Prescripción procesada exitosamente.")
        else:
            await update.message.reply_text(f"⚠️ {resultado if isinstance(resultado, str) else 'No se pudo procesar la imagen.'}")

    except Exception as e:
        logger.exception("Error procesando la prescripción.")
        await update.message.reply_text("❌ Ocurrió un error al procesar la imagen. Intenta de nuevo.")
    finally:
        # Borrar imagen temporal
        if 'temp_img_path' in locals() and temp_img_path.exists():
            temp_img_path.unlink()        



async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    session_id = context.user_data.get("session_id")

    if not session_id:
        prompt = "Tu sesión ha expirado. Por favor inicia una nueva conversación."
        await query.edit_message_text(prompt)
        return

    if data == "consent_yes":
        await handle_consent_granted(query, context, session_id)
    elif data == "consent_no":
        await handle_consent_denied(query, context, session_id)
    elif data == "pip_confirm_ok":
        await handle_pip_confirmation(query, context, True)
    elif data == "pip_confirm_modify":
        await handle_pip_confirmation(query, context, False)
    else:
        prompt = prompt_manager.get_prompt_by_keyword("Telegram", "Opción no reconocida") or "Opción no reconocida."
        await query.edit_message_text(prompt)


async def handle_consent_granted(query, context, session_id: str) -> None:
    user_id = query.from_user.id
    phone = context.user_data.get("phone")
    if phone and consent_manager:
        success = consent_manager.handle_consent_response(user_id, phone, "autorizado")

        if success:
            prompt = consent_manager.get_consent_granted_message()
            await query.edit_message_text(prompt)
        else:
            prompt = "Hubo un problema guardando tu consentimiento."
            await query.edit_message_text(prompt)


async def handle_consent_denied(query, context, session_id: str) -> None:
    user_id = query.from_user.id
    phone = context.user_data.get("phone")
    if phone and consent_manager:
        consent_manager.handle_consent_response(user_id, phone, "no autorizado")
    prompt = consent_manager.get_consent_denied_message()
    await query.edit_message_text(prompt)
    if consent_manager:
        consent_manager.session_manager.close_session(session_id, reason="no_consent")
    active_sessions.pop(session_id, None)
    context.user_data.clear()



async def handle_pip_confirmation(query, context, confirmed: bool) -> None:
    if confirmed:
        prompt = "¡Gracias por usar nuestro servicio! 😊"
        await query.edit_message_text(prompt)
    else:
        prompt = "Por favor, envíame una foto clara y legible de tu fórmula médica"
        await query.edit_message_text(prompt)
        


def setup_handlers(application: Application) -> None:
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.CONTACT & ~filters.COMMAND, process_contact))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, process_photo))
    


async def check_expired_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Verificando sesiones expiradas...")
    for session_id in list(active_sessions.keys()):
        try:
            if consent_manager and consent_manager.session_manager:
                expired = consent_manager.session_manager.check_and_expire_session(session_id, SESSION_EXPIRATION_SECONDS)
                if expired:
                    active_sessions.pop(session_id, None)
        except Exception as e:
            logger.error(f"Error verificando expiración sesión {session_id}: {e}")


def setup_job_queue(application: Application) -> None:
    job_queue = application.job_queue
    job_queue.run_repeating(check_expired_sessions, interval=60, first=10)
    logger.info("Job queue configurado para verificación de expiración de sesiones.")


def main() -> None:
    logger.info("Iniciando Bot Telegram...")
    if not (consent_manager and pip_processor_instance and claim_manager and prompt_manager):
        logger.critical("Componentes críticos no inicializados. Abortando.")
        sys.exit(1)

    application = ApplicationBuilder().token(TELEGRAM_API_TOKEN).build()
    setup_job_queue(application)
    setup_handlers(application)
    logger.info("Bot iniciado y escuchando mensajes.")
    application.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()
