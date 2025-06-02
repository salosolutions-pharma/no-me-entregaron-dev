# channels/telegram_c.py

import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

# Importar el ConsentimientoManager
from BYC.consentimiento import ConsentimientoManager

# Cargar variables de entorno
load_dotenv()

# Configuración básica de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Obtener el token de Telegram desde las variables de entorno
TELEGRAM_API_TOKEN = os.getenv("TELEGRAM_API_TOKEN")

if not TELEGRAM_API_TOKEN:
    logger.error("El token de TELEGRAM_API_TOKEN no está configurado en el archivo .env")
    exit(1)

# Inicializar el ConsentimientoManager
try:
    consent_manager = ConsentimientoManager()
except Exception as e:
    logger.critical(f"No se pudo inicializar ConsentimientoManager: {e}. El bot no puede iniciar.")
    exit(1)

# --- Funciones de manejo de comandos y mensajes ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía un mensaje de bienvenida con botones de consentimiento e inicia la sesión."""
    user = update.effective_user
    logger.info(f"Usuario {user.full_name} ({user.id}) inició la conversación.")

    # Usar el user.id de Telegram como el identificador único para la sesión.
    # En tu formato de session_id, este será el "TELEFONO" o "IDENTIFICADOR".
    telegram_user_identifier = str(user.id) 

    try:
        # Aunque handle_consent_response también llama a create_session_with_history_check,
        # lo llamamos aquí para asegurarnos de que la sesión esté iniciada y el session_id
        # esté disponible en context.user_data lo antes posible.
        session_info = consent_manager.session_manager.create_session_with_history_check(
            user_identifier=telegram_user_identifier,
            channel="TL" # 'TL' para Telegram
        )
        session_id = session_info["new_session_id"]
        # Almacenar el session_id y el user_identifier en el contexto para uso posterior
        context.user_data['session_id'] = session_id 
        context.user_data['telegram_user_identifier'] = telegram_user_identifier
        logger.info(f"Sesión iniciada con ID: {session_id} para usuario {user.full_name}.")
    except Exception as e:
        logger.error(f"Error al iniciar sesión para {user.full_name}: {e}")
        await update.message.reply_text("Lo siento, hubo un problema al iniciar tu sesión. Por favor, inténtalo de nuevo más tarde.")
        return

    # Mensaje de bienvenida y consentimiento
    welcome_message = (
        f"¡Hola {user.first_name}! 👋 Soy tu asistente virtual de 'No Me Entregaron'.\n\n"
        "Estoy aquí para ayudarte si tu EPS no te ha entregado los medicamentos prescritos. "
        "Para comenzar, necesito tu consentimiento para el tratamiento de tus datos personales.\n\n"
        "¿Autorizas el tratamiento de tus datos personales?"
    )

    # Botones de consentimiento
    keyboard = [
        [InlineKeyboardButton("Sí, autorizo", callback_data="consent_yes")],
        [InlineKeyboardButton("No autorizo", callback_data="consent_no")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(welcome_message, reply_markup=reply_markup)

async def handle_consent_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja la respuesta del usuario a la pregunta de consentimiento y la registra en BigQuery."""
    query = update.callback_query
    await query.answer()

    user_response = query.data
    user_id = query.from_user.id
    user_full_name = query.from_user.full_name
    
    # Recuperar el user_identifier del contexto
    telegram_user_identifier = context.user_data.get('telegram_user_identifier', str(user_id))
    
    consent_status_str = "no autorizado"
    response_text = ""

    if user_response == "consent_yes":
        consent_status_str = "autorizado"
        response_text = "¡Gracias por autorizar! Ahora podemos continuar con tu solicitud."
        logger.info(f"Usuario {user_full_name} ({user_id}) autorizó el tratamiento de datos.")
    elif user_response == "consent_no":
        consent_status_str = "no autorizado"
        response_text = "Entendido. Sin tu autorización, no podemos proceder con la gestión de tu caso. Si cambias de opinión, puedes volver a iniciar la conversación."
        logger.info(f"Usuario {user_full_name} ({user_id}) NO autorizó el tratamiento de datos.")

    # Registrar el consentimiento usando el ConsentimientoManager
    success = consent_manager.handle_consent_response(
        user_telegram_id=user_id,
        user_identifier_for_session=telegram_user_identifier,
        consent_status=consent_status_str
    )

    if success:
        logger.info(f"Consentimiento guardado en BigQuery para usuario {user_id}.")
    else:
        logger.error(f"Fallo al guardar el consentimiento para usuario {user_id} en BigQuery.")
        response_text += "\n\nHubo un problema al registrar tu consentimiento. Por favor, inténtalo de nuevo más tarde."

    await query.edit_message_text(text=response_text)

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Eco para mensajes que no son comandos ni callbacks."""
    logger.info(f"Mensaje recibido de {update.effective_user.full_name}: {update.message.text}")
    await update.message.reply_text("Lo siento, no entendí tu mensaje. Por favor, usa los botones de consentimiento o inicia la conversación con /start.")

# --- Función principal para iniciar el bot ---

def main() -> None:
    """Inicia el bot."""
    application = Application.builder().token(TELEGRAM_API_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_consent_response))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    logger.info("Bot de Telegram iniciado. Presiona Ctrl+C para detenerlo.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()