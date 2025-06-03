"""
Bot de Telegram para el sistema No Me Entregaron.

Este módulo implementa un bot de Telegram que gestiona conversaciones
de usuarios, solicita consentimientos para tratamiento de datos personales,
y maneja sesiones con expiración automática que se migran a BigQuery.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    KeyboardButton, 
    ReplyKeyboardMarkup, 
    ReplyKeyboardRemove, 
    Update
)
from telegram.ext import (
    Application, 
    CallbackQueryHandler, 
    ContextTypes, 
    MessageHandler, 
    filters
)

# Configuración de path del proyecto
def setup_project_path() -> None:
    """Configura el path del proyecto para importar módulos locales."""
    path_parts = os.path.abspath(__file__).split(os.sep)
    try:
        repo_root_index = path_parts.index("no-me-entregaron-dev")
        project_root = os.sep.join(path_parts[:repo_root_index + 1])
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
    except ValueError:
        project_root_fallback = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
        )
        if project_root_fallback not in sys.path:
            sys.path.insert(0, project_root_fallback)


# Configurar path antes de importar módulos locales
setup_project_path()

from BYC.consentimiento import ConsentimientoManager

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# Constantes de configuración
TELEGRAM_API_TOKEN = os.getenv("TELEGRAM_API_TOKEN")
EXPIRATION_SECONDS = 5  # Segundos de inactividad antes de expirar sesión
CHECK_INTERVAL_SECONDS = 3  # Intervalo de verificación de sesiones expiradas
INITIAL_CHECK_DELAY = 5  # Delay antes de la primera verificación

# Validación de configuración
if not TELEGRAM_API_TOKEN:
    logger.error(
        "El token de TELEGRAM_API_TOKEN no está configurado en el archivo .env"
    )
    sys.exit(1)

# Inicialización del gestor de consentimientos
consent_manager: Optional[ConsentimientoManager] = None

try:
    consent_manager = ConsentimientoManager()
except Exception as e:
    logger.critical(
        f"No se pudo inicializar ConsentimientoManager: {e}. "
        "El bot no puede iniciar."
    )
    sys.exit(1)

# Diccionario global para trackear sesiones activas
active_sessions = {}


def track_session_activity(session_id: str) -> None:
    """
    Registra actividad de una sesión en el tracker local.
    
    Args:
        session_id: ID único de la sesión a trackear.
    """
    active_sessions[session_id] = datetime.now()
    logger.debug(f"📱 Actividad registrada para sesión: {session_id}")


async def check_expired_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Job que se ejecuta periódicamente para verificar sesiones expiradas.
    
    Revisa todas las sesiones activas y marca como expiradas aquellas
    que han estado inactivas por más tiempo del configurado.
    
    Args:
        context: Contexto de Telegram para el job.
    """
    logger.info("🔍 Verificando sesiones expiradas...")
    
    # Crear copia de claves para evitar modificación durante iteración
    sessions_to_check = list(active_sessions.keys())
    
    for session_id in sessions_to_check:
        try:
            was_expired = consent_manager.session_manager.check_and_expire_session(
                session_id, 
                expiration_seconds=EXPIRATION_SECONDS
            )
            
            if was_expired:
                logger.info(
                    f"⏰ Sesión {session_id} expirada automáticamente - "
                    "debería migrar a BigQuery"
                )
                # Remover de sesiones activas locales
                active_sessions.pop(session_id, None)
                    
        except Exception as e:
            logger.error(
                f"Error verificando expiración de sesión {session_id}: {e}"
            )


def create_phone_request_keyboard() -> ReplyKeyboardMarkup:
    """
    Crea el teclado para solicitar número de teléfono.
    
    Returns:
        Teclado con botón para compartir contacto.
    """
    keyboard = [[
        KeyboardButton("📱 Compartir mi número de teléfono", request_contact=True)
    ]]
    return ReplyKeyboardMarkup(
        keyboard, 
        one_time_keyboard=True, 
        resize_keyboard=True
    )


def create_consent_keyboard() -> InlineKeyboardMarkup:
    """
    Crea el teclado para solicitar consentimiento.
    
    Returns:
        Teclado inline con opciones de consentimiento.
    """
    consent_keyboard = [
        [InlineKeyboardButton("Sí, autorizo", callback_data="consent_yes")],
        [InlineKeyboardButton("No autorizo", callback_data="consent_no")],
    ]
    return InlineKeyboardMarkup(consent_keyboard)


def get_phone_request_message() -> str:
    """
    Obtiene el mensaje de solicitud de número de teléfono.
    
    Returns:
        Mensaje formateado para Markdown V2.
    """
    return (
        "¡Hola\\! Soy tu asistente virtual de \\*\\*No me entregaron\\*\\*\\.\n\n"
        "Para poder ayudarte y gestionar tu caso, necesito tu número de teléfono\\.\n\n"
        "Por favor, presiona el botón de abajo para compartirlo\\."
    )


def get_consent_request_message() -> str:
    """
    Obtiene el mensaje de solicitud de consentimiento.
    
    Returns:
        Mensaje formateado para Markdown V2.
    """
    return (
        "¡Perfecto\\! Ahora, para que \\*\\*No me entregaron\\*\\* pueda ayudarte\\, "
        "necesito tu consentimiento para el tratamiento de tus datos personales\\.\n\n"
        "¿Autorizas el tratamiento de tus datos personales\\?"
    )


async def handle_initial_message(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Maneja el mensaje inicial del usuario y gestiona el flujo de conversación.
    
    Gestiona la solicitud de número de teléfono, creación de sesión,
    y solicitud de consentimiento.
    
    Args:
        update: Objeto Update de Telegram con información del mensaje.
        context: Contexto de la conversación.
    """
    user_id = update.effective_user.id
    
    # Verificar si ya se dio consentimiento
    if context.user_data.get("consent_given"):
        if "session_id" in context.user_data:
            track_session_activity(context.user_data["session_id"])
        await update.message.reply_text(
            "Ya hemos iniciado y registrado tu consentimiento. "
            "¿En qué más puedo ayudarte?"
        )
        return

    # Verificar si está esperando número de teléfono
    if (context.user_data.get("waiting_for_phone") and 
        not update.message.contact):
        await update.message.reply_text(
            "Por favor, comparte tu número de teléfono usando el botón."
        )
        return

    # Solicitar número de teléfono si no se ha obtenido
    if "phone_number_obtained" not in context.user_data:
        logger.info(
            f"Usuario {user_id} envió el primer mensaje: "
            f"'{update.message.text}'. Solicitando número de teléfono."
        )
        
        await update.message.reply_markdown_v2(
            get_phone_request_message(),
            reply_markup=create_phone_request_keyboard()
        )
        context.user_data["waiting_for_phone"] = True
        return
    
    phone_number_for_session = context.user_data["phone_number_obtained"]

    # Crear sesión si no existe
    if "session_id" not in context.user_data:
        try:
            session_info = (
                consent_manager.session_manager.create_session_with_history_check(
                    user_identifier=phone_number_for_session,
                    channel="TL"
                )
            )
            session_id = session_info["new_session_id"]
            context.user_data["session_id"] = session_id
            context.user_data["phone_number_for_session_id"] = phone_number_for_session
            
            # Agregar al tracker de sesiones activas
            track_session_activity(session_id)
            
            logger.info(
                f"Sesión Firestore iniciada con ID: {session_id} "
                f"para teléfono {phone_number_for_session}."
            )
            
            await update.message.reply_text(
                "Gracias por tu número. Ahora podemos continuar.", 
                reply_markup=ReplyKeyboardRemove()
            )

        except Exception as e:
            logger.error(
                f"Error al iniciar sesión Firestore para "
                f"{phone_number_for_session}: {e}"
            )
            await update.message.reply_text(
                "Lo siento, hubo un problema al iniciar tu sesión. "
                "Por favor, inténtalo de nuevo más tarde."
            )
            return
    else:
        # Trackear actividad en cada mensaje
        track_session_activity(context.user_data["session_id"])

    # Solicitar consentimiento
    await update.message.reply_markdown_v2(
        get_consent_request_message(), 
        reply_markup=create_consent_keyboard()
    )
    context.user_data["waiting_for_phone"] = False


async def process_contact(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Procesa el contacto compartido por el usuario.
    
    Extrae el número de teléfono del contacto y continúa el flujo.
    
    Args:
        update: Objeto Update de Telegram con información del contacto.
        context: Contexto de la conversación.
    """
    if update.message.contact and update.message.contact.phone_number:
        phone_number = update.message.contact.phone_number
        user_id = update.effective_user.id
        logger.info(f"Número de teléfono recibido de {user_id}: {phone_number}")

        context.user_data["phone_number_obtained"] = phone_number
        context.user_data["waiting_for_phone"] = False

        await handle_initial_message(update, context)
    else:
        logger.warning(
            f"Mensaje de contacto inválido o inesperado de usuario "
            f"{update.effective_user.id}."
        )
        await update.message.reply_text(
            "Parece que no me compartiste un número de teléfono válido. "
            "Por favor, inténtalo de nuevo usando el botón."
        )


async def handle_callback_query(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Maneja las respuestas de consentimiento del usuario.
    
    Procesa la decisión del usuario sobre el consentimiento y
    la registra en el sistema.
    
    Args:
        update: Objeto Update de Telegram con información del callback.
        context: Contexto de la conversación.
    """
    query = update.callback_query
    await query.answer()

    user_response = query.data
    user_id = query.from_user.id
    
    # Trackear actividad en callback
    if "session_id" in context.user_data:
        track_session_activity(context.user_data["session_id"])
    
    phone_number_for_session_id = context.user_data.get(
        "phone_number_for_session_id"
    )
    if not phone_number_for_session_id:
        logger.error(
            f"phone_number_for_session_id no encontrado en context.user_data "
            f"para usuario {user_id}. No se puede registrar consentimiento."
        )
        await query.edit_message_text(
            text="Lo siento, no pude registrar tu consentimiento. "
                 "Por favor, inicia la conversación de nuevo."
        )
        return

    # Procesar respuesta de consentimiento
    consent_status_str = "no autorizado"
    response_text = ""

    if user_response == "consent_yes":
        consent_status_str = "autorizado"
        response_text = "¡Gracias por autorizar! Ahora podemos continuar con tu solicitud."
        logger.info(f"Usuario {user_id} autorizó el tratamiento de datos.")
        context.user_data["consent_given"] = True
        
    elif user_response == "consent_no":
        consent_status_str = "no autorizado"
        response_text = (
            "Entendido. Sin tu autorización, no podemos proceder con la "
            "gestión de tu caso. Si cambias de opinión, puedes volver a "
            "iniciar la conversación."
        )
        logger.info(f"Usuario {user_id} NO autorizó el tratamiento de datos.")
        context.user_data["consent_given"] = False

    # Guardar consentimiento
    success = consent_manager.handle_consent_response(
        user_telegram_id=user_id,
        user_identifier_for_session=phone_number_for_session_id,
        consent_status=consent_status_str
    )

    if success:
        logger.info(f"Consentimiento guardado en Firestore para usuario {user_id}.")
    else:
        logger.error(
            f"Fallo al guardar el consentimiento para usuario {user_id} "
            "en Firestore."
        )
        response_text += (
            "\n\nHubo un problema al registrar tu consentimiento. "
            "Por favor, inténtalo de nuevo más tarde."
        )

    await query.edit_message_text(text=response_text)


def setup_handlers(application: Application) -> None:
    """
    Configura los handlers del bot.
    
    Args:
        application: Instancia de la aplicación de Telegram.
    """
    application.add_handler(
        MessageHandler(filters.CONTACT & ~filters.COMMAND, process_contact)
    )
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_initial_message)
    )


def setup_job_queue(application: Application) -> None:
    """
    Configura el job queue para verificación automática de expiración.
    
    Args:
        application: Instancia de la aplicación de Telegram.
    """
    job_queue = application.job_queue
    job_queue.run_repeating(
        check_expired_sessions, 
        interval=CHECK_INTERVAL_SECONDS,
        first=INITIAL_CHECK_DELAY
    )
    logger.info(
        f"⏰ Job de verificación de expiración configurado "
        f"(cada {CHECK_INTERVAL_SECONDS} segundos)"
    )


def main() -> None:
    """
    Función principal que inicializa y ejecuta el bot.
    
    Configura la aplicación, handlers, job queue y inicia el polling.
    """
    # Crear aplicación
    application = Application.builder().token(TELEGRAM_API_TOKEN).build()

    # Configurar componentes
    setup_job_queue(application)
    setup_handlers(application)
    
    # Mensajes de inicio
    logger.info("Bot de Telegram iniciado. Presiona Ctrl+C para detenerlo.")
    logger.info(
        f"🔥 Verificación automática de expiración activa "
        f"({EXPIRATION_SECONDS} segundos de inactividad)"
    )
    
    # Iniciar polling
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()