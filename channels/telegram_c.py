import logging
import os
import sys
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

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
)

# Configuración de path del proyecto para importaciones locales
def _setup_project_path() -> None:
    """Configura el path del proyecto para importaciones locales."""
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

_setup_project_path()

# Importaciones de módulos del proyecto
from BYC.consentimiento import ConsentimientoManager
from processor_image_prescription.pip_processor import PIPProcessor
from processor_image_prescription.bigquery_pip import insert_or_update_patient_data 

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# Constantes de configuración
TELEGRAM_API_TOKEN: str = os.getenv("TELEGRAM_API_TOKEN", "")
SESSION_EXPIRATION_SECONDS: int = int(os.getenv("SESSION_EXPIRATION_SECONDS", 100))
CHECK_INTERVAL_SECONDS: int = 3
INITIAL_CHECK_DELAY: int = 5

if not TELEGRAM_API_TOKEN:
    logger.critical("TELEGRAM_API_TOKEN no configurado. El bot no puede iniciar.")
    sys.exit(1)

# Inicialización del gestor de consentimientos
try:
    consent_manager = ConsentimientoManager()
except Exception as e:
    logger.critical(f"Error al inicializar ConsentimientoManager: {e}. El bot no puede iniciar.")
    sys.exit(1)

# Diccionario global para trackear sesiones activas (para el job de expiración)
active_sessions: Dict[str, datetime] = {}

# Estados de Conversación del Bot
class BotState:
    """Define los estados posibles de la conversación del bot."""
    INITIAL = "INITIAL"
    WAITING_PHONE = "WAITING_PHONE"
    WAITING_CONSENT = "WAITING_CONSENT"
    WAITING_PRESCRIPTION_PHOTO = "WAITING_PRESCRIPTION_PHOTO"
    PROCESSING_IMAGE = "PROCESSING_IMAGE"
    ASKING_UNDELIVERED_MEDS = "ASKING_UNDELIVERED_MEDS"
    IDLE = "IDLE"
    CLOSING_SESSION = "CLOSING_SESSION"

def get_current_state(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Obtiene el estado actual de la conversación del usuario."""
    session_id = context.user_data.get("session_id")
    if session_id and session_id not in active_sessions:
        logger.warning(f"Sesión {session_id} en user_data pero no en active_sessions. Reiniciando contexto.")
        context.user_data.clear()
        return BotState.INITIAL
    return context.user_data.get("state", BotState.INITIAL)

def set_state(context: ContextTypes.DEFAULT_TYPE, state: str) -> None:
    """Establece el estado de la conversación del usuario y lo registra."""
    session_id = context.user_data.get("session_id")
    context.user_data["state"] = state
    logger.info(f"Estado de sesión para {session_id if session_id else 'N/A'}: {state}")
    
    if consent_manager and session_id and session_id in active_sessions:
        try:
            consent_manager.session_manager.add_message_to_session(
                session_id,
                f"BotState changed to: {state}",
                sender="system",
                message_type="bot_state_change"
            )
        except Exception as e:
            logger.error(f"Error al registrar cambio de estado en Firestore para {session_id}: {e}")
            active_sessions.pop(session_id, None)
            context.user_data.clear()
            set_state(context, BotState.INITIAL)

async def track_session_activity(session_id: str) -> None:
    """Registra la actividad reciente de una sesión."""
    if session_id in active_sessions:
        active_sessions[session_id] = datetime.now()
        logger.debug(f"Actividad registrada para sesión: {session_id}")
    else:
        logger.debug(f"Intento de trackear actividad en sesión inactiva: {session_id}. No se registra.")

async def check_expired_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job periódico para verificar y expirar sesiones inactivas."""
    logger.info("Verificando sesiones expiradas...")
    sessions_to_check = list(active_sessions.keys())
    for session_id in sessions_to_check:
        try:
            if consent_manager and consent_manager.session_manager:
                was_expired = consent_manager.session_manager.check_and_expire_session(
                    session_id,
                    expiration_seconds=SESSION_EXPIRATION_SECONDS
                )
                if was_expired:
                    logger.info(f"Sesión {session_id} expirada automáticamente.")
                    active_sessions.pop(session_id, None)
            else:
                logger.warning("ConsentimientoManager o SessionManager no inicializado para check_expired_sessions.")
        except Exception as e:
            logger.error(f"Error verificando expiración de sesión {session_id}: {e}")

# Utilidades para Teclados y Mensajes
def clean_message_text(text: str) -> str:
    """Elimina caracteres Markdown y de escape de un texto."""
    text = text.replace("\\", "")
    text = re.sub(r'(\*\*|__|!!|--|`|~|\[|\]|\(|\)|\>|#|\+|=|\{|\}|\.|\!)', '', text)
    return text

def create_phone_request_keyboard() -> ReplyKeyboardMarkup:
    """Crea el teclado para solicitar el número de teléfono."""
    keyboard = [[KeyboardButton("Compartir mi número de teléfono", request_contact=True)]]
    return ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

def create_consent_keyboard() -> InlineKeyboardMarkup:
    """Crea el teclado para solicitar consentimiento."""
    consent_keyboard = [
        [InlineKeyboardButton("Sí, autorizo", callback_data="consent_yes")],
        [InlineKeyboardButton("No autorizo", callback_data="consent_no")],
    ]
    return InlineKeyboardMarkup(consent_keyboard)

def get_phone_request_message() -> str:
    """Mensaje de solicitud de número de teléfono."""
    msg = (
        "Hola Soy tu asistente virtual de No me entregaron.\n\n"
        "Para poder ayudarte y gestionar tu caso, necesito tu número de teléfono.\n\n"
        "Por favor, presiona el botón de abajo para compartirlo."
    )
    return clean_message_text(msg)

def get_consent_request_message() -> str:
    """Mensaje de solicitud de consentimiento."""
    msg = (
        "Perfecto Ahora, para que No me entregaron pueda ayudarte, "
        "necesito tu consentimiento para el tratamiento de tus datos personales.\n\n"
        "Autorizas el tratamiento de tus datos personales?"
    )
    return clean_message_text(msg)

def get_ask_photo_message() -> str:
    """Mensaje para solicitar la foto de la fórmula médica."""
    msg = (
        "Excelente Para ayudarte con tu reclamación, por favor envíame una "
        "foto clara de tu formula medica.\n\n"
        "Asegúrate de que la foto sea legible y muestre todos los detalles importantes."
    )
    return clean_message_text(msg)

def get_meds_summary_message(extracted_data: Dict[str, Any]) -> str:
    """Genera el mensaje de resumen de la fórmula extraída."""
    paciente_name = clean_message_text(extracted_data.get("paciente", "el paciente"))
    diagnostico = clean_message_text(extracted_data.get("diagnostico", "no especificado"))
    
    summary_parts = [
        f"He procesado tu formula medica Aqui esta la informacion que pude extraer:\n\n",
        f"Paciente: {paciente_name}\n",
        f"Diagnostico: {diagnostico}\n\n",
        f"Medicamentos Prescritos:\n"
    ]
    
    medicamentos = extracted_data.get("medicamentos", [])
    if medicamentos:
        for med in medicamentos:
            med_name = clean_message_text(med.get('nombre', 'Desconocido'))
            dosis = clean_message_text(med.get('dosis', 'N/A'))
            cantidad = clean_message_text(med.get('cantidad', 'N/A'))
            summary_parts.append(f"- {med_name} (Dosis: {dosis}, Cantidad: {cantidad})\n")
    else:
        summary_parts.append(clean_message_text("No se identificaron medicamentos en la formula.") + "\n")
        
    summary_parts.append(
        "\nCuales de estos medicamentos NO te han sido entregados?\n"
        "Selecciona uno o mas de la lista a continuacion."
    )
    
    return clean_message_text("".join(summary_parts))

def create_meds_selection_keyboard(extracted_meds: List[Dict[str, Any]], 
                                     selected_meds_names: List[str]) -> InlineKeyboardMarkup:
    """Crea el teclado para seleccionar medicamentos no entregados."""
    keyboard = []
    for i, med in enumerate(extracted_meds):
        med_name = med.get('nombre', f"Medicamento {i+1}")
        if med_name in selected_meds_names:
            button_text = f"☑️ {med_name}"
            callback_data = f"med_unselect_{i}"
        else:
            button_text = f"⬜ {med_name}"
            callback_data = f"med_select_{i}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    if keyboard:
        keyboard.append([InlineKeyboardButton("✅ Confirmar selección", callback_data="confirm_meds_selection")])
        keyboard.append([InlineKeyboardButton("❌ Ninguno de estos / Omitir", callback_data="skip_meds_selection")])

    return InlineKeyboardMarkup(keyboard)

# Manejadores de Eventos de Telegram
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja los mensajes de texto genéricos del usuario."""
    user_id = update.effective_user.id
    session_id = context.user_data.get("session_id")
    current_state = get_current_state(context)

    if session_id:
        await track_session_activity(session_id)
        if update.message and update.message.text:
            try:
                consent_manager.session_manager.add_message_to_session(
                    session_id, update.message.text, sender="user", message_type="conversation"
                )
            except Exception as e:
                logger.error(f"Error al registrar mensaje de usuario en Firestore para {session_id}: {e}")

    if current_state == BotState.INITIAL:
        set_state(context, BotState.WAITING_PHONE)
        logger.info(f"Usuario {user_id} envió el primer mensaje. Solicitando número de teléfono.")
        await update.message.reply_text(
            get_phone_request_message(),
            reply_markup=create_phone_request_keyboard()
        )
    elif current_state == BotState.WAITING_PRESCRIPTION_PHOTO:
        await update.message.reply_text(
            "Todavía estoy esperando la foto de tu formula medica.\n"
            "Por favor, envíamela para poder continuar."
        )
    elif current_state == BotState.ASKING_UNDELIVERED_MEDS:
        await update.message.reply_text(
            "Por favor, usa los botones para seleccionar los medicamentos no entregados o para confirmar tu selección."
        )
    elif current_state in [BotState.IDLE, BotState.CLOSING_SESSION]:
        await update.message.reply_text("Ya te he ayudado con lo anterior. ¿Hay algo más en lo que pueda asistirte?")
        set_state(context, BotState.IDLE)
    else:
        await update.message.reply_text("¿En qué más puedo ayudarte?")

async def process_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa el contacto compartido por el usuario."""
    if not update.message.contact or not update.message.contact.phone_number:
        logger.warning(f"Contacto inválido de usuario {update.effective_user.id}.")
        await update.message.reply_text(
            "Parece que no me compartiste un número de teléfono válido. "
            "Por favor, inténtalo de nuevo usando el botón."
        )
        return

    phone_number = update.message.contact.phone_number
    user_id = update.effective_user.id
    logger.info(f"Número de teléfono recibido de {user_id}: {phone_number}")

    context.user_data["phone_number_obtained"] = phone_number
    set_state(context, BotState.WAITING_CONSENT)

    try:
        session_info = consent_manager.session_manager.create_session_with_history_check(
            user_identifier=phone_number, channel="TL"
        )
        session_id = session_info["new_session_id"]
        context.user_data["session_id"] = session_id
        context.user_data["phone_number_for_session_id"] = phone_number
        context.user_data["selected_undelivered_meds_names"] = []
        context.user_data["extracted_raw_data_pip"] = {}
        context.user_data["image_url_pip"] = None
        
        active_sessions[session_id] = datetime.now()
        await track_session_activity(session_id)

        logger.info(f"Sesión Firestore iniciada con ID: {context.user_data['session_id']}.")
        
        await update.message.reply_text(
            "Gracias por tu número. Ahora podemos continuar.",
            reply_markup=ReplyKeyboardRemove()
        )
        await update.message.reply_text(
            get_consent_request_message(),
            reply_markup=create_consent_keyboard()
        )

    except Exception as e:
        logger.error(f"Error al iniciar sesión Firestore para {phone_number}: {e}")
        context.user_data.clear()
        set_state(context, BotState.INITIAL)
        await update.message.reply_text(
            "Lo siento, hubo un problema al iniciar tu sesión. "
            "Por favor, inténtalo de nuevo más tarde."
        )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja las respuestas de los botones inline (consentimiento, selección de medicamentos)."""
    query = update.callback_query
    await query.answer()

    user_response = query.data
    user_id = query.from_user.id
    session_id = context.user_data.get("session_id")

    if not session_id or session_id not in active_sessions:
        logger.warning(f"Callback para sesión inactiva o no encontrada {session_id}. Forzando reinicio.")
        context.user_data.clear()
        set_state(context, BotState.INITIAL)
        await query.edit_message_text("Tu sesión ha caducado o ha finalizado. Por favor, inicia una nueva conversación.")
        return

    await track_session_activity(session_id)
    phone_number_for_session_id = context.user_data.get("phone_number_for_session_id")
    current_state = get_current_state(context)

    if current_state == BotState.WAITING_CONSENT:
        consent_status_str = "no autorizado"
        response_text = ""

        if user_response == "consent_yes":
            consent_status_str = "autorizado"
            response_text = "¡Gracias por autorizar! Ahora podemos continuar con tu solicitud."
            context.user_data["consent_given"] = True
            
            set_state(context, BotState.WAITING_PRESCRIPTION_PHOTO)
            await query.edit_message_text(text=response_text)
            await context.bot.send_message(chat_id=query.message.chat_id,
                                           text=clean_message_text(get_ask_photo_message()))
        elif user_response == "consent_no":
            response_text = (
                "Entendido. Sin tu autorización, no podemos proceder con la "
                "gestión de tu caso. Si cambias de opinión, puedes volver a "
                "iniciar la conversación."
            )
            context.user_data["consent_given"] = False
            set_state(context, BotState.IDLE)
            consent_manager.session_manager.close_session(session_id, reason="no_consent")
            await query.edit_message_text(text=response_text)

        success = consent_manager.handle_consent_response(
            user_telegram_id=user_id,
            user_identifier_for_session=phone_number_for_session_id,
            consent_status=consent_status_str
        )
        if not success:
            logger.error(f"Fallo al guardar el consentimiento para usuario {user_id}.")
            if user_response == "consent_yes":
                await context.bot.send_message(chat_id=query.message.chat_id,
                                               text="Hubo un problema al registrar tu consentimiento. Por favor, inténtalo de nuevo más tarde.")
    elif current_state == BotState.ASKING_UNDELIVERED_MEDS:
        extracted_meds = context.user_data.get("extracted_meds_from_pip", [])
        selected_meds_names = context.user_data.get("selected_undelivered_meds_names", [])

        if user_response.startswith("med_select_"):
            med_index = int(user_response.split("_")[2])
            if 0 <= med_index < len(extracted_meds):
                med_name = extracted_meds[med_index].get('nombre')
                if med_name and med_name not in selected_meds_names:
                    selected_meds_names.append(med_name)
                    logger.info(f"Medicamento '{med_name}' seleccionado como no entregado.")
                    context.user_data["selected_undelivered_meds_names"] = selected_meds_names
                    await query.edit_message_reply_markup(
                        reply_markup=create_meds_selection_keyboard(extracted_meds, selected_meds_names)
                    )
        elif user_response.startswith("med_unselect_"):
            med_index = int(user_response.split("_")[2])
            if 0 <= med_index < len(extracted_meds):
                med_name = extracted_meds[med_index].get('nombre')
                if med_name and med_name in selected_meds_names:
                    selected_meds_names.remove(med_name)
                    logger.info(f"Medicamento '{med_name}' deseleccionado.")
                    context.user_data["selected_undelivered_meds_names"] = selected_meds_names
                    await query.edit_message_reply_markup(
                        reply_markup=create_meds_selection_keyboard(extracted_meds, selected_meds_names)
                    )
        elif user_response in ["confirm_meds_selection", "skip_meds_selection"]:
            await query.edit_message_text("Gracias por tu selección. Estoy registrando esta información.")
            
            all_extracted_meds = context.user_data.get("extracted_meds_from_pip", [])
            final_selected_meds_names = context.user_data.get("selected_undelivered_meds_names", [])

            meds_with_delivery_status = []
            for med in all_extracted_meds:
                med_copy = med.copy()
                med_copy['entregado'] = "no_entregado" if med_copy.get('nombre') in final_selected_meds_names else "entregado"
                meds_with_delivery_status.append(med_copy)
            
            original_raw_data = context.user_data.get("extracted_raw_data_pip", {})
            original_raw_data["medicamentos"] = meds_with_delivery_status

            patient_record_for_update = PIPProcessor._build_patient_record(
                data=original_raw_data,
                image_url=context.user_data.get("image_url_pip", ""),
                session_id=session_id
            )

            try:
                insert_or_update_patient_data(patient_record_for_update)
                logger.info(f"Estado de entrega de medicamentos actualizado en BigQuery para sesión {session_id}.")
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=clean_message_text("¡Listo! Tu información ha sido registrada. ¿Necesitas algo más o deseas finalizar la conversación?")
                )
                set_state(context, BotState.IDLE)
            except Exception as e:
                logger.error(f"Error al actualizar estado de medicamentos en BigQuery: {e}")
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=clean_message_text("Lo siento, hubo un error al registrar el estado de los medicamentos. Por favor, inténtalo de nuevo más tarde.")
                )
                set_state(context, BotState.IDLE)
    else:
        logger.warning(f"Callback inesperado '{user_response}' en estado '{current_state}' para usuario {user_id}.")
        await query.edit_message_text("Lo siento, no pude entender tu selección. Por favor, intenta de nuevo.")
        set_state(context, BotState.IDLE)

async def process_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa la foto de la fórmula médica enviada por el usuario."""
    session_id = context.user_data.get("session_id")
    current_state = get_current_state(context)

    if not session_id or session_id not in active_sessions:
        logger.warning(f"Foto recibida para sesión inactiva o no encontrada {session_id}. Forzando reinicio.")
        context.user_data.clear()
        set_state(context, BotState.INITIAL)
        await update.message.reply_text("Tu sesión ha caducado o ha finalizado. Por favor, inicia una nueva conversación.")
        return

    await track_session_activity(session_id)

    if current_state != BotState.WAITING_PRESCRIPTION_PHOTO:
        logger.warning(f"Foto recibida en estado inesperado '{current_state}' de usuario {update.effective_user.id}.")
        await update.message.reply_text(clean_message_text("Gracias por la imagen, pero no estoy esperando una foto en este momento. Por favor, sigue el flujo de la conversación."))
        return

    if not update.message.photo:
        await update.message.reply_text(clean_message_text("Por favor, envíame la imagen como una foto, no como un archivo o documento."))
        return

    set_state(context, BotState.PROCESSING_IMAGE)
    await update.message.reply_text(
        clean_message_text("¡Gracias por la foto! Estoy procesando tu fórmula médica para extraer la información. Esto puede tardar un momento...")
    )

    temp_dir = Path("./temp_images")
    temp_dir.mkdir(exist_ok=True)
    image_path = temp_dir / f"prescripcion_{session_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"

    try:
        photo_file = await update.message.photo[-1].get_file()
        await photo_file.download_to_drive(custom_path=image_path)
        logger.info(f"Imagen descargada a: {image_path}")

        pip_processor_instance = PIPProcessor()
        processing_result = pip_processor_instance.process_image(str(image_path), session_id)
        
        os.remove(image_path)
        logger.info(f"Imagen temporal eliminada: {image_path}")

        if isinstance(processing_result, str):
            logger.warning(f"Error al procesar imagen para {session_id}: {processing_result}")
            await update.message.reply_text(
                clean_message_text(f"Lo siento, {processing_result} Por favor, intenta con una foto más clara o diferente.")
            )
            set_state(context, BotState.WAITING_PRESCRIPTION_PHOTO)
        else:
            extracted_data: Dict[str, Any] = processing_result
            context.user_data["extracted_meds_from_pip"] = extracted_data.get("medicamentos", [])
            context.user_data["extracted_raw_data_pip"] = extracted_data
            context.user_data["image_url_pip"] = extracted_data.get("url_prescripcion_subida")

            if not extracted_data.get("medicamentos"):
                await update.message.reply_text(
                    clean_message_text(
                        f"He procesado tu fórmula, pero no pude identificar medicamentos en ella.\n"
                        f"Paciente: {extracted_data.get('paciente', 'No identificado')}\n"
                        f"Diagnóstico: {extracted_data.get('diagnostico', 'No identificado')}\n\n"
                        f"¿Hay algo más en lo que pueda ayudarte, o necesitas ayuda con otro tema?"
                    ) 
                )
                set_state(context, BotState.IDLE)
            else:
                await update.message.reply_text(
                    get_meds_summary_message(extracted_data),
                    reply_markup=create_meds_selection_keyboard(
                        extracted_data["medicamentos"], 
                        context.user_data["selected_undelivered_meds_names"]
                    )
                )
                set_state(context, BotState.ASKING_UNDELIVERED_MEDS)
    except Exception as e:
        logger.exception(f"Error inesperado al procesar la imagen para sesión {session_id}")
        await update.message.reply_text(
            clean_message_text("Lo siento, hubo un error inesperado al procesar tu foto. Por favor, inténtalo de nuevo más tarde.")
        )
        if image_path.exists():
            os.remove(image_path)
        set_state(context, BotState.WAITING_PRESCRIPTION_PHOTO)

async def handle_finish_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja la finalización de la conversación por parte del usuario."""
    session_id = context.user_data.get("session_id")
    if not session_id or not consent_manager:
        await update.message.reply_text("No hay una conversación activa para cerrar.")
        return
    
    set_state(context, BotState.CLOSING_SESSION)
    await update.message.reply_text(
        clean_message_text(
            "De acuerdo, he registrado la información. Si no necesitas nada más, "
            "daré por finalizada nuestra conversación. ¡Gracias por usar nuestros servicios!"
        ),
        reply_markup=ReplyKeyboardRemove()
    )
    if session_id in active_sessions:
        active_sessions.pop(session_id)
    consent_manager.session_manager.close_session(session_id, reason="user_completed")
    logger.info(f"Sesión {session_id} cerrada por solicitud del usuario.")
    
    context.user_data.clear()

def setup_handlers(application: Application) -> None:
    """Configura los manejadores (handlers) del bot."""
    application.add_handler(
        MessageHandler(filters.CONTACT & ~filters.COMMAND, process_contact)
    )
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(
        MessageHandler(filters.PHOTO & ~filters.COMMAND, process_photo)
    )
    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND
            & ~filters.Regex(re.compile(r"^(No necesito nada más|finalizar|listo)$", re.IGNORECASE)),
            handle_message
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(re.compile(r"^(No necesito nada más|finalizar|listo)$", re.IGNORECASE))
            & ~filters.COMMAND,
            handle_finish_conversation
        )
    )

def setup_job_queue(application: Application) -> None:
    """Configura el job queue para la verificación periódica de expiración de sesiones."""
    job_queue = application.job_queue
    job_queue.run_repeating(
        check_expired_sessions,
        interval=CHECK_INTERVAL_SECONDS,
        first=INITIAL_CHECK_DELAY
    )
    logger.info(f"Job de verificación de expiración configurado (cada {CHECK_INTERVAL_SECONDS} segundos).")

def main() -> None:
    """Función principal que inicializa y ejecuta el bot de Telegram."""
    application = Application.builder().token(TELEGRAM_API_TOKEN).build()
    setup_job_queue(application)
    setup_handlers(application)
    
    logger.info("Bot de Telegram iniciado.")
    logger.info(f"Verificación automática de expiración activa ({SESSION_EXPIRATION_SECONDS} segundos de inactividad).")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()