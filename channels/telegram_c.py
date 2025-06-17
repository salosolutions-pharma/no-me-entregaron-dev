import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

try:
    from BYC.consentimiento import ConsentManager
    from processor_image_prescription.pip_processor import PIPProcessor
    from claim_generator.claim_manager import ClaimManager
except ImportError as e:
    print(f"Error al importar módulos: {e}")
    sys.exit(1)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

TELEGRAM_API_TOKEN = os.getenv("TELEGRAM_API_TOKEN", "")


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
    logger.critical(f"Error al inicializar componentes: {e}. Abortando.")
    sys.exit(1)



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


def create_regimen_keyboard() -> InlineKeyboardMarkup:
    """Crea el teclado para la selección de régimen de salud."""
    buttons = [
        [InlineKeyboardButton("✅ Contributivo", callback_data="regimen_contributivo")],
        [InlineKeyboardButton("🤝 Subsidiado", callback_data="regimen_subsidiado")],
    ]
    return InlineKeyboardMarkup(buttons)


def create_informante_keyboard() -> InlineKeyboardMarkup:
    """Crea el teclado para seleccionar si es paciente o cuidador."""
    buttons = [
        [InlineKeyboardButton("👤 Soy el paciente", callback_data="informante_paciente")],
        [InlineKeyboardButton("👥 Soy el cuidador", callback_data="informante_cuidador")],
    ]
    return InlineKeyboardMarkup(buttons)


def create_medications_keyboard(medications: List[Dict], selected_indices: List[int], 
                               session_id: str) -> InlineKeyboardMarkup:
    """Crea o actualiza teclado para seleccionar medicamentos NO entregados."""
    buttons = []

    for i, med in enumerate(medications):
        med_name = med.get("nombre", f"Medicamento {i+1}")
        display_name = med_name[:30] + "..." if len(med_name) > 30 else med_name
        emoji = "🔴" if i in selected_indices else "⚪"
        callback_data = f"med_toggle_{session_id}_{i}"
        buttons.append([InlineKeyboardButton(f"{emoji} {display_name}", callback_data=callback_data)])

    buttons.append([
        InlineKeyboardButton("✅ Confirmar selección", callback_data=f"med_confirm_{session_id}"),
        InlineKeyboardButton("🔄 Alternar todos", callback_data=f"med_all_{session_id}"),
    ])

    return InlineKeyboardMarkup(buttons)


def get_session_context(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    """Recupera el contexto de la sesión actual del user_data."""
    return {
        "phone_shared": context.user_data.get("phone") is not None,
        "phone": context.user_data.get("phone"),
        "consent_given": context.user_data.get("consent_given", False),
        "consent_asked": context.user_data.get("consent_asked", False),
        "prescription_uploaded": context.user_data.get("prescription_uploaded", False),
        "session_id": context.user_data.get("session_id"),
        "waiting_for_field": context.user_data.get("waiting_for_field"),
        "patient_key": context.user_data.get("patient_key"),
        "detected_channel": context.user_data.get("detected_channel", "TL"),
    }


async def send_and_log_message(chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE,
                              message_type: str = "conversation", 
                              reply_markup: Optional[Any] = None) -> None:
    """Envía un mensaje al usuario y lo registra en la sesión."""
    formatted_text = format_telegram_text(text)
    await context.bot.send_message(
        chat_id=chat_id,
        text=formatted_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

    session_id = context.user_data.get("session_id")
    if session_id and consent_manager and consent_manager.session_manager:
        consent_manager.session_manager.add_message_to_session(session_id, text, "bot", message_type)
    logger.info(f"Respuesta enviada a {chat_id}.")


async def log_user_message(session_id: str, message_text: str, 
                          message_type: str = "conversation") -> None:
    """Registra un mensaje del usuario en la sesión."""
    if consent_manager and consent_manager.session_manager:
        consent_manager.session_manager.add_message_to_session(
            session_id, message_text, "user", message_type
        )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja mensajes de texto del usuario."""
    chat_id = update.effective_chat.id
    user_message = update.message.text or ""

    logger.info(f"Mensaje recibido de {chat_id}: '{user_message}'")
    session_context = get_session_context(context)
    session_id = session_context.get("session_id")

    try:
        # ✅ NUEVO: Verificar inactividad solo cuando el usuario escribe
        if session_id and consent_manager:
            session_expired = consent_manager.session_manager.check_session_inactivity(session_id)
            if session_expired:
                # Limpiar sesión expirada y empezar nueva
                context.user_data.clear()
                response = ("¡Hola de nuevo! 👋 Tu sesión anterior expiró por inactividad. "
                          "No te preocupes, podemos comenzar tu solicitud desde el inicio.")
                await send_and_log_message(chat_id, response, context)
                return

        # ✅ VERIFICAR si el usuario se está despidiendo
        if (consent_manager and 
            consent_manager.should_close_session(user_message, session_context) and 
            session_id):
            response = consent_manager.get_bot_response(user_message, session_context)
            await send_and_log_message(chat_id, response, context)
            close_user_session(session_id, context, reason="user_farewell")
            return

        if session_context.get("waiting_for_field"):
            handled = await handle_field_response(update, context)
            if handled:
                return

        if consent_manager:
            response = consent_manager.get_bot_response(user_message, session_context)
            keyboard = None

            if "teléfono" in response.lower() and not session_context.get("phone_shared"):
                keyboard = create_contact_keyboard()
                logger.info("Teclado de contacto añadido.")
            elif "autorización" in response.lower() and not session_context.get("consent_asked"):
                keyboard = create_consent_keyboard()
                context.user_data["consent_asked"] = True
                logger.info("Teclado de consentimiento añadido.")

            await send_and_log_message(chat_id, response, context, reply_markup=keyboard)
            if session_id:
                await log_user_message(session_id, user_message)
        else:
            await send_and_log_message(
                chat_id, "Lo siento, el sistema no está completamente operativo.", context
            )

    except Exception as e:
        logger.error(f"Error en handle_message: {e}")
        await send_and_log_message(
            chat_id, "Disculpa, hubo un error técnico. Por favor intenta nuevamente.", context
        )


def close_user_session(session_id: str, context: ContextTypes.DEFAULT_TYPE, reason: str) -> None:
    """Cierra la sesión del usuario y limpia su user_data."""
    if consent_manager and consent_manager.session_manager:
        consent_manager.session_manager.close_session(session_id, reason=reason)

    context.user_data.clear()
    logger.info(f"Sesión {session_id} cerrada por {reason}.")


async def process_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa el contacto compartido por el usuario."""
    chat_id = update.effective_chat.id
    contact = update.message.contact
    phone = contact.phone_number if contact else None

    logger.info(f"Contacto recibido: {phone}")

    if not phone:
        await send_and_log_message(chat_id, "No pude obtener tu número. Por favor, inténtalo de nuevo.", context)
        return

    try:
        if not consent_manager or not consent_manager.session_manager:
            raise ValueError("ConsentManager o SessionManager no inicializado.")

        new_session_id = consent_manager.session_manager.create_session(phone, channel="TL")
        context.user_data["session_id"] = new_session_id
        context.user_data["phone"] = phone
        context.user_data["phone_shared"] = True
        context.user_data["detected_channel"] = "TL"

        logger.info(f"Sesión creada: {new_session_id}")

        session_context = get_session_context(context)
        response = consent_manager.get_bot_response("He compartido mi número de teléfono", session_context)

        await log_user_message(new_session_id, f"Teléfono compartido: {phone}", "contact_shared")
        await send_and_log_message(chat_id, response, context, reply_markup=create_consent_keyboard())

    except Exception as e:
        logger.error(f"Error al procesar contacto y crear sesión: {e}")
        await send_and_log_message(chat_id, "Ocurrió un problema al crear tu sesión. Por favor, inténtalo de nuevo.", context)


async def handle_regimen_selection(query, context: ContextTypes.DEFAULT_TYPE, regimen_type: str) -> None:
    """Maneja la selección de régimen (Contributivo/Subsidiado)."""
    chat_id = query.message.chat_id
    patient_key = context.user_data.get("patient_key")

    if not claim_manager or not patient_key:
        await safe_edit_message(query, "Error del sistema o clave de paciente no encontrada. Inténtalo de nuevo.")
        await prompt_next_missing_field(chat_id, context, patient_key)
        return

    try:
        success = claim_manager.update_patient_field(patient_key, "regimen", regimen_type)
        if success:
            context.user_data.pop("waiting_for_field", None)
            await prompt_next_missing_field(chat_id, context, patient_key)
        else:
            await safe_edit_message(query, "Hubo un problema guardando tu régimen. Inténtalo de nuevo.")
    except Exception as e:
        logger.error(f"Error manejando selección de régimen: {e}", exc_info=True)
        await safe_edit_message(query, "Ocurrió un error. Inténtalo de nuevo.")


async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja las respuestas de los botones inline."""
    query = update.callback_query
    await query.answer()
    data = query.data
    session_id = context.user_data.get("session_id")

    logger.info(f"Callback recibido: {data} para sesión: {session_id}")

    if not session_id:
        await query.edit_message_text("Tu sesión ha expirado. Por favor, inicia una nueva conversación.")
        return

    if data.startswith("consent_"):
        await handle_consent_response(query, context, session_id, data == "consent_yes")
    elif data.startswith("med_"):
        await handle_medication_selection(query, context, data)
    elif data.startswith("informante_"):
        informante_type = "paciente" if "paciente" in data else "cuidador"
        await handle_informante_selection(query, context, informante_type)
    elif data.startswith("regimen_"):
        regimen_type = "Contributivo" if "contributivo" in data else "Subsidiado"
        await handle_regimen_selection(query, context, regimen_type)
    else:
        await query.edit_message_text("Opción no reconocida.")


async def handle_consent_response(query, context: ContextTypes.DEFAULT_TYPE, 
                                 session_id: str, granted: bool) -> None:
    """Maneja la respuesta de consentimiento (sí/no)."""
    user_id = query.from_user.id
    phone = context.user_data.get("phone")

    if not consent_manager or not phone:
        await query.edit_message_text("Error de sistema al procesar consentimiento. Inténtalo de nuevo.")
        return

    consent_status = "autorizado" if granted else "no autorizado"
    success = consent_manager.handle_consent_response(user_id, phone, consent_status, session_id)

    if success:
        context.user_data["consent_given"] = granted
        log_message = "Consentimiento otorgado" if granted else "Consentimiento denegado"
        await log_user_message(session_id, log_message, "consent_response")

        if granted:
            response_text = ("👩‍⚕️ Por favor, envíame una foto clara y legible de tu *fórmula médica* 📝\n\n"
                           "Es muy importante que la foto se vea bien para poder procesarla correctamente "
                           "y ayudarte con tu reclamación.\n\n⚠️ No podremos continuar si no recibimos una fórmula médica válida.")
        else:
            response_text = ("Entiendo tu decisión. Sin tu autorización no podemos continuar con el proceso. "
                           "Si cambias de opinión, solo escríbeme.")

        await query.edit_message_text(
            text=format_telegram_text(response_text),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await query.edit_message_text("Hubo un problema al guardar tu consentimiento.")


async def process_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa imágenes de recetas médicas."""
    chat_id = update.effective_chat.id
    session_context = get_session_context(context)

    if not session_context.get("consent_given"):
        await send_and_log_message(
            chat_id,
            "Primero necesito tu autorización para procesar tus datos.",
            context,
            reply_markup=create_consent_keyboard(),
        )
        return

    session_id = session_context.get("session_id")
    if not session_id:
        await send_and_log_message(chat_id, "No hay una sesión activa. Por favor, reinicia la conversación.", context)
        return

    if not pip_processor_instance:
        await send_and_log_message(chat_id, "El procesador de imágenes no está disponible.", context)
        return

    processing_msg = await update.message.reply_text(
        "📸 En estos momentos estoy leyendo tu fórmula médica, por favor espera..."
    )
    temp_image_path: Optional[Path] = None

    try:
        photo = update.message.photo[-1]
        photo_file = await photo.get_file()

        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
            temp_image_path = Path(temp_file.name)
        await photo_file.download_to_drive(temp_image_path)

        context.user_data["detected_channel"] = "TL"

        result = pip_processor_instance.process_image(temp_image_path, session_id)
        await processing_msg.delete()

        if isinstance(result, str):
            await send_and_log_message(chat_id, result, context)
            return

        if isinstance(result, dict):
            context.user_data["prescription_uploaded"] = True
            await log_user_message(session_id, "He leido tu formula y he encontrado:", "prescription_processed")
            context.user_data["patient_key"] = result["patient_key"]
            context.user_data["pip_result"] = result

            if result.get("_requires_medication_selection"):
                medications = result.get("medicamentos", [])
                selection_msg = pip_processor_instance.get_medication_selection_message(result)

                await send_and_log_message(chat_id, selection_msg, context)
                await send_and_log_message(
                    chat_id,
                    "👆 Selecciona los medicamentos que **NO** te han entregado:",
                    context,
                    reply_markup=create_medications_keyboard(medications, [], session_id),
                )
                context.user_data["pending_medications"] = medications
                context.user_data["selected_undelivered"] = []
            else:
                await continue_with_missing_fields(update, context, result)
        else:
            await send_and_log_message(chat_id, "Hubo un problema procesando tu fórmula. Por favor envia la foto nuevamente.", context)

    except Exception as e:
        logger.error(f"Error procesando imagen: {e}", exc_info=True)
        await send_and_log_message(chat_id, "Ocurrió un error procesando tu imagen. Por favor envia la foto nuevamente.", context)
    finally:
        if temp_image_path and temp_image_path.exists():
            temp_image_path.unlink()


async def safe_edit_message(query, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Edita un mensaje de forma segura, evitando errores de Telegram si el mensaje ya fue modificado."""
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception as e:
        logger.warning(f"No se pudo editar mensaje de callback: {e}. Intentando enviar uno nuevo.")
        try:
            await query.message.reply_text(text, reply_markup=reply_markup)
        except Exception as e2:
            logger.error(f"Tampoco se pudo enviar nuevo mensaje: {e2}")


async def handle_medication_selection(query, context: ContextTypes.DEFAULT_TYPE, callback_data: str) -> None:
    """Maneja la selección/deselección y confirmación de medicamentos con manejo robusto de errores."""
    logger.info(f"Procesando callback de medicamento: {callback_data}")

    try:
        parts = callback_data.split("_")
        action = parts[1]

        if action == "toggle" and len(parts) >= 4:
            med_index = int(parts[-1])
            session_id = "_".join(parts[2:-1])
        elif action in ["confirm", "all"] and len(parts) >= 3:
            session_id = "_".join(parts[2:])
            med_index = -1
        else:
            logger.error(f"No se pudo parsear el callback: {callback_data}")
            await safe_edit_message(query, "Error en la selección. Inténtalo de nuevo.")
            return

        medications = context.user_data.get("pending_medications", [])
        selected_undelivered = context.user_data.get("selected_undelivered", [])

        logger.info(f"Acción: {action}, Sesión: {session_id}, Índice: {med_index}")
        logger.info(f"Medicamentos disponibles: {len(medications)}, Seleccionados: {selected_undelivered}")

        if action == "toggle" and med_index != -1:
            if med_index < 0 or med_index >= len(medications):
                logger.error(f"Índice de medicamento fuera de rango: {med_index}")
                await query.answer("Error: medicamento no válido")
                return

            if med_index in selected_undelivered:
                selected_undelivered.remove(med_index)
            else:
                selected_undelivered.append(med_index)
            context.user_data["selected_undelivered"] = selected_undelivered
            new_keyboard = create_medications_keyboard(medications, selected_undelivered, session_id)

            await safe_edit_message(query, query.message.text, reply_markup=new_keyboard)

        elif action == "all":
            if len(selected_undelivered) == len(medications):
                context.user_data["selected_undelivered"] = []
            else:
                context.user_data["selected_undelivered"] = list(range(len(medications)))
            new_keyboard = create_medications_keyboard(
                medications, context.user_data["selected_undelivered"], session_id
            )
            await safe_edit_message(query, query.message.text, reply_markup=new_keyboard)

        elif action == "confirm":
            await process_medication_selection_safe(query, context)

    except Exception as e:
        logger.error(f"Error en handle_medication_selection: {e}", exc_info=True)
        await safe_edit_message(query, "Error procesando medicamentos. Continuando...")
        await continue_with_missing_fields_after_meds_safe(query, context)


async def process_medication_selection_safe(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa la selección de medicamentos con manejo seguro de errores."""
    medications = context.user_data.get("pending_medications", [])
    selected_indices = context.user_data.get("selected_undelivered", [])
    patient_key = context.user_data.get("patient_key")
    session_id = context.user_data.get("session_id")

    logger.info(f"Procesando selección final: {len(medications)} medicamentos, {len(selected_indices)} seleccionados")

    if not claim_manager or not patient_key:
        await safe_edit_message(query, "⚠️ Error del sistema. Continuando con el siguiente paso...")
        await continue_with_missing_fields_after_meds_safe(query, context)
        return

    try:
        undelivered_med_names = []
        for i in selected_indices:
            if 0 <= i < len(medications):
                med_name = (medications[i].get("nombre", "") if isinstance(medications[i], dict) 
                          else str(medications[i]))
                if med_name:
                    undelivered_med_names.append(med_name)

        logger.info(f"Medicamentos no entregados a actualizar: {undelivered_med_names}")

        success = False
        try:
            success = claim_manager.update_undelivered_medicines(patient_key, session_id, undelivered_med_names)
        except Exception as med_error:
            logger.error(f"Error actualizando medicamentos vía ClaimManager: {med_error}")
            success = False

        if success:
            if undelivered_med_names:
                med_list = "\n".join([f"🔴 {name}" for name in undelivered_med_names])
                message = f"✅ Medicamentos NO entregados registrados:\n\n{med_list}\n\nContinuemos completando tu información..."
            else:
                message = "✅ **Todos los medicamentos marcados como entregados.**\n\nContinuemos completando tu información..."
        else:
            message = "⚠️ Hubo un problema al registrar los medicamentos. Continuando con tu información..."

        await safe_edit_message(query, message, reply_markup=None)

        context.user_data.pop("pending_medications", None)
        context.user_data.pop("selected_undelivered", None)
        context.user_data.pop("pip_result", None)

        await continue_with_missing_fields_after_meds_safe(query, context)

    except Exception as e:
        logger.error(f"Error procesando selección final de medicamentos: {e}", exc_info=True)
        await safe_edit_message(query, "✅ Medicamentos procesados. Continuando...")
        await continue_with_missing_fields_after_meds_safe(query, context)


async def continue_with_missing_fields(update: Update, context: ContextTypes.DEFAULT_TYPE, result: Dict) -> None:
    """Continúa el flujo para pedir campos faltantes (desde procesamiento de imagen)."""
    chat_id = update.effective_chat.id
    if not claim_manager:
        await send_and_log_message(chat_id, "El gestor de reclamaciones no está disponible.", context)
        return

    patient_key = result["patient_key"]
    channel_type = context.user_data.get("detected_channel", "TL")
    claim_manager.update_patient_field(patient_key, "canal_contacto", channel_type)

    await prompt_next_missing_field(chat_id, context, patient_key)


async def continue_with_missing_fields_after_meds_safe(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Continúa con campos faltantes de forma segura después de la selección de medicamentos."""
    try:
        chat_id = query.message.chat_id
        if not claim_manager:
            await send_and_log_message(chat_id, "Continuando con el proceso...", context)
            return

        patient_key = context.user_data.get("patient_key")
        if not patient_key:
            await send_and_log_message(chat_id, "Error: Datos del paciente no encontrados.", context)
            return

        try:
            channel_type = context.user_data.get("detected_channel", "TL")
            claim_manager.update_patient_field(patient_key, "canal_contacto", channel_type)
        except Exception as channel_error:
            logger.warning(f"No se pudo actualizar canal_contacto: {channel_error}")

        await prompt_next_missing_field(chat_id, context, patient_key)

    except Exception as e:
        logger.error(f"Error en continue_with_missing_fields_after_meds_safe: {e}")
        chat_id = query.message.chat_id
        await send_and_log_message(chat_id, "Ocurrió un error. Por favor, intenta de nuevo.", context)


async def prompt_next_missing_field(chat_id: int, context: ContextTypes.DEFAULT_TYPE, patient_key: str) -> None:
    """Obtiene y solicita el siguiente campo faltante al usuario."""
    field_prompt = claim_manager.get_next_missing_field_prompt(patient_key)

    if field_prompt.get("field_name"):
        # Aún faltan campos - continuar
        field_name = field_prompt["field_name"]
        context.user_data["waiting_for_field"] = field_name
        context.user_data["patient_key"] = patient_key

        if field_name == "informante":
            await send_and_log_message(
                chat_id, "👤 Para continuar, necesito saber:", context, reply_markup=create_informante_keyboard()
            )
        elif field_name == "regimen":
            await send_and_log_message(
                chat_id, "🏥 ¿Cuál es tu régimen de salud?", context, reply_markup=create_regimen_keyboard()
            )
        else:
            await send_and_log_message(chat_id, field_prompt["prompt_text"], context)
    else:
    
        await send_and_log_message(
            chat_id,
            ("🎉 ¡Perfecto! Ya tenemos toda la información necesaria para radicar tu reclamación.\n\n"
             "📋 En las próximas 48 horas te enviaremos el número de radicado.\n\n"
             "✅ Proceso completado exitosamente. Si necesitas algo más, no dudes en contactarnos.\n\n"
             "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"),
            context
        )
        
        # ✅ AGREGAR: Cerrar sesión automáticamente al completar el proceso
        session_id = context.user_data.get("session_id")
        if session_id:
            close_user_session(session_id, context, reason="process_completed")
        
        logger.info(f"Proceso completado para paciente {patient_key} - sesión cerrada automáticamente")


async def handle_informante_selection(query, context: ContextTypes.DEFAULT_TYPE, informante_type: str) -> None:
    """Maneja la selección de 'paciente' o 'cuidador'."""
    chat_id = query.message.chat_id
    patient_key = context.user_data.get("patient_key")

    if not claim_manager or not patient_key:
        await safe_edit_message(query, "Error del sistema o clave de paciente no encontrada. Inténtalo de nuevo.")
        await prompt_next_missing_field(chat_id, context, patient_key)
        return

    try:
        if informante_type == "paciente":
            patient_record = claim_manager._get_patient_data(patient_key)
            patient_name = patient_record.get("nombre_paciente", "Paciente")
            patient_doc = patient_record.get("numero_documento", "")

            informante_data = [
                {"nombre": patient_name, "parentesco": "Mismo paciente", "identificacion": patient_doc}
            ]
            success = claim_manager.update_informante_with_merge(patient_key, informante_data)

            if success:
                context.user_data.pop("waiting_for_field", None)
                await prompt_next_missing_field(chat_id, context, patient_key)
            else:
                await safe_edit_message(query, "Error guardando información. Inténtalo de nuevo.")

        else:
            await safe_edit_message(query, "👥 ¿Cuál es tu nombre completo?")
            context.user_data["waiting_for_field"] = "cuidador_nombre"
            context.user_data["informante_type"] = "cuidador"

    except Exception as e:
        logger.error(f"Error manejando selección de informante: {e}", exc_info=True)
        await safe_edit_message(query, "Ocurrió un error. Inténtalo de nuevo.")


async def handle_field_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Maneja las respuestas de texto a campos faltantes."""
    chat_id = update.effective_chat.id
    current_field = context.user_data.get("waiting_for_field")
    patient_key = context.user_data.get("patient_key")
    user_response = update.message.text.strip() if update.message.text else ""

    if not current_field or not patient_key or not claim_manager:
        return False

    try:
        if current_field == "cuidador_nombre":
            context.user_data["cuidador_nombre"] = user_response
            context.user_data["waiting_for_field"] = "cuidador_cedula"
            await send_and_log_message(chat_id, "📋 ¿Cuál es tu número de cédula?", context)
            return True

        elif current_field == "cuidador_cedula":
            cuidador_nombre = context.user_data.get("cuidador_nombre", "")
            informante_data = [
                {"nombre": cuidador_nombre, "parentesco": "Cuidador", "identificacion": user_response}
            ]
            success = claim_manager.update_informante_with_merge(patient_key, informante_data)

            if success:
                context.user_data.pop("cuidador_nombre", None)
                context.user_data.pop("informante_type", None)
                context.user_data.pop("waiting_for_field", None)
                await prompt_next_missing_field(chat_id, context, patient_key)
            else:
                await send_and_log_message(chat_id, "Hubo un problema guardando tu información. Inténtalo de nuevo.", context)
            return True

        elif current_field == "fecha_nacimiento":
            normalized_date = claim_manager._normalize_date(user_response)
            if not normalized_date:
                await send_and_log_message(
                    chat_id,
                    ("❌ Formato de fecha inválido. Por favor, ingresa tu fecha de nacimiento en formato "
                     "DD/MM/AAAA (ej. 01/01/1990) o AAAA-MM-DD (ej. 1990-01-01)."),
                    context
                )
                return True

            success = claim_manager.update_patient_field(patient_key, current_field, normalized_date)
            if success:
                context.user_data.pop("waiting_for_field", None)
                await prompt_next_missing_field(chat_id, context, patient_key)
            else:
                await send_and_log_message(chat_id, "Hubo un problema guardando tu fecha. Inténtalo de nuevo.", context)
            return True

        else:
            success = claim_manager.update_patient_field(patient_key, current_field, user_response)
            if success:
                context.user_data.pop("waiting_for_field", None)
                await prompt_next_missing_field(chat_id, context, patient_key)
            else:
                await send_and_log_message(chat_id, "Hubo un problema guardando tu información. Inténtalo de nuevo.", context)
            return True

    except Exception as e:
        logger.error(f"Error al procesar respuesta de campo '{current_field}': {e}", exc_info=True)
        await send_and_log_message(chat_id, "Ocurrió un error procesando tu respuesta. Por favor, inténtalo de nuevo.", context)
        return True



def setup_handlers(application: Application) -> None:
    """Configura los manejadores de mensajes y callbacks."""
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.CONTACT, process_contact))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.PHOTO, process_photo))
    logger.info("Manejadores configurados.")


def setup_job_queue(application: Application) -> None:
    """Configura trabajos programados - SIN jobs periódicos"""
    job_queue = application.job_queue
    
    logger.info("job queue configurado Sin trabajos periódicos ")


def format_telegram_text(text: str) -> str:
    """Convierte formato markdown a formato Telegram correcto"""
    import re
    return re.sub(r'\*\*(.*?)\*\*', r'*\1*', text)

def main() -> None:
    "Función principal para iniciar el bot de Telegram."
    logger.info("Iniciando Bot de Telegram...")

    if not all([consent_manager, pip_processor_instance, claim_manager]):
        logger.critical("Uno o más componentes críticos no están inicializados. Abortando.")
        sys.exit(1)

    application = ApplicationBuilder().token(TELEGRAM_API_TOKEN).build()
    setup_job_queue(application)
    setup_handlers(application)

    logger.info("Bot iniciado y escuchando mensajes.")
    application.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()

"""
def create_application() -> Application:
      
    
      
      Carga y configura una instancia de Application de python-telegram-bot:
       - Verifica que los componentes críticos (ConsentManager, PIPProcessor, ClaimManager) estén inicializados.
       - Construye el Application con el TOKEN de Telegram.
       - Registra los jobs y handlers.
      Úsala para polling (local) o para procesar webhooks (production).
     
      logger.info("Configurando la aplicación del bot de Telegram...")

      # Verificación de componentes
      if not all([consent_manager, pip_processor_instance, claim_manager]):       
          logger.critical("Uno o más componentes críticos no están inicializados. Abortando.")
          sys.exit(1)

      # Construcción de la aplicación
      application = (
          Application.builder()
          .token(TELEGRAM_API_TOKEN)
          .build())

      #await application.initialize()
      # Registro de jobs periódicos (expiración de sesiones, etc.)
      setup_job_queue(application)

      # Registro de handlers de mensajes y callbacks
      setup_handlers(application)

      logger.info("Aplicación configurada correctamente.")
      return application


def main() -> None:
     Punto de entrada: arranca el bot en modo polling (desarrollo local).
     app = create_application()
     logger.info("Arrancando polling del bot...")
     app.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
     main()

"""