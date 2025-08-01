import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from patient_module.patient_module import PatientModule
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
    from claim_manager.data_collection import ClaimManager
    from claim_manager.claim_generator import ClaimGenerator
    from claim_manager.claim_generator import generar_reclamacion_eps, generar_tutela, generar_reclamacion_supersalud, validar_disponibilidad_supersalud, generar_desacato

except ImportError as e:
    print(f"Error al importar módulos: {e}")
    sys.exit(1)

def get_session_id_from_patient_key(patient_key: str) -> str:
    """Busca el session_id más reciente del paciente en BigQuery."""
    try:
        from processor_image_prescription.bigquery_pip import get_bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID
        from google.cloud import bigquery
        
        client = get_bigquery_client()
        
        query = f"""
        SELECT pres.id_session
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS t,
        UNNEST(t.prescripciones) AS pres
        WHERE t.paciente_clave = @patient_key
        AND (pres.id_session LIKE 'TL_%' OR t.canal_contacto = 'TL')  -- ✅ SOLO TELEGRAM
        ORDER BY pres.fecha_atencion DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )
        
        results = client.query(query, job_config=job_config).result()
        
        for row in results:
            session_id = row.id_session
            logger.info(f"🔍 Session ID de Telegram encontrado en BigQuery: '{session_id}'")
            return session_id or ""
            
        logger.warning(f"⚠️ No se encontró session_id de Telegram para patient_key: {patient_key}")
        return ""
        
    except Exception as e:
        logger.error(f"❌ Error buscando session_id de Telegram para {patient_key}: {e}")
        return ""
    
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

def ensure_tutela_id_in_context(context: ContextTypes.DEFAULT_TYPE) -> str:
    """
    Asegura que hay un tutela_id en el contexto del usuario.
    Si no existe, genera uno nuevo.
    
    Args:
        context: Context de Telegram
        
    Returns:
        str: tutela_id (existente o recién generado)
    """
    current_tutela_id = context.user_data.get("current_tutela_id")
    if not current_tutela_id:
        from claim_manager.claim_generator import generate_tutela_id
        current_tutela_id = generate_tutela_id()
        context.user_data["current_tutela_id"] = current_tutela_id
        logger.info(f"🆔 Generado nuevo tutela_id: {current_tutela_id}")
    else:
        logger.info(f"🆔 Usando tutela_id existente: {current_tutela_id}")
    
    return current_tutela_id

def validar_requisitos_desacato(patient_key: str) -> Dict[str, Any]:
    """
    Función auxiliar para validar requisitos de desacato.
    Usa la clase ClaimGenerator internamente.
    """
    try:
        claim_generator = ClaimGenerator()
        return claim_generator.validar_requisitos_desacato(patient_key)
    except Exception as e:
        logger.error(f"Error validando requisitos de desacato: {e}")
        return {
            "puede_desacatar": False,
            "error": f"Error técnico: {str(e)}"
        }

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
        "telegram_user_id": context.user_data.get("telegram_user_id"),
        "waiting_for_field": context.user_data.get("waiting_for_field"),
        "patient_key": context.user_data.get("patient_key"),
        "detected_channel": context.user_data.get("detected_channel", "TL"),
    }

def _get_entidad_destinataria(tipo_accion: str, patient_data: Dict[str, Any]) -> str:
    """
    Determina la entidad destinataria según el tipo de acción.
    
    Args:
        tipo_accion: Tipo de reclamación
        patient_data: Datos del paciente
        
    Returns:
        String con el nombre de la entidad destinataria
    """
    if tipo_accion == "reclamacion_eps":
        return patient_data.get("eps_estandarizada", "EPS")
    elif tipo_accion == "reclamacion_supersalud":
        return "Superintendencia Nacional de Salud"
    elif tipo_accion == "tutela":
        return "Juzgado de Tutela"
    elif tipo_accion == "desacato":
        return "Juzgado de Tutela (Desacato)"
    else:
        return "Entidad no especificada"
    
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

        if context.user_data.get("waiting_for_tutela_field"):
            handled = await handle_tutela_field_response(update, context)
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
    telegram_user_id = update.effective_user.id
    contact = update.message.contact
    phone = contact.phone_number if contact else None

    logger.info(f"Contacto recibido: {phone} de user_id: {telegram_user_id}")

    if not phone:
        await send_and_log_message(chat_id, "No pude obtener tu número. Por favor, inténtalo de nuevo.", context)
        return

    try:
        if not consent_manager or not consent_manager.session_manager:
            raise ValueError("ConsentManager o SessionManager no inicializado.")

        new_session_id = consent_manager.session_manager.create_session(
            phone, 
            channel="TL", 
            telegram_user_id=telegram_user_id
        )
        
        context.user_data["session_id"] = new_session_id
        context.user_data["phone"] = phone
        context.user_data["phone_shared"] = True
        context.user_data["canal"] = "TELEGRAM"
        context.user_data["detected_channel"] = "TL"
        context.user_data["telegram_user_id"] = telegram_user_id  # 🟢 GUARDAR para uso posterior

        logger.info(f"Sesión creada: {new_session_id} para user_id: {telegram_user_id}")

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
    
    logger.info(f"Callback recibido: {data}")

    # ✅ SIMPLIFICADO: Manejo de followup con delegación total al ClaimManager
    if data.startswith("followup_yes_") or data.startswith("followup_no_"):
        # Extraer session_id del callback data
        if data.startswith("followup_yes_"):
            session_id = data[len("followup_yes_"):]
            logger.info(f"✅ Paciente confirmó medicamentos recibidos para session: {session_id}")
            
            try:
                # Marcar como resuelto usando PatientModule con session_id
                pm = PatientModule()
                success = pm.update_reclamation_status(session_id, "resuelto")
                
                if success:
                    await query.edit_message_text(
                        text=format_telegram_text(
                            "✅ *¡Excelente!*\n\n"
                            "Tu caso ha sido marcado como resuelto.\n"
                            "¡Gracias por confiar en nosotros! 🎉"
                        ),
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await query.edit_message_text(
                        text=format_telegram_text(
                            "⚠️ Hubo un error actualizando tu caso.\n"
                            "Nuestro equipo lo revisará manualmente."
                        ),
                        parse_mode=ParseMode.MARKDOWN
                    )
            except Exception as e:
                logger.error(f"Error actualizando reclamación para session {session_id}: {e}")
                await query.edit_message_text(
                    text=format_telegram_text("❌ Error técnico. Por favor inténtalo más tarde."),
                    parse_mode=ParseMode.MARKDOWN
                )
        
        else:  # followup_no_
            session_id = data[len("followup_no_"):]
            logger.info(f"❌ Paciente NO recibió medicamentos para session: {session_id}")
            
            try:
                from claim_manager.claim_generator import determinar_tipo_reclamacion_siguiente
                tipo_reclamacion = determinar_tipo_reclamacion_siguiente(session_id)
                
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Sí, quiero escalar", callback_data=f"escalate_yes_{session_id}"),
                        InlineKeyboardButton("❌ No, por ahora no", callback_data=f"escalate_no_{session_id}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    text=format_telegram_text(
                        f"💔 Lamento que no hayas recibido tus medicamentos.\n\n"
                        f"¿Deseas escalar tu caso y entablar *{tipo_reclamacion}*?"
                    ),
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.MARKDOWN
                )
                
            except Exception as e:
                logger.error(f"Error mostrando pregunta de escalamiento para session {session_id}: {e}")
                await query.edit_message_text(
                    text=format_telegram_text("❌ Error mostrando opciones de escalamiento."),
                    parse_mode=ParseMode.MARKDOWN
                )
        return

    elif data.startswith("escalate_yes_") or data.startswith("escalate_no_"):
        if data.startswith("escalate_yes_"):
            session_id = data[len("escalate_yes_"):]
            logger.info(f"✅ Paciente ACEPTA escalar para session: {session_id}")
            
            await query.edit_message_text(
                text=format_telegram_text("🔄 *Procesando escalamiento...*\n\nEvaluando el mejor siguiente paso para tu caso."),
                parse_mode=ParseMode.MARKDOWN
            )
            
            try:
                from claim_manager.claim_generator import auto_escalate_patient
                resultado = auto_escalate_patient(session_id)
                resultado["id_session"] = session_id
                
                # ✅ PRIMERO: Verificar si requiere recolección de tutela
                if resultado.get("requiere_recoleccion_tutela") and resultado.get("tipo") == "desacato":
                    patient_key = resultado["patient_key"]
                    
                    tutela_id = resultado.get("tutela_id")
                    if not tutela_id:
                        from claim_manager.claim_generator import generate_tutela_id
                        tutela_id = generate_tutela_id()
                        logger.info(f"🆔 Generado tutela_id para escalamiento: {tutela_id}")
                    
                    field_prompt = claim_manager.get_next_missing_tutela_field_prompt(
                        patient_key, {}, tutela_id  
                    )
                    
                    await query.edit_message_text(
                        text=format_telegram_text(
                            "🔄 *Para proceder con el desacato necesito datos de tu tutela:*\n\n"
                            f"{field_prompt['prompt_text']}"
                        ),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
                    context.user_data["waiting_for_tutela_field"] = field_prompt["field_name"]
                    context.user_data["patient_key"] = patient_key
                    context.user_data["tutela_data_temp"] = {}
                    context.user_data["current_tutela_id"] = tutela_id
                    
                    return
                
                # ✅ ÚNICO CAMBIO: if → elif
                elif resultado.get("success"):
                    tipo = resultado.get("tipo", "escalamiento")
                    razon = resultado.get("razon", "")
                    nivel = resultado.get("nivel_escalamiento", "")
                    patient_key = resultado.get("patient_key", "")
                    
                    logger.info(f"✅ Escalamiento exitoso para session {session_id} → patient {patient_key}: {tipo}")
                    
                    if tipo == "sin_escalamiento":
                        await query.edit_message_text(
                            text=format_telegram_text(
                                "📋 *Caso en revisión*\n\n"
                                "Tu caso está siendo revisado por nuestro equipo especializado.\n"
                                "Te contactaremos pronto con actualizaciones."
                            ),
                            parse_mode=ParseMode.MARKDOWN
                        )
                    elif tipo.startswith("multiple_"):
                        # Escalamiento múltiple (EPS + Supersalud)
                        await query.edit_message_text(
                            text=format_telegram_text(
                                f"✅ *¡Escalamiento múltiple realizado!*\n\n"
                                f"Por la falta de respuesta suficiente, tu caso fue escalado tanto a la *EPS* como a la *Superintendencia Nacional de Salud*.\n\n"
                                f"Nivel de escalamiento: *{nivel}*\n\n"
                                f"Procesaremos ambas reclamaciones y te mantendremos informado de cualquier novedad. ¡Seguimos contigo!"
                            ),
                            parse_mode=ParseMode.MARKDOWN
                        )
                    else:
                        # Escalamiento simple
                        tipo_legible = tipo.replace("_", " ").replace("reclamacion", "reclamación").title()
                        # Personalizar mensaje para Supersalud u otros
                        if "supersalud" in tipo.lower():
                            mensaje = (
                                f"✅ *¡Tu caso ha sido escalado a la Superintendencia Nacional de Salud!*\n\n"
                                f"Detectamos que la EPS no respondió en el plazo establecido, así que tomamos acción por ti: tu solicitud fue remitida a Supersalud para una gestión prioritaria.\n\n"
                                f"Nivel de escalamiento: *{nivel}*\n\n"
                                f"Nuestro equipo hará seguimiento y te informaremos sobre cualquier novedad. ¡Seguimos acompañándote hasta que recibas tus medicamentos!"
                            )
                        else:
                            mensaje = (
                                f"✅ *{tipo_legible} generada exitosamente*\n\n"
                                f"Nivel de escalamiento: *{nivel}*\n\n"
                                f"Tu caso ha sido escalado automáticamente. Nuestro equipo procesará tu solicitud y te mantendremos informado del progreso."
                            )
                        await query.edit_message_text(
                            text=format_telegram_text(mensaje),
                            parse_mode=ParseMode.MARKDOWN
                        )
                        
                        # ✅ NUEVO: Enviar PDF para tutela y desacato
                        if tipo in ["tutela", "desacato"]:
                            await _send_pdf_for_escalation(query.message.chat_id, patient_key, tipo, context)
                            
                # ✅ TERCERO: Solo mostrar error si no es recolección Y no es exitoso
                else:
                    error = resultado.get("error", "Error desconocido")
                    logger.error(f"Error en escalamiento automático para session {session_id}: {error}")
                    await query.edit_message_text(
                        text=format_telegram_text(
                            "⚠️ *Error en escalamiento automático*\n\n"
                            "Hubo un problema procesando tu caso automáticamente.\n"
                            "Nuestro equipo revisará tu solicitud manualmente y te contactará pronto."
                        ),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
            except Exception as e:
                logger.error(f"Error ejecutando escalamiento para session {session_id}: {e}")
                await query.edit_message_text(
                    text=format_telegram_text(
                        "❌ *Error técnico*\n\n"
                        "Ocurrió un problema técnico procesando tu escalamiento.\n"
                        "Por favor contacta a nuestro equipo de soporte."
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
        
        else:  # escalate_no_
            session_id = data[len("escalate_no_"):]
            logger.info(f"❌ Paciente RECHAZA escalar para session: {session_id}")
            
            await query.edit_message_text(
                text=format_telegram_text(
                    "📝 *Entendido*\n\n"
                    "Respetamos tu decisión. Si más adelante deseas continuar con el escalamiento, "
                    "puedes contactarnos nuevamente.\n\n"
                    "✅ Tu caso queda registrado por si necesitas ayuda futura.\n\n"
                    "¡Gracias por confiar en nosotros!"
                ),
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Cerrar sesión opcionalmente
            try:
                close_user_session(session_id, context, reason="user_declined_escalation")
            except Exception as e:
                logger.warning(f"Error cerrando sesión {session_id}: {e}")
        
        return

    # Extraer session_id según el tipo de callback para otros callbacks
    if data.startswith("consent_"):
        # Para consentimiento, obtener session_id del contexto
        session_id = context.user_data.get("session_id")
        logger.info(f"Callback de consentimiento - session_id del contexto: {session_id}")
        
        if not session_id:
            await query.edit_message_text(
                text=format_telegram_text("Error: No se encontró una sesión activa. Por favor, reinicia la conversación."),
                parse_mode=ParseMode.MARKDOWN
            )
            return
            
        await handle_consent_response(query, context, session_id, data == "consent_yes")
        return
        
    elif data.startswith("med_"):
        await handle_medication_selection(query, context, data)
        return
        
    elif data.startswith("informante_"):
        informante_type = "paciente" if "paciente" in data else "cuidador"
        await handle_informante_selection(query, context, informante_type)
        return
        
    elif data.startswith("regimen_"):
        regimen_type = "Contributivo" if "contributivo" in data else "Subsidiado"
        await handle_regimen_selection(query, context, regimen_type)
        return
    
    else:
        # Para otros tipos de callback que no reconocemos
        await query.edit_message_text(
            text=format_telegram_text("Acción no reconocida. Por favor, intenta de nuevo."),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.warning(f"Callback no reconocido: {data}")
        return


async def handle_consent_response(query, context: ContextTypes.DEFAULT_TYPE, 
                                 session_id: str, granted: bool) -> None:
    """Maneja la respuesta de consentimiento (sí/no)."""
    user_id = query.from_user.id
    phone = context.user_data.get("phone")

    if not consent_manager or not phone:
        await query.edit_message_text(
            text=format_telegram_text("Error de sistema al procesar consentimiento. Inténtalo de nuevo."),
            parse_mode=ParseMode.MARKDOWN
        )
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
        await query.edit_message_text(
            text=format_telegram_text("Hubo un problema al guardar tu consentimiento."),
            parse_mode=ParseMode.MARKDOWN
        )


async def process_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa imágenes de recetas médicas."""
    chat_id = update.effective_chat.id
    telegram_user_id = update.effective_user.id
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

        result = pip_processor_instance.process_image(temp_image_path, session_id, telegram_user_id=telegram_user_id)
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
        await query.edit_message_text(
            text=format_telegram_text(text), 
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
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
        
        logger.info(f"Todos los campos completos para paciente {patient_key}. Iniciando generación de reclamación.")
        
        session_id = context.user_data.get("session_id")
        if not session_id:
            logger.error(f"No se encontró session_id para paciente {patient_key}")
            session_id = "unknown_session"

        # 1. GENERAR RECLAMACIÓN EPS AUTOMÁTICAMENTE
        try:
            resultado_reclamacion = generar_reclamacion_eps(patient_key)
            
            if resultado_reclamacion["success"]:
                # 2. GUARDAR EN TABLA RECLAMACIONES
                success_saved = await save_reclamacion_to_database(
                    patient_key=patient_key,
                    tipo_accion="reclamacion_eps",
                    texto_reclamacion=resultado_reclamacion["texto_reclamacion"],
                    estado_reclamacion="pendiente_radicacion",
                    nivel_escalamiento=1,
                    session_id=session_id,
                    resultado_claim_generator=resultado_reclamacion
                )
                
                if success_saved:
                    logger.info(f"Reclamación EPS generada y guardada exitosamente para paciente {patient_key}")
                    supersalud_disponible = validar_disponibilidad_supersalud()

                    if supersalud_disponible.get("disponible"):
                        success_message = (
                            "🎉 ¡Perfecto! Reclamación EPS generada exitosamente.¡Gracias por confiar en nosotros!\n\n"
                                "📋 En las próximas 48 horas te enviaremos el número de radicado.\n\n"
                                "📅 Cuando se cumpla el plazo de respuesta, te contactaremos para verificar si recibiste tus medicamentos."
                            )

                    else:
                        success_message = (
                            "🎉 ¡Perfecto! Reclamación EPS generada exitosamente.¡Gracias por confiar en nosotros!\n\n"
                                "📋 En las próximas 48 horas te enviaremos el número de radicado.\n\n"
                                "📅 Cuando se cumpla el plazo de respuesta, te contactaremos para verificar si recibiste tus medicamentos."
                            )

                else:
                    logger.error(f"Error guardando reclamación para paciente {patient_key}")
                    success_message = ( 
                        "⚠️ Se completó la recopilación de datos, pero hubo un problema técnico guardando tu reclamación.\n\n"
                        "📞 Nuestro equipo revisará tu caso manualmente.\n\n"
                        "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
                    )
            else:
                logger.error(f"Error generando reclamación EPS para paciente {patient_key}: {resultado_reclamacion.get('error', 'Error desconocido')}")
                success_message = (
                    "⚠️ Se completó la recopilación de datos, pero hubo un problema técnico generando tu reclamación.\n\n"
                    "📞 Nuestro equipo revisará tu caso manualmente.\n\n"
                    "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
                )
                
        except Exception as e:
            logger.error(f"Error inesperado en generación de reclamación para paciente {patient_key}: {e}")
            success_message = (
                "⚠️ Se completó la recopilación de datos. Nuestro equipo procesará tu reclamación manualmente.\n\n"
                "📞 Te contactaremos pronto.\n\n"
                "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
            )
        
        # 3. ENVIAR MENSAJE FINAL
        await send_and_log_message(chat_id, success_message, context)
        
        session_id = context.user_data.get("session_id")
        if session_id:
            close_user_session(session_id, context, reason="process_completed_with_claim")
        

        logger.info(f"Proceso completo finalizado para paciente {patient_key} - sesión Cerrada")


async def save_reclamacion_to_database(patient_key: str, tipo_accion: str, 
                                     texto_reclamacion: str, estado_reclamacion: str,
                                     nivel_escalamiento: int, session_id: str,
                                     resultado_claim_generator: Dict[str, Any] = None) -> bool:
    """
    Guarda una nueva reclamación en la tabla pacientes.
    VERSIÓN CORREGIDA: No incluye campos de radicación (numero_radicado, fecha_radicacion).
    Solo incluye campos que maneja el claim_manager según arquitectura.
    """
    try:
        from processor_image_prescription.bigquery_pip import get_bigquery_client, add_reclamacion_safe
        from google.cloud import bigquery
        
        logger.info(f"💾 Guardando reclamación {tipo_accion} para paciente {patient_key}")
        
        # Obtener medicamentos no entregados de la prescripción más reciente
        client = get_bigquery_client()
        from processor_image_prescription.bigquery_pip import PROJECT_ID, DATASET_ID, TABLE_ID
        table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        get_query = f"""
            SELECT prescripciones FROM `{table_reference}`
            WHERE paciente_clave = @patient_key LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )
        
        results = client.query(get_query, job_config=job_config).result()
        med_no_entregados_from_prescriptions = ""
        
        for row in results:
            prescripciones = row.prescripciones if row.prescripciones else []
            if prescripciones:
                ultima_prescripcion = prescripciones[-1]
                medicamentos = ultima_prescripcion.get("medicamentos", [])
                
                meds_no_entregados = []
                for med in medicamentos:
                    if isinstance(med, dict) and med.get("entregado") == "no entregado":
                        nombre = med.get("nombre", "")
                        if nombre:
                            meds_no_entregados.append(nombre)
                
                med_no_entregados_from_prescriptions = ", ".join(meds_no_entregados)
            break

        # Preparar nueva reclamación - SOLO campos que maneja claim_manager
        nueva_reclamacion = {
            "med_no_entregados": med_no_entregados_from_prescriptions,
            "tipo_accion": tipo_accion,
            "texto_reclamacion": texto_reclamacion,
            "estado_reclamacion": estado_reclamacion,
            "nivel_escalamiento": nivel_escalamiento,
            "url_documento": "",  # Se actualiza después si hay PDF
            "id_session": session_id  # ✅ INCLUIR session_id según requisitos
        }
        
        # Usar función segura para agregar reclamación
        success = add_reclamacion_safe(patient_key, nueva_reclamacion)
        
        if success:
            logger.info(f"✅ Reclamación {tipo_accion} (nivel {nivel_escalamiento}) guardada exitosamente")
        else:
            logger.error(f"❌ Error guardando reclamación {tipo_accion}")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ Error en save_reclamacion_to_database: {e}")
        return False


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
    
    if context.user_data.get("waiting_for_tutela_field"):
        return False
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

async def handle_tutela_field_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Maneja las respuestas de campos de tutela para generar desacato.
    ✅ MODIFICADO: Asegura tutela_id obligatorio desde el inicio.
    
    Args:
        update: Update de Telegram
        context: Context de Telegram
        
    Returns:
        bool: True si manejó la respuesta, False si no
    """
    chat_id = update.effective_chat.id
    field_name = context.user_data.get("waiting_for_tutela_field")
    patient_key = context.user_data.get("patient_key")
    tutela_data = context.user_data.get("tutela_data_temp", {})
    user_response = update.message.text.strip()
    
    session_context = get_session_context(context)
    current_session_id = (session_context.get("session_id") or 
                     context.user_data.get("session_id") or 
                     get_session_id_from_patient_key(patient_key) or "")
    
    current_tutela_id = ensure_tutela_id_in_context(context)

    logger.info(f"🔍 Session ID detectado: '{current_session_id}'")

    if not current_tutela_id:
        logger.error("❌ No se pudo obtener/generar tutela_id para recolección")
        await send_and_log_message(
            chat_id,
            "❌ Error técnico generando ID de tutela. Por favor intenta de nuevo.",
            context
        )
        return True
    
    logger.info(f"Procesando respuesta de campo tutela '{field_name}': {user_response[:50]}...")
    logger.info(f"🆔 Usando tutela_id: {current_tutela_id}")
    
    try:
        # Normalizar según el tipo de campo
        if field_name in ["fecha_sentencia", "fecha_radicacion_tutela"]:
            # Usar el normalizador de fechas existente
            normalized_value = claim_manager._normalize_date(user_response)
            if not normalized_value:
                await send_and_log_message(
                    chat_id,
                    "❌ Formato de fecha inválido. Por favor usa el formato DD/MM/AAAA\n\n"
                    "Ejemplo: 28/05/2025",
                    context
                )
                return True
            tutela_data[field_name] = normalized_value
        else:
            # Campos de texto normales
            tutela_data[field_name] = user_response
        
        # ✅ MODIFICADO: Usar tutela_id OBLIGATORIO
        field_prompt = claim_manager.get_next_missing_tutela_field_prompt(
            patient_key, tutela_data, current_tutela_id
        )
        
        if field_prompt.get("field_name"):
            # Aún faltan campos
            await send_and_log_message(chat_id, field_prompt["prompt_text"], context)
            context.user_data["waiting_for_tutela_field"] = field_prompt["field_name"]
            context.user_data["tutela_data_temp"] = tutela_data
            
        else:
            # ✅ TODOS LOS CAMPOS COMPLETOS
            context.user_data.pop("waiting_for_tutela_field", None)
            context.user_data.pop("tutela_data_temp", None)
            # ✅ MANTENER current_tutela_id hasta completar el proceso
            
            await send_and_log_message(
                chat_id,
                "✅ Datos de tutela recopilados correctamente.\n\n🔄 Generando incidente de desacato...",
                context
            )
            
            # 1. Guardar datos de tutela CON tutela_id OBLIGATORIO
            success = claim_manager.save_tutela_data_simple(patient_key, current_tutela_id, tutela_data)
            
            if success:
                # 2. Generar desacato CON tutela_id OBLIGATORIO
                try:
                    from claim_manager.claim_generator import generar_desacato
                    resultado_desacato = generar_desacato(patient_key, tutela_data, current_tutela_id)
                    resultado_desacato["id_session"] = current_session_id
                    
                    logger.info(f"🔍 DEBUG SESSION ID PARA DESACATO:")
                    logger.info(f"   - context.user_data keys: {list(context.user_data.keys())}")
                    logger.info(f"   - context.user_data session_id: '{context.user_data.get('session_id')}'")
                    logger.info(f"   - current_session_id variable: '{current_session_id}'")
                    logger.info(f"   - resultado_desacato id_session: '{resultado_desacato.get('id_session')}'")

                    if resultado_desacato.get("success"):
                        # 3. Guardar reclamación de desacato usando la función completa
                        try:
                            from processor_image_prescription.bigquery_pip import get_bigquery_client
                            from claim_manager.claim_generator import _guardar_escalamiento_individual
                            
                            client = get_bigquery_client()
                            
                            guardado = _guardar_escalamiento_individual(
                                client,
                                patient_key,
                                resultado_desacato,
                                5,  # nivel_escalamiento
                                current_session_id # <--- Usa esta variable que ya tiene el ID de sesión correcto
                            )

                        except Exception as e:
                            logger.error(f"Error guardando desacato con función completa: {e}")
                            guardado = False
                        
                        if guardado:
                            await send_and_log_message(
                                chat_id,
                                f"✅ *Desacato generado exitosamente*\n\n"
                                f"🆔 Tutela ID: `{current_tutela_id}`\n"
                                f"Nivel de escalamiento: *5*\n\n"
                                f"📋 Tu incidente de desacato ha sido preparado con los datos de tu tutela.\n\n"
                                f"Nuestro equipo procesará tu solicitud y te mantendremos informado del progreso.",
                                context
                            )
                            
                            # ✅ NUEVO: Enviar PDF de desacato
                            if resultado_desacato.get("pdf_url") and resultado_desacato.get("pdf_filename"):
                                await _send_document_to_telegram(
                                    chat_id,
                                    resultado_desacato["pdf_url"],
                                    resultado_desacato["pdf_filename"], 
                                    context,
                                    f"Incidente de desacato - Tutela ID: {current_tutela_id}"
                                )
                        else:
                            await send_and_log_message(
                                chat_id,
                                "⚠️ Desacato generado pero hubo un problema guardándolo en el sistema.\n\n"
                                "📞 Nuestro equipo revisará tu caso manualmente.",
                                context
                            )
                    else:
                        error_msg = resultado_desacato.get("error", "Error desconocido")
                        await send_and_log_message(
                            chat_id,
                            f"❌ Error generando desacato: {error_msg}\n\n"
                            "📞 Nuestro equipo revisará tu caso.",
                            context
                        )
                        
                except Exception as e:
                    logger.error(f"Error generando desacato para {patient_key}: {e}")
                    await send_and_log_message(
                        chat_id,
                        "❌ Error técnico generando desacato.\n\n"
                        "📞 Nuestro equipo revisará tu caso manualmente.",
                        context
                    )
            else:
                await send_and_log_message(
                    chat_id,
                    "❌ Error guardando datos de tutela.\n\n"
                    "Por favor intenta más tarde o contacta a nuestro equipo.",
                    context
                )
            
            # ✅ LIMPIAR tutela_id al finalizar el proceso
            context.user_data.pop("current_tutela_id", None)
        
        return True
        
    except Exception as e:
        logger.error(f"Error procesando respuesta de campo tutela '{field_name}': {e}")
        await send_and_log_message(
            chat_id,
            "❌ Error procesando tu respuesta. Por favor intenta de nuevo.",
            context
        )
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
    
    # ✅ CORREGIR: Telegram usa *texto* para negritas, no **texto**
    text = re.sub(r'\*\*(.*?)\*\*', r'*\1*', text)
    
    # ✅ AGREGAR: También manejar negritas simples mal formateadas
    text = re.sub(r'(?<!\*)\*([^*]+)\*(?!\*)', r'*\1*', text)
    
    # ✅ Escapar caracteres especiales de Telegram
    text = text.replace('_', r'\_')
    text = text.replace('[', r'\[')
    text = text.replace(']', r'\]')
    
    return text

async def _send_document_to_telegram(chat_id: int, document_url: str, 
                                   filename: str, context: ContextTypes.DEFAULT_TYPE,
                                   caption: str = "") -> bool:
    """Envía un documento PDF al usuario de Telegram descargando directamente desde Cloud Storage."""
    try:
        from google.cloud import storage
        import tempfile
        from pathlib import Path
        
        # Extraer bucket y path desde gs:// URL
        if not document_url.startswith("gs://"):
            logger.error(f"URL no válida para Cloud Storage: {document_url}")
            return False
            
        gs_parts = document_url.replace("gs://", "").split("/", 1)
        bucket_name = gs_parts[0]
        blob_name = gs_parts[1] if len(gs_parts) > 1 else ""
        
        logger.info(f"📥 Descargando PDF directamente desde Cloud Storage: bucket={bucket_name}, blob={blob_name}")
        
        # Crear cliente de Cloud Storage usando credenciales por defecto
        from processor_image_prescription.cloud_storage_pip import get_cloud_storage_client
        storage_client = get_cloud_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Verificar que el blob existe
        if not blob.exists():
            logger.error(f"El archivo no existe en Cloud Storage: {document_url}")
            return False
        
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            # Descargar directamente desde Cloud Storage al archivo temporal
            blob.download_to_filename(str(temp_path))
            logger.info(f"📥 PDF descargado exitosamente: {temp_path.stat().st_size} bytes")
            
            # Enviar el archivo como documento
            with open(temp_path, 'rb') as pdf_file:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=pdf_file,
                    filename=filename,
                    caption=caption,
                    parse_mode=ParseMode.MARKDOWN
                )
            
            logger.info(f"📎 PDF enviado exitosamente a Telegram: {filename}")
            return True
            
        finally:
            # Limpiar archivo temporal
            if temp_path.exists():
                temp_path.unlink()
                logger.debug(f"Archivo temporal eliminado: {temp_path}")
        
    except Exception as e:
        logger.error(f"Error enviando PDF a Telegram: {e}")
        
        # Fallback SEGURO: Solo notificar que hay un documento, SIN URLs
        try:
            fallback_message = format_telegram_text(
                f"📎 *{caption}*\n\n"
                f"Tu documento `{filename}` ha sido generado exitosamente.\n"
                f"Por motivos técnicos temporales, nuestro equipo te lo enviará por otro medio.\n\n"
                f"¡Disculpa las molestias!"
            )
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=fallback_message,
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass
            
        return False

async def _send_pdf_for_escalation(chat_id: int, patient_key: str, tipo: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Busca y envía el PDF de la reclamación recién creada."""
    try:
        from processor_image_prescription.bigquery_pip import get_bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID
        from google.cloud import bigquery
        
        client = get_bigquery_client()
        
        query_sql = f"""
        SELECT reclamaciones
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE paciente_clave = @patient_key
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )
        
        results = client.query(query_sql, job_config=job_config).result()
        
        for row in results:
            reclamaciones = row.reclamaciones if row.reclamaciones else []
            # Buscar la reclamación más reciente del tipo específico
            for rec in reversed(reclamaciones):
                if (rec.get("tipo_accion") == tipo and 
                    rec.get("url_documento") and 
                    rec.get("url_documento").strip()):
                    
                    pdf_url = rec["url_documento"]
                    pdf_filename = f"{tipo}_{patient_key}.pdf"
                    
                    await _send_document_to_telegram(
                        chat_id, pdf_url, pdf_filename, context,
                        f"Documento de {tipo}"
                    )
                    return
            break
            
        logger.warning(f"No se encontró PDF para {tipo} del paciente {patient_key}")
        
    except Exception as e:
        logger.error(f"Error enviando PDF de {tipo}: {e}")

def create_application() -> Application:
    '''Carga y configura una instancia de Application de python-telegram-bot:
    - Verifica que los componentes críticos (ConsentManager, PIPProcessor, ClaimManager) estén inicializados.
    - Construye el Application con el TOKEN de Telegram.
    - Registra los jobs y handlers.
    Se usa para desplegar en cloudrun'''
     
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

    # Registro de jobs periódicos (expiración de sesiones, etc.)
    setup_job_queue(application)

    # Registro de handlers de mensajes y callbacks
    setup_handlers(application)

    logger.info("Aplicación configurada correctamente.")
    return application


def main() -> None:
    """Punto de entrada: arranca el bot en modo polling (desarrollo local)."""
    app = create_application()
    logger.info("Arrancando polling del bot...")
    app.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
     main()