import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from channels.whatsapp_business_api import WhatsAppBusinessAPIClient, WhatsAppBusinessAPIError
from typing import Dict, Any
from google.cloud import firestore
import asyncio 
from google.api_core.exceptions import AlreadyExists
try:
    from BYC.consentimiento import ConsentManager
    from processor_image_prescription.pip_processor import PIPProcessor
    from claim_manager.data_collection import ClaimManager
    from claim_manager.claim_generator import generar_desacato
    from patient_module.patient_module import PatientModule
except ImportError as e:
    print(f"Error al importar módulos: {e}")

logger = logging.getLogger(__name__)

class WhatsAppMessageHandler:
    """Maneja los diferentes tipos de mensajes de WhatsApp."""
    
    def __init__(self, wa_client: WhatsAppBusinessAPIClient, consent_manager: ConsentManager,
                 pip_processor: PIPProcessor, claim_manager: ClaimManager):
        self.wa_client = wa_client
        self.consent_manager = consent_manager
        self.pip_processor = pip_processor
        self.claim_manager = claim_manager
        self.logger = logger

    async def handle_text_message(self, webhook_data: Dict[str, Any]) -> None:
        """Maneja mensajes de texto de WhatsApp con flujo unificado:
        1) Saludo → 2) Consentimiento (nueva sesión) → 3) Resto del flujo."""
        try:
            # Extraer datos del webhook
            message_data = self._extract_message_data(webhook_data)
            if not message_data:
                return

            phone_number = message_data["from"]
            text_obj = message_data.get("text", {})
            message_text = text_obj.get("body", "") if isinstance(text_obj, dict) else str(text_obj)
            message_id = message_data.get("id", "")

            # Marcar mensaje como leído
            try:
                self.wa_client.mark_message_as_read(message_id)
            except Exception as e:
                self.logger.warning(f"No se pudo marcar mensaje como leído: {e}")

            self.logger.info(f"WhatsApp de {phone_number}: '{message_text}'")

            # Obtener contexto de sesión
            session_context = self._get_session_context(phone_number)
            session_id = session_context.get("session_id")

            # 1) Si NO hay sesión iniciada → saludo + pedir consentimiento
            if session_id is None:
                # Saludo inicial
                await self._send_text_message(
                    phone_number,
                    "👋 ¡Hola! Bienvenido a No Me Entregaron. Estamos aquí para ayudarte con la entrega de tus medicamentos."
                )
                # Botones de consentimiento
                buttons = [
                    {"text": "✅ Sí, autorizo", "callback_data": "consent_yes"},
                    {"text": "❌ No autorizo",   "callback_data": "consent_no"}
                ]
                await self._send_interactive_message(
                    phone_number,
                    "Antes de continuar, ¿autorizas el tratamiento de tus datos personales para este fin? 🙏",
                    buttons
                )
                return

            # 2) Verificar inactividad de sesión
            if self.consent_manager and session_id:
                expired = self.consent_manager.session_manager.check_session_inactivity(session_id)
                if expired:
                    # Sesión caducada: limpiar y reiniciar
                    self._update_session_context(phone_number, {})
                    await self._send_text_message(
                        phone_number,
                        "¡Hola de nuevo! 👋 Tu sesión expiró por inactividad. Empecemos otra vez."
                    )
                    return

            # 3) Manejo de despedida
            if (self.consent_manager and
                self.consent_manager.should_close_session(message_text, session_context)):
                response = self.consent_manager.get_bot_response(message_text, session_context)
                await self._send_text_message(phone_number, response)
                self._close_user_session(session_id, phone_number, reason="user_farewell")
                return

            # 4) Manejar campos pendientes (ej. fotos, datos extras)
            if session_context.get("waiting_for_field"):
                handled = await self._handle_field_response(phone_number, message_text, session_context)
                if handled:
                    return

            # 🧩 Recolección paso a paso de datos de tutela (desacato)
            if session_context.get("waiting_for_tutela_field"):
                handled = await self.handle_tutela_field_response(phone_number, message_text, session_context)
                if handled:
                    return
   

            # 5) Flujo normal: obtener respuesta del bot
            if self.consent_manager:
                response = self.consent_manager.get_bot_response(message_text, session_context)

                # Si pide consentimiento y aún no se preguntó
                if "autorización" in response.lower() and not session_context.get("consent_asked"):
                    buttons = [
                        {"text": "✅ Sí, autorizo", "callback_data": "consent_yes"},
                        {"text": "❌ No autorizo",   "callback_data": "consent_no"}
                    ]
                    await self._send_interactive_message(phone_number, response, buttons)
                    session_context["consent_asked"] = True
                    self._update_session_context(phone_number, session_context)
                    return

                # Enviar la respuesta como texto simple
                await self._send_text_message(phone_number, response)

                # Loggear el mensaje del usuario
                if session_id:
                    await self._log_user_message(session_id, message_text)

        except Exception as e:
            self.logger.error(f"Error manejando texto WhatsApp: {e}", exc_info=True)
            await self._send_text_message(
                phone_number,
                "Disculpa, hubo un error técnico. Por favor intenta nuevamente."
            )


    async def handle_interactive_message(self, webhook_data: Dict[str, Any]) -> None:
        """Maneja respuestas a botones interactivos de WhatsApp."""
        try:
            message_data = self._extract_message_data(webhook_data)
            if not message_data:
                return

            phone_number = message_data["from"]
            interactive_data = message_data.get("interactive", {})
            button_reply = interactive_data.get("button_reply", {})
            callback_data = button_reply.get("id", "")

            self.logger.info(f"Callback WhatsApp recibido de {phone_number}: {callback_data}")

            # Obtener contexto de sesión
            session_context = self._get_session_context(phone_number)
            session_id = session_context.get("session_id")

            # Manejar diferentes tipos de callbacks
            if callback_data.startswith("consent_"):
                await self._handle_consent_response(phone_number, callback_data == "consent_yes", session_context)
            elif callback_data.startswith("regimen_"):
                regimen_type = "Contributivo" if "contributivo" in callback_data else "Subsidiado"
                await self._handle_regimen_selection(phone_number, regimen_type, session_context)
            elif callback_data.startswith("informante_"):
                informante_type = "paciente" if "paciente" in callback_data else "cuidador"
                await self._handle_informante_selection(phone_number, informante_type, session_context)
            elif callback_data.startswith("med_"):
                await self._handle_medication_selection(phone_number, callback_data, session_context)
            elif callback_data.startswith("followup_"):
                await self._handle_followup_response(phone_number, callback_data, session_context)
            elif callback_data.startswith("escalate_"):
                await self._handle_escalate_response(phone_number, callback_data, session_context)
            else:
                await self._send_text_message(phone_number, "Acción no reconocida. Por favor, intenta de nuevo.")

        except Exception as e:
            self.logger.error(f"Error manejando mensaje interactivo WhatsApp: {e}", exc_info=True)
            await self._send_text_message(phone_number, "Ocurrió un error procesando tu respuesta.")

    async def handle_image_message(self, webhook_data: Dict[str, Any]) -> None:
        """Maneja imágenes de prescripciones médicas."""
        try:
            message_data = self._extract_message_data(webhook_data)
            if not message_data:
                self.logger.warning("No se pudo extraer datos del mensaje de imagen")
                return

            phone_number = message_data["from"]
            image_data = message_data.get("image", {})
            media_id = image_data.get("id", "")

            self.logger.info(f"Procesando imagen de {phone_number}, media_id: {media_id}")

            # Verificar consentimiento
            session_context = self._get_session_context(phone_number)
            self.logger.info(f"Contexto de sesión: {session_context}")
            
            if not session_context.get("consent_given"):
                buttons = [
                    {"text": "✅ Sí, autorizo", "callback_data": "consent_yes"},
                    {"text": "❌ No autorizo", "callback_data": "consent_no"}
                ]
                await self._send_interactive_message(
                    phone_number,
                    "Primero necesito tu autorización para procesar tus datos.",
                    buttons
                )
                return

            session_id = session_context.get("session_id")
            if not session_id:
                await self._send_text_message(phone_number, "No hay una sesión activa. Por favor, reinicia la conversación.")
                return

            # Enviar mensaje de procesamiento
            await self._send_text_message(phone_number, "📸 En estos momentos estoy leyendo tu fórmula médica, por favor espera...")

            # Descargar imagen
            temp_image_path = await self._download_image(media_id)
            if not temp_image_path:
                await self._send_text_message(phone_number, "No pude descargar la imagen. Por favor, envíala nuevamente.")
                return

            try:
                # Procesar imagen con PIP
                result = self.pip_processor.process_image(temp_image_path, session_id)

                if isinstance(result, str):
                    await self._send_text_message(phone_number, result)
                    return

                if isinstance(result, dict):
                    # Actualizar contexto base
                    session_context["prescription_uploaded"] = True
                    session_context["patient_key"] = result["patient_key"]

                    await self._log_user_message(session_id, "He leído tu fórmula y he encontrado:", "prescription_processed")

                    if result.get("_requires_medication_selection"):
                        medications = result.get("medicamentos", [])
                        selection_msg = self.pip_processor.get_medication_selection_message(result)
                        await self._send_text_message(phone_number, selection_msg)
                        
                        # Crear lista de medicamentos para WhatsApp
                        await self._send_medication_list(phone_number, medications, session_id)
                        
                        # Actualizar contexto con medicamentos (consolidar todas las actualizaciones)
                        session_context["pending_medications"] = medications
                        session_context["selected_undelivered"] = []
                        
                        # Una sola actualización de contexto con todos los campos
                        self._update_session_context(phone_number, session_context)
                        
                        self.logger.info(f"Contexto completo actualizado después de procesar imagen: patient_key={result['patient_key']}, medications={len(medications)}")
                    else:
                        # Actualizar contexto sin medicamentos
                        self._update_session_context(phone_number, session_context)
                        await self._continue_with_missing_fields(phone_number, result, session_context)
                else:
                    await self._send_text_message(phone_number, "Hubo un problema procesando tu fórmula. Por favor envía la foto nuevamente.")

            finally:
                # Limpiar archivo temporal
                if temp_image_path and temp_image_path.exists():
                    temp_image_path.unlink()

        except Exception as e:
            self.logger.error(f"Error procesando imagen WhatsApp: {e}", exc_info=True)
            await self._send_text_message(phone_number, "Ocurrió un error procesando tu imagen. Por favor envía la foto nuevamente.")

    # Métodos auxiliares privados
    
    def _find_active_session(self, normalized_phone: str) -> Optional[str]:
        """Busca una sesión activa para el número de teléfono dado."""
        try:
            # Buscar sesiones activas por user_identifier
            sessions_ref = self.consent_manager.session_manager.sessions_collection_ref
            query = sessions_ref.where("user_identifier", "==", normalized_phone).where("estado_sesion", "==", "activa")
            
            docs = query.limit(1).get()
            
            for doc in docs:
                session_data = doc.to_dict()
                # Verificar que no haya expirado (6 horas)
                last_activity = session_data.get("last_activity_at")
                if last_activity:
                    from datetime import datetime, timedelta
                    import pytz
                    colombia_tz = pytz.timezone('America/Bogota')
                    current_time = datetime.now(colombia_tz)
                    last_activity_time = last_activity.astimezone(colombia_tz)
                    
                    if (current_time - last_activity_time) < timedelta(hours=6):
                        return doc.id
                    else:
                        # Sesión expirada, cerrarla
                        self.consent_manager.session_manager.close_session(doc.id, "expired")
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error buscando sesión activa para {normalized_phone}: {e}")
            return None

    def _extract_message_data(self, webhook_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extrae datos del mensaje del webhook de WhatsApp."""
        try:
            entry = webhook_data.get("entry", [{}])[0]
            changes = entry.get("changes", [{}])[0]
            value = changes.get("value", {})
            messages = value.get("messages", [])
            
            if not messages:
                return None
                
            return messages[0]
        except (IndexError, KeyError) as e:
            self.logger.error(f"Error extrayendo datos del webhook: {e}")
            return None

    def _get_session_context(self, phone_number: str) -> Dict[str, Any]:
        """Obtiene el contexto de sesión para un número de teléfono."""
        normalized_phone = self.wa_client.validate_phone_number(phone_number)
        
        # Buscar sesión activa primero
        if self.consent_manager and self.consent_manager.session_manager:
            try:
                # Buscar sesión activa existente
                session_id = self._find_active_session(normalized_phone)
                
                # Si no hay sesión activa, crear una nueva
                if not session_id:
                    session_info = self.consent_manager.session_manager.create_session_with_history_check(
                        normalized_phone, "WA"
                    )
                    session_id = session_info["new_session_id"]
                    self.logger.info(f"Nueva sesión creada: {session_id}")
                else:
                    self.logger.info(f"Usando sesión existente: {session_id}")
                
                # Verificar el consentimiento real desde la sesión
                consent_given = False
                consent_asked = False
                
                try:
                    # Obtener datos de la sesión desde Firestore
                    session_doc = self.consent_manager.session_manager.sessions_collection_ref.document(session_id).get()
                    if session_doc.exists:
                        session_data = session_doc.to_dict()
                        consent_status = session_data.get("consentimiento")
                        
                        # Manejar tanto boolean como string
                        if isinstance(consent_status, bool):
                            consent_given = consent_status
                        elif isinstance(consent_status, str):
                            consent_given = consent_status == "autorizado"
                        else:
                            consent_given = False
                            
                        consent_asked = consent_status is not None
                        self.logger.info(f"Consentimiento desde Firestore: {consent_status} (tipo: {type(consent_status)}), consent_given: {consent_given}")
                except Exception as e:
                    self.logger.warning(f"Error verificando consentimiento: {e}")
                
                # Recuperar datos adicionales de contexto desde Firestore
                session_data = {}
                try:
                    session_doc = self.consent_manager.session_manager.sessions_collection_ref.document(session_id).get()
                    if session_doc.exists:
                        session_data = session_doc.to_dict()
                        self.logger.info(f"Datos de sesión recuperados de Firestore: {list(session_data.keys())}")
                        
                        # Log específico para medicamentos para debug
                        if "pending_medications" in session_data:
                            medications = session_data["pending_medications"]
                            self.logger.info(f"Medicamentos encontrados en Firestore: {len(medications)} items")
                        else:
                            self.logger.warning(f"No se encontraron pending_medications en Firestore para sesión {session_id}")
                    else:
                        self.logger.warning(f"Documento de sesión {session_id} no existe en Firestore")
                except Exception as e:
                    self.logger.error(f"Error recuperando datos de sesión desde Firestore: {e}", exc_info=True)
                
                # Crear contexto combinando datos básicos y datos persistidos
                context = {
                    "session_id": session_id,
                    "phone": normalized_phone,
                    "phone_shared": True,
                    "consent_given": consent_given,
                    "consent_asked": consent_asked,
                    "prescription_uploaded": session_data.get("prescription_uploaded", False),
                    "detected_channel": "WA"
                }
                
                # Agregar datos específicos del flujo de medicamentos si existen
                if "pending_medications" in session_data:
                    context["pending_medications"] = session_data["pending_medications"]
                if "selected_undelivered" in session_data:
                    context["selected_undelivered"] = session_data["selected_undelivered"]
                if "medication_iteration_mode" in session_data:
                    context["medication_iteration_mode"] = session_data["medication_iteration_mode"]
                if "current_medication_index" in session_data:
                    context["current_medication_index"] = session_data["current_medication_index"]
                if "waiting_for_field" in session_data:
                    context["waiting_for_field"] = session_data["waiting_for_field"]
                if "patient_key" in session_data:
                    context["patient_key"] = session_data["patient_key"]
                if "waiting_for_tutela_field" in session_data:
                    context["waiting_for_tutela_field"] = session_data["waiting_for_tutela_field"]
                if "tutela_data_temp" in session_data:
                    context["tutela_data_temp"] = session_data["tutela_data_temp"]

                
                return context
            except Exception as e:
                self.logger.error(f"Error obteniendo contexto de sesión: {e}")
                return {}
        
        return {}

    def _update_session_context(self, phone_number: str, context: Dict[str, Any]) -> None:
        """Actualiza el contexto de sesión persistiendo en Firestore."""
        try:
            session_id = context.get("session_id")
            if not session_id or not self.consent_manager:
                self.logger.warning(f"No se puede actualizar contexto sin session_id o consent_manager")
                return
            
            # Actualizar en Firestore usando el session_manager
            session_ref = self.consent_manager.session_manager.sessions_collection_ref.document(session_id)
            
            # Preparar campos a actualizar (solo los que son serializables)
            update_fields = {}
            
            # Campos específicos que necesitamos persistir
            if "pending_medications" in context:
                update_fields["pending_medications"] = context["pending_medications"]
            if "selected_undelivered" in context:
                update_fields["selected_undelivered"] = context["selected_undelivered"]
            if "medication_iteration_mode" in context:
                update_fields["medication_iteration_mode"] = context["medication_iteration_mode"]
            if "current_medication_index" in context:
                update_fields["current_medication_index"] = context["current_medication_index"]
            if "waiting_for_field" in context:
                update_fields["waiting_for_field"] = context["waiting_for_field"]
            if "patient_key" in context:
                update_fields["patient_key"] = context["patient_key"]
            if "prescription_uploaded" in context:
                update_fields["prescription_uploaded"] = context["prescription_uploaded"]
            if "cuidador_nombre" in context:
                update_fields["cuidador_nombre"] = context["cuidador_nombre"]
            if "waiting_for_tutela_field" in context:
                update_fields["waiting_for_tutela_field"] = context["waiting_for_tutela_field"]
            if "tutela_data_temp" in context:
                update_fields["tutela_data_temp"] = context["tutela_data_temp"]
            if "last_escalate_cb" in context:
                update_fields["last_escalate_cb"] = context["last_escalate_cb"]
    

    
            
            if update_fields:
                session_ref.update(update_fields)
                self.logger.info(f"Contexto actualizado en Firestore para sesión {session_id}: {list(update_fields.keys())}")
                
                # Log específico para medicamentos
                if "pending_medications" in update_fields:
                    meds = update_fields["pending_medications"]
                    self.logger.info(f"Medicamentos guardados en Firestore: {len(meds)} items - {[m.get('nombre', 'sin nombre') for m in meds]}")
            else:
                self.logger.warning(f"No hay campos para actualizar en sesión {session_id}")
            
        except Exception as e:
            self.logger.error(f"Error actualizando contexto de sesión: {e}")
            # Log pero no fallar, para no romper el flujo

    async def _send_text_message(self, phone_number: str, message: str) -> None:
        """Envía un mensaje de texto."""
        try:
            normalized_phone = self.wa_client.validate_phone_number(phone_number)
            self.wa_client.send_text_message(normalized_phone, message)
            self.logger.info(f"Mensaje enviado a {phone_number}")
            self.logger.info(f"Contenido Mensaje {message}")
        except WhatsAppBusinessAPIError as e:
            self.logger.error(f"Error enviando mensaje a {phone_number}: {e}")

    async def _send_interactive_message(self, phone_number: str, message: str, buttons: List[Dict[str, str]]) -> None:
        """Envía un mensaje con botones interactivos."""
        try:
            normalized_phone = self.wa_client.validate_phone_number(phone_number)
            self.wa_client.send_interactive_message(normalized_phone, message, buttons)
            self.logger.info(f"Mensaje interactivo enviado a {phone_number}")
        except WhatsAppBusinessAPIError as e:
            self.logger.error(f"Error enviando mensaje interactivo a {phone_number}: {e}")

    async def _log_user_message(self, session_id: str, message_text: str, message_type: str = "conversation") -> None:
        """Registra un mensaje del usuario en la sesión."""
        if self.consent_manager and self.consent_manager.session_manager:
            self.consent_manager.session_manager.add_message_to_session(
                session_id, message_text, "user", message_type
            )

    def _close_user_session(self, session_id: str, phone_number: str, reason: str) -> None:
        """Cierra la sesión del usuario."""
        if self.consent_manager and self.consent_manager.session_manager:
            self.consent_manager.session_manager.close_session(session_id, reason=reason)
        self.logger.info(f"Sesión {session_id} cerrada por {reason}")

    async def _download_image(self, media_id: str) -> Optional[Path]:
        """Descarga una imagen de WhatsApp."""
        try:
            media_url = self.wa_client.get_media_url(media_id)
            if not media_url:
                return None
                
            image_data = self.wa_client.download_media(media_url)
            
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                temp_image_path = Path(temp_file.name)
                temp_file.write(image_data)
                
            return temp_image_path
            
        except Exception as e:
            self.logger.error(f"Error descargando imagen {media_id}: {e}")
            return None

    async def _send_medication_list(self, phone_number: str, medications: List[Dict], session_id: str) -> None:
        """Envía pregunta inicial sobre medicamentos entregados para WhatsApp."""
        try:
            # Mostrar la pregunta inicial con botones
            buttons = [
                {"text": "Ningún medicamento", "callback_data": f"med_none_{session_id}"},
                {"text": "Algunos", "callback_data": f"med_some_{session_id}"}
            ]
            
            await self._send_interactive_message(
                phone_number,
                "¿Qué medicamentos te entregaron?",
                buttons
            )
                
        except Exception as e:
            self.logger.error(f"Error enviando pregunta de medicamentos: {e}")
            await self._send_text_message(phone_number, "Error en el proceso. Continuando...")

    async def _handle_consent_response(self, phone_number: str, granted: bool, session_context: Dict[str, Any]) -> None:
        """Maneja la respuesta de consentimiento."""
        session_id = session_context.get("session_id")
        if not self.consent_manager or not session_id:
            await self._send_text_message(phone_number, "Error de sistema al procesar consentimiento.")
            return

        self.logger.info(f"Procesando consentimiento para {phone_number}, session_id: {session_id}, granted: {granted}")
        
        consent_status = "autorizado" if granted else "no autorizado"
        
        # Para WhatsApp, usar el phone_number como user_id
        success = self.consent_manager.handle_consent_response(phone_number, phone_number, consent_status, session_id)
        
        self.logger.info(f"Resultado del guardado de consentimiento: {success}")

        if success:
            # Actualizar Firestore directamente
            try:
                self.consent_manager.session_manager.update_consent_for_session(session_id, consent_status)
                self.logger.info(f"Consentimiento actualizado en Firestore: {consent_status}")
            except Exception as e:
                self.logger.error(f"Error actualizando consentimiento en Firestore: {e}")
            
            log_message = "Consentimiento otorgado" if granted else "Consentimiento denegado"
            await self._log_user_message(session_id, log_message, "consent_response")

            if granted:
                response_text = ("👩‍⚕️ Por favor, envíame una foto clara y legible de tu *fórmula médica* 📝\n\n"
                               "Es muy importante que la foto se vea bien para poder procesarla correctamente "
                               "y ayudarte con tu reclamación.\n\n⚠️ No podremos continuar si no recibimos una fórmula médica válida.")
            else:
                response_text = ("Entiendo tu decisión. Sin tu autorización no podemos continuar con el proceso. "
                               "Si cambias de opinión, solo escríbeme.")

            await self._send_text_message(phone_number, response_text)
        else:
            await self._send_text_message(phone_number, "Hubo un problema al guardar tu consentimiento.")

    async def _handle_field_response(self, phone_number: str, message_text: str, session_context: Dict[str, Any]) -> bool:
        """Maneja respuestas a campos faltantes siguiendo la lógica de Telegram."""
        try:
            waiting_for_field = session_context.get("waiting_for_field")
            if not waiting_for_field:
                return False  # No estamos esperando ningún campo
            
            patient_key = session_context.get("patient_key")
            session_id = session_context.get("session_id")
            
            if not patient_key or not session_id:
                await self._send_text_message(phone_number, "Error: No se encontró información del paciente.")
                return True
            
            # Manejo especial para campos de cuidador
            if waiting_for_field == "cuidador_nombre":
                session_context["cuidador_nombre"] = message_text.strip()
                session_context["waiting_for_field"] = "cuidador_cedula"
                self._update_session_context(phone_number, session_context)
                await self._send_text_message(phone_number, "📋 ¿Cuál es tu número de cédula?")
                return True
                
            elif waiting_for_field == "cuidador_cedula":
                session_doc = self.consent_manager.session_manager.sessions_collection_ref.document(session_id).get()
                session_data = session_doc.to_dict() if session_doc.exists else {}
                cuidador_nombre = session_data.get("cuidador_nombre", "")
                informante_data = [
                    {"nombre": cuidador_nombre, "parentesco": "Cuidador", "identificacion": message_text.strip()}
                ]
                success = self.claim_manager.update_informante_with_merge(patient_key, informante_data)

                if success:
                    
                    await self._log_user_message(session_id, f"Cuidador: {cuidador_nombre} - {message_text}", "informante_selection")
                    
                    # Limpiar campos temporales
                    session_context.pop("cuidador_nombre", None)
                    session_context.pop("informante_type", None)
                    session_context["waiting_for_field"] = None
                    self._update_session_context(phone_number, session_context)
                    
                    await self._continue_after_medication_selection(phone_number, session_context)
                else:
                    await self._send_text_message(phone_number, "❌ Error guardando información. Intenta nuevamente.")
                return True
                
            elif waiting_for_field == "fecha_nacimiento":
                # Validar formato de fecha
                normalized_date = self.claim_manager._normalize_date(message_text.strip())
                if not normalized_date:
                    await self._send_text_message(
                        phone_number,
                        "❌ Formato de fecha inválido. Por favor, ingresa tu fecha de nacimiento en formato DD/MM/AAAA (ej. 01/01/1990) o AAAA-MM-DD (ej. 1990-01-01)."
                    )
                    return True
                
                success = self.claim_manager.update_patient_field(patient_key, waiting_for_field, normalized_date)
                if success:
                
                    await self._log_user_message(session_id, f"Fecha de nacimiento: {normalized_date}", "field_response")
                    
                    session_context["waiting_for_field"] = None
                    self._update_session_context(phone_number, session_context)
                    await self._continue_after_medication_selection(phone_number, session_context)
                else:
                    await self._send_text_message(phone_number, "❌ Error guardando fecha. Intenta nuevamente.")
                return True
            else:
                # Campo genérico
                success = self.claim_manager.update_patient_field(patient_key, waiting_for_field, message_text.strip())
                
                if success:
                    
                    await self._log_user_message(session_id, f"{waiting_for_field}: {message_text}", "field_response")
                    
                    session_context["waiting_for_field"] = None
                    self._update_session_context(phone_number, session_context)
                    await self._continue_after_medication_selection(phone_number, session_context)
                else:
                    await self._send_text_message(phone_number, f"❌ Error registrando {waiting_for_field}. Intenta nuevamente.")
                return True
            
        except Exception as e:
            self.logger.error(f"Error manejando respuesta de campo: {e}")
            await self._send_text_message(phone_number, "Error procesando información. Intenta nuevamente.")
            return True

    async def _continue_with_missing_fields(self, phone_number: str, result: Dict, session_context: Dict[str, Any]) -> None:
        """Continúa el flujo para pedir campos faltantes después de procesar imagen."""
        try:
            patient_key = result["patient_key"]
            
            # Actualizar contexto con información del paciente
            session_context["patient_key"] = patient_key
            channel_type = session_context.get("detected_channel", "WA")
            self.claim_manager.update_patient_field(patient_key, "canal_contacto", channel_type)
            
            await self._continue_after_medication_selection(phone_number, session_context)
            
        except Exception as e:
            self.logger.error(f"Error continuando con campos faltantes: {e}")
            await self._send_text_message(phone_number, "Error continuando el proceso.")

    async def _handle_regimen_selection(self, phone_number: str, regimen_type: str, session_context: Dict[str, Any]) -> None:
        """Maneja selección de régimen de salud."""
        try:
            patient_key = session_context.get("patient_key")
            session_id = session_context.get("session_id")
            
            if not patient_key or not session_id:
                await self._send_text_message(phone_number, "Error: No se encontró información del paciente.")
                return
            
            # Actualizar el régimen en BigQuery
            success = self.claim_manager.update_patient_field(patient_key, "regimen", regimen_type)
            
            if success:
                
                await self._log_user_message(session_id, f"Régimen seleccionado: {regimen_type}", "regimen_selection")
                
                # Continuar con el siguiente campo faltante
                await self._continue_after_medication_selection(phone_number, session_context)
            else:
                await self._send_text_message(phone_number, "❌ Error registrando el régimen. Intenta nuevamente.")
                
        except Exception as e:
            self.logger.error(f"Error manejando selección de régimen: {e}")
            await self._send_text_message(phone_number, "Error procesando régimen. Intenta nuevamente.")

    async def _handle_informante_selection(self, phone_number: str, informante_type: str, session_context: Dict[str, Any]) -> None:
        """Maneja selección de informante siguiendo la lógica de Telegram."""
        try:
            patient_key = session_context.get("patient_key")
            session_id = session_context.get("session_id")
            
            if not patient_key or not session_id:
                await self._send_text_message(phone_number, "Error: No se encontró información del paciente.")
                return
            
            if informante_type == "paciente":
                # Si es el paciente, usar sus datos existentes
                patient_record = self.claim_manager._get_patient_data(patient_key)
                patient_name = patient_record.get("nombre_paciente", "Paciente")
                patient_doc = patient_record.get("numero_documento", "")

                informante_data = [
                    {"nombre": patient_name, "parentesco": "Mismo paciente", "identificacion": patient_doc}
                ]
                success = self.claim_manager.update_informante_with_merge(patient_key, informante_data)

                if success:

                    await self._log_user_message(session_id, "Informante seleccionado: paciente", "informante_selection")
                    # Continuar con el siguiente campo faltante
                    await self._continue_after_medication_selection(phone_number, session_context)
                else:
                    await self._send_text_message(phone_number, "❌ Error registrando información. Intenta nuevamente.")
            else:
                # Si es cuidador, solicitar nombre
                await self._send_text_message(phone_number, "👥 ¿Cuál es tu nombre completo?")
                session_context["waiting_for_field"] = "cuidador_nombre"
                session_context["informante_type"] = "cuidador"
                self._update_session_context(phone_number, session_context)
                
        except Exception as e:
            self.logger.error(f"Error manejando selección de informante: {e}")
            await self._send_text_message(phone_number, "Error procesando informante. Intenta nuevamente.")

    async def _handle_medication_selection(self, phone_number: str, callback_data: str, session_context: Dict[str, Any]) -> None:
        """Maneja selección de medicamentos."""
        try:
            session_id = session_context.get("session_id")
            medications = session_context.get("pending_medications", [])
            patient_key = session_context.get("patient_key")
            
            # Logging detallado para debug
            self.logger.info(f"=== DEBUG MEDICATION SELECTION ===")
            self.logger.info(f"Phone: {phone_number}")
            self.logger.info(f"Callback: {callback_data}")
            self.logger.info(f"Session ID: {session_id}")
            self.logger.info(f"Patient Key: {patient_key}")
            self.logger.info(f"Medications count: {len(medications)}")
            self.logger.info(f"Session context keys: {list(session_context.keys())}")
            if medications:
                self.logger.info(f"Medications: {[med.get('nombre', 'sin nombre') for med in medications]}")
            
            if not session_id or not medications:
                self.logger.error(f"Missing data - session_id: {session_id}, medications: {len(medications)}")
                await self._send_text_message(phone_number, "Error: No hay medicamentos pendientes de selección.")
                return
            
            # Manejo de "Ningún medicamento" - todos son no entregados
            if callback_data.startswith("med_none_"):
                # Marcar todos los medicamentos como no entregados
                undelivered_med_names = [med.get("nombre", "") for med in medications]
                
                # Actualizar en BigQuery
                success = self.claim_manager.update_undelivered_medicines(patient_key, session_id, undelivered_med_names)
                
                if success:

                    await self._log_user_message(session_id, f"Medicamentos no entregados: {', '.join(undelivered_med_names)}", "medication_selection")
                    
                    # Continuar con el siguiente paso del flujo
                    await self._continue_after_medication_selection(phone_number, session_context)
                else:
                    await self._send_text_message(phone_number, "❌ Hubo un error registrando los medicamentos. Intenta nuevamente.")
                    
            # Manejo de "Algunos" - iterar por cada medicamento
            elif callback_data.startswith("med_some_"):
                # Iniciar iteración individual por medicamento
                session_context["medication_iteration_mode"] = True
                session_context["current_medication_index"] = 0
                session_context["selected_undelivered"] = []
                self._update_session_context(phone_number, session_context)
                
                await self._ask_about_current_medication(phone_number, session_context)
                
            # Manejo de respuestas individuales por medicamento
            elif callback_data.startswith("med_individual_"):
                await self._handle_individual_medication_response(phone_number, callback_data, session_context)
                
        except Exception as e:
            self.logger.error(f"Error manejando selección de medicamentos: {e}", exc_info=True)
            await self._send_text_message(phone_number, "Ocurrió un error procesando tu selección. Intenta nuevamente.")

    async def _handle_followup_response(self, phone_number: str, callback_data: str, session_context: Dict[str, Any]) -> None:
        """Maneja respuestas de seguimiento para WhatsApp (equivalente a Telegram)."""
        try:
            self.logger.info(f"Procesando followup de WhatsApp: {callback_data}")
            
            if session_context.get("waiting_for_tutela_field"):
                handled = await self.handle_tutela_field_response(phone_number, callback_data, session_context)
                if handled:
                    return

            
            if callback_data.startswith("followup_yes_"):
                session_id = callback_data[len("followup_yes_"):]
                self.logger.info(f"✅ Paciente confirmó medicamentos recibidos para session: {session_id}")
                
                try:
                   
                    from patient_module.patient_module import PatientModule
                    pm = PatientModule()
                    success = pm.update_reclamation_status(session_id, "resuelto")
                    
                    if success:
                        await self._send_text_message(
                            phone_number,
                            "✅ *¡Excelente!*\n\n"
                            "Tu caso ha sido marcado como resuelto.\n"
                            "¡Gracias por confiar en nosotros! 🎉"
                        )
                    else:
                        await self._send_text_message(
                            phone_number,
                            "⚠️ Hubo un error actualizando tu caso.\n"
                            "Nuestro equipo lo revisará manualmente."
                        )
                except Exception as e:
                    self.logger.error(f"Error actualizando reclamación para session {session_id}: {e}")
                    await self._send_text_message(
                        phone_number,
                        "❌ Error técnico. Por favor inténtalo más tarde."
                    )
            
            elif callback_data.startswith("followup_no_"):
                session_id = callback_data[len("followup_no_"):]
                self.logger.info(f"❌ Paciente NO recibió medicamentos para session: {session_id}")
                
                try:
                    from claim_manager.claim_generator import determinar_tipo_reclamacion_siguiente
                    tipo_reclamacion = determinar_tipo_reclamacion_siguiente(session_id)
                    
                    buttons = [
                        {"text": "✅ Sí, quiero escalar", "callback_data": f"escalate_yes_{session_id}"},
                        {"text": "❌ No, por ahora no", "callback_data": f"escalate_no_{session_id}"}
                    ]
                    
                    await self._send_interactive_message(
                        phone_number,
                        f"💔 Lamento que no hayas recibido tus medicamentos.\n\n"
                        f"¿Deseas escalar tu caso y entablar *{tipo_reclamacion}*?",
                        buttons
                    )
                    
                except Exception as e:
                    self.logger.error(f"Error mostrando pregunta de escalamiento para session {session_id}: {e}")
                    await self._send_text_message(
                        phone_number,
                        "❌ Error mostrando opciones de escalamiento."
                    )

                    
        except Exception as e:
            self.logger.error(f"Error manejando followup de WhatsApp: {e}")
            await self._send_text_message(phone_number, "Error procesando tu respuesta. Intenta nuevamente.")
      

    async def _handle_escalate_response(self, phone_number: str, callback_data: str, session_context: Dict[str, Any]) -> None:
        """Maneja respuestas de escalamiento (escalate_yes_, escalate_no_)."""
         
        sm = self.consent_manager.session_manager
        db = sm.db                                         # cliente ya conectado a "historia"
        dedup_ref = db.collection("wa_callbacks").document(callback_data)

        loop = asyncio.get_running_loop()

        def _try_create():
            try:
                dedup_ref.create({"ts": firestore.SERVER_TIMESTAMP})
                return True           # primera vez
            except AlreadyExists:
                return False          # duplicado

        is_first = await loop.run_in_executor(None, _try_create)

        if not is_first:
            self.logger.warning("⚠️ Callback duplicado ignorado")
            return                   # ✋ salimos SIN escalar
        
        
        try:
            
            if callback_data.startswith("escalate_yes_"):
                session_id = callback_data[len("escalate_yes_"):]
                self.logger.info(f"✅ Paciente ACEPTA escalar para session: {session_id}")
                await self._send_text_message(
                        phone_number,
                        "🔄 *Procesando escalamiento...*\n\nEvaluando el mejor siguiente paso para tu caso."
                    )
                
                try:
                    from claim_manager.claim_generator import auto_escalate_patient
                    resultado = auto_escalate_patient(session_id)
                    self.logger.info(f"[WA] Resultado auto_escalate_patient: {resultado}")
                    
                    if resultado.get("requiere_recoleccion_tutela") and resultado.get("tipo") == "desacato":
                        patient_key = resultado["patient_key"]
                        
                
                        datos_existentes = self.claim_manager.get_existing_tutela_data(patient_key)
                        
                        if datos_existentes:
                            
                            from claim_manager.claim_generator import generar_desacato
                            resultado_desacato = generar_desacato(patient_key, datos_existentes)
                            
                            if resultado_desacato.get("success"):
                                await self._send_text_message(
                                    phone_number,
                                    "✅ *Desacato generado exitosamente*\n\n"
                                    f"Nivel de escalamiento: *5*\n\n"
                                    "Tu incidente de desacato ha sido preparado automáticamente usando los datos de tu tutela previa."
                                )
                            else:
                                await self._send_text_message(
                                    phone_number,
                                    "❌ Error generando desacato automáticamente."
                                )
                        else:
                            
                            field_prompt = self.claim_manager.get_next_missing_tutela_field_prompt(patient_key, {})
                            
                            await self._send_text_message(
                                phone_number,
                                "🔄 *Para proceder con el desacato necesito datos de tu tutela:*\n\n"
                                f"{field_prompt['prompt_text']}"
                            )
                            
                    
                            session_context["waiting_for_tutela_field"] = field_prompt["field_name"]
                            session_context["patient_key"] = patient_key
                            session_context["tutela_data_temp"] = {}
                            self._update_session_context(phone_number, session_context)
                        
                        return
                    
                    if resultado.get("success"):
                        tipo = resultado.get("tipo", "escalamiento")
                        razon = resultado.get("razon", "")
                        nivel = resultado.get("nivel_escalamiento", "")
                        patient_key = resultado.get("patient_key", "")
                        
                        self.logger.info(f"✅ Escalamiento exitoso para session {session_id} → patient {patient_key}: {tipo}")
                        
                        if tipo == "sin_escalamiento":
                            await self._send_text_message(
                                phone_number,
                                f"Caso en revision."
                                f"Te contactaremos pronto para darle seguimiento al proceso."
                            )
                        elif tipo.startswith("multiple_"):
                            
                            tipos_generados = tipo.replace("multiple_", "").replace("_", " y ").replace("reclamacion", "reclamación")
                            await self._send_text_message(
                                phone_number,
                                f"✅ *Escalamiento múltiple exitoso*\n\n"
                                f"Se han generado: *{tipos_generados}*\n"
                                f"Nivel de escalamiento: *{nivel}*\n\n"
                                f"📋 *Motivo:* {razon}\n\n"
                                f"Nuestro equipo procesará ambas reclamaciones y te mantendremos informado."
                            )
                        else:
                            
                            tipo_legible = tipo.replace("_", " ").replace("reclamacion", "reclamación").title()
                            await self._send_text_message(
                                phone_number,
                                f"✅ *{tipo_legible} generada exitosamente*\n\n"
                                f"Nivel de escalamiento: *{nivel}*\n\n"
                                f"📋 *Motivo:* {razon}\n\n"
                                f"Tu caso ha sido escalado automáticamente. Nuestro equipo procesará tu solicitud y te mantendremos informado del progreso."
                            )
                    else:
                        error = resultado.get("error", "Error desconocido")
                        self.logger.error(f"Error en escalamiento automático para session {session_id}: {error}")
                        await self._send_text_message(
                            phone_number,
                            "⚠️ *Error en escalamiento automático*\n\n"
                            "Hubo un problema procesando tu caso automáticamente.\n"
                            "Nuestro equipo revisará tu solicitud manualmente y te contactará pronto."
                        )
                        
                except Exception as e:
                    self.logger.error(f"Error ejecutando escalamiento para session {session_id}: {e}")
                    await self._send_text_message(
                        phone_number,
                        "❌ *Error técnico*\n\n"
                        "Ocurrió un problema técnico procesando tu escalamiento.\n"
                        "Por favor contacta a nuestro equipo de soporte."
                    )
            
            elif callback_data.startswith("escalate_no_"):
                session_id = callback_data[len("escalate_no_"):]
                self.logger.info(f"❌ Paciente RECHAZA escalar para session: {session_id}")
                
                await self._send_text_message(
                    phone_number,
                    "📝 *Entendido*\n\n"
                    "Respetamos tu decisión. Si más adelante deseas continuar con el escalamiento, "
                    "puedes contactarnos nuevamente.\n\n"
                    "✅ Tu caso queda registrado por si necesitas ayuda futura.\n\n"
                    "¡Gracias por confiar en nosotros!"
                )
                
                
                try:
                    self._close_user_session(session_id, phone_number, reason="user_declined_escalation")
                except Exception as e:
                    self.logger.warning(f"Error cerrando sesión {session_id}: {e}")
        finally:
            async def _del_later():
                await asyncio.sleep(60)
                try:
                    await loop.run_in_executor(None, dedup_ref.delete)
                except Exception as e:
                    self.logger.debug(f"No pude borrar dedup {dedup_ref.id}: {e}")

            asyncio.create_task(_del_later())           


    async def _ask_about_current_medication(self, phone_number: str, session_context: Dict[str, Any]) -> None:
        """Pregunta sobre el medicamento actual en la iteración."""
        try:
            medications = session_context.get("pending_medications", [])
            current_index = session_context.get("current_medication_index", 0)
            session_id = session_context.get("session_id")
            
            if current_index >= len(medications):
                # Terminamos la iteración
                await self._finish_medication_iteration(phone_number, session_context)
                return
            
            current_med = medications[current_index]
            med_name = current_med.get("nombre", f"Medicamento {current_index + 1}")
            med_dosis = current_med.get("dosis", "")
            
            # Crear mensaje descriptivo
            med_display = f"{med_name}"
            if med_dosis and med_dosis.strip():
                med_display += f" {med_dosis}"
            
            buttons = [
                {"text": "Sí", "callback_data": f"med_individual_yes_{session_id}_{current_index}"},
                {"text": "No", "callback_data": f"med_individual_no_{session_id}_{current_index}"}
            ]
            
            await self._send_interactive_message(
                phone_number,
                f"¿Te entregaron {med_display.upper()}?",
                buttons
            )
            
        except Exception as e:
            self.logger.error(f"Error preguntando sobre medicamento actual: {e}")
            await self._send_text_message(phone_number, "Error en el proceso. Continuando...")

    async def _handle_individual_medication_response(self, phone_number: str, callback_data: str, session_context: Dict[str, Any]) -> None:
        """Maneja respuesta individual sobre un medicamento específico."""
        try:
            # Extraer información del callback_data
            # Format: med_individual_yes_sessionid_index o med_individual_no_sessionid_index
            parts = callback_data.split("_")
            if len(parts) < 5:
                await self._send_text_message(phone_number, "Error procesando respuesta.")
                return
            
            response = parts[2]  # "yes" o "no"
            med_index = int(parts[-1])
            
            medications = session_context.get("pending_medications", [])
            selected_undelivered = session_context.get("selected_undelivered", [])
            
            if med_index >= len(medications):
                await self._send_text_message(phone_number, "Error: Medicamento no encontrado.")
                return
            
            current_med = medications[med_index]
            med_name = current_med.get("nombre", f"Medicamento {med_index + 1}")
            
            # Si respondió "No" (no se lo entregaron), agregarlo a la lista
            if response == "no":
                selected_undelivered.append(med_index)
                await self._send_text_message(phone_number, f"✅ Registrado: {med_name} - NO entregado")
            else:
                await self._send_text_message(phone_number, f"✅ Registrado: {med_name} - Entregado")
            
            # Actualizar contexto
            session_context["selected_undelivered"] = selected_undelivered
            session_context["current_medication_index"] = med_index + 1
            self._update_session_context(phone_number, session_context)
            
            # Continuar con el siguiente medicamento
            await self._ask_about_current_medication(phone_number, session_context)
            
        except Exception as e:
            self.logger.error(f"Error manejando respuesta individual de medicamento: {e}")
            await self._send_text_message(phone_number, "Error procesando respuesta. Continuando...")

    async def _finish_medication_iteration(self, phone_number: str, session_context: Dict[str, Any]) -> None:
        """Finaliza la iteración de medicamentos y actualiza BigQuery."""
        try:
            session_id = session_context.get("session_id")
            medications = session_context.get("pending_medications", [])
            selected_undelivered = session_context.get("selected_undelivered", [])
            patient_key = session_context.get("patient_key")
            
            # Extraer nombres de medicamentos no entregados
            undelivered_med_names = []
            for index in selected_undelivered:
                if index < len(medications):
                    med_name = medications[index].get("nombre", "")
                    if med_name:
                        undelivered_med_names.append(med_name)
            
            # Log detallado antes de actualizar BigQuery
            self.logger.info(f"=== FINALIZANDO ITERACIÓN DE MEDICAMENTOS ===")
            self.logger.info(f"Patient key: {patient_key}")
            self.logger.info(f"Session ID: {session_id}")
            self.logger.info(f"Selected undelivered indices: {selected_undelivered}")
            self.logger.info(f"Total medications: {len(medications)}")
            self.logger.info(f"Undelivered medicine names: {undelivered_med_names}")
            
            # Actualizar en BigQuery
            success = self.claim_manager.update_undelivered_medicines(patient_key, session_id, undelivered_med_names)
            
            self.logger.info(f"BigQuery update result: {success}")
            
            if success:
                if undelivered_med_names:
                    await self._send_text_message(phone_number, f"✅ Proceso completado. Medicamentos no entregados registrados: {', '.join(undelivered_med_names)}")
                else:
                    await self._send_text_message(phone_number, "✅ Proceso completado. Todos los medicamentos fueron entregados.")
                
                await self._log_user_message(session_id, f"Medicamentos no entregados: {', '.join(undelivered_med_names)}", "medication_selection")
                
                # Limpiar contexto de iteración
                session_context["medication_iteration_mode"] = False
                session_context["current_medication_index"] = 0
                session_context["selected_undelivered"] = []
                self._update_session_context(phone_number, session_context)
                
                # Continuar con el siguiente paso del flujo
                await self._continue_after_medication_selection(phone_number, session_context)
            else:
                await self._send_text_message(phone_number, "❌ Hubo un error registrando los medicamentos. Intenta nuevamente.")
                
        except Exception as e:
            self.logger.error(f"Error finalizando iteración de medicamentos: {e}")
            await self._send_text_message(phone_number, "Error completando el proceso. Intenta nuevamente.")

    async def _continue_after_medication_selection(self, phone_number: str, session_context: Dict[str, Any]) -> None:
        """Continúa el flujo después de seleccionar medicamentos usando la misma lógica que Telegram."""
        try:
            patient_key = session_context.get("patient_key")
            session_id = session_context.get("session_id")
            
            if not patient_key or not session_id:
                await self._send_text_message(phone_number, "Error: No se encontró información del paciente.")
                return
            
            # Actualizar canal de contacto
            self.claim_manager.update_patient_field(patient_key, "canal_contacto", "WA")
            
            # Obtener el siguiente campo faltante usando la misma función que Telegram
            field_prompt = self.claim_manager.get_next_missing_field_prompt(patient_key)
            
            # Log para debugging
            self.logger.info(f"=== PROMPT NEXT MISSING FIELD ===")
            self.logger.info(f"Patient key: {patient_key}")
            self.logger.info(f"Session ID: {session_id}")
            self.logger.info(f"Field prompt: {field_prompt}")
            
            if field_prompt and field_prompt.get("field_name"):
                # Hay campos faltantes, solicitar el siguiente
                field_name = field_prompt["field_name"]
                session_context["waiting_for_field"] = field_name
                session_context["patient_key"] = patient_key
                self._update_session_context(phone_number, session_context)
                
                await self._handle_missing_field_prompt(phone_number, field_prompt, session_context)
            else:
                # Todos los campos completos, generar reclamación
                self.logger.info(f"Todos los campos completos para paciente {patient_key}. Iniciando generación de reclamación.")
                await self._generate_final_claim(phone_number, session_context)
                
        except Exception as e:
            self.logger.error(f"Error continuando después de selección de medicamentos: {e}")
            await self._send_text_message(phone_number, "Error continuando el proceso.")

    async def _handle_missing_field_prompt(self, phone_number: str, field_prompt: Dict[str, Any], session_context: Dict[str, Any]) -> None:
        """Maneja la solicitud de campos faltantes siguiendo la lógica de Telegram."""
        try:
            field_name = field_prompt.get("field_name", "")
            prompt_text = field_prompt.get("prompt_text", field_prompt.get("prompt", ""))
            
            # Log para debugging
            self.logger.info(f"=== MISSING FIELD PROMPT ===")
            self.logger.info(f"Field name: {field_name}")
            self.logger.info(f"Prompt text: '{prompt_text}'")
            self.logger.info(f"Full field_prompt: {field_prompt}")
            
            # Validar que prompt_text no esté vacío
            if not prompt_text or prompt_text.strip() == "":
                self.logger.warning(f"Prompt text vacío para campo {field_name}, usando prompt por defecto")
                # Prompts por defecto según el campo
                if field_name == "informante":
                    prompt_text = "👤 Para continuar, necesito saber:"
                elif field_name == "regimen":
                    prompt_text = "🏥 ¿Cuál es tu régimen de salud?"
                elif field_name == "telefono":
                    prompt_text = "📞 Por favor, proporciona tu número de teléfono:"
                elif field_name == "correo":
                    prompt_text = "📧 Por favor, proporciona tu correo electrónico:"
                elif field_name == "direccion":
                    prompt_text = "🏠 Por favor, proporciona tu dirección:"
                elif field_name == "fecha_nacimiento":
                    prompt_text = "📅 ¿Cuál es tu fecha de nacimiento? (DD/MM/AAAA)"
                else:
                    prompt_text = f"Por favor, proporciona tu {field_name}:"
            
            # Marcar que estamos esperando respuesta para este campo
            session_context["waiting_for_field"] = field_name
            session_context["patient_key"] = session_context.get("patient_key")
            self._update_session_context(phone_number, session_context)
            
            if field_name == "informante":
                # Solicitar si es paciente o cuidador
                buttons = [
                    {"text": "👤 Soy el paciente", "callback_data": "informante_paciente"},
                    {"text": "👥 Soy el cuidador", "callback_data": "informante_cuidador"}
                ]
                await self._send_interactive_message(phone_number, prompt_text, buttons)
                
            elif field_name == "regimen":
                # Solicitar régimen de salud
                buttons = [
                    {"text": "✅ Contributivo", "callback_data": "regimen_contributivo"},
                    {"text": "🤝 Subsidiado", "callback_data": "regimen_subsidiado"}
                ]
                await self._send_interactive_message(phone_number, prompt_text, buttons)
                
            else:
                # Campo de texto libre
                await self._send_text_message(phone_number, prompt_text)
                
        except Exception as e:
            self.logger.error(f"Error manejando campo faltante: {e}")
            await self._send_text_message(phone_number, "Error procesando información. Continuando...")

    async def _generate_final_claim(self, phone_number: str, session_context: Dict[str, Any]) -> None:
        """Genera la reclamación final siguiendo la lógica completa de Telegram."""
        try:
            patient_key = session_context.get("patient_key")
            session_id = session_context.get("session_id")
            
            if not session_id:
                self.logger.error(f"No se encontró session_id para paciente {patient_key}")
                session_id = "unknown_session"
            
            # 1. GENERAR RECLAMACIÓN EPS AUTOMÁTICAMENTE
            try:
                from claim_manager.claim_generator import generar_reclamacion_eps, validar_disponibilidad_supersalud
                resultado_reclamacion = generar_reclamacion_eps(patient_key)
                
                if resultado_reclamacion.get("success"):
                    # 2. GUARDAR EN TABLA RECLAMACIONES
                    success_saved = await self._save_reclamation_to_database(
                        patient_key=patient_key,
                        tipo_accion="reclamacion_eps",
                        texto_reclamacion=resultado_reclamacion["texto_reclamacion"],
                        estado_reclamacion="pendiente_radicacion",
                        nivel_escalamiento=1,
                        session_id=session_id,
                        resultado_claim_generator=resultado_reclamacion
                    )
                    
                    if success_saved:
                        self.logger.info(f"Reclamación EPS generada y guardada exitosamente para paciente {patient_key}")
                        supersalud_disponible = validar_disponibilidad_supersalud()

                        if supersalud_disponible.get("disponible"):
                            success_message = (
                                "🎉 ¡Perfecto! Ya tenemos toda la información necesaria para radicar tu reclamación.\n\n"
                                "📄 **Reclamación EPS generada exitosamente**\n\n"
                                "📋 En las próximas 48 horas te enviaremos el número de radicado.\n\n"
                                "🔄 **Sistema de escalamiento activado:**\n"
                                "• Si no hay respuesta en el plazo establecido, automáticamente escalaremos tu caso a la Superintendencia Nacional de Salud\n"
                                "• Te mantendremos informado en cada paso del proceso\n\n"
                                "✅ Proceso completado exitosamente. Si necesitas algo más, no dudes en contactarnos.\n\n"
                                 "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
                            )
                        else:
                            success_message = (
                                "🎉 ¡Perfecto! Ya tenemos toda la información necesaria para radicar tu reclamación.\n\n"
                                "📄 **Reclamación EPS generada exitosamente**\n\n"
                                "📋 En las próximas 48 horas te enviaremos el número de radicado.\n\n"
                                "✅ Proceso completado exitosamente. Si necesitas algo más, no dudes en contactarnos.\n\n"
                                "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
                            )
                    else:
                        self.logger.error(f"Error guardando reclamación para paciente {patient_key}")
                        success_message = ( 
                            "⚠️ Se completó la recopilación de datos, pero hubo un problema técnico guardando tu reclamación.\n\n"
                            "📞 Nuestro equipo revisará tu caso manualmente.\n\n"
                            "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
                        )
                else:
                    self.logger.error(f"Error generando reclamación EPS para paciente {patient_key}: {resultado_reclamacion.get('error', 'Error desconocido')}")
                    success_message = (
                        "⚠️ Se completó la recopilación de datos, pero hubo un problema técnico generando tu reclamación.\n\n"
                        "📞 Nuestro equipo revisará tu caso manualmente.\n\n"
                        "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
                    )
                    
            except Exception as e:
                self.logger.error(f"Error inesperado en generación de reclamación para paciente {patient_key}: {e}")
                success_message = (
                    "⚠️ Se completó la recopilación de datos. Nuestro equipo procesará tu reclamación manualmente.\n\n"
                    "📞 Te contactaremos pronto.\n\n"
                    "🚪 Esta sesión se cerrará ahora. ¡Gracias por confiar en nosotros!"
                )
            
            # 3. ENVIAR MENSAJE FINAL
            await self._send_text_message(phone_number, success_message)
            await self._log_user_message(session_id, success_message, "completion_message")
            
            # 4. CERRAR SESIÓN
            if session_id:
                self._close_user_session(session_id, phone_number, reason="process_completed_with_claim")
            
            self.logger.info(f"Proceso completo finalizado para paciente {patient_key} - sesión cerrada")
                
        except Exception as e:
            self.logger.error(f"Error generando reclamación final: {e}")
            await self._send_text_message(phone_number, "Error generando reclamación. Intenta nuevamente.")

    async def _save_reclamation_to_database(self, patient_key: str, tipo_accion: str, texto_reclamacion: str, 
                                          estado_reclamacion: str, nivel_escalamiento: int, session_id: str,
                                          resultado_claim_generator: Dict[str, Any] = None) -> bool:
        """Guarda la reclamación en BigQuery usando la función de Telegram."""
        try:
            # Importar y usar directamente la función de Telegram para evitar duplicar código
            from channels.telegram_c import save_reclamacion_to_database
            
            success = await save_reclamacion_to_database(
                patient_key, tipo_accion, texto_reclamacion, 
                estado_reclamacion, nivel_escalamiento, session_id, resultado_claim_generator
            )
            
            self.logger.info(f"Reclamación guardada en BigQuery: {success}")
            return success
                
        except Exception as e:
            self.logger.error(f"Error guardando reclamación: {e}")
            return False

    async def _check_basic_fields_and_proceed(self, phone_number: str, session_context: Dict[str, Any]) -> None:
        """Verifica campos básicos manualmente si get_next_missing_field_prompt falla."""
        try:
            patient_key = session_context.get("patient_key")
            
            # Lista de campos básicos a verificar manualmente
            basic_fields = [
                ("informante", "¿Eres el paciente o un cuidador?"),
                ("regimen", "¿A qué régimen de salud perteneces?"),
                ("telefono", "Por favor, proporciona tu número de teléfono:"),
                ("correo", "Por favor, proporciona tu correo electrónico:")
            ]
            
            self.logger.info(f"Verificando campos básicos manualmente para patient_key: {patient_key}")
            
            # Obtener datos del paciente para verificar qué campos faltan
            try:
                datos_paciente = self.claim_manager.obtener_datos_paciente(patient_key)
                self.logger.info(f"Datos obtenidos del paciente: {list(datos_paciente.keys())}")
                
                # Verificar cada campo básico
                for field_name, prompt_text in basic_fields:
                    field_value = datos_paciente.get(field_name)
                    if not field_value or field_value in ["", None, "None"]:
                        self.logger.info(f"Campo faltante encontrado: {field_name}")
                        
                        # Crear field_prompt manual
                        field_prompt = {
                            "field_name": field_name,
                            "prompt": prompt_text,
                            "field_type": "selection" if field_name in ["informante", "regimen"] else "text"
                        }
                        
                        await self._handle_missing_field_prompt(phone_number, field_prompt, session_context)
                        return
                
                # Si llegamos aquí, todos los campos básicos están completos
                self.logger.info("Todos los campos básicos están completos, generando reclamación")
                await self._generate_final_claim(phone_number, session_context)
                
            except Exception as e:
                self.logger.error(f"Error obteniendo datos del paciente: {e}")
                # Como fallback, preguntar por informante que siempre es necesario
                field_prompt = {
                    "field_name": "informante",
                    "prompt": "¿Eres el paciente o un cuidador?",
                    "field_type": "selection"
                }
                await self._handle_missing_field_prompt(phone_number, field_prompt, session_context)
                
        except Exception as e:
            self.logger.error(f"Error verificando campos básicos: {e}")
            await self._send_text_message(phone_number, "Error verificando información. Generando reclamación con datos disponibles...")
            await self._generate_final_claim(phone_number, session_context)

    async def _check_automatic_escalation(self, phone_number: str, patient_key: str, session_id: str) -> None:
        """Verifica si hay escalamiento automático disponible."""
        try:
            # Implementar lógica de escalamiento automático si está disponible
            # Por ahora, solo enviamos mensaje informativo
            await self._send_text_message(phone_number, "🔄 Verificando disponibilidad de escalamiento automático...")
            
            # Aquí iría la lógica de escalamiento automático similar a Telegram
            
        except Exception as e:
            self.logger.error(f"Error verificando escalamiento automático: {e}")

    async def handle_tutela_field_response(self, phone, message, session_context):
        patient_key = session_context.get("patient_key")
        waiting = session_context.get("waiting_for_tutela_field")
        if not (patient_key and waiting):
            return False

        # 1) Acumular en tutel​a_data_temp (no en datos_tutela)
        temp = session_context.get("tutela_data_temp", {})
        # Normaliza fechas si toca
        if waiting in ["fecha_sentencia", "fecha_radicacion_tutela"]:
            norm = self.claim_manager._normalize_date(message)
            if not norm:
                await self._send_text_message(phone, 
                    "❌ Formato de fecha inválido. Usa DD/MM/AAAA.")
                return True
            temp[waiting] = norm
        else:
            temp[waiting] = message.strip()
        session_context["tutela_data_temp"] = temp
        self._update_session_context(phone, session_context)

        # 2) Pedir siguiente
        next_prompt = self.claim_manager.get_next_missing_tutela_field_prompt(patient_key, temp)
        if next_prompt.get("field_name"):
            session_context["waiting_for_tutela_field"] = next_prompt["field_name"]
            self._update_session_context(phone, session_context)
            await self._send_text_message(phone, next_prompt["prompt_text"])
            return True

        # 3) Si ya no faltan más, limpiar y generar desacato
        session_context.pop("waiting_for_tutela_field", None)
        session_context.pop("tutela_data_temp",       None)
        self._update_session_context(phone, session_context)

        success = self.claim_manager.save_tutela_data_simple(patient_key, temp)
        if success:
            await self._send_text_message(phone, 
                "✅ Datos de tutela guardados. 🔄 Generando incidente de desacato...")
   
            # 2) Generar el desacato
            # usar los datos que en Firestore que se guardaron en 'tutela_data_temp'
            desv_data = session_context.get("tutela_data_temp", {})
            resultado_desacato = generar_desacato(patient_key, desv_data)

            # 3) Guardar la reclamación de desacato
            if resultado_desacato.get("success"):
                guardado = await self._save_reclamation_to_database(
                    patient_key=patient_key,
                    tipo_accion="desacato",
                    texto_reclamacion=resultado_desacato["texto_reclamacion"],
                    estado_reclamacion="pendiente_radicacion",
                    nivel_escalamiento=5,
                    session_id=session_context.get("session_id", ""),
                    resultado_claim_generator=resultado_desacato
                )
                if guardado:
                    await self._send_text_message(phone,
                        "🎉 ¡Desacato generado y guardado exitosamente!")
                else:
                    await self._send_text_message(phone,
                        "⚠️ Desacato generado, pero hubo un error guardándolo.")
            else:
                await self._send_text_message(phone,
                    "❌ Error generando tu desacato automáticamente.")


        else:
            await self._send_text_message(phone, 
                "⚠️ No pude guardar los datos de tu tutela. Intenta nuevamente.")
        return True

            