import os
import time
import logging
import json
from dotenv import load_dotenv

from session_manager.session_manager import SessionManager, SessionManagerError
from google.cloud import firestore # Importar firestore para manejar Timestamps en los tests

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

session_manager = None

try:
    session_manager = SessionManager()
except SessionManagerError as e:
    logger.critical(f"Error fatal al inicializar SessionManager en las pruebas: {e}. Las pruebas no se ejecutarán.")

def run_session_tests():
    """Ejecuta un conjunto de pruebas para el SessionManager (Firestore)."""
    if session_manager is None:
        logger.error("SessionManager no pudo ser inicializado. Saltando pruebas.")
        return

    # Usaremos un número de teléfono real simulado como identificador
    user_phone_number_for_test = "3001234567" # Este será el identificador en el ID de sesión
    channel = "TL"

    try:
        logger.info("🚀 === INICIO DE PRUEBAS DEL SESSION_MANAGER (FIRESTORE) ===\n")

        # 1. Crear sesión (esto crea un documento en Firestore)
        logger.info("1️⃣ Creando o recuperando una sesión activa en Firestore con número de teléfono...")
        session_info = session_manager.create_session_with_history_check(user_phone_number_for_test, channel)
        session_id = session_info["new_session_id"]

        logger.info(f"✅ Sesión activa gestionada: {session_id}")
        logger.info(f"📊 Historial previo detectado: {session_info['has_previous_history']}")
        logger.info(f"📈 Sesiones previas encontradas: {session_info['previous_sessions_count']}")

        # Pequeña pausa para asegurar la escritura, aunque Firestore es rápido
        time.sleep(1) 

        # 2. Agregar mensajes al historial de la sesión (actualiza el documento en Firestore)
        logger.info(f"\n2️⃣ Agregando mensajes al historial de la sesión en Firestore...")
        session_manager.add_message_to_session(session_id, "Hola, necesito ayuda", "user")
        time.sleep(1)
        session_manager.add_message_to_session(session_id, "¡Hola! ¿En qué puedo ayudarte?", "bot")
        time.sleep(1)
        logger.info("Mensajes de conversación agregados.")

        # 3. Actualizar el consentimiento en los campos dedicados del documento en Firestore
        logger.info(f"\n3️⃣ Actualizando los campos 'consentimiento' y 'timestamp_consentimiento' en Firestore...")
        consent_status = "autorizado"
        success_update = session_manager.update_consent_for_session(session_id, consent_status)
        if success_update:
            logger.info(f"✅ Campos de consentimiento actualizados en Firestore para la sesión {session_id}.")
        else:
            logger.warning(f"⚠️ Fallo al actualizar los campos de consentimiento en Firestore para la sesión {session_id}.")

        time.sleep(1) # Pausa para asegurar la actualización del consentimiento

        session_manager.add_message_to_session(session_id, "Necesito información sobre mi medicamento", "user")
        logger.info("Mensaje adicional agregado.")
        time.sleep(1)

        # 4. Obtener historial completo de la sesión de Firestore
        logger.info(f"\n4️⃣ Obteniendo historial completo de la sesión de Firestore...")
        conversation = session_manager.get_conversation_history(session_id)
        logger.info(f"💬 Total de entradas en el historial de conversación: {len(conversation)}")

        logger.info("Últimas entradas del historial (buscando evento de consentimiento):")
        found_consent_event_in_history = False
        for msg in conversation[-5:]: # Mostrar las últimas 5 entradas
            sender = msg.get("sender", "unknown")
            message = msg.get("message", "")
            timestamp = msg.get("timestamp", "")
            event_type = msg.get("event_type", "N/A")
            consent_status_in_msg = msg.get("consent_status", "N/A") # Esto es lo que falla para el JSON

            # --- MODIFICAR AQUÍ ---
            # Si el evento es de consentimiento, el 'message' contiene un JSON, intentar parsearlo.
            if event_type == "consent_response" and message:
                try:
                    parsed_message_data = json.loads(message)
                    consent_status_in_msg = parsed_message_data.get("consent_status", "N/A")
                except json.JSONDecodeError:
                    pass # Si no es un JSON válido, no pasa nada
            # --- FIN MODIFICACIÓN ---

            logger.info(f"   [{timestamp}] ({event_type}) {sender}: {message} (Consent: {consent_status_in_msg})")
            if event_type == "consent_response" and consent_status_in_msg == "autorizado":
                found_consent_event_in_history = True

        if found_consent_event_in_history:
            logger.info("✅ Evento de consentimiento 'autorizado' encontrado en el historial de conversación.")
        else:
            logger.warning("⚠️ Evento de consentimiento 'autorizado' NO encontrado en el historial de conversación.")

        # 5. Prueba de expiración (Firestore gestiona el estado de sesión 'activa'/'cerrado')
        expiration_test_seconds = 5 # Tiempo corto para la prueba
        logger.info(f"\n5️⃣ Probando expiración (se cerrará la sesión en Firestore si está inactiva por {expiration_test_seconds}s)...")
        time.sleep(expiration_test_seconds + 2) # Esperar un poco más para que 'expire'

        was_expired = session_manager.check_and_expire_session(session_id, expiration_seconds=expiration_test_seconds)
        if was_expired:
            logger.info("✅ Sesión expirada y marcada como 'cerrado' en Firestore.")
        else:
            logger.warning("⚠️ Sesión aún no ha expirado o hubo un problema al cerrarla.")

        # 6. Mostrar sesiones previas (verificando el consentimiento desde Firestore)
        logger.info(f"\n6️⃣ Buscando todas las sesiones del identificador '{user_phone_number_for_test}' en Firestore...")
        all_sessions = session_manager.get_previous_sessions_by_phone(user_phone_number_for_test)
        logger.info(f"📋 Total sesiones encontradas para '{user_phone_number_for_test}': {len(all_sessions)}")

        if all_sessions:
            logger.info("Las 3 sesiones más recientes (verificando consentimiento en el documento de Firestore):")
            for session in all_sessions[:3]:
                logger.info(
                    f"   - ID: {session['session_id']}, Creada: {session['created_at']}, "
                    f"Consentimiento (desde Firestore): {session['consentimiento_status']} ({session['timestamp_consentimiento']})"
                )
        else:
            logger.info("No se encontraron sesiones previas para este identificador en Firestore.")

        logger.info("\n✅ === PRUEBAS DEL SESSION_MANAGER (FIRESTORE) COMPLETADAS EXITOSAMENTE ===\n")

    except SessionManagerError as e:
        logger.exception(f"❌ Error específico de SessionManager durante las pruebas: {e}")
        logger.error("\n❌ === PRUEBAS DEL SESSION_MANAGER FINALIZADAS CON ERRORES ESPECÍFICOS ===\n")
    except Exception as e:
        logger.exception(f"❌ Error crítico inesperado durante las pruebas del SessionManager: {e}")
        logger.error("\n❌ === PRUEBAS DEL SESSION_MANAGER FINALIZADAS CON ERRORES INESPERADOS ===\n")

if __name__ == "__main__":
    run_session_tests()