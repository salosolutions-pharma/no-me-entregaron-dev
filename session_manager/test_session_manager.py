import os
import time
import logging
import json
from dotenv import load_dotenv

from session_manager.session_manager import SessionManager, SessionManagerError

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
    """Ejecuta un conjunto de pruebas para el SessionManager."""
    if session_manager is None:
        logger.error("SessionManager no pudo ser inicializado. Saltando pruebas.")
        return

    user_id = "test_user_telegram_multiple_rows_456" # Nuevo ID para diferenciar
    channel = "TL"

    try:
        logger.info("🚀 === INICIO DE PRUEBAS DEL SESSION_MANAGER (MÚLTIPLES FILAS - STREAMING) ===\n")

        # 1. Crear sesión (inserta la fila inicial)
        logger.info("1️⃣ Creando una nueva sesión (fila inicial)...")
        session_info = session_manager.create_session_with_history_check(user_id, channel)
        session_id = session_info["new_session_id"]

        logger.info(f"✅ Nueva sesión creada: {session_id}")
        logger.info(f"📊 Historial previo: {session_info['has_previous_history']}")
        logger.info(f"📈 Sesiones previas encontradas: {session_info['previous_sessions_count']}")

        # **PAUSA:** Dar tiempo para que la fila inicial se asiente (aunque no haremos UPDATE sobre ella)
        time.sleep(5) # API de Streaming es rápida, pero damos un pequeño margen

        # 2. Agregar mensajes (cada uno inserta una nueva fila)
        logger.info(f"\n2️⃣ Agregando conversación como nuevas filas...")
        session_manager.add_message_to_session(session_id, "Hola, necesito ayuda", "user")
        session_manager.add_message_to_session(session_id, "¡Hola! ¿En qué puedo ayudarte?", "bot")
        logger.info("Mensajes de conversación agregados.")

        # 3. Registrar el consentimiento (inserta una NUEVA FILA con los campos específicos llenos)
        logger.info(f"\n3️⃣ Registrando el consentimiento en una nueva fila con campos dedicados...")
        consent_status = "autorizado"
        success_consent_record = session_manager.record_consent_event(session_id, consent_status, int(user_id.split('_')[-1])) # Usar user_id simulado
        if success_consent_record:
            logger.info(f"✅ Consentimiento '{consent_status}' registrado exitosamente en una fila separada para sesión {session_id}.")
        else:
            logger.error(f"❌ Fallo al registrar el consentimiento en una fila separada para sesión {session_id}.")

        session_manager.add_message_to_session(session_id, "Necesito información sobre mi medicamento", "user")
        logger.info("Mensajes adicionales agregados.")

        # 4. Obtener historial completo (unirá los JSON de todas las filas)
        logger.info(f"\n4️⃣ Obteniendo historial completo (uniendo JSONs de múltiples filas)...")
        conversation = session_manager.get_conversation_history(session_id)
        logger.info(f"💬 Total de entradas en el historial JSON unificado: {len(conversation)}")

        logger.info("Últimas 5 entradas del historial JSON (buscando evento de consentimiento):")
        found_consent_event = False
        for msg in conversation[-5:]:
            sender = msg.get("sender", "unknown")
            message = msg.get("message", "")
            timestamp = msg.get("timestamp", "")
            event_type = msg.get("event_type", "N/A")
            consent_status_in_msg = msg.get("consent_status", "N/A")

            logger.info(f"   [{timestamp}] ({event_type}) {sender}: {message} (Consent: {consent_status_in_msg})")
            if event_type == "consent_response" and consent_status_in_msg == "autorizado":
                found_consent_event = True
        
        if found_consent_event:
            logger.info("✅ Evento de consentimiento 'autorizado' encontrado en el historial JSON unificado.")
        else:
            logger.warning("⚠️ Evento de consentimiento 'autorizado' NO encontrado en el historial JSON unificado.")

        # 5. Prueba de expiración
        expiration_test_seconds = 5
        logger.info(f"\n5️⃣ Probando expiración (esperando {expiration_test_seconds + 2} segundos para asegurar)...")
        time.sleep(expiration_test_seconds + 2)

        was_expired = session_manager.check_and_expire_session(session_id, expiration_seconds=expiration_test_seconds)
        if was_expired:
            logger.info("✅ Sesión expirada y marcada como cerrada (agregando nueva fila de evento).")
        else:
            logger.warning("⚠️ Sesión aún no ha expirado o hubo un problema al cerrarla.")
        
        # 6. Mostrar sesiones previas (verificando el consentimiento de la fila relevante)
        logger.info(f"\n6️⃣ Buscando todas las sesiones del identificador '{user_id}'...")
        all_sessions = session_manager.get_previous_sessions_by_phone(user_id)
        logger.info(f"📋 Total sesiones encontradas para '{user_id}': {len(all_sessions)}")

        if all_sessions:
            logger.info("Las 3 sesiones más recientes (verificando consentimiento en la fila dedicada):")
            for session in all_sessions[:3]:
                logger.info(
                    f"   - ID: {session['session_id']}, Creada: {session['created_at']}, "
                    f"Consentimiento (desde columna): {session['consentimiento_status']} ({session['timestamp_consentimiento']})"
                )
        else:
            logger.info("No se encontraron sesiones previas para este identificador.")

        logger.info("\n✅ === PRUEBAS DEL SESSION_MANAGER (MÚLTIPLES FILAS - STREAMING) COMPLETADAS EXITOSAMENTE ===\n")

    except SessionManagerError as e:
        logger.exception(f"❌ Error específico de SessionManager durante las pruebas: {e}")
        logger.error("\n❌ === PRUEBAS DEL SESSION_MANAGER FINALIZADAS CON ERRORES ESPECÍFICOS ===\n")
    except Exception as e:
        logger.exception(f"❌ Error crítico inesperado durante las pruebas del SessionManager: {e}")
        logger.error("\n❌ === PRUEBAS DEL SESSION_MANAGER FINALIZADAS CON ERRORES INESPERADOS ===\n")
        
# Este bloque es CRÍTICO para que el script de pruebas se ejecute.
if __name__ == "__main__":
    run_session_tests()