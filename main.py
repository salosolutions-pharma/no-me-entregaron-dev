from flask import Request
import logging
from datetime import datetime, timedelta
import pytz

# Nivel de logging para Cloud Functions
logging.getLogger().setLevel(logging.INFO)

def scheduled_session_cleanup(request: Request):
    """
    Trigger diario para cierre automático de sesiones inactivas.
    Ejecuta a las 11:59 PM UTC-5 (hora Colombia).
    
    Cloud Scheduler config:
    - Cron: 59 23 * * *
    - Timezone: America/Bogota
    - URL: https://REGION-PROJECT.cloudfunctions.net/scheduled_session_cleanup
    """
    logging.info("🧹 Iniciando scheduled_session_cleanup DIARIO")
    
    try:
        from BYC.consentimiento import ConsentManager
        
        consent_manager = ConsentManager()
        session_manager = consent_manager.session_manager
        
        if not session_manager:
            logging.error("❌ SessionManager no disponible")
            return "SessionManager not available", 500
        
        colombia_tz = pytz.timezone('America/Bogota')
        current_time = datetime.now(colombia_tz)
        time_limit = current_time - timedelta(hours=6)
        
        logging.info(f"⏰ Hora actual Colombia: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"🚫 Cerrando sesiones inactivas desde: {time_limit.strftime('%Y-%m-%d %H:%M:%S')}")
        
        expired_sessions = session_manager.sessions_collection_ref.where(
            "estado_sesion", "==", "activa"
        ).where(
            "last_activity_at", "<", time_limit
        ).get()
        
        closed_count = 0
        
        for session_doc in expired_sessions:
            session_id = session_doc.id
            
            try:
                if session_manager.check_session_inactivity(session_id, hours_limit=6):
                    closed_count += 1
                    logging.info(f"✅ Sesión auto-cerrada: {session_id[:20]}...")
                    
            except Exception as e:
                logging.error(f"❌ Error cerrando sesión {session_id[:20]}...: {e}")
        
        total_checked = len(expired_sessions)
        logging.info(f"📊 RESULTADO: {closed_count}/{total_checked} sesiones cerradas")
        
        if closed_count > 0:
            logging.info(f"🚀 {closed_count} sesiones migrarán automáticamente a BigQuery")
        
        logging.info("🧹 scheduled_session_cleanup DIARIO completado")
        return f"Session cleanup executed successfully: {closed_count} sessions closed", 200
        
    except Exception as e:
        logging.error(f"💥 Error en scheduled_session_cleanup: {e}")
        return f"Error in session cleanup: {str(e)}", 500

def manual_session_cleanup(request: Request):
    """
    Trigger manual para testing del cierre de sesiones.
    
    POST con JSON: {"hours_limit": 6}  # Opcional
    """
    logging.info("🔧 Iniciando manual_session_cleanup")
    
    try:
        request_json = request.get_json(silent=True) or {}
        hours_limit = request_json.get('hours_limit', 6)
        
        from BYC.consentimiento import ConsentManager
        
        consent_manager = ConsentManager()
        session_manager = consent_manager.session_manager
        
        if not session_manager:
            return "SessionManager not available", 500
        
        colombia_tz = pytz.timezone('America/Bogota')
        current_time = datetime.now(colombia_tz)
        time_limit = current_time - timedelta(hours=hours_limit)
        
        logging.info(f"🔧 Limpieza MANUAL (límite: {hours_limit} horas)")
        
        expired_sessions = session_manager.sessions_collection_ref.where(
            "estado_sesion", "==", "activa"
        ).where(
            "last_activity_at", "<", time_limit
        ).get()
        
        closed_count = 0
        
        for session_doc in expired_sessions:
            session_id = session_doc.id
            
            try:
                if session_manager.check_session_inactivity(session_id, hours_limit=hours_limit):
                    closed_count += 1
                    logging.info(f"✅ Sesión cerrada manualmente: {session_id[:20]}...")
                    
            except Exception as e:
                logging.error(f"❌ Error: {e}")
        
        logging.info(f"🔧 manual_session_cleanup completado: {closed_count} sesiones")
        return f"Manual cleanup executed: {closed_count} sessions closed", 200
        
    except Exception as e:
        logging.error(f"💥 Error en manual_session_cleanup: {e}")
        return f"Error: {str(e)}", 500