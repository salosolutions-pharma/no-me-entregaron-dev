from flask import Request
import logging
from patient_module.patient_module import PatientModule
from utils.logger_config import setup_structured_logging

# Nivel de logging para Cloud Functions
setup_structured_logging()

def scheduled_followup(request: Request):
    """Trigger para seguimientos de TELEGRAM únicamente."""
    logging.info("🔵 Iniciando scheduled_followup TELEGRAM")
    pm = PatientModule()
    pm.check_telegram_followups()  # 🔧 NUEVO: Solo Telegram
    logging.info("🔵 scheduled_followup TELEGRAM completado")
    return "Telegram follow-up executed successfully", 200

# Para WhatsApp (scheduled_followup_v2)  
def scheduled_followup_v2(request: Request):
    """Trigger para seguimientos de WHATSAPP únicamente."""
    logging.info("🟢 Iniciando scheduled_followup_v2 WHATSAPP")
    pm = PatientModule()
    pm.check_whatsapp_followups()  # 🔧 NUEVO: Solo WhatsApp
    logging.info("🟢 scheduled_followup_v2 WHATSAPP completado")
    return "WhatsApp follow-up executed successfully", 200