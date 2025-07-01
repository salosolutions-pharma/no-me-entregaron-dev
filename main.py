from flask import Request
import logging
from patient_module.patient_module import PatientModule

# Nivel de logging para Cloud Functions
logging.getLogger().setLevel(logging.INFO)

def scheduled_followup(request: Request):
    """
    Trigger: Cloud Scheduler (pubsub o HTTP).
    Ejecuta el check de follow-ups en PatientModule.
    """
    logging.info("Iniciando scheduled_followup")
    pm = PatientModule()
    pm.check_and_send_followups()
    logging.info("scheduled_followup completado")
    return "Scheduled follow-up executed successfully", 200


