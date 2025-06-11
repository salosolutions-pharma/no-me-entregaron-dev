import json
import logging
import os
import tempfile
from pathlib import Path
from contextlib import suppress
from typing import Any, Dict, Optional

import functions_framework
import pytz
from google.cloud import bigquery, firestore
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Variables de entorno
PROJECT_ID = os.getenv("GCP_PROJECT", "")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "NME_dev")
TABLE_ID_HISTORY = os.getenv("BIGQUERY_TABLE_ID", "historial_conversacion")
FIRESTORE_DATABASE_NAME = "historia"
FIRESTORE_COLLECTION_NAME = "sesiones_activas"

bigquery_client: Optional[bigquery.Client] = None
firestore_client: Optional[firestore.Client] = None

try:
    if not PROJECT_ID:
        raise RuntimeError("La variable de entorno GCP_PROJECT no está configurada.")

    bigquery_client = bigquery.Client(project=PROJECT_ID)
    firestore_client = firestore.Client(database=FIRESTORE_DATABASE_NAME, project=PROJECT_ID)
    logger.info("Clientes de BigQuery y Firestore inicializados correctamente.")
except Exception as e:
    logger.critical(f"Error al inicializar clientes de Google Cloud: {e}")

COLOMBIA_TIMEZONE = pytz.timezone("America/Bogota")
BIGQUERY_TABLE_REFERENCE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_HISTORY}"


def _extract_session_id_from_resource(resource: str) -> Optional[str]:
    if not resource:
        logger.warning("El campo 'resource' está vacío.")
        return None
    parts = resource.split("/")
    if len(parts) >= 6 and parts[4] == "documents" and parts[5] == FIRESTORE_COLLECTION_NAME:
        return parts[-1]
    logger.warning(f"No se pudo extraer session_id del resource: {resource}")
    return None


def _get_session_data_from_firestore(session_id: str) -> Optional[Dict[str, Any]]:
    if firestore_client is None:
        logger.error("Firestore client no inicializado.")
        return None
    try:
        doc_ref = firestore_client.collection(FIRESTORE_COLLECTION_NAME).document(session_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        else:
            logger.warning(f"Documento sesión {session_id} no encontrado en Firestore.")
            return None
    except GoogleAPIError as e:
        logger.error(f"Error Firestore al obtener sesión {session_id}: {e}")
        return None


@functions_framework.cloud_event
def migrate_session_to_bigquery(cloud_event: Any) -> None:
    logger.info("Función iniciar migrate_session_to_bigquery.")

    if bigquery_client is None or firestore_client is None:
        logger.critical("Clientes de Google Cloud no inicializados, abortando.")
        return

    resource = cloud_event.data.get("resource", "")
    if FIRESTORE_COLLECTION_NAME not in resource:
        logger.info("Evento ignorado: no pertenece a la colección sesiones_activas.")
        return

    session_id = _extract_session_id_from_resource(resource)
    if not session_id:
        logger.error("No se pudo extraer session_id del evento.")
        return

    session_data = _get_session_data_from_firestore(session_id)
    if not session_data:
        logger.warning(f"No data para sesión {session_id}, eliminando documento si existe.")
        _delete_session_from_firestore(session_id)
        return

    if session_data.get("estado_sesion") != "cerrado":
        logger.info(f"Sesión {session_id} no está cerrada, no se migrará ahora.")
        return

    # Convertir timestamps Firestore a string ISO
    for field in ['created_at', 'last_activity_at', 'timestamp_consentimiento', 'closed_at']:
        val = session_data.get(field)
        if val and hasattr(val, "isoformat"):
            try:
                session_data[field] = val.isoformat()
            except Exception as e:
                logger.warning(f"No pudo convertir campo {field}: {e}")

    # Preparar archivo JSONL temporal
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as temp_file:
        json.dump(session_data, temp_file, ensure_ascii=False)
        temp_file.write("\n")
        temp_path = Path(temp_file.name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    try:
        with temp_path.open("rb") as file_obj:
            load_job = bigquery_client.load_table_from_file(file_obj, BIGQUERY_TABLE_REFERENCE, job_config=job_config)
        load_job.result()

        if load_job.errors:
            logger.error(f"Errores durante carga en BigQuery: {load_job.errors}")
            return

        logger.info(f"Sesión {session_id} migrada exitosamente a BigQuery.")

        # Eliminar documento Firestore tras migración exitosa
        firestore_client.collection(FIRESTORE_COLLECTION_NAME).document(session_id).delete()
        logger.info(f"Documento Firestore {session_id} eliminado tras migración.")

    except GoogleAPIError as e:
        logger.error(f"Error BigQuery API al migrar sesión {session_id}: {e}")
    except Exception as e:
        logger.error(f"Error inesperado migrando sesión {session_id}: {e}")
    finally:
        with suppress(Exception):
            temp_path.unlink()


def _delete_session_from_firestore(session_id: str) -> bool:
    if firestore_client is None:
        logger.error("Firestore client no inicializado para eliminar sesión.")
        return False
    try:
        firestore_client.collection(FIRESTORE_COLLECTION_NAME).document(session_id).delete()
        logger.info(f"Sesión {session_id} eliminada de Firestore.")
        return True
    except Exception as e:
        logger.error(f"Error eliminando sesión {session_id} de Firestore: {e}")
        return False