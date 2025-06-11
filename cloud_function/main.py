import json
import logging
import os
from typing import Any, Dict, Optional

import functions_framework
import pytz
from google.cloud import bigquery, firestore
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
    logger.critical(f"Error al inicializar los clientes de Google Cloud: {e}")

COLOMBIA_TIMEZONE = pytz.timezone("America/Bogota")
BIGQUERY_TABLE_REFERENCE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_HISTORY}"


def _extract_session_id_from_resource(resource: str) -> Optional[str]:
    """Extrae el session_id de la ruta del recurso del evento."""
    if not resource:
        logger.warning("El campo 'resource' está vacío.")
        return None

    parts = resource.split("/")
    if len(parts) >= 6 and parts[4] == "documents" and parts[5] == FIRESTORE_COLLECTION_NAME:
        return parts[-1]

    logger.warning(f"No se pudo extraer el session_id del recurso: {resource}")
    return None


def _get_session_data_from_firestore(session_id: str) -> Optional[Dict[str, Any]]:
    """Recupera los datos completos de la sesión desde Firestore."""
    if firestore_client is None:
        logger.error("Cliente de Firestore no inicializado.")
        return None

    try:
        doc_ref = firestore_client.collection(FIRESTORE_COLLECTION_NAME).document(session_id)
        doc = doc_ref.get()

        if doc.exists:
            data = doc.to_dict()
            logger.info(f"Datos de sesión obtenidos: {session_id}")
            return data
        logger.warning(f"Documento de sesión {session_id} no encontrado en Firestore.")
        return None

    except GoogleAPIError as e:
        logger.error(f"Error de Firestore al obtener la sesión {session_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado al obtener la sesión {session_id}: {e}")
        return None


def _prepare_session_for_bigquery(session_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepara una fila única para BigQuery con toda la conversación.
    """
    for field in ['created_at', 'last_activity_at', 'timestamp_consentimiento', 'closed_at']:
        val = session_data.get(field)
        if val and hasattr(val, "isoformat"):
            try:
                session_data[field] = val.isoformat()
            except Exception as e:
                logger.warning(f"No se pudo convertir el campo {field}: {e}")
                session_data[field] = None

    conversation_array = session_data.get('conversation', [])
    conversation_json = json.dumps(conversation_array, ensure_ascii=False, default=str)

    record = {
        'id_sesion': session_data.get('id_sesion'),
        'conversacion': conversation_json,
        'consentimiento': session_data.get('consentimiento'),
        'timestamp_consentimiento': session_data.get('timestamp_consentimiento')
    }
    logger.info(f"Registro preparado para BigQuery: {record['id_sesion']} con {len(conversation_array)} eventos.")
    return record


def _insert_session_to_bigquery_direct(session_record: Dict[str, Any]) -> bool:
    """
    Carga datos directamente usando load_table_from_json sin un búfer de streaming.
    """
    if bigquery_client is None:
        logger.error("Cliente de BigQuery no inicializado.")
        return False

    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER
        )

        job = bigquery_client.load_table_from_json(
            json_rows=[session_record],
            destination=BIGQUERY_TABLE_REFERENCE,
            job_config=job_config
        )
        job.result()

        if job.errors:
            logger.error(f"Errores en la carga directa: {job.errors}")
            return False

        logger.info(f"Sesión {session_record['id_sesion']} insertada SIN BÚFER usando load_table_from_json.")
        return True

    except GoogleAPIError as e:
        logger.error(f"Error de la API de BigQuery: {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado en BigQuery: {e}")
        return False


def _delete_session_from_firestore(session_id: str) -> bool:
    """Elimina el documento de sesión de Firestore."""
    if firestore_client is None:
        logger.error("Cliente de Firestore no inicializado para eliminar la sesión.")
        return False

    try:
        firestore_client.collection(FIRESTORE_COLLECTION_NAME).document(session_id).delete()
        logger.info(f"Sesión {session_id} eliminada de Firestore.")
        return True
    except Exception as e:
        logger.error(f"Error al eliminar la sesión {session_id} de Firestore: {e}")
        return False


def _check_if_session_exists_in_bigquery(session_id: str) -> bool:
    """
    Verifica si la sesión ya existe en BigQuery para evitar duplicados.
    """
    if bigquery_client is None:
        return False

    try:
        query = f"""
            SELECT COUNT(*) as count
            FROM `{BIGQUERY_TABLE_REFERENCE}`
            WHERE id_sesion = @session_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("session_id", "STRING", session_id)]
        )
        results = bigquery_client.query(query, job_config=job_config).result()

        for row in results:
            exists = row.count > 0
            if exists:
                logger.info(f"Sesión {session_id} YA existe en BigQuery.")
            return exists
        return False

    except Exception as e:
        logger.warning(f"Error al verificar la existencia de la sesión {session_id}: {e}")
        return False


@functions_framework.cloud_event
def migrate_session_to_bigquery(cloud_event: Any) -> None:
    """
    Función de Cloud para migrar datos de sesión a BigQuery sin búfer de streaming usando load_table_from_json.
    """
    logger.info("Iniciando la migración DIRECTA de la sesión a BigQuery.")

    if bigquery_client is None or firestore_client is None:
        logger.critical("Clientes de Google Cloud no inicializados, abortando.")
        return

    resource = cloud_event.data.get("resource", "")
    if FIRESTORE_COLLECTION_NAME not in resource:
        logger.info("Evento ignorado: no pertenece a la colección sesiones_activas.")
        return

    session_id = _extract_session_id_from_resource(resource)
    if not session_id:
        logger.error("No se pudo extraer el session_id del evento.")
        return

    if _check_if_session_exists_in_bigquery(session_id):
        logger.info(f"Sesión {session_id} ya migrada, eliminando de Firestore.")
        _delete_session_from_firestore(session_id)
        return

    session_data = _get_session_data_from_firestore(session_id)
    if not session_data:
        logger.warning(f"No hay datos para la sesión {session_id}, eliminando el documento si existe.")
        _delete_session_from_firestore(session_id)
        return

    if session_data.get("estado_sesion") != "cerrado":
        logger.info(f"La sesión {session_id} no está cerrada, no se migrará ahora.")
        return

    logger.info(f"Procesando sesión cerrada: {session_id}.")

    session_record = _prepare_session_for_bigquery(session_data)

    if _insert_session_to_bigquery_direct(session_record):
        _delete_session_from_firestore(session_id)
        logger.info(f"Migración SIN BÚFER completa: {session_id} → BigQuery.")
    else:
        logger.error(f"La migración de {session_id} falló, se mantiene en Firestore.")