import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import functions_framework
import pytz
from google.api_core.exceptions import GoogleAPIError
from google.cloud import firestore

from processor_image_prescription.bigquery_pip import (
    get_bigquery_client,
    load_table_from_json_direct,
    BigQueryServiceError,
    PROJECT_ID,
    DATASET_ID,
)

logger = logging.getLogger(__name__)

TABLE_ID_HISTORY = os.getenv("BIGQUERY_TABLE_ID_HISTORY", "historial_conversacion")
FIRESTORE_DATABASE_NAME = "historia"
FIRESTORE_COLLECTION_NAME = "sesiones_activas"

firestore_client: Optional[firestore.Client] = None

try:
    if not PROJECT_ID:
        raise RuntimeError("La variable de entorno PROJECT_ID no está configurada.")

    _ = get_bigquery_client()

    firestore_client = firestore.Client(database=FIRESTORE_DATABASE_NAME, project=PROJECT_ID)
    logger.info("Clientes de BigQuery (vía bigquery_pip) y Firestore inicializados correctamente.")
except Exception as e:
    logger.critical(f"Error al inicializar los clientes de Google Cloud: {e}")

COLOMBIA_TIMEZONE = pytz.timezone("America/Bogota")
BIGQUERY_TABLE_REFERENCE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_HISTORY}"


def _extract_session_id_from_resource(resource: str) -> Optional[str]:
    """Extrae el session_id de la ruta del recurso del evento de Firestore."""
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
    """Prepara una fila única para BigQuery con toda la conversación."""
    record = {
        'id_sesion': session_data.get('id_sesion'),
        'user_identifier': session_data.get('user_identifier'),
        'channel': session_data.get('channel'),
        'consentimiento': session_data.get('consentimiento'),
        'timestamp_consentimiento': (
            session_data['timestamp_consentimiento'].isoformat()
            if isinstance(session_data.get('timestamp_consentimiento'), datetime)
            else None
        ),
        'estado_sesion': session_data.get('estado_sesion'),
        'created_at': (
            session_data['created_at'].isoformat()
            if isinstance(session_data.get('created_at'), datetime)
            else None
        ),
        'last_activity_at': (
            session_data['last_activity_at'].isoformat()
            if isinstance(session_data.get('last_activity_at'), datetime)
            else None
        ),
        'closed_at': (
            session_data['closed_at'].isoformat()
            if isinstance(session_data.get('closed_at'), datetime)
            else None
        ),
        'close_reason': session_data.get('close_reason'),
        'conversacion': json.dumps(session_data.get('conversation', []), ensure_ascii=False, default=str),
    }
    logger.info(f"Registro preparado para BigQuery: {record.get('id_sesion')} con {len(session_data.get('conversation', []))} eventos.")
    return record


def _insert_session_to_bigquery(session_record: Dict[str, Any]) -> bool:
    """Carga datos de sesión directamente usando `load_table_from_json` a la tabla de historial."""
    try:
        load_table_from_json_direct([session_record], BIGQUERY_TABLE_REFERENCE)
        logger.info(f"Sesión {session_record['id_sesion']} insertada en BigQuery (historial).")
        return True
    except BigQueryServiceError as e:
        logger.error(f"Error al insertar sesión en BigQuery (historial): {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado al insertar sesión en BigQuery (historial): {e}")
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
    """Verifica si la sesión ya existe en la tabla de historial de BigQuery para evitar duplicados."""
    from google.cloud import bigquery
    
    client = get_bigquery_client()

    try:
        query = f"""
            SELECT COUNT(*) as count
            FROM `{BIGQUERY_TABLE_REFERENCE}`
            WHERE id_sesion = @session_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("session_id", "STRING", session_id)]
        )
        results = client.query(query, job_config=job_config).result()

        for row in results:
            exists = row.count > 0
            if exists:
                logger.info(f"Sesión {session_id} YA existe en BigQuery (historial).")
            return exists
        return False

    except Exception as e:
        logger.warning(f"Error al verificar la existencia de la sesión {session_id} en BigQuery: {e}")
        return False


@functions_framework.cloud_event
def migrate_session_to_bigquery(cloud_event: Any) -> None:
    """Función de Cloud para migrar datos de sesión de Firestore a BigQuery."""
    logger.info("Iniciando la migración de la sesión a BigQuery.")

    if firestore_client is None or get_bigquery_client() is None:
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
        logger.info(f"Sesión {session_id} ya migrada, procediendo a eliminar de Firestore si existe.")
        _delete_session_from_firestore(session_id)
        return

    session_data = _get_session_data_from_firestore(session_id)
    if not session_data:
        logger.warning(f"No hay datos para la sesión {session_id} en Firestore.")
        _delete_session_from_firestore(session_id)
        return

    if session_data.get("estado_sesion") != "cerrado":
        logger.info(f"La sesión {session_id} no está cerrada ('{session_data.get('estado_sesion')}'). No se migrará ahora.")
        return

    logger.info(f"Procesando sesión cerrada: {session_id}.")

    session_record = _prepare_session_for_bigquery(session_data)

    if _insert_session_to_bigquery(session_record):
        _delete_session_from_firestore(session_id)
        logger.info(f"Migración completa: Sesión {session_id} → BigQuery (historial).")
    else:
        logger.error(f"La migración de {session_id} falló, se mantiene en Firestore.")