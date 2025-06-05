import json
import logging
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, Optional

import functions_framework
import pytz
from google.cloud import bigquery, firestore
from google.api_core.exceptions import NotFound, GoogleAPIError # Importar GoogleAPIError

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constantes de configuración (obtenidas de variables de entorno)
# PROJECT_ID se obtiene de 'GCP_PROJECT' que es estándar en Cloud Functions
PROJECT_ID: str = os.environ.get("GCP_PROJECT", "")
DATASET_ID: str = os.environ.get("BIGQUERY_DATASET_ID", "NME_dev")
TABLE_ID_HISTORY: str = os.environ.get("BIGQUERY_TABLE_ID", "historial_conversacion")
FIRESTORE_DATABASE_NAME: str = "historia" # Nombre de la base de datos de Firestore
FIRESTORE_COLLECTION_NAME: str = "sesiones_activas"

# Clientes de Google Cloud (inicializados globalmente para reuso en el entorno de la función)
try:
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    firestore_client = firestore.Client(database=FIRESTORE_DATABASE_NAME, project=PROJECT_ID)
    logger.info("Clientes de BigQuery y Firestore inicializados.")
except Exception as e:
    logger.critical(f"Error fatal al inicializar clientes de Google Cloud: {e}. La función no operará.")
    # En un entorno de Cloud Functions, un error aquí puede hacer que la función falle al desplegar o al invocar.
    # Por ahora, simplemente loggeamos, pero en un caso real, podría requerir un manejo de errores más sofisticado
    # como reintentos o un fallback.

# Zona horaria para consistencia en timestamps
COLOMBIA_TIMEZONE = pytz.timezone("America/Bogota")

# Referencia completa de la tabla de BigQuery
BIGQUERY_TABLE_REFERENCE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_HISTORY}"

# --- Funciones Auxiliares para la Migración ---

def _extract_session_id_from_subject(subject: str) -> Optional[str]:
    """
    Extrae el ID de sesión del campo 'subject' de un evento de cambio de Firestore.
    Ej: projects/my-project/databases/(default)/documents/sesiones_activas/SESSION_ID_123
    """
    if not subject:
        logger.warning("El campo 'subject' del evento está vacío.")
        return None
    
    # El ID del documento es el último elemento en la ruta
    parts = subject.split("/")
    if len(parts) >= 6 and parts[4] == "documents": # Asegurar que es una ruta de documento
        return parts[-1]
    logger.warning(f"Formato de 'subject' inesperado. No se pudo extraer session_id de: {subject}")
    return None

def _get_session_data_from_firestore(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene los datos completos de un documento de sesión desde Firestore.

    Args:
        session_id: El ID del documento de sesión a recuperar.

    Returns:
        Un diccionario con los datos de la sesión, o None si el documento no existe
        o hay un error al recuperarlo.
    """
    try:
        session_doc_ref = firestore_client.collection(
            FIRESTORE_COLLECTION_NAME
        ).document(session_id)
        session_doc = session_doc_ref.get()
        
        if not session_doc.exists:
            logger.warning(f"Documento de sesión '{session_id}' no encontrado en Firestore.")
            return None
            
        logger.info(f"Datos de sesión '{session_id}' obtenidos de Firestore.")
        return session_doc.to_dict()
        
    except GoogleAPIError as e:
        logger.error(f"Error de la API de Firestore al obtener datos para '{session_id}': {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error inesperado al obtener datos de Firestore para '{session_id}': {e}", exc_info=True)
        return None

def _prepare_data_for_bigquery_jsonl(session_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepara un diccionario de datos de sesión para ser serializado como JSONL
    para BigQuery. Convierte los objetos firestore.Timestamp a cadenas ISO 8601.
    """
    prepared_data = session_data.copy() # Trabaja con una copia para no modificar el original

    # Lista de campos que se espera que sean Timestamps de Firestore
    timestamp_fields = ["created_at", "last_activity_at", "timestamp_consentimiento", "closed_at"]

    for field in timestamp_fields:
        if field in prepared_data and hasattr(prepared_data[field], "isoformat"):
            try:
                prepared_data[field] = prepared_data[field].isoformat()
            except Exception as e:
                logger.warning(f"No se pudo convertir el campo '{field}' a ISO format para sesión '{prepared_data.get('id_sesion', 'N/A')}': {e}. Manteniendo valor original.")
    
    # Nota: Los campos anidados como 'conversation' (que es una lista de dicts) se serializan
    # correctamente a JSON si sus contenidos son tipos básicos o diccionarios/listas anidadas.
    # No necesitan un procesamiento especial aquí, ya que el esquema de BigQuery debe coincidir.

    return prepared_data

def _load_single_record_to_bigquery(client: bigquery.Client,
                                     table_reference: str,
                                     record_data: Dict[str, Any]) -> bool:
    """
    Carga un único registro (fila) en formato JSONL a una tabla de BigQuery.
    Utiliza un archivo temporal para el stream de carga.

    Args:
        client: Cliente autenticado de BigQuery.
        table_reference: Referencia completa de la tabla (ej. "proyecto.dataset.tabla").
        record_data: Diccionario con la fila a cargar.

    Returns:
        True si la carga fue exitosa, False en caso contrario.
    """
    tmp_file_path: Optional[Path] = None
    session_id_for_log = record_data.get('id_sesion', 'N/A')

    try:
        # 1. Crear un archivo temporal JSONL con el registro
        with tempfile.NamedTemporaryFile(mode="w",
                                         encoding="utf-8",
                                         newline="\n",
                                         suffix=".jsonl",
                                         delete=False) as tmp_file:
            json.dump(record_data, tmp_file, ensure_ascii=False)
            tmp_file.write("\n") # Cada objeto JSON en una nueva línea
            tmp_file_path = Path(tmp_file.name)
        logger.info(f"Datos de sesión '{session_id_for_log}' serializados a archivo temporal: {tmp_file_path}")

        # 2. Configurar el job de carga de BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Añadir la nueva fila
            autodetect=False, # Asume que el esquema de la tabla de destino ya está definido
        )
        
        # 3. Cargar el archivo temporal a BigQuery
        with tmp_file_path.open("rb") as file_handle:
            job = client.load_table_from_file(file_handle, table_reference, job_config=job_config)
        
        job.result() # Esperar a que el job de carga finalice
        
        if job.errors:
            logger.error(f"Errores en BigQuery load job para sesión '{session_id_for_log}': {job.errors}")
            return False
        
        logger.info(f"✅ BigQuery load: {job.output_rows} fila(s) cargada(s) para sesión '{session_id_for_log}'.")
        return True

    except GoogleAPIError as e:
        logger.error(f"Error de la API de BigQuery al cargar datos para '{session_id_for_log}': {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error inesperado al cargar datos en BigQuery para '{session_id_for_log}': {e}", exc_info=True)
        return False
    finally:
        # Asegurarse de eliminar el archivo temporal, incluso si hay errores
        if tmp_file_path:
            with suppress(FileNotFoundError):
                tmp_file_path.unlink(missing_ok=True)

def _delete_session_from_firestore(session_id: str) -> bool:
    """
    Elimina un documento de sesión de la colección 'sesiones_activas' en Firestore.

    Args:
        session_id: El ID del documento a eliminar.

    Returns:
        True si el documento fue eliminado o no existía, False si hubo un error.
    """
    try:
        firestore_client.collection(FIRESTORE_COLLECTION_NAME).document(session_id).delete()
        logger.info(f"Documento de sesión '{session_id}' eliminado de Firestore.")
        return True
    except NotFound:
        logger.warning(f"Documento de sesión '{session_id}' no encontrado en Firestore para eliminar (posiblemente ya borrado).")
        return True # Si no existe, consideramos que el objetivo (eliminar) se logró
    except GoogleAPIError as e:
        logger.error(f"Error de la API de Firestore al eliminar documento '{session_id}': {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error inesperado al eliminar documento '{session_id}' de Firestore: {e}", exc_info=True)
        return False

# --- Cloud Function Entrypoint ---

@functions_framework.cloud_event
def migrate_session_to_bigquery(cloud_event: Any) -> None:
    """
    Cloud Function que migra sesiones cerradas de Firestore a BigQuery.
    Esta función se activa automáticamente cuando un documento en la colección
    'sesiones_activas' de Firestore tiene su campo 'estado_sesion' cambiado a 'cerrado'.

    Args:
        cloud_event: El objeto CloudEvent que contiene la información del evento de Firestore.
    """
    logger.info("⏩ Iniciando Cloud Function: migrate_session_to_bigquery.")
    
    try:
        # Extraer el ID de la sesión del 'subject' del evento
        subject = cloud_event.get("subject")
        session_id = _extract_session_id_from_subject(subject)
        
        if not session_id:
            logger.error("No se pudo extraer el ID de sesión del evento. Abortando migración.")
            return # Salir si no hay session_id
            
        logger.info(f"🔍 Procesando evento para sesión: '{session_id}'.")
        
        # 1. Obtener los datos completos de la sesión de Firestore
        session_data = _get_session_data_from_firestore(session_id)
        if not session_data:
            logger.warning(f"No se pudieron obtener datos para sesión '{session_id}'. "
                           "Esto podría indicar que el documento fue borrado rápidamente o no existía. "
                           "Intentando borrar por si acaso quedó un rastro.")
            _delete_session_from_firestore(session_id) # Intentar limpiar si no se encontró, pero el evento se disparó
            return # No hay datos para migrar
        
        # 2. Verificar el estado de la sesión antes de migrar
        session_state = session_data.get("estado_sesion")
        if session_state != "cerrado":
            logger.info(
                f"Sesión '{session_id}' no está en estado 'cerrado' (estado actual: '{session_state}'). "
                "No se migrará en este momento. La función se activó por otro cambio."
            )
            return # Solo migrar si la sesión está explícitamente "cerrado"
        
        logger.info(f"🚀 Iniciando migración de sesión marcada como 'cerrado': '{session_id}'.")
        
        # 3. Preparar los datos para el formato de BigQuery (JSONL)
        bigquery_ready_data = _prepare_data_for_bigquery_jsonl(session_data)
        
        # 4. Cargar los datos preparados a BigQuery
        if not _load_single_record_to_bigquery(bigquery_client, BIGQUERY_TABLE_REFERENCE, bigquery_ready_data):
            logger.error(f"Falló la carga de datos en BigQuery para sesión '{session_id}'. "
                         "El documento de Firestore NO será eliminado para permitir una posible reintentos manuales o automáticos.")
            # Es crucial NO borrar el documento de Firestore si la carga falla.
            # Esto permite que la función pueda ser reintentada o que se depure el problema.
            return 
        
        # 5. Eliminar el documento de Firestore solo si la carga a BigQuery fue exitosa
        if _delete_session_from_firestore(session_id):
            logger.info(f"🎉 Migración y limpieza completada exitosamente para sesión '{session_id}'.")
        else:
            logger.warning(
                f"Datos cargados en BigQuery para sesión '{session_id}', "
                f"pero no se pudo eliminar el documento de Firestore. "
                "Revisar manualmente el documento en Firestore."
            )
            
    except Exception as e:
        logger.critical(f"❌ Error crítico e inesperado en la función migrate_session_to_bigquery: {e}", exc_info=True)
        # Un error crítico aquí indica un fallo que debe ser investigado.
        # En Cloud Functions, esto podría llevar a reintentos automáticos si la política está configurada.