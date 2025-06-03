"""
Cloud Function para migrar sesiones de Firestore a BigQuery.

Este módulo contiene la función que se ejecuta automáticamente cuando
una sesión se marca como 'cerrado' en Firestore, migrando los datos
a BigQuery y eliminando el documento de Firestore.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import functions_framework
import pytz
from google.cloud import bigquery, firestore

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constantes de configuración
PROJECT_ID = os.environ.get("GCP_PROJECT", "no-me-entregaron")
DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "NME_dev")
TABLE_ID_HISTORY = os.environ.get("BIGQUERY_TABLE_ID", "historial_conversacion")
FIRESTORE_DATABASE = "historia"
FIRESTORE_COLLECTION = "sesiones_activas"

# Clientes de Google Cloud
bigquery_client = bigquery.Client()
firestore_client = firestore.Client(database=FIRESTORE_DATABASE)

# Zona horaria
COLOMBIA_TZ = pytz.timezone("America/Bogota")

# Referencias de tabla
BIGQUERY_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_HISTORY}"


def extract_session_id_from_subject(subject: str) -> Optional[str]:
    """
    Extrae el ID de sesión del subject del evento de Firestore.
    
    Args:
        subject: Subject del cloud event, formato: 
                "documents/sesiones_activas/TL_573226743144_20250602_232422"
    
    Returns:
        ID de la sesión o None si no se puede extraer.
    """
    if not subject:
        return None
    
    parts = subject.split("/")
    return parts[-1] if len(parts) >= 3 else None


def get_session_data_from_firestore(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene los datos de una sesión desde Firestore.
    
    Args:
        session_id: ID único de la sesión.
    
    Returns:
        Diccionario con los datos de la sesión o None si no existe.
    """
    try:
        session_doc = firestore_client.collection(
            FIRESTORE_COLLECTION
        ).document(session_id).get()
        
        if not session_doc.exists:
            logger.warning(f"Documento {session_id} no encontrado en Firestore")
            return None
            
        return session_doc.to_dict()
        
    except Exception as e:
        logger.error(f"Error obteniendo datos de Firestore para {session_id}: {e}")
        return None


def prepare_bigquery_row(session_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepara la fila de datos para insertar en BigQuery.
    
    Args:
        session_id: ID único de la sesión.
        data: Datos de la sesión obtenidos de Firestore.
    
    Returns:
        Diccionario con los datos formateados para BigQuery.
    """
    # Convertir timestamps a formato ISO
    timestamp_consentimiento_iso = None
    if data.get("timestamp_consentimiento"):
        timestamp_consentimiento_iso = data["timestamp_consentimiento"].isoformat()
    
    # Serializar conversación a JSON
    conversation_json = json.dumps(
        data.get("conversation", []), 
        ensure_ascii=False
    )
    
    # Construir fila base
    row = {
        "id_sesion": session_id,
        "conversacion": conversation_json,
        "consentimiento": data.get("consentimiento"),
    }
    
    # Agregar timestamp solo si existe
    if timestamp_consentimiento_iso:
        row["timestamp_consentimiento"] = timestamp_consentimiento_iso
    
    return row


def insert_to_bigquery(row_data: Dict[str, Any]) -> bool:
    """
    Inserta una fila de datos en BigQuery.
    
    Args:
        row_data: Diccionario con los datos a insertar.
    
    Returns:
        True si la inserción fue exitosa, False en caso contrario.
    """
    try:
        errors = bigquery_client.insert_rows_json(BIGQUERY_TABLE_REF, [row_data])
        
        if errors:
            logger.error(f"Errores al insertar en BigQuery: {errors}")
            return False
            
        logger.info("Datos insertados exitosamente en BigQuery")
        return True
        
    except Exception as e:
        logger.error(f"Error crítico al insertar en BigQuery: {e}")
        return False


def delete_from_firestore(session_id: str) -> bool:
    """
    Elimina un documento de sesión de Firestore.
    
    Args:
        session_id: ID único de la sesión a eliminar.
    
    Returns:
        True si la eliminación fue exitosa, False en caso contrario.
    """
    try:
        firestore_client.collection(FIRESTORE_COLLECTION).document(session_id).delete()
        logger.info(f"Documento de sesión {session_id} eliminado de Firestore")
        return True
        
    except Exception as e:
        logger.error(f"Error eliminando documento {session_id} de Firestore: {e}")
        return False


@functions_framework.cloud_event
def migrate_session_to_bigquery(cloud_event) -> None:
    """
    Cloud Function que migra sesiones cerradas de Firestore a BigQuery.
    
    Esta función se ejecuta automáticamente cuando se actualiza un documento
    en la colección 'sesiones_activas' de Firestore. Solo procesa documentos
    con estado_sesion = 'cerrado'.
    
    Args:
        cloud_event: Evento de Firestore con información del documento actualizado.
    """
    logger.info("Iniciando función de migración de sesiones")
    
    try:
        # Extraer información del evento
        subject = cloud_event.get("subject")
        session_id = extract_session_id_from_subject(subject)
        
        if not session_id:
            logger.error("No se pudo extraer session_id del evento")
            return
            
        logger.info(f"Procesando sesión: {session_id}")
        
        # Obtener datos de Firestore
        session_data = get_session_data_from_firestore(session_id)
        if not session_data:
            logger.error(f"No se pudieron obtener datos para sesión {session_id}")
            return
        
        # Verificar que la sesión esté cerrada
        estado_sesion = session_data.get("estado_sesion")
        if estado_sesion != "cerrado":
            logger.info(
                f"Sesión {session_id} no está cerrada (estado: {estado_sesion}). "
                "No se migrará."
            )
            return
        
        logger.info(f"Iniciando migración de sesión cerrada: {session_id}")
        
        # Preparar datos para BigQuery
        bigquery_row = prepare_bigquery_row(session_id, session_data)
        logger.debug(f"Datos preparados para BigQuery: {bigquery_row}")
        
        # Insertar en BigQuery
        if not insert_to_bigquery(bigquery_row):
            logger.error(f"Falló la inserción en BigQuery para sesión {session_id}")
            return
        
        # Eliminar de Firestore si la inserción fue exitosa
        if delete_from_firestore(session_id):
            logger.info(f"Migración completada exitosamente para sesión {session_id}")
        else:
            logger.warning(
                f"Datos insertados en BigQuery pero no se pudo eliminar "
                f"de Firestore: {session_id}"
            )
            
    except Exception as e:
        logger.error(f"Error general en migración de sesión: {e}", exc_info=True)


def convert_firestore_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convierte campos de Firestore del formato de evento a formato Python.
    
    Esta función maneja la conversión de tipos específicos de Firestore
    como timestamps, arrays y objetos anidados.
    
    Args:
        fields: Diccionario con campos en formato de evento de Firestore.
    
    Returns:
        Diccionario con campos convertidos a tipos Python nativos.
    
    Note:
        Esta función actualmente no se usa en el flujo principal,
        pero se mantiene para compatibilidad futura.
    """
    converted_data = {}
    
    for field_name, field_value in fields.items():
        if "stringValue" in field_value:
            converted_data[field_name] = field_value["stringValue"]
            
        elif "booleanValue" in field_value:
            converted_data[field_name] = field_value["booleanValue"]
            
        elif "timestampValue" in field_value:
            timestamp_str = field_value["timestampValue"]
            converted_data[field_name] = datetime.fromisoformat(
                timestamp_str.replace("Z", "+00:00")
            )
            
        elif "arrayValue" in field_value:
            array_data = []
            if "values" in field_value["arrayValue"]:
                for item in field_value["arrayValue"]["values"]:
                    if "mapValue" in item and "fields" in item["mapValue"]:
                        array_data.append(
                            convert_firestore_fields(item["mapValue"]["fields"])
                        )
            converted_data[field_name] = array_data
            
        elif "mapValue" in field_value:
            if "fields" in field_value["mapValue"]:
                converted_data[field_name] = convert_firestore_fields(
                    field_value["mapValue"]["fields"]
                )
    
    return converted_data