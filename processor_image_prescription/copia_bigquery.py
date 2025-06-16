import json
import logging
import os
from typing import Any, Dict, List, Optional
from datetime import date, datetime

from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROJECT_ID: Optional[str] = os.getenv("PROJECT_ID")
DATASET_ID: Optional[str] = os.getenv("DATASET_ID")
TABLE_ID: Optional[str] = os.getenv("TABLE_ID")


class BigQueryServiceError(RuntimeError):
    """Excepción genérica para errores de BigQuery."""


_BQ_CLIENT: Optional[bigquery.Client] = None


def get_bigquery_client() -> bigquery.Client:
    """Devuelve una instancia del cliente de BigQuery, creándola si no existe."""
    global _BQ_CLIENT
    if _BQ_CLIENT is not None:
        return _BQ_CLIENT

    if not PROJECT_ID:
        raise BigQueryServiceError("La variable de entorno PROJECT_ID no está configurada.")

    try:
        _BQ_CLIENT = bigquery.Client(project=PROJECT_ID)
        logger.info("Cliente de BigQuery creado correctamente.")
        return _BQ_CLIENT
    except Exception as exc:
        logger.exception("Error al crear el cliente de BigQuery.")
        raise BigQueryServiceError(f"Fallo al crear el cliente de BigQuery: {exc}") from exc


def _convert_bq_row_to_dict_recursive(row: bigquery.Row) -> Dict[str, Any]:
    """
    Convierte una fila de BigQuery (o un objeto de campo anidado) en un diccionario,
    manejando recursivamente campos anidados y corrigiendo tipos de fecha/hora.
    """
    result = {}
    for key, value in row.items():
        if isinstance(value, bigquery.Row):
            result[key] = _convert_bq_row_to_dict_recursive(value)
        elif isinstance(value, (datetime, date)):
            # Convertir a formato ISO para JSON, si es necesario, o a string simple
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def _get_table_reference() -> str:
    """Construye y devuelve la referencia completa de la tabla de BigQuery."""
    if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
        raise BigQueryServiceError(
            "Variables de entorno PROJECT_ID, DATASET_ID o TABLE_ID no configuradas."
        )
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def load_table_from_json_direct(
    json_rows: List[Dict[str, Any]], table_reference: str
) -> bigquery.LoadJob:
    """
    Carga datos en una tabla de BigQuery directamente desde una lista de diccionarios JSON
    sin usar un archivo temporal. Utiliza el método `insert_rows_json`.
    """
    client = get_bigquery_client()
    table = client.get_table(table_reference)

    # Convertir a JSON strings para insert_rows_json, ajustando las fechas
    rows_for_insert = []
    for row_dict in json_rows:
        # Asegurarse de que las fechas y datetimes estén en formato string ISO 8601
        def convert_dates(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            if isinstance(obj, dict):
                return {k: convert_dates(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert_dates(elem) for elem in obj]
            return obj
        rows_for_insert.append(convert_dates(row_dict))

    errors = client.insert_rows_json(table, rows_for_insert)

    if errors:
        logger.error(f"Errores al insertar filas en BigQuery: {errors}")
        raise BigQueryServiceError(f"Errores al insertar filas: {errors}")
    logger.info(f"✅ {len(json_rows)} fila(s) insertada(s) directamente en {table_reference}.")
    return True # Devolver True para indicar éxito, ya que insert_rows_json no devuelve un LoadJob

def insert_or_update_patient_data(
    patient_data: Dict[str, Any],
    session_id: str,
    fields_to_update: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Inserta o actualiza un registro de paciente en BigQuery.
    Si el paciente ya existe (por paciente_clave), actualiza su registro existente.
    Si no, inserta uno nuevo. La lógica maneja la adición de nuevas prescripciones
    o la actualización de una existente.

    Args:
        patient_data (Dict[str, Any]): Datos del paciente, incluyendo al menos
                                       'paciente_clave' y la 'prescripcion' a añadir/actualizar.
        session_id (str): ID de la sesión para identificar la prescripción.
        fields_to_update (Optional[Dict[str, Any]]): Campos adicionales fuera de 'prescripciones'
                                                    para actualizar en el registro del paciente.

    Returns:
        bool: True si la operación fue exitosa, False en caso contrario.
    """
    client = get_bigquery_client()
    table_reference = _get_table_reference()
    patient_key = patient_data.get("paciente_clave") # Asumimos que patient_data ya trae paciente_clave
    
    if not patient_key:
        logger.error("No se proporcionó 'paciente_clave' en los datos del paciente.")
        return False

    try:
        # 1️⃣ Intentar obtener el registro existente del paciente
        query = f"""
            SELECT * FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
            ]
        )
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        current_data: Dict[str, Any] = {}
        for row in results:
            current_data = _convert_bq_row_to_dict_recursive(row)
            break

        # 2️⃣ Preparar los datos para la inserción/actualización
        new_prescription_data = patient_data.get("prescripcion", {})
        if not new_prescription_data:
            logger.warning(f"No hay datos de prescripción para la sesión {session_id}.")
            # Aún así, si solo se actualizan otros campos, podemos proceder.
        
        # Asegurarse de que la nueva prescripción tiene el session_id
        new_prescription_data["id_session"] = session_id
        # Añadir timestamp de subida
        #new_prescription_data["upload_timestamp"] = datetime.now().isoformat()
        
        # Inicializar o actualizar la lista de prescripciones
        updated_prescriptions = current_data.get("prescripciones", [])
        
        prescription_updated = False
        if new_prescription_data:
            for i, p in enumerate(updated_prescriptions):
                if p.get("id_session") == session_id:
                    updated_prescriptions[i] = new_prescription_data
                    prescription_updated = True
                    logger.info(f"Prescripción existente para sesión '{session_id}' actualizada.")
                    break
            if not prescription_updated:
                updated_prescriptions.append(new_prescription_data)
                logger.info(f"Nueva prescripción para sesión '{session_id}' añadida.")
        
        # Actualizar los campos del paciente
        updated_patient_data = {
            "paciente_clave": patient_key,
            "numero_documento": patient_data.get("numero_documento", current_data.get("numero_documento")),
            "tipo_documento": patient_data.get("tipo_documento", current_data.get("tipo_documento")),
            "nombre_paciente": patient_data.get("nombre_paciente", current_data.get("nombre_paciente")),
            "fecha_nacimiento": patient_data.get("fecha_nacimiento", current_data.get("fecha_nacimiento")),
            "eps_cruda": patient_data.get("eps_cruda", current_data.get("eps_cruda")),
            "telefono_contacto": patient_data.get("telefono_contacto", current_data.get("telefono_contacto")),
            "correo": patient_data.get("correo", current_data.get("correo")),
            #"diagnostico": patient_data.get("diagnostico", current_data.get("diagnostico")),
            #"categoria_riesgo": patient_data.get("categoria_riesgo", current_data.get("categoria_riesgo")),
            #"gs_url": patient_data.get("gs_url", current_data.get("gs_url")), # URL de la última imagen
            "prescripciones": updated_prescriptions
        }

        # Aplicar cualquier campo adicional para actualizar
        if fields_to_update:
            updated_patient_data.update(fields_to_update)

        # 3️⃣ Eliminar el registro existente (si lo hay) y luego insertar el nuevo
        # Esto es un "upsert" manual con DELETE + INSERT
        if current_data:
            delete_query = f"""
                DELETE FROM `{table_reference}`
                WHERE paciente_clave = @patient_key
            """
            delete_job = client.query(delete_query, job_config=job_config)
            delete_job.result() # Esperar a que la eliminación se complete
            logger.info(f"🗑️ Registro existente para '{patient_key}' eliminado para actualización.")

        # Insertar el registro actualizado o nuevo
        load_table_from_json_direct([updated_patient_data], table_reference)
        logger.info(f"✅ Datos de paciente para '{patient_key}' actualizados/insertados en BigQuery.")
        return True

    except GoogleAPIError as e:
        logger.error(f"❌ Error de BigQuery al insertar/actualizar datos del paciente: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error inesperado al insertar/actualizar datos del paciente: {e}")
        return False


def get_patient_data(patient_key: str) -> Optional[Dict[str, Any]]:
    """
    Recupera los datos de un paciente de BigQuery por su clave de paciente.

    Args:
        patient_key (str): La clave única del paciente.

    Returns:
        Optional[Dict[str, Any]]: Un diccionario con los datos del paciente, o None si no se encuentra.
    """
    client = get_bigquery_client()
    table_reference = _get_table_reference()

    query = f"""
        SELECT * FROM `{table_reference}`
        WHERE paciente_clave = @patient_key
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        for row in results:
            return _convert_bq_row_to_dict_recursive(row)
        
        logger.info(f"Paciente con clave '{patient_key}' no encontrado.")
        return None

    except GoogleAPIError as e:
        logger.error(f"Error de BigQuery al recuperar datos del paciente '{patient_key}': {e}")
        raise BigQueryServiceError(f"Error al recuperar datos del paciente: {e}") from e
    except Exception as e:
        logger.error(f"Error inesperado al recuperar datos del paciente '{patient_key}': {e}")
        raise BigQueryServiceError(f"Error inesperado: {e}") from e


def update_patient_medications_no_buffer(
    patient_key: str,
    session_id: str,
    medications_to_mark_delivered: Optional[List[str]] = None,
) -> bool:
    """
    Actualiza el estado de los medicamentos para una prescripción específica de un paciente
    en BigQuery, sin usar un búfer. Realiza una operación de eliminación y luego inserción.

    Args:
        patient_key (str): Clave única del paciente.
        session_id (str): ID de la sesión de la prescripción a actualizar.
        medications_to_mark_delivered (Optional[List[str]]): Lista de nombres de medicamentos
                                                             que **no fueron entregados** (se marcarán como 'no entregado').
                                                             Los demás se marcarán como 'entregado'.

    Returns:
        bool: True si la actualización fue exitosa, False en caso contrario.
    """
    client = get_bigquery_client()
    table_reference = _get_table_reference()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
        ]
    )

    try:
        # 1️⃣ Obtener el registro actual del paciente
        query = f"""
            SELECT * FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
            LIMIT 1
        """
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        current_data: Optional[Dict[str, Any]] = None
        for row in results:
            current_data = _convert_bq_row_to_dict_recursive(row)
            break

        if not current_data:
            logger.warning(f"No se encontró paciente con clave '{patient_key}'.")
            return False

        # 2️⃣ Modificar el estado de los medicamentos en la prescripción específica
        updated_prescriptions = []
        prescription_found = False
        for prescripcion in current_data.get("prescripciones", []):
            if prescripcion.get("session_id") == session_id:
                prescription_found = True
                updated_meds = []
                for med in prescripcion.get("medicamentos", []):
                    med_name = med.get("nombre")
                    if medications_to_mark_delivered and med_name in medications_to_mark_delivered:
                        med["entregado"] = "no entregado"
                        logger.info(f"Medicamento '{med_name}' marcado como 'no entregado'")
                    else:
                        med["entregado"] = "entregado"
                        logger.info(f"Medicamento '{med_name}' marcado como 'entregado'")
                    updated_meds.append(med)
                
                prescripcion["medicamentos"] = updated_meds
                updated_prescriptions.append(prescripcion)
            else:
                updated_prescriptions.append(prescripcion)

        if not prescription_found:
            logger.warning(f"No se encontró prescripción para session_id '{session_id}'")
            return False

        # 3️⃣ Actualizar el registro completo
        current_data["prescripciones"] = updated_prescriptions

        # 4️⃣ DELETE + INSERT SIN BÚFER
        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """

        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        logger.info(f"🗑️ Registro {patient_key} eliminado para actualizar medicamentos.")

        load_table_from_json_direct([current_data], table_reference)
        logger.info(f"✅ Medicamentos actualizados SIN BÚFER para paciente '{patient_key}' en sesión '{session_id}'")
        return True

    except Exception as e:
        logger.error(f"❌ Error actualizando medicamentos SIN BÚFER para paciente '{patient_key}': {e}")
        return False