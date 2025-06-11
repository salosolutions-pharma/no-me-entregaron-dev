import json
import logging
import os
from typing import Any, Dict, List, Optional

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
    """Convierte recursivamente un objeto bigquery.Row a un diccionario Python estándar."""
    out: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, list):
            out[key] = [
                _convert_bq_row_to_dict_recursive(item) if isinstance(item, bigquery.Row) else item
                for item in value
            ]
        elif isinstance(value, bigquery.Row):
            out[key] = _convert_bq_row_to_dict_recursive(value)
        else:
            out[key] = value
    return out


def load_table_from_json_direct(data: List[Dict[str, Any]], table_reference: str) -> None:
    """
    Carga datos directamente utilizando load_table_from_json sin un búfer de streaming.

    Args:
        data: Lista de diccionarios con los datos a cargar.
        table_reference: Referencia completa de la tabla (proyecto.dataset.tabla).
    """
    if not data:
        logger.info("No hay datos para cargar en BigQuery.")
        return

    client = get_bigquery_client()

    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER
        )

        job = client.load_table_from_json(
            json_rows=data,
            destination=table_reference,
            job_config=job_config
        )

        job.result()

        if job.errors:
            logger.error(f"Errores en la carga directa: {job.errors}")
            raise BigQueryServiceError(f"Errores en la carga directa: {job.errors}")

        logger.info(f"Carga directa exitosa: {job.output_rows} filas en '{table_reference}' SIN BÚFER.")

    except GoogleAPIError as exc:
        logger.exception("Error de la API de BigQuery durante la carga directa.")
        raise BigQueryServiceError(f"Error de BigQuery en la carga directa: {exc}") from exc
    except Exception as exc:
        logger.exception("Error inesperado durante la carga directa en BigQuery.")
        raise BigQueryServiceError(f"Error inesperado en la carga directa: {exc}") from exc


def update_patient_direct(patient_key: str, updates: Dict[str, Any]) -> bool:
    """
    Actualiza un paciente sin búfer utilizando DELETE + INSERT.

    Args:
        patient_key: Clave del paciente a actualizar.
        updates: Campos a actualizar.

    Returns:
        bool: True si es exitoso.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        logger.critical("Variables de entorno de BigQuery incompletas.")
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        get_query = f"""
            SELECT * FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )

        results = client.query(get_query, job_config=job_config).result()
        current_data = None

        for row in results:
            current_data = _convert_bq_row_to_dict_recursive(row)
            break

        if not current_data:
            logger.error(f"Paciente {patient_key} no encontrado para actualizar.")
            return False

        current_data.update(updates)

        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """

        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()

        logger.info(f"Registro {patient_key} eliminado para actualizar.")

        load_table_from_json_direct([current_data], table_reference)

        logger.info(f"Paciente {patient_key} actualizado SIN BÚFER.")
        return True

    except Exception as e:
        logger.error(f"Error al actualizar el paciente {patient_key}: {e}")
        return False


def delete_patient_direct(patient_key: str) -> bool:
    """
    Elimina un paciente sin búfer.

    Args:
        patient_key: Clave del paciente a eliminar.

    Returns:
        bool: True si es exitoso.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )

        job = client.query(delete_query, job_config=job_config)
        job.result()

        if job.errors:
            logger.error(f"Errores al eliminar {patient_key}: {job.errors}")
            return False

        logger.info(f"Paciente {patient_key} eliminado SIN BÚFER.")
        return True

    except Exception as e:
        logger.error(f"Error al eliminar el paciente {patient_key}: {e}")
        return False


def insert_or_update_patient_data(
    patient_data: Dict[str, Any],
    fields_to_update: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Inserta o actualiza datos del paciente utilizando métodos directos sin búfer.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        logger.critical("Variables de entorno de BigQuery incompletas.")
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    patient_key = patient_data.get("paciente_clave")

    if not patient_key:
        logger.error("Se requiere paciente_clave.")
        raise BigQueryServiceError("paciente_clave es nulo o vacío.")

    logger.info(f"Procesando paciente '{patient_key}' SIN BÚFER...")

    exists_query = f"""
        SELECT 1 FROM `{table_reference}`
        WHERE paciente_clave = @patient_key LIMIT 1
    """

    try:
        exists_job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )
        exists_query_results = client.query(exists_query, job_config=exists_job_config).result()
        patient_exists = bool(list(exists_query_results))
    except Exception as exc:
        logger.exception(f"Error al verificar el paciente '{patient_key}'.")
        raise BigQueryServiceError(f"Fallo al verificar el paciente: {exc}") from exc

    if patient_exists:
        logger.info(f"El paciente '{patient_key}' existe. Actualizando SIN BÚFER...")

        if fields_to_update:
            success = update_patient_direct(patient_key, fields_to_update)
            if not success:
                raise BigQueryServiceError(f"Error al actualizar el paciente {patient_key}")

        if patient_data.get("prescripciones"):
            get_query = f"""
                SELECT * FROM `{table_reference}`
                WHERE paciente_clave = @patient_key
            """
            results = client.query(get_query, job_config=exists_job_config).result()
            for row in results:
                current_data = _convert_bq_row_to_dict_recursive(row)
                current_prescriptions = current_data.get("prescripciones", [])
                current_prescriptions.extend(patient_data["prescripciones"])
                current_data["prescripciones"] = current_prescriptions
                update_patient_direct(patient_key, {"prescripciones": current_prescriptions})
                break
    else:
        logger.info(f"El paciente '{patient_key}' no existe. Insertando SIN BÚFER...")

        new_patient_record = {
            "paciente_clave": patient_key,
            "pais": patient_data.get("pais", "CO"),
            "tipo_documento": patient_data.get("tipo_documento"),
            "numero_documento": patient_data.get("numero_documento"),
            "nombre_paciente": patient_data.get("nombre_paciente"),
            "fecha_nacimiento": patient_data.get("fecha_nacimiento"),
            "correo": patient_data.get("correo", []),
            "telefono_contacto": patient_data.get("telefono_contacto", []),
            "canal_contacto": patient_data.get("canal_contacto"),
            "regimen": patient_data.get("regimen"),
            "ciudad": patient_data.get("ciudad"),
            "direccion": patient_data.get("direccion"),
            "operador_logistico": patient_data.get("operador_logistico"),
            "sede_farmacia": patient_data.get("sede_farmacia"),
            "eps_cruda": patient_data.get("eps_cruda"),
            "eps_estandarizada": patient_data.get("eps_estandarizada"),
            "informante": patient_data.get("informante", []),
            "sesiones": patient_data.get("sesiones", []),
            "prescripciones": patient_data.get("prescripciones", []),
            "reclamaciones": patient_data.get("reclamaciones", []),
        }

        if fields_to_update:
            new_patient_record.update(fields_to_update)

        load_table_from_json_direct([new_patient_record], table_reference)
        logger.info(f"Paciente '{patient_key}' insertado SIN BÚFER.")


def _escape_sql_string_value(s: Any) -> str:
    """Escapa valores para SQL STRING."""
    if s is None:
        return "NULL"
    escaped = str(s).replace("'", "''")
    return f"'{escaped}'"


def _build_medicamento_struct_sql(med: Dict[str, Any]) -> str:
    """Construye un STRUCT SQL para un medicamento."""
    nombre_sql = _escape_sql_string_value(med.get("nombre"))
    dosis_sql = _escape_sql_string_value(med.get("dosis"))
    cantidad_sql = _escape_sql_string_value(med.get("cantidad"))
    entregado_sql = _escape_sql_string_value(med.get("entregado", "pendiente"))

    return (
        "STRUCT("
        f"{nombre_sql} AS nombre, "
        f"{dosis_sql} AS dosis, "
        f"{cantidad_sql} AS cantidad, "
        f"{entregado_sql} AS entregado"
        ")"
    )


def _build_prescription_struct_sql(presc: Dict[str, Any]) -> str:
    """Construye un STRUCT SQL para una prescripción."""
    meds_array_sql = ", ".join(_build_medicamento_struct_sql(m) for m in presc.get("medicamentos", []))

    id_session_sql = _escape_sql_string_value(presc.get("id_session"))
    url_prescripcion_sql = _escape_sql_string_value(presc.get("url_prescripcion"))
    diagnostico_sql = _escape_sql_string_value(presc.get("diagnostico"))
    ips_sql = _escape_sql_string_value(presc.get("IPS"))
    fecha_atencion_sql = _escape_sql_string_value(presc.get("fecha_atencion"))
    categoria_riesgo_sql = _escape_sql_string_value(presc.get("categoria_riesgo"))

    return (
        "STRUCT("
        f"{id_session_sql} AS id_session, "
        f"{url_prescripcion_sql} AS url_prescripcion, "
        f"{categoria_riesgo_sql} AS categoria_riesgo, "
        f"{fecha_atencion_sql} AS fecha_atencion, "
        f"{diagnostico_sql} AS diagnostico, "
        f"{ips_sql} AS IPS, "
        f"ARRAY<STRUCT<nombre STRING, dosis STRING, cantidad STRING, entregado STRING>>[{meds_array_sql}] AS medicamentos"
        ")"
    )