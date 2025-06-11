import json
import logging
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv

load_dotenv()

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROJECT_ID: Optional[str] = os.getenv("PROJECT_ID")
DATASET_ID: Optional[str] = os.getenv("DATASET_ID")
TABLE_ID: Optional[str] = os.getenv("TABLE_ID")  # Tabla de pacientes


class BigQueryServiceError(RuntimeError):
    """Excepción genérica para errores en BigQuery."""


_BQ_CLIENT: Optional[bigquery.Client] = None


def get_bigquery_client() -> bigquery.Client:
    global _BQ_CLIENT
    if _BQ_CLIENT is not None:
        return _BQ_CLIENT

    if not PROJECT_ID:
        raise BigQueryServiceError("Variable de entorno PROJECT_ID no configurada.")

    try:
        _BQ_CLIENT = bigquery.Client(project=PROJECT_ID)
        logger.info("Cliente BigQuery creado exitosamente.")
        return _BQ_CLIENT
    except Exception as exc:
        logger.exception("Error creando cliente BigQuery.")
        raise BigQueryServiceError("Fallo al crear cliente BigQuery.") from exc


def _convert_bq_row_to_dict_recursive(row: bigquery.Row) -> Dict[str, Any]:
    """
    Convierte recursivamente un objeto bigquery.Row (o sub-Rows) en un diccionario
    Python estándar, serializable y fácil de manipular.
    """
    out: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, list):
            # Si es una lista, procesa cada elemento recursivamente
            out[key] = [
                _convert_bq_row_to_dict_recursive(item) if isinstance(item, bigquery.Row) else item
                for item in value
            ]
        elif isinstance(value, bigquery.Row):
            # Si es un objeto anidado (otro Row), procesa recursivamente
            out[key] = _convert_bq_row_to_dict_recursive(value)
        else:
            out[key] = value
    return out

def _escape_sql_string_value(s: Any) -> str:
    """Escapa valores para SQL STRING, usando comillas simples duplicadas."""
    if s is None:
        return "NULL"
    escaped = str(s).replace("'", "''")
    return f"'{escaped}'"


def _escape_sql_string_value_typed(s: Any) -> str:
    """
    Escapa valores para campos STRING en BigQuery con CAST explícito.
    Útil cuando se requiere mantener tipo STRING, incluso si es NULL.
    """
    if s is None:
        return "CAST(NULL AS STRING)"
    escaped = str(s).replace("'", "''")
    return f"'{escaped}'"


def _build_medicamento_struct_sql(med: Dict[str, Any]) -> str:
    """Construye un STRUCT SQL para un medicamento con nombre, dosis, cantidad y entregado."""
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


def _build_informante_struct_sql(informante_data: Dict[str, Any]) -> str:
    """Construye un STRUCT SQL para un informante (nombre, parentesco, identificación)."""
    nombre_sql = _escape_sql_string_value(informante_data.get("nombre"))
    parentesco_sql = _escape_sql_string_value(informante_data.get("parentesco"))
    identificacion_sql = _escape_sql_string_value(informante_data.get("identificacion"))

    return (
        "STRUCT("
        f"{nombre_sql} AS nombre, "
        f"{parentesco_sql} AS parentesco, "
        f"{identificacion_sql} AS identificacion"
        ")"
    )


def _get_medicamento_schema_type() -> str:
    return "STRUCT<nombre STRING, dosis STRING, cantidad STRING, entregado STRING>"


def _build_prescription_struct_sql(presc: Dict[str, Any]) -> str:
    """
    Construye un STRUCT SQL para una prescripción, incluyendo el array de medicamentos.
    """
    meds_array_sql = ", ".join(_build_medicamento_struct_sql(m) for m in presc.get("medicamentos", []))

    id_session_sql = _escape_sql_string_value(presc.get("id_session"))
    url_prescripcion_sql = _escape_sql_string_value(presc.get("url_prescripcion"))
    diagnostico_sql = _escape_sql_string_value(presc.get("diagnostico"))
    ips_sql = _escape_sql_string_value(presc.get("IPS"))

    fecha_atencion_sql = _escape_sql_string_value_typed(presc.get("fecha_atencion"))
    categoria_riesgo_sql = _escape_sql_string_value_typed(presc.get("categoria_riesgo"))

    return (
        "STRUCT("
        f"{id_session_sql} AS id_session, "
        f"{url_prescripcion_sql} AS url_prescripcion, "
        f"{categoria_riesgo_sql} AS categoria_riesgo, "
        f"{fecha_atencion_sql} AS fecha_atencion, "
        f"{diagnostico_sql} AS diagnostico, "
        f"{ips_sql} AS IPS, "
        f"ARRAY<{_get_medicamento_schema_type()}>[{meds_array_sql}] AS medicamentos"
        ")"
    )


def batch_load_to_bigquery(
    data: List[Dict[str, Any]], client: bigquery.Client, table_reference: str
) -> None:
    """
    Carga masiva JSONL en BigQuery (append). Usa archivo temporal para streaming.
    """
    if not data:
        logger.info("No hay datos para cargar en BigQuery.")
        return

    temp_file_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            suffix=".jsonl",
            delete=False,
        ) as temp_file:
            for record in data:
                json.dump(record, temp_file, ensure_ascii=False)
                temp_file.write("\n")
            temp_file_path = Path(temp_file.name)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )

        with temp_file_path.open("rb") as file_handle:
            job = client.load_table_from_file(file_handle, table_reference, job_config=job_config)
        job.result()

        if job.errors:
            logger.error(f"Errores en carga masiva: {job.errors}")
            raise BigQueryServiceError(f"Errores en carga masiva: {job.errors}")

        logger.info(f"Carga masiva exitosa: {job.output_rows} filas en '{table_reference}'.")

    except GoogleAPIError as exc:
        logger.exception("Error API BigQuery durante carga masiva.")
        raise BigQueryServiceError(f"Error BigQuery en carga masiva: {exc}") from exc
    except Exception as exc:
        logger.exception("Error inesperado durante carga masiva en BigQuery.")
        raise BigQueryServiceError(f"Error inesperado en carga masiva: {exc}") from exc
    finally:
        if temp_file_path:
            with suppress(FileNotFoundError):
                temp_file_path.unlink()


def insert_or_update_patient_data(
    patient_data: Dict[str, Any],
    fields_to_update: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Inserta o actualiza registro de paciente en BigQuery.
    Actualiza campos específicos si ya existe.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        logger.critical("Variables entorno BigQuery incompletas.")
        raise BigQueryServiceError("Variables entorno BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    patient_key = patient_data.get("paciente_clave")

    if not patient_key:
        logger.error("paciente_clave es requerido.")
        raise BigQueryServiceError("paciente_clave es nulo o vacío.")
    logger.info(f"📥 Intentando insertar o actualizar datos para paciente '{patient_key}' en BigQuery...")

    # Verifica existencia
    exists_query_sql = f"""
        SELECT 1 FROM `{table_reference}`
        WHERE paciente_clave = @patient_key LIMIT 1
    """
    try:
        exists_job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )
        exists_query_results = client.query(exists_query_sql, job_config=exists_job_config).result()
        patient_exists = bool(list(exists_query_results))
    except GoogleAPIError as exc:
        logger.exception(f"Error BigQuery al verificar paciente '{patient_key}'.")
        raise BigQueryServiceError(f"Fallo al verificar paciente: {exc}") from exc
    except Exception as exc:
        logger.exception(f"Error inesperado al verificar paciente '{patient_key}'.")
        raise BigQueryServiceError(f"Error inesperado al verificar paciente: {exc}") from exc

    if patient_exists:
        logger.info(f"Paciente '{patient_key}' existe. Actualizando...")

        if not fields_to_update and not patient_data.get("prescripciones"):
            logger.info(f"No hay campos ni prescripciones para actualizar para paciente '{patient_key}'.")
            return

        update_clauses = []
        update_params = [bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]

        # Manejo especial campo informante
        if fields_to_update and "informante" in fields_to_update:
            informante_value = fields_to_update["informante"]
            logger.info(f"Actualizando campo informante paciente '{patient_key}'.")

            if isinstance(informante_value, list) and informante_value:
                informante_structs = [
                    _build_informante_struct_sql(inf) for inf in informante_value
                ]
                informante_array_sql = f"[{', '.join(informante_structs)}]"
                update_clauses.append(f"informante = {informante_array_sql}")
            else:
                update_clauses.append("informante = NULL")

            fields_to_update.pop("informante")

        # Otros campos
        if fields_to_update:
            for field, value in fields_to_update.items():
                if isinstance(value, list):
                    escaped_list = [_escape_sql_string_value(item) for item in value]
                    update_clauses.append(f"{field} = ARRAY<STRING>[{', '.join(escaped_list)}]")
                else:
                    param_name = f"new_{field}"
                    update_clauses.append(f"{field} = @{param_name}")
                    update_params.append(
                        bigquery.ScalarQueryParameter(param_name, "STRING", value)
                    )

        # Añadir prescripciones con ARRAY_CONCAT si aplica
        if "prescripciones" in patient_data and patient_data["prescripciones"]:
            current_presc = patient_data["prescripciones"][0]
            new_presc_struct = _build_prescription_struct_sql(current_presc)
            update_clauses.append(
                f"prescripciones = ARRAY_CONCAT(IFNULL(prescripciones, []), [{new_presc_struct}])"
            )

        if update_clauses:
            update_sql = f"""
                UPDATE `{table_reference}`
                SET {', '.join(update_clauses)}
                WHERE paciente_clave = @patient_key
            """
            try:
                job = client.query(update_sql, job_config=bigquery.QueryJobConfig(query_parameters=update_params))
                job.result()
                if job.errors:
                    logger.error(f"Errores en UPDATE paciente '{patient_key}': {job.errors}")
                    raise BigQueryServiceError(f"Errores en UPDATE: {job.errors}")

                logger.info(f"Paciente '{patient_key}' actualizado correctamente.")
            except GoogleAPIError as exc:
                logger.exception(f"Error BigQuery UPDATE paciente '{patient_key}'.")
                raise BigQueryServiceError(f"Fallo UPDATE paciente: {exc}") from exc
            except Exception as exc:
                logger.exception(f"Error inesperado UPDATE paciente '{patient_key}'.")
                raise BigQueryServiceError(f"Error inesperado UPDATE: {exc}") from exc
        else:
            logger.info(f"No hay cláusulas para actualizar paciente '{patient_key}'.")

    else:
        logger.info(f"Paciente '{patient_key}' no existe. Insertando nuevo registro.")

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

        batch_load_to_bigquery([new_patient_record], client, table_reference)
        logger.info(f"Paciente '{patient_key}' insertado correctamente.")
