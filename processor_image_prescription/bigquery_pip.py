import json
import logging
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

load_dotenv()

logger = logging.getLogger(__name__)

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
                _convert_bq_row_to_dict_recursive(item) if isinstance(item, bigquery.Row)
                else _convert_date_values(item)
                for item in value
            ]
        elif isinstance(value, bigquery.Row):
            out[key] = _convert_bq_row_to_dict_recursive(value)
        else:
            out[key] = _convert_date_values(value)
    return out


def _convert_date_values(value: Any) -> Any:
    """Convierte objetos date/datetime a strings ISO para evitar errores JSON."""
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, date):
        return value.strftime('%Y-%m-%d')
    return value


def _clean_record_for_json(record: Dict[str, Any]) -> Dict[str, Any]:
    """Limpia un registro recursivamente convirtiendo fechas a strings."""
    cleaned = {}
    for key, value in record.items():
        if isinstance(value, (date, datetime)):
            cleaned[key] = (value.isoformat() if isinstance(value, datetime)
                          else value.strftime('%Y-%m-%d'))
        elif isinstance(value, dict):
            cleaned[key] = _clean_record_for_json(value)
        elif isinstance(value, list):
            cleaned[key] = [
                _clean_record_for_json(item) if isinstance(item, dict)
                else (item.isoformat() if isinstance(item, datetime)
                      else item.strftime('%Y-%m-%d') if isinstance(item, date)
                      else item)
                for item in value
            ]
        else:
            cleaned[key] = value
    return cleaned


def load_table_from_json_direct(data: List[Dict[str, Any]], table_reference: str) -> None:
    """Carga datos SIN BÚFER usando load_table_from_json."""
    if not data:
        logger.info("No hay datos para cargar en BigQuery.")
        return

    client = get_bigquery_client()

    try:
        cleaned_data = []
        for record in data:
            cleaned_record = _clean_record_for_json(record)
            cleaned_data.append(cleaned_record)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER
        )

        job = client.load_table_from_json(
            json_rows=cleaned_data,
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


def update_single_field_dml(patient_key: str, field_name: str, field_value: Any) -> bool:
    """UPDATE instantáneo SIN BÚFER para campos simples usando DML."""
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        if field_value is None:
            sql_value = "NULL"
        elif isinstance(field_value, str):
            escaped_value = field_value.replace("'", "''")
            sql_value = f"'{escaped_value}'"
        elif isinstance(field_value, bool):
            sql_value = "TRUE" if field_value else "FALSE"
        elif isinstance(field_value, (int, float)):
            sql_value = str(field_value)
        elif isinstance(field_value, list):
            if all(isinstance(item, str) for item in field_value):
                escaped_items = [f"'{item.replace(chr(39), chr(39)+chr(39))}'" 
                               for item in field_value if item.strip()]
                sql_value = f"[{', '.join(escaped_items)}]"
            else:
                logger.warning(f"Array complejo no soportado para UPDATE directo: {field_value}")
                return False
        else:
            logger.warning(f"Tipo no soportado para UPDATE directo: {type(field_value)}")
            return False

        update_query = f"""
            UPDATE `{table_reference}`
            SET {field_name} = {sql_value}
            WHERE paciente_clave = @patient_key
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
            ]
        )

        logger.info(f"UPDATE SIN BÚFER: campo '{field_name}' para paciente '{patient_key}'")
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()

        if query_job.errors:
            logger.error(f"Errores en UPDATE DML: {query_job.errors}")
            return False

        rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
        if rows_affected == 0:
            logger.warning(f"No se encontró paciente '{patient_key}' para actualizar")
            return False

        logger.info(f"UPDATE SIN BÚFER exitoso: {rows_affected} fila(s) afectada(s)")
        return True

    except Exception as e:
        logger.error(f"Error en UPDATE DML SIN BÚFER: {e}")
        return False


def update_prescriptions_with_load_table(patient_key: str, new_prescriptions: List[Dict[str, Any]]) -> bool:
    """Actualiza prescripciones usando DELETE + load_table_from_json SIN BÚFER."""
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
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
            logger.error(f"Paciente {patient_key} no encontrado para actualizar prescripciones.")
            return False

        current_prescriptions = current_data.get("prescripciones", [])
        current_prescriptions.extend(new_prescriptions)
        current_data["prescripciones"] = current_prescriptions

        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """

        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        logger.info(f"Registro {patient_key} eliminado para actualizar prescripciones.")

        load_table_from_json_direct([current_data], table_reference)
        logger.info(f"Prescripciones actualizadas SIN BÚFER para paciente {patient_key}")
        return True

    except Exception as e:
        logger.error(f"Error actualizando prescripciones SIN BÚFER: {e}")
        return False


def insert_or_update_patient_data(patient_data: Dict[str, Any],
                                 fields_to_update: Optional[Dict[str, Any]] = None) -> None:
    """ENFOQUE HÍBRIDO OPTIMIZADO SIN BÚFER."""
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        logger.critical("Variables de entorno de BigQuery incompletas.")
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    patient_key = patient_data.get("paciente_clave")

    if not patient_key:
        logger.error("Se requiere paciente_clave.")
        raise BigQueryServiceError("paciente_clave es nulo o vacío.")

    logger.info(f"Procesando paciente '{patient_key}' HÍBRIDO SIN BÚFER...")

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
        logger.info(f"Paciente '{patient_key}' existe. Usando UPDATES HÍBRIDOS SIN BÚFER...")

        simple_fields = [
            "fecha_nacimiento", "correo", "telefono_contacto", "regimen",
            "ciudad", "direccion", "canal_contacto", "eps_estandarizada",
            "farmacia", "sede_farmacia"
        ]

        if fields_to_update:
            for field_name, field_value in fields_to_update.items():
                if field_name in simple_fields:
                    logger.info(f"UPDATE DML instantáneo para campo '{field_name}'")
                    success = update_single_field_dml(patient_key, field_name, field_value)
                    if not success:
                        logger.warning(f"No se pudo actualizar '{field_name}' con DML, usando fallback")
                        _fallback_update_complete_record(patient_key, {field_name: field_value})

        if patient_data.get("prescripciones"):
            logger.info("Actualizando prescripciones con load_table_from_json SIN BÚFER")
            success = update_prescriptions_with_load_table(patient_key, patient_data["prescripciones"])
            if not success:
                raise BigQueryServiceError(f"Error al actualizar prescripciones del paciente {patient_key}")

    else:
        logger.info(f"Paciente '{patient_key}' no existe. Insertando SIN BÚFER...")

        new_patient_record = _prepare_clean_patient_record(patient_data, fields_to_update)
        load_table_from_json_direct([new_patient_record], table_reference)
        logger.info(f"Paciente '{patient_key}' insertado SIN BÚFER.")


def _fallback_update_complete_record(patient_key: str, updates: Dict[str, Any]) -> bool:
    """Fallback: DELETE + INSERT completo usando load_table_from_json SIN BÚFER."""
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
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
            logger.error(f"Paciente {patient_key} no encontrado para fallback update.")
            return False

        current_data.update(updates)

        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """

        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        logger.info(f"Registro {patient_key} eliminado para fallback update.")

        load_table_from_json_direct([current_data], table_reference)
        logger.info(f"Fallback update completado SIN BÚFER para paciente {patient_key}")
        return True

    except Exception as e:
        logger.error(f"Error en fallback update SIN BÚFER: {e}")
        return False


def _prepare_clean_patient_record(patient_data: Dict[str, Any], 
                                 fields_to_update: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Prepara un registro limpio del paciente asegurando que todos los campos requeridos tengan valores válidos."""
    new_patient_record = {
        "paciente_clave": patient_data.get("paciente_clave", ""),
        "pais": patient_data.get("pais", "CO"),
        "tipo_documento": patient_data.get("tipo_documento") or "",
        "numero_documento": patient_data.get("numero_documento") or "",
        "nombre_paciente": patient_data.get("nombre_paciente") or "",
        "fecha_nacimiento": patient_data.get("fecha_nacimiento"),
        "correo": patient_data.get("correo", []),
        "telefono_contacto": patient_data.get("telefono_contacto", []),
        "canal_contacto": patient_data.get("canal_contacto"),
        "regimen": patient_data.get("regimen"),
        "ciudad": patient_data.get("ciudad"),
        "direccion": patient_data.get("direccion"),
        "farmacia": patient_data.get("farmacia"),
        "sede_farmacia": patient_data.get("sede_farmacia"),
        "eps_cruda": patient_data.get("eps_cruda"),
        "eps_estandarizada": patient_data.get("eps_estandarizada"),
        "informante": patient_data.get("informante", []),
        "prescripciones": patient_data.get("prescripciones", []),
        "reclamaciones": patient_data.get("reclamaciones", []),
    }

    if fields_to_update:
        for key, value in fields_to_update.items():
            if key in new_patient_record:
                new_patient_record[key] = value
            else:
                logger.warning(f"Campo '{key}' no reconocido en el esquema, ignorando.")

    if not new_patient_record["paciente_clave"]:
        raise BigQueryServiceError("paciente_clave no puede estar vacío")

    if not new_patient_record["tipo_documento"]:
        logger.warning("tipo_documento está vacío, usando valor por defecto")
        new_patient_record["tipo_documento"] = "CC"

    if not new_patient_record["numero_documento"]:
        logger.warning("numero_documento está vacío, usando valor por defecto")
        new_patient_record["numero_documento"] = "00000000"

    if not new_patient_record["nombre_paciente"]:
        logger.warning("nombre_paciente está vacío, usando valor por defecto")
        new_patient_record["nombre_paciente"] = "Paciente Sin Nombre"

    for key, value in new_patient_record.items():
        if value is None:
            if key in ["correo", "telefono_contacto", "informante", "prescripciones", "reclamaciones"]:
                new_patient_record[key] = []
            else:
                new_patient_record[key] = ""

    return new_patient_record


def update_patient_medications_no_buffer(patient_key: str, session_id: str, 
                                        undelivered_med_names: List[str]) -> bool:
    """Actualiza medicamentos SIN BÚFER usando DELETE + load_table_from_json."""
    try:
        if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
            raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

        client = get_bigquery_client()
        table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

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
            logger.warning(f"Paciente con clave '{patient_key}' no encontrado.")
            return False

        prescripciones = current_data.get("prescripciones", [])
        updated_prescriptions = []
        prescription_found = False

        for prescripcion in prescripciones:
            if prescripcion.get("id_session") == session_id:
                prescription_found = True
                updated_meds = []

                for med in prescripcion.get("medicamentos", []):
                    med_name = med.get("nombre", "")
                    if med_name in undelivered_med_names:
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

        current_data["prescripciones"] = updated_prescriptions

        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """

        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        logger.info(f"Registro {patient_key} eliminado para actualizar medicamentos.")

        load_table_from_json_direct([current_data], table_reference)
        logger.info(f"Medicamentos actualizados SIN BÚFER para paciente '{patient_key}' en sesión '{session_id}'")
        return True

    except Exception as e:
        logger.error(f"Error actualizando medicamentos SIN BÚFER para paciente '{patient_key}': {e}")
        return False
    
def save_document_url_to_reclamacion(patient_key: str, nivel_escalamiento: int, 
                                    url_documento: str, tipo_documento: str) -> bool:
    """
    Actualiza la URL del documento generado en la reclamación correspondiente.
    
    Args:
        patient_key: Clave del paciente
        nivel_escalamiento: Nivel de escalamiento de la reclamación (2=Supersalud, 3=Tutela, 4=Desacato)
        url_documento: URL del documento en Cloud Storage
        tipo_documento: Tipo de documento ("tutela", "desacato")
        
    Returns:
        bool: True si se actualizó correctamente
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # Obtener datos actuales del paciente
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
            logger.error(f"Paciente {patient_key} no encontrado para actualizar URL de documento.")
            return False
        
        # Encontrar y actualizar la reclamación correspondiente
        reclamaciones = current_data.get("reclamaciones", [])
        reclamacion_actualizada = False
        
        for reclamacion in reclamaciones:
            if reclamacion.get("nivel_escalamiento") == nivel_escalamiento:
                reclamacion["url_documento"] = url_documento
                reclamacion["fecha_generacion_documento"] = datetime.now().isoformat()
                reclamacion["tipo_documento"] = tipo_documento
                reclamacion_actualizada = True
                logger.info(f"URL de documento actualizada para nivel {nivel_escalamiento}: {url_documento}")
                break
        
        if not reclamacion_actualizada:
            logger.warning(f"No se encontró reclamación de nivel {nivel_escalamiento} para paciente {patient_key}")
            return False
        
        # Actualizar usando DELETE + INSERT
        current_data["reclamaciones"] = reclamaciones
        
        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """
        
        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        logger.info(f"Registro {patient_key} eliminado para actualizar URL de documento.")
        
        load_table_from_json_direct([current_data], table_reference)
        logger.info(f"URL de documento guardada para paciente {patient_key}, nivel {nivel_escalamiento}")
        return True
        
    except Exception as e:
        logger.error(f"Error guardando URL de documento: {e}")
        return False


def update_reclamacion_status(patient_key: str, nivel_escalamiento: int, 
                            nuevo_estado: str, numero_radicado: str = None, 
                            fecha_radicacion: str = None) -> bool:
    """
    Actualiza el estado y radicado de una reclamación específica.
    
    Args:
        patient_key: Clave del paciente
        nivel_escalamiento: Nivel de escalamiento de la reclamación
        nuevo_estado: Nuevo estado ("pendiente_radicacion", "radicado", "resuelto", etc.)
        numero_radicado: Número de radicado (opcional)
        fecha_radicacion: Fecha de radicación (opcional)
        
    Returns:
        bool: True si se actualizó correctamente
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # Obtener datos actuales del paciente
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
            logger.error(f"Paciente {patient_key} no encontrado para actualizar estado de reclamación.")
            return False
        
        # Encontrar y actualizar la reclamación correspondiente
        reclamaciones = current_data.get("reclamaciones", [])
        reclamacion_actualizada = False
        
        for reclamacion in reclamaciones:
            if reclamacion.get("nivel_escalamiento") == nivel_escalamiento:
                reclamacion["estado_reclamacion"] = nuevo_estado
                
                if numero_radicado:
                    reclamacion["numero_radicado"] = numero_radicado
                    
                if fecha_radicacion:
                    reclamacion["fecha_radicacion"] = fecha_radicacion
                else:
                    # Si se proporciona radicado pero no fecha, usar fecha actual
                    if numero_radicado:
                        reclamacion["fecha_radicacion"] = datetime.now().strftime("%Y-%m-%d")
                
                reclamacion_actualizada = True
                logger.info(f"Estado de reclamación actualizado para nivel {nivel_escalamiento}: {nuevo_estado}")
                break
        
        if not reclamacion_actualizada:
            logger.warning(f"No se encontró reclamación de nivel {nivel_escalamiento} para paciente {patient_key}")
            return False
        
        # Actualizar usando DELETE + INSERT
        current_data["reclamaciones"] = reclamaciones
        
        delete_query = f"""
            DELETE FROM `{table_reference}`
            WHERE paciente_clave = @patient_key
        """
        
        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        logger.info(f"Registro {patient_key} eliminado para actualizar estado de reclamación.")
        
        load_table_from_json_direct([current_data], table_reference)
        logger.info(f"Estado de reclamación actualizado para paciente {patient_key}, nivel {nivel_escalamiento}")
        return True
        
    except Exception as e:
        logger.error(f"Error actualizando estado de reclamación: {e}")
        return False