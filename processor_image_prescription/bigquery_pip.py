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


# ✅ NUEVA FUNCIÓN: UPDATE granular sin DELETE
def update_single_field_safe(patient_key: str, field_name: str, field_value: Any) -> bool:
    """
    UPDATE instantáneo y SEGURO para campos simples usando DML.
    NO borra datos, solo actualiza el campo específico.
    ACTUALIZADO: Soporte para campo informante.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # ✅ NUEVO: Manejo especial para campo informante
        if field_name == "informante" and isinstance(field_value, list) and field_value:
            informante = field_value[0] if field_value else {}
            
            update_query = f"""
                UPDATE `{table_reference}`
                SET informante = [
                    STRUCT(
                        @nombre AS nombre,
                        @parentesco AS parentesco,
                        @identificacion AS identificacion
                    )
                ]
                WHERE paciente_clave = @patient_key
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
                    bigquery.ScalarQueryParameter("nombre", "STRING", str(informante.get("nombre", ""))),
                    bigquery.ScalarQueryParameter("parentesco", "STRING", str(informante.get("parentesco", ""))),
                    bigquery.ScalarQueryParameter("identificacion", "STRING", str(informante.get("identificacion", "")))
                ]
            )
            
            logger.info(f"🔄 UPDATE INFORMANTE para paciente '{patient_key}'")
            
            query_job = client.query(update_query, job_config=job_config)
            query_job.result()

            if query_job.errors:
                logger.error(f"Errores en UPDATE informante: {query_job.errors}")
                return False

            rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
            if rows_affected == 0:
                logger.warning(f"No se encontró paciente '{patient_key}' para actualizar informante")
                return False

            logger.info(f"✅ Informante actualizado exitosamente")
            return True

        # ✅ RESTO DEL CÓDIGO EXISTENTE SIN CAMBIOS
        # Preparar valor para SQL
        # if field_value is None:
        #     sql_value = "NULL"
        # elif isinstance(field_value, str):
        #     escaped_value = field_value.replace("'", "''")
        #     sql_value = f"'{escaped_value}'"
        # elif isinstance(field_value, bool):
        #     sql_value = "TRUE" if field_value else "FALSE"
        # elif isinstance(field_value, (int, float)):
        #     sql_value = str(field_value)
        # elif isinstance(field_value, list):
        #     if all(isinstance(item, str) for item in field_value):
        #         escaped_items = [f"'{item.replace(chr(39), chr(39)+chr(39))}'" 
        #                        for item in field_value if item.strip()]
        #         sql_value = f"[{', '.join(escaped_items)}]"
        #     else:
        #         logger.warning(f"Array complejo no soportado para UPDATE directo: {field_value}")
        #         return False
        # else:
        #     logger.warning(f"Tipo no soportado para UPDATE directo: {type(field_value)}")
        #     return False

        if field_name in ["correo", "telefono_contacto", "informante"]:  # los campos tipo array
            # Si el valor es string, conviértelo en array
            if isinstance(field_value, str):
                value_for_bq = [field_value] if field_value else []
            elif isinstance(field_value, list):
                value_for_bq = field_value
            else:
                value_for_bq = []
            field_type = "ARRAY<STRING>"
        else:
            value_for_bq = field_value
            if isinstance(field_value, bool):
                field_type = "BOOL"
            elif isinstance(field_value, int):
                field_type = "INT64"
            elif isinstance(field_value, float):
                field_type = "FLOAT64"
            else:
                field_type = "STRING"

        # UPDATE seguro
        update_query = f"""
            UPDATE `{table_reference}`
            SET {field_name} = @field_value
            WHERE paciente_clave = @patient_key
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("field_value", "STRING", value_for_bq)
                if field_type.startswith("ARRAY") else
                bigquery.ScalarQueryParameter("field_value", field_type, value_for_bq),
                bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
            ]
        )

        logger.info(f"🔄 UPDATE SEGURO: campo '{field_name}' para paciente '{patient_key}'")
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()

        if query_job.errors:
            logger.error(f"Errores en UPDATE: {query_job.errors}")
            return False

        rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
        if rows_affected == 0:
            logger.warning(f"No se encontró paciente '{patient_key}' para actualizar")
            return False

        logger.info(f"✅ UPDATE exitoso: {rows_affected} fila(s) afectada(s)")
        return True

    except Exception as e:
        logger.error(f"❌ Error en UPDATE seguro: {e}")
        return False


# Reemplazar la función add_reclamacion_safe en bigquery_pip.py:

def add_reclamacion_safe(patient_key: str, nueva_reclamacion: Dict[str, Any]) -> bool:
    """
    Agrega una nueva reclamación usando solo los campos que existen en el esquema.
    VERSIÓN CORREGIDA basada en el esquema real de la tabla.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # ✅ USAR solo los campos que existen en el esquema real
        update_query = f"""
            UPDATE `{table_reference}`
            SET reclamaciones = (
                SELECT ARRAY_CONCAT(
                    IFNULL(reclamaciones, []),
                    [STRUCT(
                        @med_no_entregados AS med_no_entregados,
                        @tipo_accion AS tipo_accion,
                        @texto_reclamacion AS texto_reclamacion,
                        @estado_reclamacion AS estado_reclamacion,
                        @nivel_escalamiento AS nivel_escalamiento,
                        @url_documento AS url_documento,
                        @numero_radicado AS numero_radicado,
                        @fecha_radicacion AS fecha_radicacion,
                        @fecha_revision AS fecha_revision,
                        @id_session AS id_session
                    )]
                )
                FROM UNNEST([1])
            )
            WHERE paciente_clave = @patient_key
            """

        query_parameters = [
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
            bigquery.ScalarQueryParameter("med_no_entregados", "STRING", 
                str(nueva_reclamacion.get('med_no_entregados', ''))),
            bigquery.ScalarQueryParameter("tipo_accion", "STRING", 
                str(nueva_reclamacion.get('tipo_accion', ''))),
            bigquery.ScalarQueryParameter("texto_reclamacion", "STRING", 
                str(nueva_reclamacion.get('texto_reclamacion', ''))),
            bigquery.ScalarQueryParameter("estado_reclamacion", "STRING", 
                str(nueva_reclamacion.get('estado_reclamacion', 'pendiente_radicacion'))),
            bigquery.ScalarQueryParameter("nivel_escalamiento", "INTEGER", 
                int(nueva_reclamacion.get('nivel_escalamiento', 1))),
            bigquery.ScalarQueryParameter("url_documento", "STRING", 
                str(nueva_reclamacion.get('url_documento', ''))),
            bigquery.ScalarQueryParameter("numero_radicado", "STRING", 
                str(nueva_reclamacion.get('numero_radicado', ''))),
            bigquery.ScalarQueryParameter("fecha_radicacion", "DATE", 
                nueva_reclamacion.get('fecha_radicacion')),
            bigquery.ScalarQueryParameter("fecha_revision", "DATE", 
                nueva_reclamacion.get('fecha_revision')),
            bigquery.ScalarQueryParameter("id_session", "STRING", 
                str(nueva_reclamacion.get('id_session', '')))
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        logger.info(f"➕ Agregando reclamación {nueva_reclamacion.get('tipo_accion')} para paciente {patient_key}")
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()

        if query_job.errors:
            logger.error(f"Errores agregando reclamación: {query_job.errors}")
            return False

        rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
        if rows_affected == 0:
            logger.warning(f"No se encontró paciente '{patient_key}' para agregar reclamación")
            return False

        logger.info(f"✅ Reclamación agregada exitosamente: {rows_affected} fila(s) afectada(s)")
        return True

    except Exception as e:
        logger.error(f"❌ Error agregando reclamación: {e}")
        return False
    
def update_reclamacion_by_level_safe(patient_key: str, nivel_escalamiento: int, 
                                   updates: Dict[str, Any]) -> bool:
    """
    Actualiza una reclamación específica por nivel de escalamiento de forma segura.
    VERSIÓN CORREGIDA - No maneja campos de radicación.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        update_query = f"""
        UPDATE `{table_reference}`
        SET reclamaciones = ARRAY(
            SELECT AS STRUCT
                rec.med_no_entregados,
                rec.tipo_accion,
                rec.texto_reclamacion,
                CASE 
                    WHEN rec.nivel_escalamiento = @nivel_escalamiento THEN
                        COALESCE(@new_estado, rec.estado_reclamacion)
                    ELSE rec.estado_reclamacion
                END AS estado_reclamacion,
                rec.nivel_escalamiento,
                CASE 
                    WHEN rec.nivel_escalamiento = @nivel_escalamiento THEN
                        COALESCE(@new_url, rec.url_documento)
                    ELSE rec.url_documento
                END AS url_documento,
                rec.id_session
            FROM UNNEST(reclamaciones) AS rec
        )
        WHERE paciente_clave = @patient_key
        """

        query_parameters = [
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
            bigquery.ScalarQueryParameter("nivel_escalamiento", "INTEGER", nivel_escalamiento),
            bigquery.ScalarQueryParameter("new_estado", "STRING", updates.get('estado_reclamacion')),
            bigquery.ScalarQueryParameter("new_url", "STRING", updates.get('url_documento'))
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        logger.info(f"🔄 Actualizando reclamación nivel {nivel_escalamiento} para paciente {patient_key}")
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()

        if query_job.errors:
            logger.error(f"Errores actualizando reclamación: {query_job.errors}")
            return False

        rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
        if rows_affected == 0:
            logger.warning(f"No se encontró paciente '{patient_key}' para actualizar reclamación")
            return False

        logger.info(f"✅ Reclamación actualizada exitosamente")
        return True

    except Exception as e:
        logger.error(f"❌ Error actualizando reclamación: {e}")
        return False

# FUNCIÓN FALTANTE PARA AGREGAR AL FINAL DE bigquery_pip.py

def update_reclamacion_by_session_safe(patient_key: str, session_id: str, 
                                      updates: Dict[str, Any]) -> bool:
    """
    Actualiza una reclamación específica por session_id de forma segura.
    NUEVA FUNCIÓN para corregir el error en claim_generator.py
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        update_query = f"""
        UPDATE `{table_reference}`
        SET reclamaciones = ARRAY(
            SELECT AS STRUCT
                rec.med_no_entregados,
                rec.tipo_accion,
                rec.texto_reclamacion,
                CASE 
                    WHEN rec.id_session = @session_id THEN
                        COALESCE(@new_estado, rec.estado_reclamacion)
                    ELSE rec.estado_reclamacion
                END AS estado_reclamacion,
                rec.nivel_escalamiento,
                CASE 
                    WHEN rec.id_session = @session_id THEN
                        COALESCE(@new_url, rec.url_documento)
                    ELSE rec.url_documento
                END AS url_documento,
                rec.numero_radicado,
                rec.fecha_radicacion,
                rec.fecha_revision,
                rec.id_session
            FROM UNNEST(reclamaciones) AS rec
        )
        WHERE paciente_clave = @patient_key
        """

        query_parameters = [
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id),
            bigquery.ScalarQueryParameter("new_estado", "STRING", updates.get('estado_reclamacion')),
            bigquery.ScalarQueryParameter("new_url", "STRING", updates.get('url_documento'))
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        logger.info(f"🔄 Actualizando reclamación session_id {session_id} para paciente {patient_key}")
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()

        if query_job.errors:
            logger.error(f"Errores actualizando reclamación por session: {query_job.errors}")
            return False

        rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
        if rows_affected == 0:
            logger.warning(f"No se encontró paciente '{patient_key}' con session_id '{session_id}' para actualizar reclamación")
            return False

        logger.info(f"✅ Reclamación actualizada exitosamente por session_id")
        return True

    except Exception as e:
        logger.error(f"❌ Error actualizando reclamación por session_id: {e}")
        return False
    
def save_document_url_to_reclamacion(patient_key: str, nivel_escalamiento: int = None, session_id: str = None, 
                                    url_documento: str = "", tipo_documento: str = "") -> bool:
    """
    Actualiza la URL del documento generado en la reclamación correspondiente.
    VERSIÓN SEGURA que no borra datos.
    """
    try:
        updates = {"url_documento": url_documento}
        
        if session_id:
            # Preferir búsqueda por session_id (más precisa)
            success = update_reclamacion_by_session_safe(
                patient_key=patient_key,
                session_id=session_id,
                updates=updates
            )
            logger.info(f"URL actualizada por session_id: {session_id}")
        elif nivel_escalamiento is not None:
            # Fallback a búsqueda por nivel
            success = update_reclamacion_by_level_safe(
                patient_key=patient_key,
                nivel_escalamiento=nivel_escalamiento,
                updates=updates
            )
            logger.info(f"URL actualizada por nivel: {nivel_escalamiento}")
        else:
            logger.error("Se requiere session_id O nivel_escalamiento")
            return False
            
        return success
        
    except Exception as e:
        logger.error(f"Error guardando URL de documento: {e}")
        return False


# ✅ FUNCIÓN CORREGIDA: update_reclamacion_status  
def update_reclamacion_status(patient_key: str, nuevo_estado: str, 
                            nivel_escalamiento: int = None, session_id: str = None) -> bool:
    """
    Actualiza el estado y radicado de una reclamación específica.
    VERSIÓN SEGURA que no borra datos.
    """
    try:
        updates = {"estado_reclamacion": nuevo_estado}
        
        if session_id:
            success = update_reclamacion_by_session_safe(
                patient_key=patient_key,
                session_id=session_id,
                updates=updates
            )
        elif nivel_escalamiento is not None:
            success = update_reclamacion_by_level_safe(
                patient_key=patient_key,
                nivel_escalamiento=nivel_escalamiento,
                updates=updates
            )
        else:
            logger.error("Se requiere session_id O nivel_escalamiento")
            return False
            
        return success
        
    except Exception as e:
        logger.error(f"Error actualizando estado de reclamación: {e}")
        return False


# ✅ MANTENER FUNCIÓN ORIGINAL para casos específicos donde sea necesario
def load_table_from_json_direct(data: List[Dict[str, Any]], table_reference: str) -> None:
    """
    ⚠️ USAR SOLO PARA INSERTS NUEVOS, NO PARA UPDATES.
    Carga datos SIN BÚFER usando load_table_from_json.
    """
    if not data:
        logger.info("No hay datos para cargar en BigQuery.")
        return

    client = get_bigquery_client()

    try:
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

        logger.info(f"✅ Carga directa exitosa: {job.output_rows} filas en '{table_reference}'")

    except GoogleAPIError as exc:
        logger.exception("Error de la API de BigQuery durante la carga directa.")
        raise BigQueryServiceError(f"Error de BigQuery en la carga directa: {exc}") from exc
    except Exception as exc:
        logger.exception("Error inesperado durante la carga directa en BigQuery.")
        raise BigQueryServiceError(f"Error inesperado en la carga directa: {exc}") from exc

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


# ✅ FUNCIÓN PRINCIPAL para insertar/actualizar pacientes
def insert_or_update_patient_data(patient_data: Dict[str, Any],
                                 fields_to_update: Optional[Dict[str, Any]] = None) -> None:
    """
    VERSIÓN MEJORADA que usa UPDATE cuando es posible, INSERT solo para nuevos pacientes.
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

    logger.info(f"🔄 Procesando paciente '{patient_key}' con UPDATE seguro...")

    # Verificar si existe
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
        # Usar UPDATE para campos existentes
        logger.info(f"✅ Paciente '{patient_key}' existe. Usando UPDATE seguro...")
        
        if fields_to_update:
            for field_name, field_value in fields_to_update.items():
                if field_name in ['fecha_nacimiento', 'correo', 'telefono_contacto', 'regimen',
                                 'ciudad', 'direccion', 'canal_contacto', 'eps_estandarizada',
                                 'farmacia', 'sede_farmacia']:
                    success = update_single_field_safe(patient_key, field_name, field_value)
                    if not success:
                        logger.warning(f"⚠️ No se pudo actualizar '{field_name}'")

        # Para nuevas prescripciones, usar operación específica
        if patient_data.get("prescripciones"):
            logger.info("➕ Agregando nueva prescripción...")
            success = update_prescriptions_with_load_table(patient_key, patient_data["prescripciones"])
            if not success:
                raise BigQueryServiceError(f"❌ No se pudo actualizar las prescripciones para '{patient_key}'")

    else:
        # Solo para pacientes completamente nuevos
        logger.info(f"➕ Paciente '{patient_key}' no existe. Insertando...")
        new_patient_record = _prepare_clean_patient_record(patient_data, fields_to_update)
        load_table_from_json_direct([new_patient_record], table_reference)
        logger.info(f"✅ Paciente '{patient_key}' insertado.")

def _get_next_patient_index() -> int:
    """
    Obtiene el próximo índice auto-incremental para un nuevo paciente.
    Consulta el máximo índice actual y retorna el siguiente.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # Obtener el máximo índice actual
        max_index_query = f"""
            SELECT MAX(`index`) as max_index
            FROM `{table_reference}`
        """

        logger.info("🔢 Obteniendo próximo índice de paciente...")
        query_job = client.query(max_index_query)
        results = query_job.result()

        max_index = 0
        for row in results:
            max_index = row.max_index if row.max_index is not None else 0
            break

        next_index = max_index + 1
        logger.info(f"✅ Próximo índice de paciente: {next_index}")
        return next_index

    except Exception as e:
        logger.error(f"❌ Error obteniendo próximo índice: {e}")
        # En caso de error, usar timestamp como fallback para evitar duplicados
        import time
        fallback_index = int(time.time()) % 1000000  # Usar últimos 6 dígitos del timestamp
        logger.warning(f"⚠️ Usando índice fallback: {fallback_index}")
        return fallback_index        


def _prepare_clean_patient_record(patient_data: Dict[str, Any], 
                                 fields_to_update: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Prepara un registro limpio del paciente."""
    next_index = _get_next_patient_index()
    new_patient_record = {
        "index": next_index,
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

    # Validaciones
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

    # Convertir None a valores por defecto
    for key, value in new_patient_record.items():
        if value is None:
            if key in ["correo", "telefono_contacto", "informante", "prescripciones", "reclamaciones"]:
                new_patient_record[key] = []
            else:
                new_patient_record[key] = ""

    return new_patient_record


# ✅ FUNCIÓN MEJORADA: update_patient_medications_no_buffer
def update_patient_medications_no_buffer(patient_key: str, session_id: str, 
                                        undelivered_med_names: List[str]) -> bool:
    """
    Actualiza medicamentos usando UPDATE seguro en lugar de DELETE+INSERT.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # Usar UPDATE para modificar medicamentos específicos
        update_query = f"""
        UPDATE `{table_reference}`
        SET prescripciones = ARRAY(
            SELECT AS STRUCT
                presc.id_session,
                presc.user_id,
                presc.url_prescripcion,
                presc.categoria_riesgo,
                presc.justificacion_riesgo,
                presc.fecha_atencion,
                presc.diagnostico,
                presc.IPS,
                ARRAY(
                    SELECT AS STRUCT
                        med.nombre,
                        med.dosis,
                        med.cantidad,
                        CASE 
                            WHEN presc.id_session = @session_id AND med.nombre IN UNNEST(@undelivered_meds)
                            THEN "no entregado"
                            WHEN presc.id_session = @session_id
                            THEN "entregado"
                            ELSE med.entregado
                        END AS entregado
                    FROM UNNEST(presc.medicamentos) AS med
                ) AS medicamentos
            FROM UNNEST(prescripciones) AS presc
        )
        WHERE paciente_clave = @patient_key
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
                bigquery.ScalarQueryParameter("session_id", "STRING", session_id),
                bigquery.ArrayQueryParameter("undelivered_meds", "STRING", undelivered_med_names)
            ]
        )

        logger.info(f"🔄 Actualizando medicamentos para paciente '{patient_key}' en sesión '{session_id}'")
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()

        if query_job.errors:
            logger.error(f"Errores actualizando medicamentos: {query_job.errors}")
            return False

        rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
        if rows_affected == 0:
            logger.warning(f"No se encontró paciente '{patient_key}' para actualizar medicamentos")
            return False

        logger.info(f"✅ Medicamentos actualizados exitosamente para paciente '{patient_key}'")
        return True

    except Exception as e:
        logger.error(f"❌ Error actualizando medicamentos para paciente '{patient_key}': {e}")
        return False