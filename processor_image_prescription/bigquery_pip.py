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
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # Preparar valor para SQL
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

        # UPDATE seguro
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


# ✅ NUEVA FUNCIÓN: Agregar reclamación sin borrar datos existentes
def add_reclamacion_safe(patient_key: str, nueva_reclamacion: Dict[str, Any]) -> bool:
    """
    Agrega una nueva reclamación al array existente sin borrar el paciente.
    VERSIÓN CORREGIDA que escapa comillas y caracteres especiales correctamente.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # ✅ FUNCIÓN HELPER para escapar strings SQL
        def escape_sql_string(value: str) -> str:
            """Escapa comillas y caracteres especiales para SQL."""
            if not isinstance(value, str):
                return str(value)
            # Escapar comillas simples duplicándolas
            escaped = value.replace("'", "''")
            # Escapar caracteres de nueva línea
            escaped = escaped.replace('\n', '\\n')
            escaped = escaped.replace('\r', '\\r')
            # Escapar backslashes
            escaped = escaped.replace('\\', '\\\\')
            return escaped

        # ✅ USAR PARÁMETROS EN LUGAR DE CONCATENACIÓN DIRECTA
        update_query = f"""
        UPDATE `{table_reference}`
        SET reclamaciones = ARRAY_CONCAT(
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
                @fecha_revision AS fecha_revision
            )]
        )
        WHERE paciente_clave = @patient_key
        """

        # ✅ PREPARAR PARÁMETROS SEGUROS
        query_parameters = [
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
            bigquery.ScalarQueryParameter("med_no_entregados", "STRING", 
                nueva_reclamacion.get('med_no_entregados', '')),
            bigquery.ScalarQueryParameter("tipo_accion", "STRING", 
                nueva_reclamacion.get('tipo_accion', '')),
            bigquery.ScalarQueryParameter("texto_reclamacion", "STRING", 
                nueva_reclamacion.get('texto_reclamacion', '')),
            bigquery.ScalarQueryParameter("estado_reclamacion", "STRING", 
                nueva_reclamacion.get('estado_reclamacion', '')),
            bigquery.ScalarQueryParameter("nivel_escalamiento", "INTEGER", 
                nueva_reclamacion.get('nivel_escalamiento', 0)),
            bigquery.ScalarQueryParameter("url_documento", "STRING", 
                nueva_reclamacion.get('url_documento', '')),
            bigquery.ScalarQueryParameter("numero_radicado", "STRING", 
                nueva_reclamacion.get('numero_radicado', '')),
            bigquery.ScalarQueryParameter("fecha_radicacion", "DATE", 
                nueva_reclamacion.get('fecha_radicacion')),
            bigquery.ScalarQueryParameter("fecha_revision", "DATE", 
                nueva_reclamacion.get('fecha_revision'))
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        logger.info(f"➕ Agregando reclamación {nueva_reclamacion.get('tipo_accion')} para paciente {patient_key}")
        logger.debug(f"Texto length: {len(nueva_reclamacion.get('texto_reclamacion', ''))} caracteres")
        
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
        # ✅ LOG ADICIONAL para debugging
        if nueva_reclamacion.get('texto_reclamacion'):
            logger.debug(f"Texto problemático (primeros 200 chars): {nueva_reclamacion['texto_reclamacion'][:200]}...")
        return False


# ✅ NUEVA FUNCIÓN: Actualizar reclamación específica
def update_reclamacion_by_level_safe(patient_key: str, nivel_escalamiento: int, 
                                   updates: Dict[str, Any]) -> bool:
    """
    Actualiza una reclamación específica por nivel de escalamiento de forma segura.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno de BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # ✅ NUEVA SINTAXIS CORRECTA usando ARRAY() con SELECT AS STRUCT
        update_query = f"""
        UPDATE `{table_reference}`
        SET reclamaciones = ARRAY(
            SELECT AS STRUCT
                rec.med_no_entregados,
                rec.tipo_accion,
                rec.texto_reclamacion,
                CASE 
                    WHEN rec.nivel_escalamiento = @nivel_escalamiento THEN
                        CASE 
                            WHEN @new_estado IS NOT NULL THEN @new_estado
                            ELSE rec.estado_reclamacion
                        END
                    ELSE rec.estado_reclamacion
                END AS estado_reclamacion,
                rec.nivel_escalamiento,
                CASE 
                    WHEN rec.nivel_escalamiento = @nivel_escalamiento THEN
                        CASE 
                            WHEN @new_url IS NOT NULL THEN @new_url
                            ELSE rec.url_documento
                        END
                    ELSE rec.url_documento
                END AS url_documento,
                CASE 
                    WHEN rec.nivel_escalamiento = @nivel_escalamiento THEN
                        CASE 
                            WHEN @new_radicado IS NOT NULL THEN @new_radicado
                            ELSE rec.numero_radicado
                        END
                    ELSE rec.numero_radicado
                END AS numero_radicado,
                CASE 
                    WHEN rec.nivel_escalamiento = @nivel_escalamiento THEN
                        CASE 
                            WHEN @new_fecha_radicacion IS NOT NULL THEN DATE(@new_fecha_radicacion)
                            ELSE rec.fecha_radicacion
                        END
                    ELSE rec.fecha_radicacion
                END AS fecha_radicacion,
                rec.fecha_revision
            FROM UNNEST(reclamaciones) AS rec
        )
        WHERE paciente_clave = @patient_key
        """

        # Preparar parámetros con valores por defecto
        query_parameters = [
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
            bigquery.ScalarQueryParameter("nivel_escalamiento", "INTEGER", nivel_escalamiento),
            bigquery.ScalarQueryParameter("new_estado", "STRING", updates.get('estado_reclamacion')),
            bigquery.ScalarQueryParameter("new_url", "STRING", updates.get('url_documento')),
            bigquery.ScalarQueryParameter("new_radicado", "STRING", updates.get('numero_radicado')),
            bigquery.ScalarQueryParameter("new_fecha_radicacion", "STRING", updates.get('fecha_radicacion'))
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        logger.info(f"🔄 Actualizando reclamación nivel {nivel_escalamiento} para paciente {patient_key}")
        logger.debug(f"Updates: {updates}")
        
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


# ✅ FUNCIÓN CORREGIDA: save_document_url_to_reclamacion
def save_document_url_to_reclamacion(patient_key: str, nivel_escalamiento: int, 
                                    url_documento: str, tipo_documento: str) -> bool:
    """
    Actualiza la URL del documento generado en la reclamación correspondiente.
    VERSIÓN SEGURA que no borra datos.
    """
    try:
        updates = {
            "url_documento": url_documento
        }
        
        success = update_reclamacion_by_level_safe(
            patient_key=patient_key,
            nivel_escalamiento=nivel_escalamiento, 
            updates=updates
        )
        
        if success:
            logger.info(f"✅ URL de documento guardada para paciente {patient_key}, nivel {nivel_escalamiento}")
        else:
            logger.error(f"❌ Error guardando URL de documento para paciente {patient_key}")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ Error guardando URL de documento: {e}")
        return False


# ✅ FUNCIÓN CORREGIDA: update_reclamacion_status  
def update_reclamacion_status(patient_key: str, nivel_escalamiento: int, 
                            nuevo_estado: str, numero_radicado: str = None, 
                            fecha_radicacion: str = None) -> bool:
    """
    Actualiza el estado y radicado de una reclamación específica.
    VERSIÓN SEGURA que no borra datos.
    """
    try:
        updates = {
            "estado_reclamacion": nuevo_estado
        }
        
        if numero_radicado:
            updates["numero_radicado"] = numero_radicado
            
        if fecha_radicacion:
            updates["fecha_radicacion"] = fecha_radicacion
        elif numero_radicado:
            # Si se proporciona radicado pero no fecha, usar fecha actual
            updates["fecha_radicacion"] = datetime.now().strftime("%Y-%m-%d")
        
        success = update_reclamacion_by_level_safe(
            patient_key=patient_key,
            nivel_escalamiento=nivel_escalamiento,
            updates=updates
        )
        
        if success:
            logger.info(f"✅ Estado de reclamación actualizado para paciente {patient_key}, nivel {nivel_escalamiento}")
        else:
            logger.error(f"❌ Error actualizando estado de reclamación para paciente {patient_key}")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ Error actualizando estado de reclamación: {e}")
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
            # Implementar lógica específica para prescripciones si es necesario

    else:
        # Solo para pacientes completamente nuevos
        logger.info(f"➕ Paciente '{patient_key}' no existe. Insertando...")
        new_patient_record = _prepare_clean_patient_record(patient_data, fields_to_update)
        load_table_from_json_direct([new_patient_record], table_reference)
        logger.info(f"✅ Paciente '{patient_key}' insertado.")


def _prepare_clean_patient_record(patient_data: Dict[str, Any], 
                                 fields_to_update: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Prepara un registro limpio del paciente."""
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