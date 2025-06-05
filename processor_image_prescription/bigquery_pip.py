import json
import logging
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import GoogleAPIError # Importar excepción específica

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configuración de variables de entorno para BigQuery
# Usamos Optional[str] para indicar que pueden ser None si no están configuradas
PROJECT_ID: Optional[str] = os.getenv("PROJECT_ID")
DATASET_ID: Optional[str] = os.getenv("DATASET_ID")
TABLE_ID: Optional[str] = os.getenv("TABLE_ID")

class BigQueryServiceError(RuntimeError):
    """Excepción genérica para fallos no recuperables del servicio BigQuery."""
    pass

# --- Cliente BigQuery ---
def _get_credentials_from_env() -> Optional[Credentials]:
    """
    Carga y devuelve las credenciales de servicio de Google Cloud a partir
    de la variable de entorno GOOGLE_APPLICATION_CREDENTIALS.
    """
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if path and Path(path).exists():
        logger.info("Cargando credenciales desde GOOGLE_APPLICATION_CREDENTIALS.")
        return Credentials.from_service_account_file(path)
    logger.warning("GOOGLE_APPLICATION_CREDENTIALS no está configurada o el archivo no existe.")
    return None

def get_bigquery_client() -> bigquery.Client:
    """
    Crea y devuelve un cliente autenticado de BigQuery.

    Raises:
        BigQueryServiceError: Si no se puede crear el cliente de BigQuery.

    Returns:
        bigquery.Client: Instancia del cliente de BigQuery.
    """
    try:
        # PROJECT_ID puede ser None, bigquery.Client puede inferirlo de las credenciales
        credentials = _get_credentials_from_env()
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        logger.info("Cliente de BigQuery creado exitosamente.")
        return client
    except Exception as exc:
        logger.exception("❌ Error fatal: No se pudo crear el cliente de BigQuery.")
        raise BigQueryServiceError("Fallo en la creación del cliente de BigQuery.") from exc

# --- Utilidades ---
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
                _convert_bq_row_to_dict_recursive(item) if hasattr(item, "items") else item
                for item in value
            ]
        elif hasattr(value, "items"):
            # Si es un objeto anidado (otro Row), procesa recursivamente
            out[key] = _convert_bq_row_to_dict_recursive(value)
        else:
            out[key] = value
    return out

def _build_medicamento_struct_sql(med: Dict[str, Any]) -> str:
    """
    Construye el literal SQL STRUCT para un medicamento, incluyendo los campos
    'nombre', 'dosis', 'cantidad' y 'entregado'.
    Los valores de string se escapan para ser seguros en una consulta SQL.
    """
    # Escapar comillas dobles para SQL literals
    nombre_escaped = med.get("nombre", "").replace('"', '\\"')
    dosis_escaped = med.get("dosis", "").replace('"', '\\"')
    cantidad_escaped = med.get("cantidad", "").replace('"', '\\"')
    entregado_status = med.get("entregado", "pendiente").replace('"', '\\"')

    return (
        "STRUCT("
        f'"{nombre_escaped}" AS nombre, '
        f'"{dosis_escaped}" AS dosis, '
        f'"{cantidad_escaped}" AS cantidad, '
        f'"{entregado_status}" AS entregado'
        ")"
    )

def _build_prescription_struct_sql(presc: Dict[str, Any]) -> str:
    """
    Construye el literal SQL STRUCT para una prescripción, incluyendo el
    array de medicamentos con su estado de entrega.
    Los valores de string se escapan para ser seguros en una consulta SQL.
    """
    # Construye el array de medicamentos SQL
    meds_array_sql = ", ".join(_build_medicamento_struct_sql(m) for m in presc.get("medicamentos", []))
    
    # Escapar los valores de string para la sentencia SQL
    id_session_escaped = presc.get("id_session", "").replace('"', '\\"')
    url_prescripcion_escaped = presc.get("url_prescripcion", "").replace('"', '\\"')
    diagnostico_escaped = presc.get("diagnostico", "").replace('"', '\\"')
    ips_escaped = presc.get("IPS", "").replace('"', '\\"')
    
    # Manejo de fecha_atencion que puede ser None; BigQuery requiere CAST(NULL AS STRING)
    fecha_atencion_sql = f'"{presc.get("fecha_atencion")}"' if presc.get("fecha_atencion") else "CAST(NULL AS STRING)"
    
    # categoria_riesgo es un STRING y puede ser None
    categoria_riesgo_sql = f'"{presc.get("categoria_riesgo")}"' if presc.get("categoria_riesgo") else "CAST(NULL AS STRING)"

    return f'''STRUCT(
        "{id_session_escaped}" AS id_session,
        "{url_prescripcion_escaped}" AS url_prescripcion,
        {categoria_riesgo_sql} AS categoria_riesgo,
        {fecha_atencion_sql} AS fecha_atencion,
        "{diagnostico_escaped}" AS diagnostico,
        "{ips_escaped}" AS IPS,
        [{meds_array_sql}] AS medicamentos
    )'''

# --- Carga masiva ---
def batch_load_to_bigquery(client: bigquery.Client,
                           table_reference: str,
                           data: List[Dict[str, Any]]) -> None:
    """
    Carga una lista de diccionarios como filas JSONL en una tabla de BigQuery.
    Utiliza un archivo temporal y el modo WRITE_APPEND para añadir datos.

    Args:
        client: Cliente autenticado de BigQuery.
        table_reference: Referencia completa de la tabla (ej. "proyecto.dataset.tabla").
        data: Lista de diccionarios, donde cada diccionario representa una fila a cargar.

    Raises:
        BigQueryServiceError: Si la carga masiva falla o hay errores en el job.
    """
    if not data:
        logger.info("No hay datos para cargar en BigQuery (batch_load_to_bigquery).")
        return

    temp_file_path: Optional[Path] = None
    try:
        # Crea un archivo temporal para escribir los datos en formato JSONL
        with tempfile.NamedTemporaryFile(mode="w",
                                         encoding="utf-8",
                                         newline="\n",
                                         suffix=".jsonl",
                                         delete=False) as temp_file:
            for record in data:
                json.dump(record, temp_file, ensure_ascii=False)
                temp_file.write("\n")
            temp_file_path = Path(temp_file.name)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False, # Si el esquema es conocido, False es más seguro. True es para detección automática.
        )

        with temp_file_path.open("rb") as file_handle:
            job = client.load_table_from_file(file_handle, table_reference, job_config=job_config)
        job.result() # Espera a que el job de carga finalice

        if job.errors:
            logger.error(f"Errores encontrados durante la carga masiva en BigQuery: {job.errors}")
            raise BigQueryServiceError(f"Errores en la carga masiva: {job.errors}")

        logger.info("✅ Carga masiva exitosa: %s filas cargadas en '%s'.", job.output_rows, table_reference)

    except GoogleAPIError as exc:
        logger.exception(f"❌ Error de la API de BigQuery durante la carga masiva.")
        raise BigQueryServiceError(f"Error de BigQuery en carga masiva: {exc}") from exc
    except Exception as exc:
        logger.exception(f"❌ Error inesperado durante la carga masiva en BigQuery.")
        raise BigQueryServiceError(f"Error inesperado en carga masiva: {exc}") from exc
    finally:
        # Asegura que el archivo temporal se elimine
        if temp_file_path:
            with suppress(FileNotFoundError):
                temp_file_path.unlink(missing_ok=True) # `missing_ok=True` evita error si ya se borró

# --- Inserción / actualización de pacientes ---
def insert_or_update_patient_data(patient_data: Dict[str, Any]) -> None:
    """
    Inserta un nuevo registro de paciente o actualiza uno existente
    añadiendo una nueva prescripción. Maneja el campo 'entregado' en medicamentos.

    Args:
        patient_data: Diccionario con la información del paciente y la prescripción.

    Raises:
        BigQueryServiceError: Si las variables de entorno para BigQuery son incompletas,
                              o si la operación de inserción/actualización falla.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        logger.critical("Variables de entorno para BigQuery incompletas. "
                        "Asegúrese de configurar PROJECT_ID, DATASET_ID y TABLE_ID.")
        raise BigQueryServiceError("Variables de entorno BigQuery incompletas.")

    client = get_bigquery_client()
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    patient_key = patient_data["paciente_clave"]

    # 1. Verificar si el paciente existe
    exists_query_sql = f"""
        SELECT 1
        FROM `{table_reference}`
        WHERE paciente_clave = @patient_key
        LIMIT 1
    """
    try:
        exists_job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)]
        )
        exists_query_results = client.query(exists_query_sql, job_config=exists_job_config).result()
        patient_exists = bool(list(exists_query_results))
    except GoogleAPIError as exc:
        logger.exception(f"❌ Error de BigQuery al verificar existencia del paciente '{patient_key}'.")
        raise BigQueryServiceError(f"Fallo al verificar paciente en BigQuery: {exc}") from exc
    except Exception as exc:
        logger.exception(f"❌ Error inesperado al verificar existencia del paciente '{patient_key}'.")
        raise BigQueryServiceError(f"Error inesperado al verificar paciente: {exc}") from exc


    if patient_exists:  # -------------------------------------------------- UPDATE
        logger.info(f"Paciente '{patient_key}' existe. Procediendo con UPDATE.")

        # La prescripción siempre se espera como el primer elemento en 'prescripciones'
        current_prescription = patient_data["prescripciones"][0]
        
        # Construye el STRUCT SQL para la nueva prescripción
        new_prescription_struct_sql = _build_prescription_struct_sql(current_prescription)

        # SQL para actualizar el registro del paciente existente
        update_sql = f"""
            UPDATE `{table_reference}`
            SET
                nombre_paciente   = @nombre_paciente,
                telefono_contacto = @telefono_contacto,
                regimen           = @regimen,
                ciudad            = @ciudad,
                direccion         = @direccion,
                eps_cruda         = @eps_cruda,
                prescripciones    = ARRAY_CONCAT(IFNULL(prescripciones, []), [{new_prescription_struct_sql}])
            WHERE paciente_clave = @patient_key
        """

        # Parámetros para la consulta de actualización
        update_params = [
            bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key),
            bigquery.ScalarQueryParameter("nombre_paciente", "STRING", patient_data.get("nombre_paciente")),
            bigquery.ArrayQueryParameter("telefono_contacto", "STRING", patient_data.get("telefono_contacto", [])),
            bigquery.ScalarQueryParameter("regimen", "STRING", patient_data.get("regimen")),
            bigquery.ScalarQueryParameter("ciudad", "STRING", patient_data.get("ciudad")),
            bigquery.ScalarQueryParameter("direccion", "STRING", patient_data.get("direccion")),
            bigquery.ScalarQueryParameter("eps_cruda", "STRING", patient_data.get("eps_cruda")),
        ]

        try:
            job = client.query(update_sql, job_config=bigquery.QueryJobConfig(query_parameters=update_params))
            job.result() # Espera a que el job de actualización finalice
            if job.errors:
                logger.error(f"Errores durante el UPDATE del paciente '{patient_key}': {job.errors}")
                raise BigQueryServiceError(f"Errores en UPDATE: {job.errors}")

            logger.info(f"✅ Paciente '{patient_key}' actualizado con nueva prescripción.")
        except GoogleAPIError as exc:
            logger.exception(f"❌ Error de BigQuery durante el UPDATE del paciente '{patient_key}'.")
            raise BigQueryServiceError(f"Fallo en UPDATE de paciente en BigQuery: {exc}") from exc
        except Exception as exc:
            logger.exception(f"❌ Error inesperado durante el UPDATE del paciente '{patient_key}'.")
            raise BigQueryServiceError(f"Error inesperado en UPDATE de paciente: {exc}") from exc

    else:  # --------------------------------------------------------------- INSERT nuevo
        logger.info(f"Paciente '{patient_key}' no existe. Procediendo con INSERT.")

        # Asegurar que los datos del nuevo paciente estén completos según el esquema de BigQuery.
        # Asumimos que `patient_data` ya viene estructurado de `_build_patient_record`
        # en `pip_processor.py`.
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

        # La carga masiva es adecuada para insertar una única fila también.
        batch_load_to_bigquery(client, table_reference, [new_patient_record])
        logger.info(f"✅ Paciente '{patient_key}' insertado exitosamente.")