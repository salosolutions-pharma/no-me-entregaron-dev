from __future__ import annotations

import json
import logging
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROJECT_ID: str | None = os.getenv("PROJECT_ID")
DATASET_ID: str | None = os.getenv("DATASET_ID")
TABLE_ID:   str | None = os.getenv("TABLE_ID")


class BigQueryServiceError(RuntimeError):
    """Excepción genérica para fallos no recuperables del servicio BigQuery."""


# --------------------------------------------------------------------------- #
#  Cliente
# --------------------------------------------------------------------------- #
def _credentials_from_env() -> Credentials | None:
    """Devuelve credenciales de servicio si la variable GOOGLE_APPLICATION_CREDENTIALS existe."""
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if path and Path(path).exists():
        return Credentials.from_service_account_file(path)
    return None


def get_bigquery_client() -> bigquery.Client:
    """Crea y devuelve un cliente autenticado de BigQuery."""
    try:
        creds = _credentials_from_env()
        return bigquery.Client(credentials=creds, project=PROJECT_ID)
    except Exception as exc:  # pragma: no cover
        logger.exception("❌ No se pudo crear el cliente de BigQuery")
        raise BigQueryServiceError("Fallo en la creación del cliente") from exc


# --------------------------------------------------------------------------- #
#  Utilidades
# --------------------------------------------------------------------------- #
def convert_bq_row_to_dict(row: bigquery.Row) -> Dict[str, Any]:
    """Convierte recursivamente un Row en dict serializable."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, list):
            out[k] = [
                convert_bq_row_to_dict(i) if hasattr(i, "items") else i  # nested STRUCT/REPEATED
                for i in v
            ]
        elif hasattr(v, "items"):
            out[k] = convert_bq_row_to_dict(v)
        else:
            out[k] = v
    return out


def _build_medicamento_struct(med: Dict[str, Any]) -> str:
    """Devuelve el SQL STRUCT literal para un medicamento."""
    return (
        "STRUCT("
        f'"{med.get("nombre", "")}", '
        f'"{med.get("dosis", "")}", '
        f'"{med.get("cantidad", "")}", '
        "CAST(NULL AS STRING)"
        ")"
    )


# --------------------------------------------------------------------------- #
#  Carga masiva
# --------------------------------------------------------------------------- #
def batch_load_to_bigquery(client: bigquery.Client,
                           table_ref: str,
                           data: List[Dict[str, Any]]) -> None:
    """Carga datos en formato JSONL usando WRITE_APPEND."""
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w",
                                         encoding="utf-8",
                                         newline="\n",
                                         suffix=".jsonl",
                                         delete=False) as tmp:
            for record in data:
                json.dump(record, tmp, ensure_ascii=False)
                tmp.write("\n")
            tmp_path = Path(tmp.name)

        job_cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )

        with tmp_path.open("rb") as fh:
            job = client.load_table_from_file(fh, table_ref, job_config=job_cfg)
        job.result()  # bloqueante

        if job.errors:
            raise BigQueryServiceError(f"Errores en batch load: {job.errors}")

        logger.info("✅ Batch load: %s filas cargadas", job.output_rows)

    finally:
        if tmp_path:
            with suppress(FileNotFoundError):
                tmp_path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
#  Inserción / actualización de pacientes
# --------------------------------------------------------------------------- #
def insert_or_update_patient_data(paciente: Dict[str, Any]) -> None:
    """
    Inserta un paciente nuevo o añade una prescripción a uno existente.

    Lógica:
    1. Consultar si el paciente (paciente_clave) ya existe.
    2. Si existe → UPDATE con ARRAY_CONCAT en prescripciones.
    3. Si no existe → batch_load_to_bigquery() con el documento completo.
    """
    if not all((PROJECT_ID, DATASET_ID, TABLE_ID)):
        raise BigQueryServiceError("Variables de entorno incompletas")

    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    clave = paciente["paciente_clave"]

    # 1) ¿Existe?
    exists = client.query(
        f"""
        SELECT 1
        FROM `{table_ref}`
        WHERE paciente_clave = @clave
        LIMIT 1
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("clave", "STRING", clave)]
        ),
    ).result()

    if list(exists):  # ---------------------------------------------------------------- update
        logger.info("Paciente %s existe → UPDATE", clave)

        presc = paciente["prescripciones"][0]
        meds_array = ", ".join(_build_medicamento_struct(m) for m in presc.get("medicamentos", []))
        new_presc_struct = f'''STRUCT(
            "{presc.get("id_session", "")}",
            "{presc.get("url_prescripcion", "")}",
            CAST(NULL AS STRING),
            CAST(NULL AS STRING),
            "{presc.get("diagnostico", "")}",
            "{presc.get("IPS", "")}",
            [{meds_array}]
        )'''

        update_sql = f"""
            UPDATE `{table_ref}`
            SET
                nombre_paciente    = @nombre_paciente,
                telefono_contacto  = @telefono_contacto,
                regimen            = @regimen,
                ciudad             = @ciudad,
                direccion          = @direccion,
                eps_cruda          = @eps_cruda,
                prescripciones     = ARRAY_CONCAT(IFNULL(prescripciones, []), [{new_presc_struct}])
            WHERE paciente_clave = @clave
        """

        params = [
            bigquery.ScalarQueryParameter("clave", "STRING", clave),
            bigquery.ScalarQueryParameter("nombre_paciente", "STRING", paciente.get("nombre_paciente")),
            bigquery.ArrayQueryParameter("telefono_contacto", "STRING", paciente.get("telefono_contacto", [])),
            bigquery.ScalarQueryParameter("regimen", "STRING", paciente.get("regimen")),
            bigquery.ScalarQueryParameter("ciudad", "STRING", paciente.get("ciudad")),
            bigquery.ScalarQueryParameter("direccion", "STRING", paciente.get("direccion")),
            bigquery.ScalarQueryParameter("eps_cruda", "STRING", paciente.get("eps_cruda")),
        ]

        job = client.query(update_sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        job.result()
        if job.errors:
            raise BigQueryServiceError(f"Errores en UPDATE: {job.errors}")

        logger.info("✅ Paciente %s actualizado", clave)

    else:  # -------------------------------------------------------------- insert nuevo
        logger.info("Paciente %s nuevo → INSERT", clave)

        presc = paciente["prescripciones"][0]
        nuevo = {
            "paciente_clave": clave,
            "pais": paciente.get("pais", "CO"),
            "tipo_documento": paciente.get("tipo_documento"),
            "numero_documento": paciente.get("numero_documento"),
            "nombre_paciente": paciente.get("nombre_paciente"),
            "fecha_nacimiento": None,
            "correo": [],
            "telefono_contacto": paciente.get("telefono_contacto", []),
            "canal_contacto": None,
            "regimen": paciente.get("regimen"),
            "ciudad": paciente.get("ciudad"),
            "direccion": paciente.get("direccion"),
            "operador_logistico": None,
            "sede_farmacia": None,
            "eps_cruda": paciente.get("eps_cruda"),
            "eps_estandarizada": None,
            "informante": [],
            "sesiones": [],
            "prescripciones": [presc],
            "reclamaciones": [],
        }

        batch_load_to_bigquery(client, table_ref, [nuevo])
        logger.info("✅ Paciente %s insertado", clave)
