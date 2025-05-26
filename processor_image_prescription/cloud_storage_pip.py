from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Final
import pytz

from google.cloud import storage
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Prefijo (carpeta) para todas las prescripciones.
_PRESC_PREFIX: Final[str] = ""

class CloudStorageServiceError(RuntimeError):
    """Excepción genérica de la capa de Google Cloud Storage."""


# --------------------------------------------------------------------------- #
#  Cliente
# --------------------------------------------------------------------------- #
def _credentials_from_env() -> Credentials | None:
    """Devuelve credenciales a partir de GOOGLE_APPLICATION_CREDENTIALS, si existen."""
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if path and Path(path).exists():
        return Credentials.from_service_account_file(path)
    return None


def get_storage_client() -> storage.Client:
    """Crea y devuelve un cliente autenticado de Cloud Storage."""
    try:
        creds = _credentials_from_env()
        return storage.Client(credentials=creds)
    except Exception as exc:  # pragma: no cover
        logger.exception("❌ No se pudo crear el cliente de Storage")
        raise CloudStorageServiceError("Fallo al crear el cliente de Storage") from exc


# --------------------------------------------------------------------------- #
#  Utils
# --------------------------------------------------------------------------- #
def generate_blob_name(paciente_clave: str, original_filename: str) -> str:
    """
    Genera un nombre de blob único: prescripciones/<clave><timestamp>.<ext>

    Parameters
    ----------
    paciente_clave : str
        Ej. "COCC8048589".
    original_filename : str
        Nombre original para obtener la extensión.

    Returns
    -------
    str
        Nombre final del objeto dentro del bucket.
    """
    
    colombia_tz = pytz.timezone('America/Bogota') # Obtener la zona horaria de Colombia
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S") # Si se quiere cambiar utc por Colombia, ingresar utcnow(colombia_tz)
    extension = Path(original_filename).suffix
    return f"{paciente_clave}{timestamp}{extension}"


# --------------------------------------------------------------------------- #
#  Upload
# --------------------------------------------------------------------------- #
def upload_image_to_bucket(bucket_name: str,
                           image_path: str | os.PathLike[str],
                           paciente_clave: str) -> str:
    """
    Sube `image_path` al bucket y devuelve su URL formateada gs://.

    Parameters
    ----------
    bucket_name : str
        Puede recibirse con o sin prefijo 'gs://'.
    image_path : str | Path
        Ruta local de la imagen.
    paciente_clave : str
        Usado para generar el nombre del blob.

    Returns
    -------
    str
        URL del objeto (gs://bucket/objeto).
    """
    client = get_storage_client()

    # Sanitizar bucket_name y validar existencia
    bucket_id = bucket_name.replace("gs://", "").split("/", maxsplit=1)[0]
    bucket = client.bucket(bucket_id)

    if not bucket.exists():
        raise CloudStorageServiceError(f"El bucket '{bucket_id}' no existe o no es accesible.")

    image_path = Path(image_path)
    if not image_path.is_file():
        raise CloudStorageServiceError(f"La ruta {image_path} no es un archivo válido.")

    blob_name = generate_blob_name(paciente_clave, image_path.name)
    blob = bucket.blob(blob_name)

    try:
        blob.upload_from_filename(image_path)
        gs_url = f"gs://{bucket_id}/{blob_name}"
        logger.info("✅ Imagen '%s' subida a %s", image_path.name, gs_url)
        return gs_url

    except Exception as exc:  # pragma: no cover
        logger.exception("❌ Error subiendo imagen a Storage")
        raise CloudStorageServiceError("Error subiendo imagen") from exc
