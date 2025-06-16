import logging
import os
from datetime import datetime
from pathlib import Path

import pytz
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

_PRESCRIPTION_STORAGE_PREFIX = "prescripciones"


class CloudStorageServiceError(RuntimeError):
    """Excepción genérica para errores en la capa de Google Cloud Storage."""


def _get_credentials_from_env() -> Credentials | None:
    """Carga y devuelve las credenciales de servicio de Google Cloud desde variables de entorno."""
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if path and Path(path).exists():
        logger.info("Cargando credenciales desde GOOGLE_APPLICATION_CREDENTIALS.")
        return Credentials.from_service_account_file(path)
    logger.warning("GOOGLE_APPLICATION_CREDENTIALS no está configurada o el archivo no existe.")
    return None


def get_cloud_storage_client() -> storage.Client:
    """Crea y devuelve un cliente autenticado de Google Cloud Storage."""
    try:
        credentials = _get_credentials_from_env()
        client = storage.Client(credentials=credentials)
        logger.info("Cliente de Cloud Storage creado exitosamente.")
        return client
    except Exception as exc:
        logger.exception("Error fatal: No se pudo crear el cliente de Cloud Storage.")
        raise CloudStorageServiceError("Fallo al crear el cliente de Cloud Storage.") from exc


def _generate_blob_name(patient_key: str, original_filename: str) -> str:
    """Genera un nombre de blob único para la imagen dentro del bucket de Cloud Storage."""
    colombia_tz = pytz.timezone('America/Bogota')
    timestamp = datetime.now(colombia_tz).strftime("%Y%m%d_%H%M%S")
    extension = Path(original_filename).suffix

    base_name = f"{patient_key}_{timestamp}{extension}"

    if _PRESCRIPTION_STORAGE_PREFIX:
        return f"{_PRESCRIPTION_STORAGE_PREFIX}/{base_name}"
    return base_name


def upload_image_to_bucket(bucket_name: str, image_path: str | Path, patient_key: str,
                          prefix: str = _PRESCRIPTION_STORAGE_PREFIX) -> str:
    """Sube una imagen desde una ruta local a un bucket de Google Cloud Storage."""
    client = get_cloud_storage_client()

    bucket_id = bucket_name.replace("gs://", "").split("/", maxsplit=1)[0]
    bucket = client.bucket(bucket_id)
    blob_name = ""  # Inicializar para evitar UnboundVariable

    try:
        if not bucket.exists():
            logger.error(f"El bucket '{bucket_id}' no existe o no es accesible.")
            raise CloudStorageServiceError(f"El bucket '{bucket_id}' no existe o no es accesible.")

        local_image_path = Path(image_path)
        if not local_image_path.is_file():
            logger.error(f"La ruta local '{local_image_path}' no es un archivo válido o no existe.")
            raise CloudStorageServiceError(f"La ruta {local_image_path} no es un archivo válido.")

        blob_name = _generate_blob_name(patient_key, local_image_path.name)

        if prefix and prefix != _PRESCRIPTION_STORAGE_PREFIX:
            colombia_tz = pytz.timezone('America/Bogota')
            timestamp = datetime.now(colombia_tz).strftime('%Y%m%d_%H%M%S')
            blob_name = f"{prefix}/{patient_key}_{timestamp}{local_image_path.suffix}"

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_image_path)
        gs_url = f"gs://{bucket_id}/{blob_name}"
        logger.info("Imagen '%s' subida a %s.", local_image_path.name, gs_url)
        return gs_url

    except GoogleAPIError as exc:
        logger.exception(f"Error de la API de Google Cloud Storage al subir la imagen a '{bucket_id}/{blob_name}'.")
        raise CloudStorageServiceError(f"Error de Cloud Storage al subir imagen: {exc}") from exc
    except Exception as exc:
        logger.exception(f"Error inesperado al subir la imagen '{image_path}' a Cloud Storage.")
        raise CloudStorageServiceError(f"Error inesperado al subir imagen: {exc}") from exc