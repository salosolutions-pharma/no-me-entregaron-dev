from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Final
import pytz

from google.cloud import storage
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Prefijo (carpeta) para todas las prescripciones en Cloud Storage
# Puede ser útil si se desea organizar los archivos en subcarpetas.
_PRESCRIPTION_STORAGE_PREFIX: Final[str] = "prescripciones"

class CloudStorageServiceError(RuntimeError):
    """Excepción genérica para errores en la capa de Google Cloud Storage."""
    pass

# --------------------------------------------------------------------------- #
# Cliente de Cloud Storage
# --------------------------------------------------------------------------- #
def _get_credentials_from_env() -> Credentials | None:
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

def get_cloud_storage_client() -> storage.Client:
    """
    Crea y devuelve un cliente autenticado de Google Cloud Storage.

    Raises:
        CloudStorageServiceError: Si no se puede crear el cliente de Storage.

    Returns:
        storage.Client: Instancia del cliente de Cloud Storage.
    """
    try:
        credentials = _get_credentials_from_env()
        client = storage.Client(credentials=credentials)
        logger.info("Cliente de Cloud Storage creado exitosamente.")
        return client
    except Exception as exc:
        logger.exception("❌ Error fatal: No se pudo crear el cliente de Cloud Storage.")
        raise CloudStorageServiceError("Fallo al crear el cliente de Cloud Storage.") from exc

# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def _generate_blob_name(patient_key: str, original_filename: str) -> str:
    """
    Genera un nombre de blob único para la imagen dentro del bucket de Cloud Storage.
    Formato: <PREFIJO>/<clave_paciente>_<timestamp>.<extension>

    Args:
        patient_key: Clave única del paciente (ej. "COCC8048589").
        original_filename: Nombre original del archivo para obtener su extensión.

    Returns:
        str: Nombre final del objeto (blob) dentro del bucket, incluyendo el prefijo de la carpeta.
    """
    # Usar la zona horaria de Colombia para el timestamp
    colombia_tz = pytz.timezone('America/Bogota') 
    timestamp = datetime.now(colombia_tz).strftime("%Y%m%d_%H%M%S")
    extension = Path(original_filename).suffix

    # Asegurar que el prefijo no termine con barra si ya la incluimos aquí
    base_name = f"{patient_key}_{timestamp}{extension}"
    
    if _PRESCRIPTION_STORAGE_PREFIX:
        return f"{_PRESCRIPTION_STORAGE_PREFIX}/{base_name}"
    return base_name

# --------------------------------------------------------------------------- #
# Funciones de Subida
# --------------------------------------------------------------------------- #
def upload_image_to_bucket(bucket_name: str,
                           image_path: str | Path,
                           patient_key: str,
                           prefix: str = _PRESCRIPTION_STORAGE_PREFIX) -> str:
    """
    Sube una imagen desde una ruta local a un bucket de Google Cloud Storage
    y devuelve su URL en formato 'gs://'.

    Args:
        bucket_name: Nombre del bucket de destino. Puede recibirse con o sin prefijo 'gs://'.
        image_path: Ruta local del archivo de imagen a subir.
        patient_key: Clave única del paciente, usada para generar el nombre del blob.
        prefix: Prefijo de carpeta opcional dentro del bucket. Por defecto, usa `_PRESCRIPTION_STORAGE_PREFIX`.

    Returns:
        str: URL del objeto subido en formato 'gs://bucket_nombre/nombre_del_blob'.

    Raises:
        CloudStorageServiceError: Si el bucket no existe, la ruta de la imagen es inválida,
                                  o si ocurre un error durante la subida a Cloud Storage.
    """
    client = get_cloud_storage_client()

    # Sanitizar el nombre del bucket para obtener solo el ID
    bucket_id = bucket_name.replace("gs://", "").split("/", maxsplit=1)[0]
    bucket = client.bucket(bucket_id)

    try:
        # Verificar si el bucket existe y es accesible
        if not bucket.exists():
            logger.error(f"El bucket '{bucket_id}' no existe o no es accesible. Verifique los permisos o el nombre.")
            raise CloudStorageServiceError(f"El bucket '{bucket_id}' no existe o no es accesible.")

        # Validar la ruta del archivo local
        local_image_path = Path(image_path)
        if not local_image_path.is_file():
            logger.error(f"La ruta local '{local_image_path}' no es un archivo válido o no existe.")
            raise CloudStorageServiceError(f"La ruta {local_image_path} no es un archivo válido.")

        # Generar el nombre completo del blob, incluyendo el prefijo
        blob_name = _generate_blob_name(patient_key, local_image_path.name)
        
        # Si se pasa un prefijo diferente al predeterminado, sobrescribir para esta subida
        if prefix and prefix != _PRESCRIPTION_STORAGE_PREFIX:
            # Asegurar que el prefijo no termine con barra si ya la incluimos aquí
            blob_name = f"{prefix}/{patient_key}_{datetime.now(pytz.timezone('America/Bogota')).strftime('%Y%m%d_%H%M%S')}{local_image_path.suffix}"


        blob = bucket.blob(blob_name)

        blob.upload_from_filename(local_image_path)
        gs_url = f"gs://{bucket_id}/{blob_name}"
        logger.info("✅ Imagen '%s' subida a %s.", local_image_path.name, gs_url)
        return gs_url

    except GoogleAPIError as exc:
        logger.exception(f"❌ Error de la API de Google Cloud Storage al subir la imagen a '{bucket_id}/{blob_name}'.")
        raise CloudStorageServiceError(f"Error de Cloud Storage al subir imagen: {exc}") from exc
    except Exception as exc:
        logger.exception(f"❌ Error inesperado al subir la imagen '{image_path}' a Cloud Storage.")
        raise CloudStorageServiceError(f"Error inesperado al subir imagen: {exc}") from exc