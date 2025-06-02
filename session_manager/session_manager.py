import os
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
import pytz

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from google.cloud.exceptions import GoogleCloudError, NotFound
from typing_extensions import Dict, Any
# --- Excepciones personalizadas ---
class SessionManagerError(Exception):
    """Excepción base para errores en el SessionManager."""
    pass

# --- Configuración básica de logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Cargar variables de entorno ---
load_dotenv()

# Variables de entorno para BigQuery
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID_HISTORY = os.getenv("TABLE_ID_H")  # historial_conversacion


class SessionManager:
    """
    Gestiona las sesiones de conversación, registrando la fila inicial de la sesión
    y eventos adicionales (incluido el consentimiento) como nuevas filas.
    Utiliza la API de Streaming para todas las inserciones.
    """

    def __init__(self):
        """Inicializa el gestor de sesiones, estableciendo la conexión con BigQuery."""
        if not all([PROJECT_ID, DATASET_ID, TABLE_ID_HISTORY]):
            raise ValueError(
                "Las variables de entorno de BigQuery (PROJECT_ID, DATASET_ID, TABLE_ID_H) "
                "no están configuradas correctamente."
            )
        self.client = self._get_bigquery_client()
        self.table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_HISTORY}"
        self.colombia_tz = pytz.timezone('America/Bogota')
        logger.info(f"SessionManager inicializado. Conectado a BigQuery tabla: {self.table_ref}")

        try:
            table_schema = self.client.get_table(self.table_ref).schema
            field_names = [field.name for field in table_schema]
            required_fields = ["id_sesion", "conversacion", "consentimiento", "timestamp_consentimiento"]
            if not all(field in field_names for field in required_fields):
                logger.warning(
                    f"La tabla BigQuery '{self.table_ref}' no tiene todos los campos esperados: {required_fields}. "
                    "Asegúrate de que los tipos coincidan: 'conversacion' (STRING), 'consentimiento' (BOOL), "
                    "y 'timestamp_consentimiento' (TIMESTAMP)."
                )
        except NotFound:
            logger.error(f"La tabla BigQuery '{self.table_ref}' no existe. Por favor, créala con el esquema adecuado.")
            raise SessionManagerError(f"La tabla BigQuery '{self.table_ref}' no existe.")
        except Exception as e:
            logger.error(f"Error al verificar el esquema de la tabla BigQuery '{self.table_ref}': {e}")


    def _get_bigquery_client(self) -> bigquery.Client:
        """Crea y retorna una instancia del cliente de BigQuery."""
        creds = self._get_credentials()
        return bigquery.Client(credentials=creds, project=PROJECT_ID)

    def _get_credentials(self) -> Credentials:
        """
        Obtiene las credenciales de servicio de Google Cloud.
        Prioriza GOOGLE_APPLICATION_CREDENTIALS, luego rutas comunes, finalmente credenciales por defecto.
        """
        path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if path and Path(path).exists():
            logger.info("Usando credenciales desde GOOGLE_APPLICATION_CREDENTIALS: %s", path)
            return Credentials.from_service_account_file(path)

        common_paths = [
            "./credentials/no-me-entregaron-5d051d4e8784.json",
            "./credentials.json",
            "./key.json",
            "./service-account.json"
        ]

        for p in common_paths:
            if Path(p).exists():
                logger.info("Usando credenciales desde ruta común: %s", p)
                return Credentials.from_service_account_file(p)

        try:
            import google.auth
            credentials, _ = google.auth.default()
            logger.info("Usando credenciales por defecto de la aplicación (ADC).")
            return credentials
        except Exception as e:
            logger.error(f"No se pudieron obtener las credenciales por defecto de la aplicación: {e}")
            raise SessionManagerError(
                "No se encontraron credenciales válidas para Google Cloud. "
                "Por favor, configura GOOGLE_APPLICATION_CREDENTIALS, "
                "asegúrate de que el archivo JSON esté en una ruta accesible, "
                "o ejecuta 'gcloud auth application-default login'."
            ) from e

    def generate_session_id(self, user_identifier: str, channel: str) -> str:
        """
        Genera un ID de sesión único basado en el identificador de usuario, canal y timestamp.
        Formato: CANAL_IDENTIFICADOR_YYYYMMDD_HHmmss

        Args:
            user_identifier (str): Identificador único del usuario (ej: número de teléfono 573XXXXXXXXX, o user_id de Telegram).
            channel (str): Canal de comunicación ("WA", "TL", etc.).

        Returns:
            str: ID de sesión.
        """
        clean_identifier = user_identifier.replace(" ", "").replace("-", "").strip()
        if len(clean_identifier) == 10 and clean_identifier.startswith("3"):
            normalized_identifier = f"57{clean_identifier}"
        else:
            normalized_identifier = clean_identifier

        now = datetime.now(self.colombia_tz)
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        session_id = f"{channel}_{normalized_identifier}_{timestamp_str}"
        logger.info("Session ID generado: %s", session_id)
        return session_id

    def extract_timestamp_from_session_id(self, session_id: str) -> datetime:
        """
        Extrae el timestamp de un ID de sesión.

        Args:
            session_id (str): ID de sesión (ej: "WA_573001234567_20250529_191057").

        Returns:
            datetime: Objeto datetime con la fecha y hora de creación de la sesión.

        Raises:
            ValueError: Si el ID de sesión tiene un formato inválido.
        """
        try:
            parts = session_id.split("_")
            if len(parts) < 3:
                raise ValueError("Formato de session_id incompleto.")
            
            timestamp_part = f"{parts[-2]}_{parts[-1]}"
            return datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
        except (ValueError, IndexError) as exc:
            logger.error(f"Error extrayendo timestamp de session_id '{session_id}': {exc}")
            raise ValueError(f"Session ID con formato inválido: {session_id}") from exc

    def extract_user_identifier_from_session_id(self, session_id: str) -> str:
        """
        Extrae el identificador de usuario (número de teléfono o ID de Telegram) de un ID de sesión.
        """
        try:
            parts = session_id.split('_')
            if len(parts) >= 2:
                return parts[1]
            return "unknown_identifier"
        except IndexError:
            return "unknown_identifier"
            
    def _insert_row_via_streaming(self, row_data: Dict[str, Any]) -> bool:
        """
        Inserta una fila directamente en BigQuery usando la API de Streaming.
        """
        try:
            table = self.client.get_table(self.table_ref)
            errors = self.client.insert_rows_json(table, [row_data])

            if errors:
                for error in errors:
                    logger.error(f"Error en fila al insertar en BigQuery (Streaming): {error}")
                return False
            return True
        except GoogleCloudError as e:
            logger.exception(f"Error de BigQuery al insertar fila (Streaming): {e}")
            return False
        except Exception as e:
            logger.exception(f"Error inesperado al insertar fila (Streaming): {e}")
            return False


    def create_session(self, user_identifier: str, channel: str = "WA") -> str:
        """
        Crea una nueva sesión insertando la fila inicial en BigQuery
        con la conversación de inicio y los campos de consentimiento nulos.

        Args:
            user_identifier (str): Identificador único del usuario.
            channel (str): Canal de comunicación (ej. "WA", "TL").

        Returns:
            str: El ID de la sesión creada.

        Raises:
            SessionManagerError: Si falla al guardar la sesión en BigQuery.
        """
        session_id = self.generate_session_id(user_identifier, channel)
        current_time = datetime.now(self.colombia_tz).isoformat()

        initial_conversation_data = [] # Esto se llenaría si quisieras un historial aquí
        initial_system_message = {
            "timestamp": current_time,
            "sender": "system",
            "message": "Sesión iniciada.",
            "event_type": "session_started",
            "user_id": user_identifier
        }
        initial_conversation_data.append(initial_system_message) # Añadir el mensaje de inicio

        row_data = {
            "id_sesion": session_id,
            "conversacion": json.dumps(initial_conversation_data, ensure_ascii=False), # JSON con mensaje de inicio
            "consentimiento": None, # En la fila inicial, estos son NULL
            "timestamp_consentimiento": None # En la fila inicial, estos son NULL
        }
        
        if not self._insert_row_via_streaming(row_data):
            raise SessionManagerError(f"Error al guardar la sesión inicial en BigQuery para {session_id}")
            
        logger.info(f"Sesión '{session_id}' creada y guardada en BigQuery.")
        return session_id

    def add_message_to_session(self, session_id: str, message_content: str, sender: str = "user", message_type: str = "conversation") -> None:
        """
        Agrega un mensaje/evento al historial de conversación de una sesión.
        Crea una nueva fila para cada mensaje/evento.

        Args:
            session_id (str): ID de la sesión.
            message_content (str): Contenido del mensaje.
            sender (str): Quién envía el mensaje ("user", "bot", "system").
            message_type (str): Tipo de mensaje (e.g., "conversation", "consent_response", "event").
        """
        current_time = datetime.now(self.colombia_tz).isoformat()
        user_identifier = self.extract_user_identifier_from_session_id(session_id)

        message_entry = {
            "timestamp": current_time,
            "sender": sender,
            "message": message_content,
            "message_type": message_type,
            "user_id": user_identifier
        }
        
        row_data = {
            "id_sesion": session_id,
            "conversacion": json.dumps([message_entry], ensure_ascii=False), # JSON con un solo mensaje/evento
            "consentimiento": None, # Sigue siendo NULL aquí, solo se llena en la fila de consentimiento
            "timestamp_consentimiento": None # Sigue siendo NULL aquí
        }

        if not self._insert_row_via_streaming(row_data):
            logger.error(f"Fallo al agregar mensaje a sesión {session_id}. Mensaje: {message_content[:50]}...")
        else:
            logger.info(f"Mensaje agregado a sesión {session_id} [{sender}]: {message_content[:50]}...")


    def record_consent_event(self, session_id: str, consent_status: str, user_telegram_id: int) -> bool:
        """
        Registra el evento de consentimiento en una NUEVA FILA separada en BigQuery.
        Esta fila contendrá los campos `consentimiento` y `timestamp_consentimiento` llenos.

        Args:
            session_id (str): El ID de la sesión.
            consent_status (str): El estado del consentimiento ('autorizado', 'no autorizado').
            user_telegram_id (int): El ID de usuario de Telegram para el log interno.

        Returns:
            bool: True si la inserción fue exitosa, False en caso contrario.
        """
        current_time = datetime.now(self.colombia_tz).isoformat()
        consent_bool_value = True if consent_status == "autorizado" else False
        user_identifier_from_session = self.extract_user_identifier_from_session_id(session_id)

        # Crear una entrada de conversación para este evento
        consent_event_data = {
            "timestamp": current_time,
            "sender": "user", # El usuario da el consentimiento
            "message": f"Consentimiento de datos: {consent_status}",
            "event_type": "consent_response",
            "consent_status": consent_status, # Registrar el status dentro del JSON también
            "user_id": str(user_telegram_id)
        }

        # La fila a insertar tendrá los campos de consentimiento llenos
        row_data = {
            "id_sesion": session_id,
            "conversacion": json.dumps([consent_event_data], ensure_ascii=False), # El evento como un JSON
            "consentimiento": consent_bool_value, # Este sí se llena
            "timestamp_consentimiento": current_time # Este sí se llena
        }

        if not self._insert_row_via_streaming(row_data):
            logger.error(f"Fallo al registrar evento de consentimiento para sesión {session_id}.")
            return False
        
        logger.info(f"Evento de consentimiento '{consent_status}' registrado en nueva fila para sesión {session_id}.")
        return True


    def get_conversation_history(self, session_id: str) -> list:
        """
        Obtiene todo el historial de conversación para una sesión dada,
        uniendo los eventos de la columna 'conversacion' de todas las filas con ese session_id.

        Args:
            session_id (str): ID de la sesión.

        Returns:
            list: Lista de mensajes/eventos ordenados por su timestamp interno.
        """
        try:
            # Seleccionamos 'conversacion' (JSON) y el 'timestamp_consentimiento'
            # para ordenar correctamente y poder decidir la fila principal.
            query = f"""
                SELECT conversacion, timestamp_consentimiento
                FROM `{self.table_ref}`
                WHERE id_sesion = @id_sesion
                ORDER BY timestamp_consentimiento ASC NULLS FIRST, id_sesion ASC
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("id_sesion", "STRING", session_id)
                ]
            )

            logger.info(f"Consultando historial de conversación para sesión {session_id}...")
            results = self.client.query(query, job_config=job_config).result()

            all_messages = []
            for row in results:
                if row.conversacion:
                    try:
                        messages_in_row = json.loads(row.conversacion)
                        if isinstance(messages_in_row, list):
                            all_messages.extend(messages_in_row)
                        elif isinstance(messages_in_row, dict):
                             all_messages.append(messages_in_row)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Error decodificando JSON en fila de conversación para {session_id}: {e}. Contenido: {row.conversacion[:100]}")
                        continue
            
            all_messages.sort(key=lambda x: x.get("timestamp", ""))
            logger.info(f"Historial de conversación recuperado para sesión {session_id}. Total mensajes: {len(all_messages)}")
            return all_messages

        except GoogleCloudError as e:
            logger.exception(f"Error de BigQuery al obtener historial de sesión {session_id}: {e}")
            return []
        except Exception as e:
            logger.exception(f"Error inesperado al obtener historial de sesión {session_id}: {e}")
            return []

    def get_previous_sessions_by_phone(self, user_identifier: str) -> list:
        """
        Obtiene sesiones previas de un identificador de usuario.
        Busca la fila inicial de cada sesión y su fila de consentimiento (si existe y tiene campos llenos).

        Args:
            user_identifier (str): Identificador único del usuario (ej: número de teléfono o ID de Telegram).

        Returns:
            list: Lista de diccionarios con información de sesiones previas.
        """
        try:
            clean_identifier = user_identifier.replace(" ", "").replace("-", "").strip()
            if len(clean_identifier) == 10 and clean_identifier.startswith("3"):
                normalized_identifier = f"57{clean_identifier}"
            else:
                normalized_identifier = clean_identifier
            
            # Queremos encontrar la fila de la sesión principal (la que tiene el mensaje de inicio)
            # y la fila del consentimiento (la que tiene el consentimiento y timestamp llenos).
            # Agruparemos por id_sesion y obtendremos los metadatos relevantes.
            query = f"""
                SELECT
                    id_sesion,
                    ARRAY_AGG(STRUCT(conversacion, consentimiento, timestamp_consentimiento)) AS session_data
                FROM `{self.table_ref}`
                WHERE id_sesion LIKE '%_{normalized_identifier}_%'
                GROUP BY id_sesion
                ORDER BY id_sesion DESC
                LIMIT 10
            """
            
            logger.info(f"Consultando sesiones previas para {user_identifier}...")
            results = self.client.query(query).result()
            
            previous_sessions = []
            for row in results:
                session_id = row.id_sesion
                created_at_str = None
                
                try:
                    created_at_dt = self.extract_timestamp_from_session_id(session_id)
                    created_at_str = created_at_dt.isoformat()
                except ValueError:
                    logger.warning(f"No se pudo extraer timestamp de session_id '{session_id}'.")

                # Buscar el estado de consentimiento y su timestamp
                consent_status = None
                consent_timestamp = None
                has_conversation_data = False

                for data_entry in row.session_data:
                    if data_entry.get("consentimiento") is not None:
                        consent_status = data_entry.get("consentimiento")
                        consent_timestamp = data_entry.get("timestamp_consentimiento").isoformat() if data_entry.get("timestamp_consentimiento") else None
                    if data_entry.get("conversacion"):
                        try:
                            if json.loads(data_entry.get("conversacion")):
                                has_conversation_data = True
                        except json.JSONDecodeError:
                            pass

                previous_sessions.append({
                    "session_id": session_id,
                    "user_identifier": normalized_identifier,
                    "created_at": created_at_str,
                    "has_conversation": has_conversation_data,
                    "consentimiento_status": consent_status,
                    "timestamp_consentimiento": consent_timestamp
                })
            
            logger.info("Encontradas %d sesiones previas para %s", len(previous_sessions), normalized_identifier)
            return previous_sessions
            
        except GoogleCloudError as e:
            logger.exception(f"Error de BigQuery buscando sesiones previas para {user_identifier}: {e}")
            return []
        except Exception as e:
            logger.exception(f"Error inesperado buscando sesiones previas para {user_identifier}: {e}")
            return []

    def create_session_with_history_check(self, user_identifier: str, channel: str = "WA") -> dict:
        """
        Crea una nueva sesión, pero verifica si hay historial previo para el identificador de usuario.
        Si la sesión ya existe (determinada por el ID), simplemente la retorna.
        Si no, crea una nueva.

        Args:
            user_identifier (str): Identificador único del usuario.
            channel (str): Canal de comunicación.

        Returns:
            dict: Información de la sesión (existente o nueva), incluyendo historial previo.
        """
        previous_sessions = self.get_previous_sessions_by_phone(user_identifier)
        new_session_id = self.create_session(user_identifier, channel)

        if previous_sessions:
            history_message = (
                f"Usuario tiene {len(previous_sessions)} sesiones previas. "
                f"Última iniciada: {previous_sessions[0]['created_at']}."
            )
            self.add_message_to_session(new_session_id, history_message, "system", "event")
            logger.info("Historial previo encontrado para el nuevo usuario y mensaje añadido.")
        else:
            logger.info("Usuario nuevo sin historial previo.")

        return {
            "new_session_id": new_session_id,
            "user_identifier": user_identifier,
            "channel": channel,
            "has_previous_history": len(previous_sessions) > 0,
            "previous_sessions_count": len(previous_sessions),
            "previous_sessions": previous_sessions[:3]
        }

    def is_session_expired(self, session_id: str, expiration_seconds: int = 24 * 3600) -> bool:
        """
        Verifica si una sesión ha expirado basándose en el timestamp del ID de sesión
        y un tiempo de inactividad configurable.

        Args:
            session_id (str): ID de la sesión.
            expiration_seconds (int): Segundos para considerar la sesión expirada (por defecto 24 horas).

        Returns:
            bool: True si la sesión ha expirado, False si sigue activa.
        """
        try:
            session_start_time = self.extract_timestamp_from_session_id(session_id)
            current_time = datetime.now(self.colombia_tz).replace(tzinfo=None)
            session_start_time_naive = session_start_time.replace(tzinfo=None)

            expiration_time = session_start_time_naive + timedelta(seconds=expiration_seconds)
            
            is_expired = current_time > expiration_time

            age_seconds = (current_time - session_start_time_naive).total_seconds()
            
            if is_expired:
                logger.info(
                    "Sesión %s expirada. Creada: %s, Expira: %s, Edad: %.1f segundos.",
                    session_id,
                    session_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    expiration_time.strftime("%Y-%m-%d %H:%M:%S"),
                    age_seconds
                )
            else:
                time_left = (expiration_time - current_time).total_seconds()
                logger.debug(
                    "Sesión %s activa. Creada: %s, Expira: %s, Quedan: %.1f segundos.",
                    session_id,
                    session_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    expiration_time.strftime("%Y-%m-%d %H:%M:%S"),
                    time_left
                )
            
            return is_expired
            
        except ValueError as exc:
            logger.error(f"Error verificando expiración de sesión {session_id}: {exc}. Considerada expirada por seguridad.")
            return True
        except Exception as exc:
            logger.exception(f"Error inesperado al verificar expiración de sesión {session_id}: {exc}")
            return True

    def close_session(self, session_id: str, reason: str = "completed") -> None:
        """
        Cierra una sesión agregando un evento de cierre al historial como una nueva fila.

        Args:
            session_id (str): ID de la sesión a cerrar.
            reason (str): Motivo del cierre ("completed", "expired", "manual", etc.).
        """
        current_time = datetime.now(self.colombia_tz).isoformat()

        close_message = {
            "timestamp": current_time,
            "sender": "system",
            "message": f"Sesión cerrada por: {reason}",
            "event_type": "session_closed",
            "reason": reason
        }
        
        self.add_message_to_session(session_id, json.dumps(close_message), "system", "event")
        logger.info("Sesión '%s' marcada como cerrada por: '%s'.", session_id, reason)

    def check_and_expire_session(self, session_id: str, expiration_seconds: int = 24 * 3600) -> bool:
        """
        Verifica si una sesión está expirada y la cierra automáticamente si es el caso.

        Args:
            session_id (str): ID de la sesión a verificar.
            expiration_seconds (int): Segundos para considerar la sesión expirada.

        Returns:
            bool: True si la sesión fue expirada y cerrada, False si sigue activa.
        """
        if self.is_session_expired(session_id, expiration_seconds):
            self.close_session(session_id, "expired")
            logger.info("Sesión '%s' expirada y cerrada automáticamente.", session_id)
            return True
        return False
    
    def get_session_info(self, session_id: str) -> dict:
        """
        Obtiene información detallada de una sesión.

        Args:
            session_id (str): ID de sesión.

        Returns:
            dict: Información de la sesión (ID, identificador, canal, creación, edad, expiración).
        """
        try:
            session_time = self.extract_timestamp_from_session_id(session_id)
            current_time = datetime.now(self.colombia_tz).replace(tzinfo=None)
            session_time_naive = session_time.replace(tzinfo=None)
            
            age_seconds = (current_time - session_time_naive).total_seconds()
            expiration_time_naive = session_time_naive + timedelta(seconds=24 * 3600)
            
            return {
                "session_id": session_id,
                "user_identifier": self.extract_user_identifier_from_session_id(session_id),
                "channel": session_id.split('_')[0] if len(session_id.split('_')) > 0 else "N/A",
                "created_at": session_time.isoformat(),
                "age_seconds": round(age_seconds, 2),
                "is_expired": self.is_session_expired(session_id),
                "expires_at": expiration_time_naive.isoformat()
            }
        except ValueError as exc:
            logger.error(f"Error obteniendo información de sesión {session_id}: {exc}")
            return {"error": str(exc), "session_id": session_id}
        except Exception as exc:
            logger.exception(f"Error inesperado al obtener información de sesión {session_id}: {exc}")
            return {"error": "Error interno al obtener información de sesión.", "session_id": session_id}