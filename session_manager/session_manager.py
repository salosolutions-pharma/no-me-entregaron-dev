import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
import pytz
import json
from dotenv import load_dotenv
from google.cloud import firestore
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import GoogleAPIError # Para errores generales de Google Cloud

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

# Variables de entorno para Firestore
PROJECT_ID = os.getenv("PROJECT_ID")
# Para Firestore, la "tabla" de historial de conversaciones activas es una colección.
FIRESTORE_COLLECTION_SESSIONS_ACTIVE = "sesiones_activas" # Nombre de la colección en Firestore [cite: 1, 4]


class SessionManager:
    """
    Gestiona las sesiones de conversación como documentos en la colección 'sesiones_activas' de Firestore.
    Cada documento representa una sesión activa y contiene todo el historial de conversación en un array.
    """

    def __init__(self):
        """Inicializa el gestor de sesiones, estableciendo la conexión con Firestore."""
        if not PROJECT_ID:
            raise ValueError(
                "La variable de entorno PROJECT_ID no está configurada para Firestore."
            )
        self.db = self._get_firestore_client()
        self.sessions_collection_ref = self.db.collection(FIRESTORE_COLLECTION_SESSIONS_ACTIVE)
        self.colombia_tz = pytz.timezone('America/Bogota')
        logger.info(f"SessionManager inicializado. Conectado a Firestore colección: '{FIRESTORE_COLLECTION_SESSIONS_ACTIVE}'.")

    def _get_firestore_client(self) -> firestore.Client:
        """Crea y retorna una instancia del cliente de Firestore."""
        # firestore.Client detecta automáticamente las credenciales si GOOGLE_APPLICATION_CREDENTIALS está configurado
        try:
            return firestore.Client(project=PROJECT_ID, database="historia")
        except Exception as e:
            logger.exception(f"Error al inicializar el cliente de Firestore: {e}")
            raise SessionManagerError(f"Fallo al crear cliente de Firestore: {e}") from e

    def generate_session_id(self, user_identifier: str, channel: str) -> str:
        """
        Genera un ID de sesión único basado en el identificador de usuario, canal y timestamp.
        Formato: CANAL_IDENTIFICADOR_YYYYMMDD_HHmmss

        Args:
            user_identifier (str): Identificador único del usuario (número de celular).
            channel (str): Canal de comunicación ("WA", "TL", etc.).

        Returns:
            str: ID de sesión.
        """
        # Limpiar cualquier caracter no numérico del identificador y asegurar formato
        clean_identifier = "".join(filter(str.isdigit, user_identifier))
        
        # Formato de número de teléfono colombiano con +57 o sin si ya tiene 10 dígitos y empieza con 3
        if not clean_identifier.startswith("57") and len(clean_identifier) == 10 and clean_identifier.startswith("3"):
            normalized_identifier = f"57{clean_identifier}"
        elif clean_identifier.startswith("57") and len(clean_identifier) == 12: # Ya tiene 57
             normalized_identifier = clean_identifier
        else: # Otros casos, usar tal cual o manejar error si se espera un formato estricto
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
            # Devuelve un datetime naive para facilitar comparaciones si no hay tz en el otro lado
            return datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
        except (ValueError, IndexError) as exc:
            logger.error(f"Error extrayendo timestamp de session_id '{session_id}': {exc}")
            raise ValueError(f"Session ID con formato inválido: {session_id}") from exc

    def extract_user_identifier_from_session_id(self, session_id: str) -> str:
        """
        Extrae el identificador de usuario (número de teléfono) de un ID de sesión.
        """
        try:
            parts = session_id.split('_')
            if len(parts) >= 2:
                return parts[1]
            return "unknown_identifier"
        except IndexError:
            return "unknown_identifier"
            

    def create_session(self, user_identifier: str, channel: str = "WA") -> str:
        """
        Crea un nuevo documento de sesión en Firestore para una sesión activa.
        La sesión se inicializa con un mensaje de sistema y campos de consentimiento nulos.

        Args:
            user_identifier (str): Identificador único del usuario (número de celular).
            channel (str): Canal de comunicación (ej. "WA", "TL").

        Returns:
            str: El ID de la sesión creada.

        Raises:
            SessionManagerError: Si falla al crear la sesión en Firestore.
        """
        session_id = self.generate_session_id(user_identifier, channel)
        current_time_iso = datetime.now(self.colombia_tz).isoformat()

        initial_conversation_data = []
        initial_system_message = {
            "timestamp": current_time_iso,
            "sender": "system",
            "message": "Sesión iniciada.",
            "event_type": "session_started",
            "user_id": user_identifier # Aquí se guarda el número de celular
        }
        initial_conversation_data.append(initial_system_message)

        session_data = {
            "id_sesion": session_id,
            "user_identifier": user_identifier, # Campo para consultas directas por usuario
            "channel": channel,
            "created_at": firestore.SERVER_TIMESTAMP, # Firestore gestiona timestamps nativamente
            "last_activity_at": firestore.SERVER_TIMESTAMP, # Para seguimiento de inactividad
            "conversation": initial_conversation_data, # Lista de objetos mensaje
            "consentimiento": None, # Valor inicial nulo
            "timestamp_consentimiento": None, # Valor inicial nulo
            "estado_sesion": "activa" # Campo clave para la Cloud Function [cite: 8]
        }
        
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.set(session_data) # set() crea el documento si no existe, o lo sobrescribe
            logger.info(f"Sesión '{session_id}' creada en Firestore.")
            return session_id
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al crear sesión {session_id}: {e}")
            raise SessionManagerError(f"Error al crear sesión en Firestore: {e}") from e
        except Exception as e:
            logger.exception(f"Error inesperado al crear sesión {session_id}: {e}")
            raise SessionManagerError(f"Error inesperado al crear sesión en Firestore: {e}") from e


    def add_message_to_session(self, session_id: str, message_content: str, sender: str = "user", message_type: str = "conversation") -> None:
        """
        Agrega un mensaje/evento al array 'conversation' del documento de sesión en Firestore.
        También actualiza 'last_activity_at'.

        Args:
            session_id (str): ID de la sesión.
            message_content (str): Contenido del mensaje.
            sender (str): Quién envía el mensaje ("user", "bot", "system").
            message_type (str): Tipo de mensaje (e.g., "conversation", "consent_response", "event").
        """
        current_time_iso = datetime.now(self.colombia_tz).isoformat()
        user_identifier = self.extract_user_identifier_from_session_id(session_id)

        new_message_entry = {
            "timestamp": current_time_iso,
            "sender": sender,
            "message": message_content,
            "event_type": message_type,
            "user_id": user_identifier # Aquí se guarda el número de celular
        }
        
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                'conversation': firestore.ArrayUnion([new_message_entry]), # Añadir al array [cite: 5]
                'last_activity_at': firestore.SERVER_TIMESTAMP # Actualizar actividad
            })
            logger.info(f"Mensaje agregado a sesión {session_id} [{sender}]: {message_content[:50]}...")
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al agregar mensaje a sesión {session_id}: {e}")
        except Exception as e:
            logger.exception(f"Error inesperado al agregar mensaje a sesión {session_id}: {e}")


    def update_consent_for_session(self, session_id: str, consent_status: str) -> bool:
        """
        Actualiza los campos 'consentimiento' y 'timestamp_consentimiento'
        del documento de sesión en Firestore, y añade un evento de consentimiento al historial de conversación.

        Args:
            session_id (str): El ID de la sesión cuya información de consentimiento se actualizará.
            consent_status (str): El estado del consentimiento ('autorizado', 'no autorizado').

        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario.
        """
        current_time_iso = datetime.now(self.colombia_tz).isoformat()
        consent_bool_value = True if consent_status == "autorizado" else False

        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                "consentimiento": consent_bool_value,
                "timestamp_consentimiento": firestore.SERVER_TIMESTAMP,
                "last_activity_at": firestore.SERVER_TIMESTAMP # También actualizar actividad
            })
            logger.info(f"Consentimiento de sesión {session_id} actualizado a '{consent_status}' en Firestore.")
            
            # --- AÑADIR ESTE BLOQUE ---
            # También añadir el evento de consentimiento al array 'conversation'
            consent_event_data = {
                "timestamp": current_time_iso,
                "sender": "system", # O "user" si el bot es el que registra la acción del usuario
                "message": f"Consentimiento de datos: {consent_status}",
                "event_type": "consent_response",
                "consent_status": consent_status,
                "user_id": self.extract_user_identifier_from_session_id(session_id)
            }
            # Llamamos a add_message_to_session para que lo agregue al array 'conversation'
            self.add_message_to_session(session_id, json.dumps(consent_event_data), "system", "consent_response")
            logger.info(f"Evento de consentimiento también añadido al array 'conversation' para sesión {session_id}.")
            # --- FIN DEL BLOQUE AÑADIDO ---

            return True
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al actualizar consentimiento para sesión {session_id}: {e}")
            return False
        except Exception as e:
            logger.exception(f"Error inesperado al actualizar consentimiento para sesión {session_id}: {e}")
            return False

    def get_conversation_history(self, session_id: str) -> list:
        """
        Obtiene el historial de conversación de un documento de sesión de Firestore.

        Args:
            session_id (str): ID de la sesión.

        Returns:
            list: Lista de mensajes/eventos ordenados por su timestamp interno.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get()
            if doc.exists:
                session_data = doc.to_dict()
                conversation_list = session_data.get('conversation', [])
                # Asegurar orden por timestamp, aunque Firestore tiende a mantener el orden de inserción en arrays
                conversation_list.sort(key=lambda x: x.get("timestamp", "")) 
                logger.info(f"Historial de conversación recuperado para sesión {session_id}. Total mensajes: {len(conversation_list)}")
                return conversation_list
            logger.info(f"Sesión {session_id} no encontrada en Firestore.")
            return []
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al obtener historial de sesión {session_id}: {e}")
            return []
        except Exception as e:
            logger.exception(f"Error inesperado al obtener historial de sesión {session_id}: {e}")
            return []

    def get_previous_sessions_by_phone(self, user_identifier: str) -> list:
        """
        Obtiene sesiones previas de un identificador de usuario (número de celular)
        de la colección de sesiones activas en Firestore.

        Args:
            user_identifier (str): Identificador único del usuario (número de celular).

        Returns:
            list: Lista de diccionarios con información de sesiones previas.
        """
        try:
            # Normalizar el identificador para la consulta
            clean_identifier = "".join(filter(str.isdigit, user_identifier))
            if not clean_identifier.startswith("57") and len(clean_identifier) == 10 and clean_identifier.startswith("3"):
                normalized_identifier = f"57{clean_identifier}"
            elif clean_identifier.startswith("57") and len(clean_identifier) == 12:
                 normalized_identifier = clean_identifier
            else:
                 normalized_identifier = clean_identifier
            
            # Consultar Firestore por sesiones de este usuario, ordenadas por actividad reciente
            query = (
                self.sessions_collection_ref
                .where('user_identifier', '==', normalized_identifier) # Filtra por el campo user_identifier
                .where('estado_sesion', '==', 'activa') # Solo sesiones activas
                .order_by('last_activity_at', direction=firestore.Query.DESCENDING) # Ordena por la última actividad
                .limit(10)
            )
            
            logger.info(f"Consultando sesiones activas previas para {user_identifier} en Firestore...")
            results = query.stream()
            
            previous_sessions = []
            for doc in results:
                data = doc.to_dict()
                session_id = doc.id # El ID del documento es el id_sesion
                created_at_dt = data.get('created_at')
                created_at_str = created_at_dt.isoformat() if created_at_dt else None # Firestore Timestamp a ISO string

                consent_status = data.get('consentimiento')
                consent_timestamp_dt = data.get('timestamp_consentimiento')
                consent_timestamp_str = consent_timestamp_dt.isoformat() if consent_timestamp_dt else None

                previous_sessions.append({
                    "session_id": session_id,
                    "user_identifier": data.get('user_identifier'),
                    "channel": data.get('channel'),
                    "created_at": created_at_str,
                    "has_conversation": bool(data.get('conversation')),
                    "consentimiento_status": consent_status,
                    "timestamp_consentimiento": consent_timestamp_str
                })
            
            logger.info("Encontradas %d sesiones activas previas para %s en Firestore.", len(previous_sessions), user_identifier)
            return previous_sessions
            
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore buscando sesiones previas para {user_identifier}: {e}")
            return []
        except Exception as e:
            logger.exception(f"Error inesperado buscando sesiones previas para {user_identifier}: {e}")
            return []

    def create_session_with_history_check(self, user_identifier: str, channel: str = "WA") -> dict:
        """
        Crea una nueva sesión para el identificador de usuario si no existe una activa y no expirada,
        o devuelve la existente.

        Args:
            user_identifier (str): Identificador único del usuario (número de celular).
            channel (str): Canal de comunicación.

        Returns:
            dict: Información de la sesión (existente o nueva).
        """
        normalized_identifier = "".join(filter(str.isdigit, user_identifier))
        if not normalized_identifier.startswith("57") and len(normalized_identifier) == 10 and normalized_identifier.startswith("3"):
            normalized_identifier = f"57{normalized_identifier}"
        elif normalized_identifier.startswith("57") and len(normalized_identifier) == 12:
             normalized_identifier = normalized_identifier
        else:
             normalized_identifier = normalized_identifier
        
        # Buscar una sesión activa existente para este identificador
        query_active_session = (
            self.sessions_collection_ref
            .where('user_identifier', '==', normalized_identifier)
            .where('estado_sesion', '==', 'activa')
            .order_by('last_activity_at', direction=firestore.Query.DESCENDING)
            .limit(1)
        )
        logger.info(f"Buscando sesión activa existente para {normalized_identifier} en Firestore...")
        
        try:
            active_session_doc = None
            for doc in query_active_session.stream():
                active_session_doc = doc
                break

            if active_session_doc:
                session_data = active_session_doc.to_dict()
                session_id = active_session_doc.id
                # Aquí podrías añadir lógica para comprobar si la sesión activa está expirada por inactividad
                # Si lo está, podrías cerrarla aquí y crear una nueva.
                # Por simplicidad, si está activa, la reutilizamos.
                logger.info(f"Sesión activa encontrada y reutilizada para {normalized_identifier}: {session_id}.")
                return {
                    "new_session_id": session_id,
                    "user_identifier": user_identifier,
                    "channel": session_data.get('channel'),
                    "has_previous_history": True,
                    "previous_sessions_count": 1, # Simplificado
                    "previous_sessions": [] # Simplificado
                }
            
            logger.info(f"No se encontró sesión activa para {normalized_identifier}. Creando una nueva.")
            # Si no hay sesión activa, creamos una nueva
            new_session_id = self.create_session(user_identifier, channel)
            
            # Obtener detalles de sesiones previas (cerradas o inactivas)
            previous_sessions_details = self.get_previous_sessions_by_phone(user_identifier)
            if previous_sessions_details:
                history_message = (
                    f"Usuario tiene {len(previous_sessions_details)} sesiones previas. "
                    f"Última iniciada: {previous_sessions_details[0]['created_at']}."
                )
                self.add_message_to_session(new_session_id, history_message, "system", "event")
                logger.info("Mensaje de historial previo añadido a la nueva sesión.")
            else:
                logger.info("Usuario nuevo sin historial previo.")

            return {
                "new_session_id": new_session_id,
                "user_identifier": user_identifier,
                "channel": channel,
                "has_previous_history": len(previous_sessions_details) > 0,
                "previous_sessions_count": len(previous_sessions_details),
                "previous_sessions": previous_sessions_details[:3]
            }

        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al buscar/crear sesión para {user_identifier}: {e}")
            raise SessionManagerError(f"Error al gestionar sesión: {e}") from e
        except Exception as e:
            logger.exception(f"Error inesperado al gestionar sesión para {user_identifier}: {e}")
            raise SessionManagerError(f"Error inesperado al gestionar sesión: {e}") from e


    def is_session_expired(self, session_id: str, expiration_seconds: int = 24 * 3600) -> bool:
        """
        Verifica si una sesión ha expirado basándose en su 'last_activity_at' en Firestore.

        Args:
            session_id (str): ID de la sesión.
            expiration_seconds (int): Segundos para considerar la sesión expirada (por defecto 24 horas).

        Returns:
            bool: True si la sesión ha expirado, False si sigue activa.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get(['last_activity_at', 'estado_sesion']) # Obtener solo los campos necesarios

            if not doc.exists:
                logger.warning(f"Sesión {session_id} no encontrada para verificar expiración. Considerada expirada.")
                return True
            
            session_data = doc.to_dict()
            estado_sesion = session_data.get('estado_sesion')
            if estado_sesion == "cerrado":
                logger.info(f"Sesión {session_id} ya está marcada como 'cerrado'. Considerada expirada.")
                return True # Ya está cerrada, entonces expirada para el propósito del check

            last_activity_at = session_data.get('last_activity_at')
            if not last_activity_at:
                logger.warning(f"Sesión {session_id} sin 'last_activity_at'. Considerada expirada.")
                return True # Si no hay actividad, consideramos expirada.

            # Convertir Firestore Timestamp a datetime nativo (naive para comparación)
            last_activity_datetime_naive = last_activity_at.astimezone(self.colombia_tz).replace(tzinfo=None)
            current_time_naive = datetime.now(self.colombia_tz).replace(tzinfo=None)

            expiration_time_naive = last_activity_datetime_naive + timedelta(seconds=expiration_seconds)
            
            is_expired = current_time_naive > expiration_time_naive

            age_seconds = (current_time_naive - last_activity_datetime_naive).total_seconds()
            
            if is_expired:
                logger.info(
                    "Sesión %s expirada. Última actividad: %s, Expira: %s, Edad: %.1f segundos.",
                    session_id,
                    last_activity_datetime_naive.strftime("%Y-%m-%d %H:%M:%S"),
                    expiration_time_naive.strftime("%Y-%m-%d %H:%M:%S"),
                    age_seconds
                )
            else:
                time_left = (expiration_time_naive - current_time_naive).total_seconds()
                logger.debug(
                    "Sesión %s activa. Última actividad: %s, Expira: %s, Quedan: %.1f segundos.",
                    session_id,
                    last_activity_datetime_naive.strftime("%Y-%m-%d %H:%M:%S"),
                    expiration_time_naive.strftime("%Y-%m-%d %H:%M:%S"),
                    time_left
                )
            
            return is_expired
            
        except GoogleAPIError as exc:
            logger.exception(f"Error de Firestore verificando expiración de sesión {session_id}: {exc}. Considerada expirada por seguridad.")
            return True
        except Exception as exc:
            logger.exception(f"Error inesperado al verificar expiración de sesión {session_id}: {exc}")
            return True

    def close_session(self, session_id: str, reason: str = "completed") -> None:
        """
        Cierra una sesión en Firestore marcando el campo 'estado_sesion' como "cerrado".
        Esta acción activará la Cloud Function para migrar a BigQuery.

        Args:
            session_id (str): ID de la sesión a cerrar.
            reason (str): Motivo del cierre ("completed", "expired", "manual", etc.).
        """
        current_time_iso = datetime.now(self.colombia_tz).isoformat()
        
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                "estado_sesion": "cerrado", # Marcamos la sesión como cerrada para la Cloud Function [cite: 8]
                "closed_at": firestore.SERVER_TIMESTAMP,
                "close_reason": reason,
                "last_activity_at": firestore.SERVER_TIMESTAMP # Última actividad al cerrar
            })
            logger.info("Sesión '%s' marcada como cerrada por: '%s' en Firestore. Esto debería disparar la Cloud Function.", session_id, reason)
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al cerrar sesión {session_id}: {e}")
        except Exception as e:
            logger.exception(f"Error inesperado al cerrar sesión {session_id}: {e}")

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
            logger.info("Sesión '%s' expirada y marcada como cerrada en Firestore.", session_id)
            return True
        return False
    
    def get_session_info(self, session_id: str) -> dict:
        """
        Obtiene información detallada de una sesión directamente de Firestore.

        Args:
            session_id (str): ID de sesión.

        Returns:
            dict: Información de la sesión.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                created_at_dt = data.get('created_at')
                created_at_str = created_at_dt.isoformat() if created_at_dt else None

                last_activity_dt = data.get('last_activity_at')
                last_activity_str = last_activity_dt.isoformat() if last_activity_dt else None
                
                # Calcular edad y expiración basado en created_at o last_activity_at
                session_start_time = self.extract_timestamp_from_session_id(session_id)
                current_time_naive = datetime.now(self.colombia_tz).replace(tzinfo=None)
                session_start_time_naive = session_start_time.replace(tzinfo=None)
                age_seconds = (current_time_naive - session_start_time_naive).total_seconds()
                expiration_time_naive = session_start_time_naive + timedelta(seconds=24 * 3600) # Default 24h
                
                return {
                    "session_id": session_id,
                    "user_identifier": data.get('user_identifier'),
                    "channel": data.get('channel'),
                    "created_at": created_at_str,
                    "last_activity_at": last_activity_str,
                    "estado_sesion": data.get('estado_sesion'),
                    "consentimiento": data.get('consentimiento'),
                    "timestamp_consentimiento": data.get('timestamp_consentimiento').isoformat() if data.get('timestamp_consentimiento') else None,
                    "age_seconds": round(age_seconds, 2),
                    "is_expired": self.is_session_expired(session_id), # Llama a la función para verificar
                    "expires_at": expiration_time_naive.isoformat()
                }
            logger.warning(f"Sesión {session_id} no encontrada en Firestore para get_session_info.")
            return {"error": "Sesión no encontrada", "session_id": session_id}
        except GoogleAPIError as exc:
            logger.exception(f"Error de Firestore obteniendo info de sesión {session_id}: {exc}")
            return {"error": str(exc), "session_id": session_id}
        except Exception as exc:
            logger.exception(f"Error inesperado al obtener info de sesión {session_id}: {exc}")
            return {"error": "Error interno al obtener info de sesión.", "session_id": session_id}