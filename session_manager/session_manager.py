# session_manager.py
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
from google.api_core.exceptions import GoogleAPIError

# --- Excepciones personalizadas ---
class SessionManagerError(Exception):
    """Excepción base para errores en el SessionManager."""

# --- Configuración básica de logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Cargar variables de entorno ---
load_dotenv()

# Variables de entorno para Firestore
PROJECT_ID: str = os.getenv("PROJECT_ID", "")
FIRESTORE_COLLECTION_SESSIONS_ACTIVE: str = "sesiones_activas"

class SessionManager:
    """
    Gestiona las sesiones de conversación como documentos en la colección
    'sesiones_activas' de Firestore.
    Cada documento representa una sesión activa y contiene todo el historial
    de conversación en un array.
    """

    def __init__(self):
        """Inicializa el gestor de sesiones, estableciendo la conexión con Firestore."""
        if not PROJECT_ID:
            raise SessionManagerError(
                "La variable de entorno PROJECT_ID no está configurada para Firestore."
            )
        self.db = self._get_firestore_client()
        self.sessions_collection_ref = self.db.collection(FIRESTORE_COLLECTION_SESSIONS_ACTIVE)
        self.colombia_tz = pytz.timezone('America/Bogota')
        logger.info(f"SessionManager inicializado. Conectado a Firestore colección: '{FIRESTORE_COLLECTION_SESSIONS_ACTIVE}'.")

    def _get_firestore_client(self) -> firestore.Client:
        """Crea y retorna una instancia del cliente de Firestore."""
        try:
            return firestore.Client(project=PROJECT_ID, database="historia") # "historia" es el nombre de la base de datos de Firestore
        except Exception as e:
            logger.exception(f"Error al inicializar el cliente de Firestore: {e}")
            raise SessionManagerError(f"Fallo al crear cliente de Firestore: {e}") from e

    def generate_session_id(self, user_identifier: str, channel: str) -> str:
        """
        Genera un ID de sesión único basado en el identificador de usuario, canal y timestamp.
        Formato: CANAL_IDENTIFICADOR_YYYYMMDD_HHmmss
        """
        clean_identifier = "".join(filter(str.isdigit, user_identifier))
        # Normalización del identificador para el contexto colombiano
        if not clean_identifier.startswith("57") and len(clean_identifier) == 10 and clean_identifier.startswith("3"):
            normalized_identifier = f"57{clean_identifier}"
        elif clean_identifier.startswith("57") and len(clean_identifier) == 12:
             normalized_identifier = clean_identifier
        else:
             normalized_identifier = clean_identifier # Mantener si no se ajusta al patrón colombiano típico

        now = datetime.now(self.colombia_tz)
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        session_id = f"{channel}_{normalized_identifier}_{timestamp_str}"
        logger.info("Session ID generado: %s", session_id)
        return session_id

    def extract_timestamp_from_session_id(self, session_id: str) -> datetime:
        """Extrae el timestamp de un ID de sesión."""
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
        """Extrae el identificador de usuario (número de teléfono) de un ID de sesión."""
        try:
            parts = session_id.split('_')
            if len(parts) >= 2:
                return parts[1]
            return "unknown_identifier"
        except IndexError:
            return "unknown_identifier"
            
    def create_session(self, user_identifier: str, channel: str = "WA") -> str:
        """Crea un nuevo documento de sesión en Firestore para una sesión activa."""
        session_id = self.generate_session_id(user_identifier, channel)
        current_time_iso = datetime.now(self.colombia_tz).isoformat()

        initial_conversation_data = []
        initial_system_message = {
            "timestamp": current_time_iso,
            "sender": "system",
            "message": "Sesión iniciada.",
            "event_type": "session_started",
            "user_id": user_identifier
        }
        initial_conversation_data.append(initial_system_message)

        session_data = {
            "id_sesion": session_id,
            "user_identifier": user_identifier,
            "channel": channel,
            "created_at": firestore.SERVER_TIMESTAMP,
            "last_activity_at": firestore.SERVER_TIMESTAMP,
            "conversation": initial_conversation_data,
            "consentimiento": None,
            "timestamp_consentimiento": None,
            "estado_sesion": "activa"
        }
        
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.set(session_data)
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
        """
        current_time_iso = datetime.now(self.colombia_tz).isoformat()
        user_identifier = self.extract_user_identifier_from_session_id(session_id)

        new_message_entry = {
            "timestamp": current_time_iso,
            "sender": sender,
            "message": message_content,
            "event_type": message_type,
            "user_id": user_identifier
        }
        
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                'conversation': firestore.ArrayUnion([new_message_entry]),
                'last_activity_at': firestore.SERVER_TIMESTAMP
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
        """
        current_time_iso = datetime.now(self.colombia_tz).isoformat()
        consent_bool_value = True if consent_status == "autorizado" else False

        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                "consentimiento": consent_bool_value,
                "timestamp_consentimiento": firestore.SERVER_TIMESTAMP,
                "last_activity_at": firestore.SERVER_TIMESTAMP
            })
            logger.info(f"Consentimiento de sesión {session_id} actualizado a '{consent_status}' en Firestore.")
            
            # También añadir el evento de consentimiento al array 'conversation'
            consent_event_data = {
                "timestamp": current_time_iso,
                "sender": "system",
                "message": f"Consentimiento de datos: {consent_status}",
                "event_type": "consent_response",
                "consent_status": consent_status,
                "user_id": self.extract_user_identifier_from_session_id(session_id)
            }
            self.add_message_to_session(session_id, json.dumps(consent_event_data), "system", "consent_response")
            logger.info(f"Evento de consentimiento también añadido al array 'conversation' para sesión {session_id}.")

            return True
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al actualizar consentimiento para sesión {session_id}: {e}")
            return False
        except Exception as e:
            logger.exception(f"Error inesperado al actualizar consentimiento para sesión {session_id}: {e}")
            return False

    def get_conversation_history(self, session_id: str) -> list:
        """Obtiene el historial de conversación de un documento de sesión de Firestore."""
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get()
            if doc.exists:
                session_data = doc.to_dict()
                conversation_list = session_data.get('conversation', [])
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
        """Obtiene sesiones previas activas de un identificador de usuario."""
        try:
            clean_identifier = "".join(filter(str.isdigit, user_identifier))
            if not clean_identifier.startswith("57") and len(clean_identifier) == 10 and clean_identifier.startswith("3"):
                normalized_identifier = f"57{clean_identifier}"
            elif clean_identifier.startswith("57") and len(clean_identifier) == 12:
                 normalized_identifier = clean_identifier
            else:
                 normalized_identifier = clean_identifier
            
            query = (
                self.sessions_collection_ref
                .where('user_identifier', '==', normalized_identifier)
                .where('estado_sesion', '==', 'activa')
                .order_by('last_activity_at', direction=firestore.Query.DESCENDING)
                .limit(10)
            )
            
            logger.info(f"Consultando sesiones activas previas para {user_identifier} en Firestore...")
            results = query.stream()
            
            previous_sessions = []
            for doc in results:
                data = doc.to_dict()
                session_id = doc.id
                created_at_dt = data.get('created_at')
                created_at_str = created_at_dt.isoformat() if created_at_dt else None

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
            
            logger.info(f"Encontradas {len(previous_sessions)} sesiones activas previas para {user_identifier} en Firestore.")
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
        """
        clean_identifier = "".join(filter(str.isdigit, user_identifier)) # Esta línea define clean_identifier
        normalized_identifier: str = clean_identifier # Inicializar normalized_identifier con clean_identifier por defecto
        if not clean_identifier.startswith("57") and len(clean_identifier) == 10 and clean_identifier.startswith("3"):
            normalized_identifier = f"57{clean_identifier}"
        elif clean_identifier.startswith("57") and len(clean_identifier) == 12:
             normalized_identifier = clean_identifier
        
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
                logger.info(f"Sesión activa encontrada y reutilizada para {normalized_identifier}: {session_id}.")
                return {
                    "new_session_id": session_id,
                    "user_identifier": user_identifier,
                    "channel": session_data.get('channel'),
                    "has_previous_history": True,
                    "previous_sessions_count": 1,
                    "previous_sessions": []
                }
            
            logger.info(f"No se encontró sesión activa para {normalized_identifier}. Creando una nueva.")
            new_session_id = self.create_session(user_identifier, channel)
            
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
        """Verifica si una sesión ha expirado basándose en su 'last_activity_at'."""
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get(['last_activity_at', 'estado_sesion'])

            if not doc.exists:
                logger.warning(f"Sesión {session_id} no encontrada para verificar expiración. Considerada expirada.")
                return True
            
            session_data = doc.to_dict()
            estado_sesion = session_data.get('estado_sesion')
            if estado_sesion == "cerrado":
                logger.info(f"Sesión {session_id} ya está marcada como 'cerrado'. Considerada expirada.")
                return True

            last_activity_at = session_data.get('last_activity_at')
            if not last_activity_at:
                logger.warning(f"Sesión {session_id} sin 'last_activity_at'. Considerada expirada.")
                return True

            last_activity_datetime_naive = last_activity_at.astimezone(self.colombia_tz).replace(tzinfo=None)
            current_time_naive = datetime.now(self.colombia_tz).replace(tzinfo=None)

            expiration_time_naive = last_activity_datetime_naive + timedelta(seconds=expiration_seconds)
            
            is_expired = current_time_naive > expiration_time_naive

            age_seconds = (current_time_naive - last_activity_datetime_naive).total_seconds()
            
            if is_expired:
                logger.info(
                    f"Sesión {session_id} expirada. Última actividad: {last_activity_datetime_naive.strftime('%Y-%m-%d %H:%M:%S')}, Expira: {expiration_time_naive.strftime('%Y-%m-%d %H:%M:%S')}, Edad: {age_seconds:.1f} segundos."
                )
            else:
                time_left = (expiration_time_naive - current_time_naive).total_seconds()
                logger.debug(
                    f"Sesión {session_id} activa. Última actividad: {last_activity_datetime_naive.strftime('%Y-%m-%d %H:%M:%S')}, Expira: {expiration_time_naive.strftime('%Y-%m-%d %H:%M:%S')}, Quedan: {time_left:.1f} segundos."
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
        """
        # No es necesario current_time_iso aquí, firestore.SERVER_TIMESTAMP lo maneja.
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                "estado_sesion": "cerrado",
                "closed_at": firestore.SERVER_TIMESTAMP,
                "close_reason": reason,
                "last_activity_at": firestore.SERVER_TIMESTAMP
            })
            logger.info(f"Sesión '{session_id}' marcada como cerrada por: '{reason}' en Firestore.")
        except GoogleAPIError as e:
            logger.exception(f"Error de Firestore al cerrar sesión {session_id}: {e}")
        except Exception as e:
            logger.exception(f"Error inesperado al cerrar sesión {session_id}: {e}")

    def check_and_expire_session(self, session_id: str, expiration_seconds: int = 24 * 3600) -> bool:
        """Verifica si una sesión está expirada y la cierra automáticamente si es el caso."""
        if self.is_session_expired(session_id, expiration_seconds):
            self.close_session(session_id, "expired")
            logger.info(f"Sesión '{session_id}' expirada y marcada como cerrada en Firestore.")
            return True
        return False
    
    def get_session_info(self, session_id: str) -> dict:
        """Obtiene información detallada de una sesión directamente de Firestore."""
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                
                # Conversión segura de Timestamps a ISO
                created_at_str = data.get('created_at').isoformat() if data.get('created_at') else None
                last_activity_str = data.get('last_activity_at').isoformat() if data.get('last_activity_at') else None
                consent_timestamp_str = data.get('timestamp_consentimiento').isoformat() if data.get('timestamp_consentimiento') else None
                
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
                    "timestamp_consentimiento": consent_timestamp_str,
                    "age_seconds": round(age_seconds, 2),
                    "is_expired": self.is_session_expired(session_id),
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