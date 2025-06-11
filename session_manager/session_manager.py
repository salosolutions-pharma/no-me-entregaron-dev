import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict
import pytz

from dotenv import load_dotenv
load_dotenv()

from google.cloud import firestore
from google.api_core.exceptions import GoogleAPIError, NotFound

class SessionManagerError(Exception):
    """Excepción base para errores en SessionManager."""

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

PROJECT_ID: str = os.getenv("PROJECT_ID", "")
FIRESTORE_COLLECTION_SESSIONS_ACTIVE: str = "sesiones_activas"
FIRESTORE_DATABASE_NAME: str = "historia"

class SessionManager:
    """
    Gestiona las sesiones de conversación como documentos en la colección 'sesiones_activas' de Firestore.
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
        logger.info(f"SessionManager inicializado. Conectado a la colección de Firestore: '{FIRESTORE_COLLECTION_SESSIONS_ACTIVE}'.")

    def _get_firestore_client(self) -> firestore.Client:
        """Crea y devuelve una instancia del cliente de Firestore."""
        try:
            return firestore.Client(project=PROJECT_ID, database=FIRESTORE_DATABASE_NAME)
        except Exception as e:
            logger.exception(f"Error al inicializar el cliente de Firestore para el proyecto '{PROJECT_ID}' y la base de datos '{FIRESTORE_DATABASE_NAME}': {e}")
            raise SessionManagerError(f"Fallo al crear el cliente de Firestore: {e}") from e

    def _normalize_user_identifier(self, user_identifier: str) -> str:
        """Normaliza el identificador de usuario, típicamente un número de teléfono."""
        clean_identifier = "".join(filter(str.isdigit, user_identifier))
        if len(clean_identifier) == 10 and clean_identifier.startswith("3"):
            return f"57{clean_identifier}"
        elif len(clean_identifier) == 12 and clean_identifier.startswith("57"):
            return clean_identifier
        return clean_identifier[:15]

    def generate_session_id(self, user_identifier: str, channel: str) -> str:
        """
        Genera un ID de sesión único y legible.
        Formato: CANAL_IDENTIFICADOR_AAAAMMDD_HHmmss.
        """
        normalized_identifier = self._normalize_user_identifier(user_identifier)
        now = datetime.now(self.colombia_tz)
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        session_id = f"{channel}_{normalized_identifier}_{timestamp_str}"
        return session_id

    def create_session(self, user_identifier: str, channel: str = "TL") -> str:
        """
        Crea un nuevo documento de sesión en Firestore.
        """
        session_id = self.generate_session_id(user_identifier, channel)
        current_time_iso = datetime.now(self.colombia_tz).isoformat()

        session_data = {
            "id_sesion": session_id,
            "user_identifier": user_identifier,
            "channel": channel,
            "created_at": firestore.SERVER_TIMESTAMP,
            "last_activity_at": firestore.SERVER_TIMESTAMP,
            "conversation": [{
                "timestamp": current_time_iso,
                "sender": "system",
                "message": "Sesión iniciada.",
                "event_type": "session_started",
                "user_id": user_identifier,
            }],
            "consentimiento": None,
            "timestamp_consentimiento": None,
            "estado_sesion": "activa",
        }

        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.set(session_data)
            logger.info(f"Sesión '{session_id}' creada en Firestore.")
            return session_id
        except GoogleAPIError as e:
            logger.error(f"Error de Firestore al crear la sesión '{session_id}': {e}", exc_info=True)
            raise SessionManagerError(f"Error al crear la sesión en Firestore: {e}") from e
        except Exception as e:
            logger.error(f"Error inesperado al crear la sesión '{session_id}': {e}", exc_info=True)
            raise SessionManagerError(f"Error inesperado al crear la sesión en Firestore: {e}") from e

    def add_message_to_session(
        self, session_id: str, message_content: str, sender: str = "user", message_type: str = "conversation"
    ) -> None:
        """
        Añade un mensaje al array 'conversation' del documento de sesión.
        """
        current_time_iso = datetime.now(self.colombia_tz).isoformat()
        user_identifier = self.extract_user_identifier_from_session_id(session_id)

        new_message_entry = {
            "timestamp": current_time_iso,
            "sender": sender,
            "message": message_content,
            "event_type": message_type,
            "user_id": user_identifier,
        }

        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                'conversation': firestore.ArrayUnion([new_message_entry]),
                'last_activity_at': firestore.SERVER_TIMESTAMP,
            })
            logger.debug(f"Mensaje añadido a la sesión '{session_id[:20]}...': [{sender}] {message_content[:50]}...")
        except NotFound:
            logger.warning(f"Sesión '{session_id}' no encontrada para añadir mensaje.")
        except GoogleAPIError as e:
            logger.warning(f"Error de Firestore al añadir mensaje a la sesión '{session_id}': {e}")
        except Exception as e:
            logger.warning(f"Error inesperado al añadir mensaje a la sesión '{session_id}': {e}")

    def update_consent_for_session(self, session_id: str, consent_status: str) -> bool:
        """
        Actualiza el estado de consentimiento de la sesión.
        """
        current_time_iso = datetime.now(self.colombia_tz).isoformat()
        consent_bool_value = consent_status == "autorizado"

        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                "consentimiento": consent_bool_value,
                "timestamp_consentimiento": firestore.SERVER_TIMESTAMP,
                "last_activity_at": firestore.SERVER_TIMESTAMP,
            })
            logger.info(f"Consentimiento '{consent_status}' actualizado para la sesión '{session_id[:20]}...'.")

            consent_event_data = {
                "timestamp": current_time_iso,
                "sender": "system",
                "message": f"Consentimiento de datos: {consent_status}",
                "event_type": "consent_response",
                "consent_status": consent_status,
                "user_id": self.extract_user_identifier_from_session_id(session_id),
            }

            try:
                self.add_message_to_session(session_id, json.dumps(consent_event_data), "system", "consent_response")
            except Exception as e:
                logger.warning(f"Fallo al añadir evento de consentimiento: {e}")
            return True
        except NotFound:
            logger.warning(f"Sesión '{session_id}' no encontrada para actualizar consentimiento.")
            return False
        except GoogleAPIError as e:
            logger.error(f"Error de Firestore al actualizar el consentimiento para la sesión '{session_id}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error inesperado al actualizar el consentimiento para la sesión '{session_id}': {e}", exc_info=True)
            return False

    def close_session(self, session_id: str, reason: str = "completed") -> None:
        """
        Cierra una sesión actualizando su estado a 'cerrado'.
        Esto debería activar la Cloud Function para migrar a BigQuery.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                "estado_sesion": "cerrado",
                "closed_at": firestore.SERVER_TIMESTAMP,
                "close_reason": reason,
                "last_activity_at": firestore.SERVER_TIMESTAMP,
            })
            logger.info(f"Sesión '{session_id[:20]}...' cerrada debido a: '{reason}'. Debería migrar a BigQuery.")
            self.add_message_to_session(session_id, f"Sesión cerrada: {reason}", "system", "session_closed")

        except NotFound:
            logger.warning(f"Intento de cerrar la sesión '{session_id}' falló: Documento no encontrado.")
        except Exception as e:
            logger.warning(f"Error inesperado al cerrar la sesión '{session_id}': {e}")

    def auto_close_inactive_sessions(self, inactivity_seconds: int = 300) -> int:
        """
        Cierra automáticamente las sesiones inactivas después del tiempo especificado.

        Args:
            inactivity_seconds: Segundos de inactividad antes de cerrar (predeterminado: 5 minutos).

        Returns:
            int: Número de sesiones cerradas.
        """
        try:
            cutoff_time = datetime.now(self.colombia_tz) - timedelta(seconds=inactivity_seconds)

            query = (
                self.sessions_collection_ref
                .where(filter=firestore.FieldFilter('estado_sesion', '==', 'activa'))
                .where(filter=firestore.FieldFilter('last_activity_at', '<', cutoff_time))
                .limit(10)
            )

            sessions_to_close = query.stream()
            closed_count = 0

            for doc in sessions_to_close:
                session_id = doc.id
                try:
                    self.close_session(session_id, "auto_inactivity")
                    closed_count += 1
                    logger.info(f"Sesión inactiva cerrada automáticamente: {session_id[:20]}...")
                except Exception as e:
                    logger.error(f"Error al cerrar automáticamente la sesión {session_id}: {e}")

            if closed_count > 0:
                logger.info(f"{closed_count} sesiones cerradas por inactividad.")
            return closed_count

        except Exception as e:
            logger.error(f"Error en auto_close_inactive_sessions: {e}")
            return 0

    def create_session_with_history_check(self, user_identifier: str, channel: str = "TL") -> Dict[str, Any]:
        """
        Crea una nueva sesión inmediatamente.
        """
        normalized_identifier = self._normalize_user_identifier(user_identifier)

        try:
            logger.info(f"Creando sesión para '{normalized_identifier}'.")
            new_session_id = self.create_session(normalized_identifier, channel)

            return {
                "new_session_id": new_session_id,
                "user_identifier": normalized_identifier,
                "channel": channel,
                "has_previous_history": False,
                "previous_sessions_count": 0,
                "previous_sessions": [],
            }

        except Exception as e:
            logger.error(f"Error al crear la sesión para '{user_identifier}': {e}", exc_info=True)
            fallback_session_id = f"TEMP_{channel}_{normalized_identifier}_{int(datetime.now().timestamp())}"
            logger.info(f"Usando ID de sesión de respaldo: '{fallback_session_id}'.")
            return {
                "new_session_id": fallback_session_id,
                "user_identifier": normalized_identifier,
                "channel": channel,
                "has_previous_history": False,
                "previous_sessions_count": 0,
                "previous_sessions": [],
            }

    def get_session_info(self, session_id: str) -> Dict[str, Any]:
        """
        Recupera información detallada sobre una sesión.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()

                created_at_str = data['created_at'].isoformat() if isinstance(data.get('created_at'), datetime) else None
                last_activity_str = data['last_activity_at'].isoformat() if isinstance(data.get('last_activity_at'), datetime) else None

                return {
                    "session_id": session_id,
                    "user_identifier": data.get('user_identifier'),
                    "channel": data.get('channel'),
                    "created_at": created_at_str,
                    "last_activity_at": last_activity_str,
                    "estado_sesion": data.get('estado_sesion'),
                    "consentimiento": data.get('consentimiento'),
                    "conversation_count": len(data.get('conversation', [])),
                }
            return {"error": "Sesión no encontrada", "session_id": session_id}

        except Exception as e:
            logger.warning(f"Error al obtener la información de la sesión para '{session_id}': {e}")
            return {"error": str(e), "session_id": session_id}

    def extract_user_identifier_from_session_id(self, session_id: str) -> str:
        """Extrae el identificador de usuario del ID de la sesión."""
        try:
            parts = session_id.split('_')
            if len(parts) >= 2:
                return parts[1]
            return "unknown_identifier"
        except IndexError:
            return "unknown_identifier"

    def check_and_expire_session(self, session_id: str, expiration_seconds: int = 24 * 3600) -> bool:
        """
        Verifica si una sesión ha expirado y la cierra si es necesario.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get(['last_activity_at', 'estado_sesion'])

            if not doc.exists:
                logger.debug(f"Sesión '{session_id}' no encontrada. Considerada expirada.")
                return True

            session_data = doc.to_dict()
            estado_sesion = session_data.get('estado_sesion')

            if estado_sesion == "cerrado":
                return True

            last_activity_at = session_data.get('last_activity_at')
            if not last_activity_at or not isinstance(last_activity_at, datetime):
                self.close_session(session_id, "invalid_timestamp")
                return True

            current_time = datetime.now(self.colombia_tz)
            last_activity_time = last_activity_at.astimezone(self.colombia_tz)
            age_seconds = (current_time - last_activity_time).total_seconds()

            if age_seconds > expiration_seconds:
                self.close_session(session_id, "expired")
                return True
            return False

        except Exception as e:
            logger.warning(f"Error al verificar la expiración de la sesión para '{session_id}': {e}")
            return True