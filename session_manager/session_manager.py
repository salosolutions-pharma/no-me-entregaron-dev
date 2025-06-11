import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import pytz

# Importar firestore después de cargar dotenv si PROJECT_ID depende de ello
from dotenv import load_dotenv
load_dotenv()

from google.cloud import firestore
from google.api_core.exceptions import GoogleAPIError, NotFound


# --- Excepciones personalizadas ---
class SessionManagerError(Exception):
    """Excepción base para errores en el SessionManager."""


# --- Configuración básica de logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# --- Variables de entorno para Firestore ---
PROJECT_ID: str = os.getenv("PROJECT_ID", "")
FIRESTORE_COLLECTION_SESSIONS_ACTIVE: str = "sesiones_activas"
# Nombre de la base de datos de Firestore, si no es '(default)'
FIRESTORE_DATABASE_NAME: str = "historia"


class SessionManager:
    """
    Gestiona las sesiones de conversación como documentos en la colección
    'sesiones_activas' de Firestore, priorizando rendimiento y robustez.
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
        """
        Crea y retorna una instancia del cliente de Firestore.
        Reutiliza el cliente si ya ha sido creado.
        """
        # Se puede usar una variable global para el cliente si se garantiza que es thread-safe
        # y que las credenciales no cambian en el ciclo de vida de la aplicación.
        # Para Cloud Functions, una instancia global es común y eficiente.
        try:
            return firestore.Client(project=PROJECT_ID, database=FIRESTORE_DATABASE_NAME)
        except Exception as e:
            logger.exception(f"Error al inicializar el cliente de Firestore para el proyecto '{PROJECT_ID}' y base de datos '{FIRESTORE_DATABASE_NAME}': {e}")
            raise SessionManagerError(f"Fallo al crear cliente de Firestore: {e}") from e

    def generate_session_id(self, user_identifier: str, channel: str) -> str:
        """
        Genera un ID de sesión único y legible.
        Formato: CANAL_IDENTIFICADOR_YYYYMMDD_HHmmss.
        Normaliza el identificador de usuario si es un número de teléfono.
        """
        # Limpiar y normalizar el identificador de usuario.
        clean_identifier = "".join(filter(str.isdigit, user_identifier))
        
        # Lógica de normalización de número de teléfono.
        if len(clean_identifier) == 10 and clean_identifier.startswith("3"):
            normalized_identifier = f"57{clean_identifier}"
        elif len(clean_identifier) == 12 and clean_identifier.startswith("57"):
            normalized_identifier = clean_identifier
        else:
            # Si no es un formato de teléfono esperado, se trunca para evitar IDs muy largos.
            # Se podría considerar un hash si la longitud es un problema y la legibilidad no lo es.
            normalized_identifier = clean_identifier[:15] # Truncar a un tamaño razonable.
        
        now = datetime.now(self.colombia_tz)
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        session_id = f"{channel}_{normalized_identifier}_{timestamp_str}"
        return session_id

    def extract_timestamp_from_session_id(self, session_id: str) -> datetime:
        """
        Extrae el timestamp de un ID de sesión.
        Retorna el tiempo actual de Colombia si el ID no tiene un formato válido.
        """
        try:
            parts = session_id.split("_")
            # El timestamp siempre es el último componente después del último '_'
            # y el penúltimo componente antes del último '_'.
            if len(parts) >= 3:
                timestamp_part = f"{parts[-2]}_{parts[-1]}"
                return datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
            raise ValueError("Formato de session_id incompleto o inesperado.")
        except (ValueError, IndexError) as exc:
            logger.error(f"Error extrayendo timestamp de session_id '{session_id}': {exc}. Retornando tiempo actual como fallback.")
            return datetime.now(self.colombia_tz)  # Fallback a tiempo actual en caso de error.

    def extract_user_identifier_from_session_id(self, session_id: str) -> str:
        """
        Extrae el identificador de usuario (típicamente número de teléfono) de un ID de sesión.
        Retorna 'unknown_identifier' si no se puede extraer.
        """
        try:
            parts = session_id.split('_')
            # El identificador de usuario es el segundo componente en el formato estándar.
            if len(parts) >= 2:
                return parts[1]
            return "unknown_identifier"
        except IndexError:
            return "unknown_identifier"
            
    def create_session(self, user_identifier: str, channel: str = "WA") -> str:
        """
        Crea un nuevo documento de sesión en Firestore de forma rápida y con una
        estructura inicial optimizada.
        """
        session_id = self.generate_session_id(user_identifier, channel)
        current_time_iso = datetime.now(self.colombia_tz).isoformat()

        session_data = {
            "id_sesion": session_id,
            "user_identifier": user_identifier,
            "channel": channel,
            "created_at": firestore.SERVER_TIMESTAMP, # Timestamp de Firestore.
            "last_activity_at": firestore.SERVER_TIMESTAMP, # Timestamp de Firestore.
            "conversation": [{ # Inicializa con el evento de inicio de sesión.
                "timestamp": current_time_iso,
                "sender": "system",
                "message": "Sesión iniciada.",
                "event_type": "session_started",
                "user_id": user_identifier,
            }],
            "consentimiento": None,
            "timestamp_consentimiento": None, # Timestamp de Firestore.
            "estado_sesion": "activa",
        }
        
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.set(session_data)
            logger.info(f"Sesión '{session_id}' creada rápidamente en Firestore.")
            return session_id
        except GoogleAPIError as e:
            logger.error(f"Error de Firestore al crear sesión '{session_id}': {e}")
            raise SessionManagerError(f"Error al crear sesión en Firestore: {e}") from e
        except Exception as e:
            logger.error(f"Error inesperado al crear sesión '{session_id}': {e}")
            raise SessionManagerError(f"Error inesperado al crear sesión en Firestore: {e}") from e

    def add_message_to_session(
        self, session_id: str, message_content: str, sender: str = "user", message_type: str = "conversation"
    ) -> None:
        """
        Agrega un mensaje o evento al array 'conversation' del documento de sesión,
        actualizando también 'last_activity_at'. Los errores se loguean pero no bloquean.
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
            logger.debug(f"Mensaje agregado a sesión '{session_id}'. Remitente: '{sender}'. Tipo: '{message_type}'. Mensaje: '{message_content[:50]}...'")
        except NotFound:
            logger.warning(f"No se pudo agregar mensaje: Sesión '{session_id}' no encontrada en Firestore.")
        except GoogleAPIError as e:
            logger.warning(f"Error de Firestore al agregar mensaje a sesión '{session_id}': {e}")
        except Exception as e:
            logger.warning(f"Error inesperado al agregar mensaje a sesión '{session_id}': {e}")

    def update_consent_for_session(self, session_id: str, consent_status: str) -> bool:
        """
        Actualiza el estado de consentimiento de la sesión y registra la acción en el historial.
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
            logger.info(f"Consentimiento '{consent_status}' actualizado para sesión '{session_id}'.")
            
            # Añadir evento de consentimiento al historial de forma no bloqueante.
            consent_event_data = {
                "timestamp": current_time_iso,
                "sender": "system",
                "message": f"Consentimiento de datos: {consent_status}",
                "event_type": "consent_response",
                "consent_status": consent_status,
                "user_id": self.extract_user_identifier_from_session_id(session_id),
            }
            
            # Envuelve la llamada en un try-except para que no afecte el flujo principal.
            try:
                self.add_message_to_session(session_id, json.dumps(consent_event_data), "system", "consent_response")
            except Exception as e:
                logger.warning(f"Fallo al agregar evento de consentimiento al historial para sesión '{session_id}': {e}")

            return True
        except NotFound:
            logger.warning(f"No se pudo actualizar consentimiento: Sesión '{session_id}' no encontrada en Firestore.")
            return False
        except GoogleAPIError as e:
            logger.error(f"Error de Firestore al actualizar consentimiento para sesión '{session_id}': {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado al actualizar consentimiento para sesión '{session_id}': {e}")
            return False

    def get_conversation_history(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Obtiene el historial completo de conversación de una sesión.
        Retorna una lista vacía si la sesión no existe o si ocurre un error.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get(["conversation"]) # Solo obtiene el campo 'conversation' para eficiencia.
            if doc.exists:
                session_data = doc.to_dict()
                conversation_list = session_data.get('conversation', [])
                # Ordena la lista por timestamp para asegurar la cronología.
                conversation_list.sort(key=lambda x: x.get("timestamp", ""))
                logger.debug(f"Historial recuperado para sesión '{session_id}'. Total: {len(conversation_list)} mensajes.")
                return conversation_list
            logger.debug(f"Sesión '{session_id}' no encontrada en Firestore para obtener historial.")
            return []
        except Exception as e:
            logger.warning(f"Error al obtener historial de sesión '{session_id}': {e}")
            return []

    def create_session_with_history_check(self, user_identifier: str, channel: str = "WA") -> Dict[str, Any]:
        """
        Crea una nueva sesión inmediatamente. Las verificaciones de historial previo
        se marcan como "asíncronas" para mantener la función rápida.
        """
        # Normalización del identificador similar a generate_session_id para consistencia.
        clean_identifier = "".join(filter(str.isdigit, user_identifier))
        if len(clean_identifier) == 10 and clean_identifier.startswith("3"):
            normalized_identifier = f"57{clean_identifier}"
        elif len(clean_identifier) == 12 and clean_identifier.startswith("57"):
            normalized_identifier = clean_identifier
        else:
            normalized_identifier = clean_identifier[:15] # Asegura un tamaño razonable.
        
        try:
            logger.info(f"🚀 Creando sesión inmediata para '{normalized_identifier}'.")
            new_session_id = self.create_session(normalized_identifier, channel)
            
            logger.info(f"✅ Sesión creada exitosamente: '{new_session_id}'.")
            
            return {
                "new_session_id": new_session_id,
                "user_identifier": normalized_identifier,
                "channel": channel,
                "has_previous_history": False,  # Marcado para indicar que una consulta asíncrona es necesaria.
                "previous_sessions_count": 0,
                "previous_sessions": [],
            }

        except Exception as e:
            logger.error(f"Error creando sesión rápida para '{user_identifier}': {e}")
            
            # Fallback rápido: Sesión temporal si la creación principal falla.
            fallback_session_id = f"TEMP_{channel}_{normalized_identifier}_{int(datetime.now().timestamp())}"
            logger.info(f"🔄 Usando ID de sesión fallback: '{fallback_session_id}'.")
            
            return {
                "new_session_id": fallback_session_id,
                "user_identifier": normalized_identifier,
                "channel": channel,
                "has_previous_history": False,
                "previous_sessions_count": 0,
                "previous_sessions": [],
            }

    def get_previous_sessions_by_phone(self, user_identifier: str) -> List[Dict[str, Any]]:
        """
        Consulta y retorna una lista limitada de sesiones previas cerradas para un identificador de usuario dado.
        Optimizado para ser una consulta rápida (limitada a 3 resultados).
        """
        try:
            # Normalización del identificador para la consulta.
            clean_identifier = "".join(filter(str.isdigit, user_identifier))
            if len(clean_identifier) == 10 and clean_identifier.startswith("3"):
                normalized_identifier = f"57{clean_identifier}"
            elif len(clean_identifier) == 12 and clean_identifier.startswith("57"):
                normalized_identifier = clean_identifier
            else:
                normalized_identifier = clean_identifier
            
            # Consulta a Firestore con filtros y límite para eficiencia.
            query = (
                self.sessions_collection_ref
                .where(filter=firestore.FieldFilter('user_identifier', '==', normalized_identifier))
                .where(filter=firestore.FieldFilter('estado_sesion', '==', 'cerrado'))
                .order_by('last_activity_at', direction=firestore.Query.DESCENDING)
                .limit(3) # Limita a los 3 más recientes para rapidez.
            )
            
            logger.debug(f"Consultando sesiones previas para '{normalized_identifier}'.")
            results = query.stream()
            
            previous_sessions = []
            for doc in results:
                data = doc.to_dict()
                # Conversión segura de timestamps a ISO format para la respuesta.
                created_at_iso = data.get('created_at').isoformat() if isinstance(data.get('created_at'), datetime) else None
                
                previous_sessions.append({
                    "session_id": doc.id,
                    "user_identifier": data.get('user_identifier'),
                    "channel": data.get('channel'),
                    "created_at": created_at_iso,
                    "estado_sesion": data.get('estado_sesion')
                })
                
                # Se detiene si ya se encontraron 2 sesiones para una verificación rápida.
                if len(previous_sessions) >= 2: # Puede ser 2 para tener una idea rápida de si hay "historial"
                    break
            
            logger.debug(f"Encontradas {len(previous_sessions)} sesiones previas para '{normalized_identifier}'.")
            return previous_sessions
            
        except Exception as e:
            logger.warning(f"Error consultando sesiones previas para '{user_identifier}': {e}")
            return []

    def is_session_expired(self, session_id: str, expiration_seconds: int = 24 * 3600) -> bool:
        """
        Verifica rápidamente si una sesión ha expirado o está cerrada.
        Retorna True en caso de error para asumir seguridad (expirada).
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            # Solo obtiene los campos necesarios para la verificación de expiración.
            doc = doc_ref.get(['last_activity_at', 'estado_sesion'])

            if not doc.exists:
                logger.debug(f"Sesión '{session_id}' no encontrada. Considerada expirada.")
                return True # Si no existe, se asume que expiró o fue eliminada.
            
            session_data = doc.to_dict()
            estado_sesion = session_data.get('estado_sesion')
            
            # Si el estado es "cerrado", la sesión ya no está activa.
            if estado_sesion == "cerrado":
                logger.debug(f"Sesión '{session_id}' ya está en estado 'cerrado'.")
                return True

            last_activity_at = session_data.get('last_activity_at')
            # Si no hay timestamp de última actividad, se considera expirada (estado inconsistente).
            if not last_activity_at or not isinstance(last_activity_at, datetime):
                logger.warning(f"Sesión '{session_id}' sin 'last_activity_at' válido. Considerada expirada.")
                return True

            # Calcula la edad de la sesión en segundos.
            current_time = datetime.now(self.colombia_tz)
            # Asegura que el timestamp de Firestore esté en la misma zona horaria para la comparación.
            last_activity_time = last_activity_at.astimezone(self.colombia_tz)
            age_seconds = (current_time - last_activity_time).total_seconds()
            
            is_expired = age_seconds > expiration_seconds
            
            if is_expired:
                logger.debug(f"Sesión '{session_id}' expirada. Edad: {age_seconds:.0f}s (Límite: {expiration_seconds}s).")
            
            return is_expired
            
        except Exception as e:
            logger.warning(f"Error verificando expiración de sesión '{session_id}': {e}. Asumiendo expirada.")
            return True  # Asumir expirada en caso de cualquier error para evitar sesiones huérfanas.

    def close_session(self, session_id: str, reason: str = "completed") -> None:
        """
        Cierra una sesión en Firestore actualizando su estado a 'cerrado' y el motivo.
        Los errores se loguean pero no bloquean el flujo principal.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc_ref.update({
                "estado_sesion": "cerrado",
                "closed_at": firestore.SERVER_TIMESTAMP, # Timestamp de cierre de Firestore.
                "close_reason": reason,
                "last_activity_at": firestore.SERVER_TIMESTAMP, # Actualiza la última actividad al cerrar.
            })
            logger.info(f"Sesión '{session_id}' cerrada por motivo: '{reason}'.")
        except NotFound:
            logger.warning(f"Intento de cerrar sesión '{session_id}' falló: Documento no encontrado.")
        except Exception as e:
            logger.warning(f"Error inesperado al cerrar sesión '{session_id}': {e}")

    def check_and_expire_session(self, session_id: str, expiration_seconds: int = 24 * 3600) -> bool:
        """
        Verifica si una sesión ha expirado y la cierra si es el caso.
        Retorna True si la sesión fue encontrada y marcada como expirada/cerrada, False en caso contrario.
        """
        try:
            if self.is_session_expired(session_id, expiration_seconds):
                self.close_session(session_id, "expired")
                logger.debug(f"Sesión '{session_id}' detectada como expirada y marcada como 'cerrado'.")
                return True
            return False
        except Exception as e:
            logger.warning(f"Error en 'check_and_expire_session' para '{session_id}': {e}. Retornando False.")
            return False
        
    def get_session_info(self, session_id: str) -> Dict[str, Any]:
        """
        Obtiene la información detallada de una sesión.
        Retorna un diccionario con los datos de la sesión o un diccionario de error
        si la sesión no se encuentra o hay un problema.
        """
        try:
            doc_ref = self.sessions_collection_ref.document(session_id)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                
                # Conversión segura de objetos Timestamp de Firestore a cadenas ISO 8601.
                created_at_str = data['created_at'].isoformat() if isinstance(data.get('created_at'), datetime) else None
                last_activity_str = data['last_activity_at'].isoformat() if isinstance(data.get('last_activity_at'), datetime) else None
                consent_timestamp_str = data['timestamp_consentimiento'].isoformat() if isinstance(data.get('timestamp_consentimiento'), datetime) else None
                
                return {
                    "session_id": session_id,
                    "user_identifier": data.get('user_identifier'),
                    "channel": data.get('channel'),
                    "created_at": created_at_str,
                    "last_activity_at": last_activity_str,
                    "estado_sesion": data.get('estado_sesion'),
                    "consentimiento": data.get('consentimiento'),
                    "timestamp_consentimiento": consent_timestamp_str,
                    # Calcula la expiración al momento de la solicitud de info, no almacena.
                    "is_expired": self.is_session_expired(session_id),
                }
            
            logger.debug(f"Sesión '{session_id}' no encontrada para 'get_session_info'.")
            return {"error": "Sesión no encontrada", "session_id": session_id}
            
        except Exception as e:
            logger.warning(f"Error obteniendo información de sesión '{session_id}': {e}")
            return {"error": str(e), "session_id": session_id}