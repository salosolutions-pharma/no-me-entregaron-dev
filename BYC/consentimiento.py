import logging
from typing import Any
from google.api_core.exceptions import GoogleAPIError

from session_manager.session_manager import SessionManager, SessionManagerError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ConsentimientoManager:
    """
    Gestiona el proceso de bienvenida y el manejo del consentimiento del usuario.
    Se integra con SessionManager para crear, gestionar y actualizar sesiones,
    así como para registrar las decisiones de consentimiento en Firestore.
    """
    def __init__(self):
        """
        Inicializa el ConsentimientoManager, creando una instancia de SessionManager.
        
        Raises:
            SessionManagerError: Si hay un error al inicializar SessionManager,
                                 lo cual es crítico para el funcionamiento.
        """
        try:
            self.session_manager = SessionManager()
            logger.info("ConsentimientoManager inicializado exitosamente con SessionManager.")
        except SessionManagerError as e:
            logger.critical(f"Error fatal al inicializar SessionManager en ConsentimientoManager: {e}. "
                            "El módulo de consentimiento no puede operar sin una conexión válida a la base de datos de sesiones.")
            raise # Relanzar la excepción ya que es un error crítico de inicialización

    def handle_consent_response(self, user_telegram_id: int, user_identifier_for_session: str, consent_status: str) -> bool:
        """
        Procesa la respuesta del usuario respecto al consentimiento de datos.
        
        Esta función:
        1. Obtiene o crea una sesión activa para el usuario.
        2. Actualiza el estado de consentimiento en el documento de la sesión en Firestore.
        3. Registra un evento de consentimiento en el historial de conversación de la sesión.
        
        Args:
            user_telegram_id (int): El ID de usuario de Telegram.
            user_identifier_for_session (str): El identificador único del usuario (ej. número de teléfono),
                                                utilizado para gestionar la sesión en Firestore.
            consent_status (str): La decisión del usuario, típicamente 'autorizado' o 'no autorizado'.
            
        Returns:
            bool: True si la operación de registro de consentimiento fue exitosa, False en caso contrario.
        """
        session_id: str = "" # Inicializar para asegurar que siempre tenga un valor

        try:
            # Paso 1: Obtener o crear una sesión activa para el usuario.
            # Esta función de SessionManager ya es síncrona y robusta.
            session_info = self.session_manager.create_session_with_history_check(
                user_identifier=user_identifier_for_session,
                channel="TL" # Asumiendo "TL" para Telegram
            )
            session_id = session_info["new_session_id"]
            logger.info(f"Sesión activa obtenida/creada para el manejo de consentimiento: '{session_id}'.")
        except SessionManagerError as e:
            logger.error(f"Fallo de SessionManager al obtener/crear sesión para el consentimiento de '{user_identifier_for_session}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error inesperado al gestionar sesión para el consentimiento de '{user_identifier_for_session}': {e}", exc_info=True)
            return False

        # Paso 2 y 3: Actualizar el estado de consentimiento y registrar el evento en el historial.
        # La función `update_consent_for_session` del SessionManager ya se encarga de AMBAS cosas,
        # lo que simplifica enormemente este método.
        try:
            success = self.session_manager.update_consent_for_session(session_id, consent_status)
            if not success:
                logger.warning(f"No se pudieron actualizar los campos de consentimiento en Firestore para la sesión '{session_id}'.")
            return success
        except GoogleAPIError as e:
            logger.error(f"Error de la API de Google Cloud al actualizar el consentimiento para la sesión '{session_id}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error inesperado al actualizar el consentimiento para la sesión '{session_id}': {e}", exc_info=True)
            return False