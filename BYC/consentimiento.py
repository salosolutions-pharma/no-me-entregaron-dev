import logging
import json
from google.cloud.exceptions import GoogleCloudError

from session_manager.session_manager import SessionManager, SessionManagerError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ConsentimientoManager:
    """
    Gestiona el proceso de bienvenida y consentimiento del usuario.
    Se integra con SessionManager para manejar sesiones y registrar el consentimiento.
    """
    def __init__(self):
        try:
            self.session_manager = SessionManager()
            logger.info("ConsentimientoManager inicializado con SessionManager.")
        except SessionManagerError as e:
            logger.error(f"Error al inicializar SessionManager en ConsentimientoManager: {e}")
            raise

    def handle_consent_response(self, user_telegram_id: int, user_identifier_for_session: str, consent_status: str) -> bool:
        """
        Gestiona la respuesta del usuario al consentimiento y la registra en BigQuery.
        Inserta una NUEVA FILA separada con el evento de consentimiento y los campos dedicados llenos.

        Args:
            user_telegram_id (int): ID del usuario de Telegram.
            user_identifier_for_session (str): Identificador único del usuario (ej: user_id de Telegram)
                                               que se usará para el session_id y en los logs.
            consent_status (str): 'autorizado' o 'no autorizado'.

        Returns:
            bool: True si la operación fue exitosa, False en caso contrario.
        """
        # Crear o recuperar sesión usando la lógica de SessionManager
        try:
            # create_session_with_history_check ya inserta la fila inicial.
            session_info = self.session_manager.create_session_with_history_check(
                user_identifier=user_identifier_for_session,
                channel="TL" # 'TL' para Telegram
            )
            session_id = session_info["new_session_id"]
            logger.info(f"Sesión obtenida/creada para el consentimiento: {session_id}")
        except SessionManagerError as e:
            logger.error(f"Error de SessionManager al crear o recuperar sesión para consentimiento: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado al crear o recuperar sesión para consentimiento: {e}")
            return False

        # Registrar el evento de consentimiento en una NUEVA FILA con los campos específicos llenos
        try:
            success = self.session_manager.record_consent_event(session_id, consent_status, user_telegram_id)
            if success:
                logger.info(f"Consentimiento '{consent_status}' registrado en BigQuery en una nueva fila para sesión {session_id}.")
            else:
                logger.error(f"Fallo al registrar evento de consentimiento para sesión {session_id}.")
            return success
        except GoogleCloudError as e:
            logger.error(f"Error de BigQuery al registrar evento de consentimiento para sesión {session_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado al registrar evento de consentimiento para sesión {session_id}: {e}")
            return False