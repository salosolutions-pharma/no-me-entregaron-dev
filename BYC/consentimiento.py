import logging
from typing import Optional

from manual_instrucciones.prompt_manager import prompt_manager
from session_manager.session_manager import SessionManager
from session_manager.session_manager import SessionManager, SessionManagerError
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ConsentManager:
    """
    Gestiona el flujo de consentimiento y bienvenida,
    obteniendo los textos de forma dinámica desde la tabla de prompts.
    """

    def __init__(self):
        self.session_manager = SessionManager()
        logger.info("ConsentManager inicializado.")

    def get_welcome_message(self) -> str:
        msg = "👋 ¡Hola! Bienvenido a No Me Entregaron."
        return msg

    def get_consent_request_message(self) -> str:
        msg = "Para procesar tu fórmula médica, necesito tu autorización para el tratamiento de datos personales. ¿Autorizas?"
        return msg

    def get_consent_granted_message(self) -> str:
        msg =  "✅ ¡Perfecto! Gracias por autorizar el tratamiento de tus datos."
        return msg

    def get_consent_denied_message(self) -> str:
        msg = (
                "Entiendo tu decisión. Sin tu autorización no podemos continuar con el proceso. "
                "Si cambias de opinión, solo escríbeme."
            )
        return msg
    
    
    def handle_consent_response(
        self, user_telegram_id: int, user_identifier_for_session: str, consent_status: str
    ) -> bool:
        """
        Procesa la respuesta del usuario respecto al consentimiento de datos.

        Esta función:
        1. Obtiene o crea una sesión activa para el usuario.
        2. Actualiza el estado de consentimiento en el documento de la sesión en Firestore.

        Args:
            user_telegram_id (int): El ID de usuario de Telegram.
            user_identifier_for_session (str): El identificador único del usuario (ej. número de teléfono),
                                                utilizado para gestionar la sesión en Firestore.
            consent_status (str): La decisión del usuario, típicamente 'autorizado' o 'no autorizado'.

        Returns:
            bool: True si la operación de registro de consentimiento fue exitosa, False en caso contrario.
        """
        try:
            session_info = self.session_manager.create_session_with_history_check(
                user_identifier=user_identifier_for_session, channel="TL"
            )
            session_id = session_info["new_session_id"]
            logger.info(f"Sesión activa obtenida/creada para el manejo de consentimiento: '{session_id}'.")
        except SessionManagerError as e:
            logger.error(
                f"Fallo de SessionManager al obtener/crear sesión para el consentimiento de "
                f"'{user_identifier_for_session}': {e}",
                exc_info=True,
            )
            return False
        except Exception as e:
            logger.error(
                f"Error inesperado al gestionar sesión para el consentimiento de "
                f"'{user_identifier_for_session}': {e}",
                exc_info=True,
            )
            return False

        try:
            success = self.session_manager.update_consent_for_session(session_id, consent_status)
            if not success:
                logger.warning(
                    f"No se pudieron actualizar los campos de consentimiento en Firestore para la sesión '{session_id}'."
                )
            return success
        except GoogleAPIError as e:
            logger.error(
                f"Error de la API de Google Cloud al actualizar el consentimiento para la sesión '{session_id}': {e}",
                exc_info=True,
            )
            return False
        except Exception as e:
            logger.error(
                f"Error inesperado al actualizar el consentimiento para la sesión '{session_id}': {e}", exc_info=True
            )
            return False
