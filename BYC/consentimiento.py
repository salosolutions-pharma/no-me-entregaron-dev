# BYC/consentimiento.py

import logging
import json
from datetime import datetime # Asegúrate de que datetime esté importado aquí
from google.api_core.exceptions import GoogleAPIError

from session_manager.session_manager import SessionManager, SessionManagerError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ConsentimientoManager:
    """
    Gestiona el proceso de bienvenida y consentimiento del usuario.
    Se integra con SessionManager para manejar sesiones y registrar el consentimiento en Firestore.
    """
    def __init__(self):
        try:
            self.session_manager = SessionManager()
            logger.info("ConsentimientoManager inicializado con SessionManager (Firestore).")
        except SessionManagerError as e:
            logger.error(f"Error al inicializar SessionManager en ConsentimientoManager: {e}")
            raise

    # La función handle_consent_response ahora es síncrona, ya que sus llamadas al SessionManager son síncronas.
    # No necesita ser 'async def' si no hace operaciones 'await' directas (fuera de otras llamadas síncronas).
    def handle_consent_response(self, user_telegram_id: int, user_identifier_for_session: str, consent_status: str) -> bool:
        """
        Gestiona la respuesta del usuario al consentimiento y la registra en Firestore.
        Actualiza los campos de consentimiento del documento de sesión activo y añade un evento al historial.

        Args:
            user_telegram_id (int): ID del usuario de Telegram.
            user_identifier_for_session (str): Identificador único del usuario (número de celular)
                                               que se usará para el session_id.
            consent_status (str): 'autorizado' o 'no autorizado'.

        Returns:
            bool: True si la operación fue exitosa, False en caso contrario.
        """
        try:
            # Eliminar 'await' aquí - la función de SessionManager es síncrona
            session_info = self.session_manager.create_session_with_history_check(
                user_identifier=user_identifier_for_session,
                channel="TL"
            )
            session_id = session_info["new_session_id"]
            logger.info(f"Sesión activa obtenida/creada para el consentimiento: {session_id}.")
        except SessionManagerError as e:
            logger.error(f"Error de SessionManager al gestionar sesión para consentimiento: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado al gestionar sesión para consentimiento: {e}")
            return False

        # 1. Actualizar los campos 'consentimiento' y 'timestamp_consentimiento' del documento de sesión en Firestore.
        try:
            # Eliminar 'await' aquí - la función de SessionManager es síncrona
            success_update_fields = self.session_manager.update_consent_for_session(session_id, consent_status)
            if not success_update_fields:
                logger.warning(f"No se pudieron actualizar los campos de consentimiento en Firestore para sesión {session_id}.")
        except GoogleAPIError as e:
            logger.error(f"Error de Firestore al actualizar campos de consentimiento para sesión {session_id}: {e}")
        except Exception as e:
            logger.error(f"Error inesperado al actualizar campos de consentimiento para sesión {session_id}: {e}")
        return success_update_fields
        # 2. Registrar el evento de la respuesta de consentimiento como un elemento en el array 'conversation'.
        # consent_event_data = {
        #     "timestamp": datetime.now(self.session_manager.colombia_tz).isoformat(),
        #     "sender": "user",
        #     "message": f"Respuesta de consentimiento: {consent_status}",
        #     "event_type": "consent_response",
        #     "consent_status": consent_status,
        #     "user_id": str(user_telegram_id)
        # }
        
        # try:
        #     self.session_manager.add_message_to_session(session_id, json.dumps(consent_event_data), "user", "consent_response")
        #     logger.info(f"Evento de consentimiento '{consent_status}' añadido al historial de sesión {session_id}.")
        #     return True
        # except GoogleAPIError as e:
        #     logger.error(f"Error de Firestore al añadir evento de consentimiento al historial para sesión {session_id}: {e}")
        #     return False
        # except Exception as e:
        #     logger.error(f"Error inesperado al añadir evento de consentimiento al historial para sesión {session_id}: {e}")
        #     return False