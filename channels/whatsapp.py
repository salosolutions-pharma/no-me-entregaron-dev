import logging
import os
import sys
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from channels.whatsapp_business_api import WhatsAppBusinessAPIClient, WhatsAppBusinessAPIError
from channels.whatsapp_handlers import WhatsAppMessageHandler

try:
    from BYC.consentimiento import ConsentManager
    from processor_image_prescription.pip_processor import PIPProcessor
    from claim_manager.data_collection import ClaimManager
except ImportError as e:
    print(f"Error al importar modulos: {e}")
    sys.exit(1)

load_dotenv()

logger = logging.getLogger(__name__)

class WhatsAppService:
    """Servicio principal para manejar WhatsApp Business API."""
    
    def __init__(self):
        self.client: Optional[WhatsAppBusinessAPIClient] = None
        self.message_handler: Optional[WhatsAppMessageHandler] = None
        self.consent_manager: Optional[ConsentManager] = None
        self.pip_processor: Optional[PIPProcessor] = None
        self.claim_manager: Optional[ClaimManager] = None
        self.webhook_verify_token = os.getenv("WHATSAPP_WEBHOOK_VERIFY_TOKEN", "")
        
        self._initialize_components()

    def _initialize_components(self) -> None:
        """Inicializa todos los componentes necesarios."""
        try:
            # Inicializar cliente de WhatsApp Business API
            self.client = WhatsAppBusinessAPIClient()
            
            # Inicializar componentes del sistema
            self.consent_manager = ConsentManager()
            self.pip_processor = PIPProcessor()
            self.claim_manager = ClaimManager()
            
            # Inicializar manejador de mensajes
            self.message_handler = WhatsAppMessageHandler(
                self.client,
                self.consent_manager,
                self.pip_processor,
                self.claim_manager
            )
            
            logger.info("WhatsApp Service inicializado correctamente")
            
        except Exception as e:
            logger.critical(f"Error inicializando WhatsApp Service: {e}")
            raise

    async def handle_webhook(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """Maneja webhooks entrantes de WhatsApp."""
        try:
            # Verificar que el webhook contenga mensajes
            if not self._is_valid_webhook(webhook_data):
                return {"status": "ignored", "reason": "no_messages"}

            # Extraer tipo de mensaje
            message_type = self._get_message_type(webhook_data)
            
            logger.info(f"Webhook WhatsApp recibido: tipo {message_type}")

            # Delegar al manejador apropiado
            if message_type == "text":
                await self.message_handler.handle_text_message(webhook_data)
            elif message_type == "interactive":
                await self.message_handler.handle_interactive_message(webhook_data)
            elif message_type == "image":
                await self.message_handler.handle_image_message(webhook_data)
            elif message_type == "document":
                # Tratar documentos como imágenes si son prescripciones
                await self.message_handler.handle_image_message(webhook_data)
            else:
                logger.warning(f"Tipo de mensaje no soportado: {message_type}")
                return {"status": "ignored", "reason": f"unsupported_type_{message_type}"}

            return {"status": "success"}

        except Exception as e:
            logger.error(f"Error procesando webhook WhatsApp: {e}", exc_info=True)
            return {"status": "error", "error": str(e)}

    def verify_webhook(self, mode: str, token: str, challenge: str) -> Optional[str]:
        """Verifica el webhook de WhatsApp durante la configuración."""
        if mode == "subscribe" and token == self.webhook_verify_token:
            logger.info("Webhook de WhatsApp verificado exitosamente")
            return challenge
        else:
            logger.warning("Verificacion de webhook WhatsApp fallida")
            return None

    async def send_message(self, phone_number: str, message: str, buttons: Optional[list] = None) -> bool:
        """Envía un mensaje a través de WhatsApp."""
        try:
            if not self.client:
                logger.error("Cliente de WhatsApp no inicializado")
                return False
            
            logger.info(f"📤 Enviando WhatsApp a {phone_number}: {message}")
            logger.info(f"📤 Botones: {buttons if buttons else 'Sin botones'}")

            if buttons:
                # Convertir botones al formato de WhatsApp
                wa_buttons = []
                for button in buttons[:3]:  # WhatsApp máximo 3 botones
                    wa_buttons.append({
                        "text": button.get("text", "")[:20],  # WhatsApp límite 20 chars
                        "callback_data": button.get("callback_data", "")
                    })
                
                response = self.client.send_interactive_message(phone_number, message, wa_buttons)
            else:
                response = self.client.send_text_message(phone_number, message)

            logger.info(f"📥 Respuesta del proveedor: {response}")  
            logger.info(f"✅ Mensaje enviado satisfactoriamente a {phone_number}")
            return True

        except WhatsAppBusinessAPIError as e:
            logger.error(f"Error enviando mensaje WhatsApp a {phone_number}: {e}")
            return False

    async def send_document(self, phone_number: str, document_url: str, filename: str, caption: str = "") -> bool:
        """Envía un documento a través de WhatsApp."""
        try:
            if not self.client:
                logger.error("Cliente de WhatsApp no inicializado")
                return False

            self.client.send_document_message(phone_number, document_url, filename, caption)
            logger.info(f"Documento enviado exitosamente a {phone_number}")
            return True

        except WhatsAppBusinessAPIError as e:
            logger.error(f"Error enviando documento WhatsApp a {phone_number}: {e}")
            return False

    def get_business_profile(self) -> Optional[Dict[str, Any]]:
        """Obtiene informacion del perfil de negocio."""
        try:
            if not self.client:
                return None
            return self.client.get_business_profile()
        except Exception as e:
            logger.error(f"Error obteniendo perfil de negocio: {e}")
            return None

    def _is_valid_webhook(self, webhook_data: Dict[str, Any]) -> bool:
        """Verifica si el webhook contiene mensajes válidos."""
        try:
            entry = webhook_data.get("entry", [{}])[0]
            changes = entry.get("changes", [{}])[0]
            value = changes.get("value", {})
            messages = value.get("messages", [])
            
            return len(messages) > 0
            
        except (IndexError, KeyError):
            return False

    def _get_message_type(self, webhook_data: Dict[str, Any]) -> str:
        """Extrae el tipo de mensaje del webhook."""
        try:
            entry = webhook_data.get("entry", [{}])[0]
            changes = entry.get("changes", [{}])[0]
            value = changes.get("value", {})
            messages = value.get("messages", [{}])
            
            if not messages:
                return "unknown"
                
            message = messages[0]
            
            # Determinar tipo de mensaje
            if "text" in message:
                return "text"
            elif "interactive" in message:
                return "interactive"
            elif "image" in message:
                return "image"
            elif "document" in message:
                return "document"
            elif "audio" in message:
                return "audio"
            elif "video" in message:
                return "video"
            else:
                return "unknown"
                
        except (IndexError, KeyError):
            return "unknown"

    def health_check(self) -> Dict[str, Any]:
        """Verifica el estado del servicio WhatsApp."""
        status = {
            "whatsapp_client": self.client is not None,
            "consent_manager": self.consent_manager is not None,
            "pip_processor": self.pip_processor is not None,
            "claim_manager": self.claim_manager is not None,
            "message_handler": self.message_handler is not None
        }
        
        all_healthy = all(status.values())
        
        return {
            "healthy": all_healthy,
            "components": status,
            "service": "WhatsApp Business API"
        }


def create_whatsapp_service() -> WhatsAppService:
    """Factory function para crear el servicio de WhatsApp."""
    try:
        service = WhatsAppService()
        logger.info("Servicio de WhatsApp creado exitosamente")
        return service
    except Exception as e:
        logger.critical(f"Error creando servicio WhatsApp: {e}")
        raise


if __name__ == "__main__":
    # Test básico del servicio
    service = create_whatsapp_service()
    health = service.health_check()
    print(f"Estado del servicio: {health}")