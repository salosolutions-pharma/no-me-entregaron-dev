import logging
import os
import requests
import time
from typing import Any, Dict, List, Optional, Union
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class WhatsAppBusinessAPIError(Exception):
    """Excepción personalizada para errores de WhatsApp Business API."""
    pass

class WhatsAppBusinessAPIClient:
    """Cliente para interactuar con WhatsApp Business API."""
    
    def __init__(self):
        self.access_token = os.getenv("WHATSAPP_ACCESS_TOKEN")
        self.phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        self.business_account_id = os.getenv("WHATSAPP_BUSINESS_ACCOUNT_ID")
        self.api_version = os.getenv("WHATSAPP_API_VERSION", "v17.0")
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        
        if not all([self.access_token, self.phone_number_id]):
            raise WhatsAppBusinessAPIError("Faltan variables de entorno requeridas para WhatsApp Business API")
        
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        logger.info("WhatsApp Business API Client inicializado")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                     retries: int = 3) -> Dict[str, Any]:
        """Realiza una petición HTTP a la API con reintentos."""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(retries):
            try:
                if method.upper() == "GET":
                    response = requests.get(url, headers=self.headers, params=data, timeout=30)
                else:
                    response = requests.post(url, headers=self.headers, json=data, timeout=30)
                
                response.raise_for_status()
                logger.info(f"📥 [WA-HTTP] Meta respondió OK: {response.status_code} - {response.text}")
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Intento {attempt + 1} fallido para {endpoint}: {e}")
                # Log detallado del error para debugging
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Status: {e.response.status_code}, Response: {e.response.text}")
                    logger.error(f"Request URL: {url}")
                    logger.error(f"Request headers: {self.headers}")
                    logger.error(f"Request data: {data}")
                if attempt == retries - 1:
                    raise WhatsAppBusinessAPIError(f"Error en petición después de {retries} intentos: {e}")
                time.sleep(1 * (attempt + 1))  # Backoff exponencial
        
        raise WhatsAppBusinessAPIError("Error desconocido en petición")

    def send_text_message(self, to: str, message: str) -> Dict[str, Any]:
        """Envía un mensaje de texto a un número de WhatsApp."""
        # Validar y limpiar número de teléfono
        clean_to = self.validate_phone_number(to)
        
        data = {
            "messaging_product": "whatsapp",
            "to": clean_to,
            "type": "text",
            "text": {"body": message}
        }
        
        logger.info(f"Enviando mensaje de texto a {clean_to}: {data}")
        
        endpoint = f"{self.phone_number_id}/messages"
        return self._make_request("POST", endpoint, data)

    def send_interactive_message(self, to: str, message: str, buttons: List[Dict[str, str]]) -> Dict[str, Any]:
        """Envía un mensaje interactivo con botones."""
        if not buttons or len(buttons) > 3:
            raise WhatsAppBusinessAPIError("WhatsApp permite máximo 3 botones por mensaje")
        
        # Validar y limpiar número de teléfono
        clean_to = self.validate_phone_number(to)
        
        interactive_buttons = []
        for i, button in enumerate(buttons):
            interactive_buttons.append({
                "type": "reply",
                "reply": {
                    "id": button.get("callback_data", f"btn_{i}"),
                    "title": button.get("text", f"Opción {i+1}")[:20]  # WhatsApp límite 20 chars
                }
            })
        
        data = {
            "messaging_product": "whatsapp",
            "to": clean_to,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {"text": message},
                "action": {"buttons": interactive_buttons}
            }
        }
        
        logger.info(f"Enviando mensaje interactivo a {clean_to}: {data}")
        
        endpoint = f"{self.phone_number_id}/messages"
        return self._make_request("POST", endpoint, data)

    def send_image_message(self, to: str, image_url: str, caption: Optional[str] = None) -> Dict[str, Any]:
        """Envía una imagen con caption opcional."""
        data = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "image",
            "image": {"link": image_url}
        }
        
        if caption:
            data["image"]["caption"] = caption
        
        endpoint = f"{self.phone_number_id}/messages"
        return self._make_request("POST", endpoint, data)

    def send_document_message(self, to: str, document_url: str, filename: str, 
                            caption: Optional[str] = None) -> Dict[str, Any]:
        """Envía un documento."""
        data = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "document",
            "document": {
                "link": document_url,
                "filename": filename
            }
        }
        
        if caption:
            data["document"]["caption"] = caption
        
        endpoint = f"{self.phone_number_id}/messages"
        return self._make_request("POST", endpoint, data)

    def send_list_message(self, to: str, header: str, body: str, footer: str, 
                         button_text: str, sections: List[Dict]) -> Dict[str, Any]:
        """Envía un mensaje con lista de opciones (máximo 10 opciones)."""
        data = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "interactive",
            "interactive": {
                "type": "list",
                "header": {"type": "text", "text": header},
                "body": {"text": body},
                "footer": {"text": footer},
                "action": {
                    "button": button_text,
                    "sections": sections
                }
            }
        }
        
        endpoint = f"{self.phone_number_id}/messages"
        return self._make_request("POST", endpoint, data)

    def mark_message_as_read(self, message_id: str) -> Dict[str, Any]:
        """Marca un mensaje como leído."""
        data = {
            "messaging_product": "whatsapp",
            "status": "read",
            "message_id": message_id
        }
        
        endpoint = f"{self.phone_number_id}/messages"
        return self._make_request("POST", endpoint, data)

    def get_media_url(self, media_id: str) -> str:
        """Obtiene la URL de un archivo multimedia."""
        endpoint = f"{media_id}"
        response = self._make_request("GET", endpoint)
        return response.get("url", "")

    def download_media(self, media_url: str) -> bytes:
        """Descarga un archivo multimedia."""
        try:
            response = requests.get(media_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            raise WhatsAppBusinessAPIError(f"Error descargando archivo: {e}")

    def validate_phone_number(self, phone: str) -> str:
        """Valida y normaliza un número de teléfono para WhatsApp."""
        # Remover caracteres no numéricos excepto +
        phone = ''.join(c for c in phone if c.isdigit() or c == '+')
        
        # Si no tiene código de país, asumir Colombia (+57)
        if not phone.startswith('+'):
            if phone.startswith('57'):
                phone = '+' + phone
            else:
                phone = '+57' + phone
        
        # Remover el + para la API
        return phone.lstrip('+')

    def get_business_profile(self) -> Dict[str, Any]:
        """Obtiene información del perfil de negocio."""
        endpoint = f"{self.phone_number_id}/whatsapp_business_profile"
        return self._make_request("GET", endpoint, {"fields": "about,address,description,email,profile_picture_url,websites,vertical"})

    def set_webhook(self, webhook_url: str, verify_token: str) -> bool:
        """Configura el webhook para recibir mensajes."""
        # Nota: Esto generalmente se hace a través de la interfaz de Facebook
        # o usando la Graph API de una manera más compleja
        logger.info(f"Para configurar webhook, usa la interfaz de Facebook: {webhook_url}")
        return True