import logging
import re
from typing import Optional


class SessionLoggerFormatter(logging.Formatter):
    """Formateador que extrae session_id y teléfono automáticamente."""
    
    def format(self, record):
        # Extraer session_id del mensaje
        session_id = self._extract_session_id(record.getMessage())
        phone = self._extract_phone_from_session_id(session_id) if session_id else None
        
        # Agregar información al record
        record.session_id = session_id or "NO_SESSION"
        record.phone = phone or "NO_PHONE"
        record.channel = self._get_channel_from_session_id(session_id) if session_id else "UNKNOWN"
        
        return super().format(record)
    
    def _extract_session_id(self, message: str) -> Optional[str]:
        """Extrae session_id del mensaje."""
        patterns = [
            r'session[_\s]*id[:\s]*([A-Z]{2}_\d+_\d{8}_\d{6})',
            r'session[:\s]*([A-Z]{2}_\d+_\d{8}_\d{6})',
            r'([A-Z]{2}_\d+_\d{8}_\d{6})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group(1)
        return None
    
    def _extract_phone_from_session_id(self, session_id: str) -> Optional[str]:
        """Extrae teléfono del session_id."""
        if not session_id:
            return None
        try:
            parts = session_id.split("_")
            return parts[1] if len(parts) >= 2 else None
        except:
            return None
    
    def _get_channel_from_session_id(self, session_id: str) -> str:
        """Extrae canal del session_id."""
        if not session_id:
            return "UNKNOWN"
        if session_id.startswith("WA_"):
            return "WA"
        elif session_id.startswith("TL_"):
            return "TL"
        return "UNKNOWN"


def setup_structured_logging():
    """Configura logging estructurado para toda la aplicación."""
    format_template = (
        "%(asctime)s | %(channel)s | %(phone)s | %(session_id)s | "
        "%(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
    )
    
    formatter = SessionLoggerFormatter(format_template)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Limpiar handlers existentes
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    root_logger.addHandler(console_handler)
    return root_logger