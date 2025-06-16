import os
from base64 import b64encode
from pathlib import Path
from typing import List

from .openai_service import ask_openai, ask_openai_image, OpenAIServiceError
from .gemini_service import ask_gemini, ask_gemini_image, GeminiServiceError
from .claude_service import ask_claude, ask_claude_image, ClaudeServiceError


def _encode_image_to_b64(img_path: str | Path) -> str:
    """Convierte imagen a base64."""
    data = Path(img_path).read_bytes()
    return b64encode(data).decode()


class LLMCore:
    """Capa de abstracción para modelos de lenguaje. ask_text() → texto ↦ texto, ask_image() → (imagen + prompt) ↦ texto"""

    def __init__(self, default_model: str = "openai", 
                 fallback_priority: List[str] = None):
        self.default_model = default_model
        self.fallback_priority = fallback_priority or ["gemini", "claude"]

    def ask_text(self, prompt: str, *, model: str = None, timeout: int = None) -> str:
        """Procesa consultas de texto usando el modelo especificado o por defecto."""
        model_to_use = model or self.default_model
        
        try:
            if model_to_use == "openai":
                return ask_openai(prompt, model, timeout)
            elif model_to_use == "gemini":
                return ask_gemini(prompt, model, timeout)
            elif model_to_use == "claude":
                return ask_claude(prompt, model, timeout)
            else:
                raise ValueError(f"Modelo de texto no reconocido: {model_to_use}")
                
        except (OpenAIServiceError, GeminiServiceError, ClaudeServiceError, Exception) as exc:
            for fallback_model in self.fallback_priority:
                if fallback_model != model_to_use:
                    try:
                        return self.ask_text(prompt, model=fallback_model, timeout=timeout)
                    except Exception:
                        continue
            raise RuntimeError(f"Todos los modelos fallaron para texto: {exc}")

    def ask_image(self, prompt: str, image_path: str | Path, *, 
                  model: str = None, timeout: int = 60) -> str:
        """Procesa consultas con imagen usando el modelo especificado o por defecto."""
        model_to_use = model or self.default_model
        
        try:
            if model_to_use == "openai":
                return ask_openai_image(prompt, image_path, model, timeout)
            elif model_to_use == "gemini":
                return ask_gemini_image(prompt, image_path, model, timeout)
            elif model_to_use == "claude":
                return ask_claude_image(prompt, image_path, model, timeout)
            else:
                raise ValueError(f"Modelo de imagen no reconocido: {model_to_use}")
                
        except (Exception, OpenAIServiceError, GeminiServiceError, ClaudeServiceError) as exc:
            for fallback_model in self.fallback_priority:
                if fallback_model != model_to_use:
                    try:
                        return self.ask_image(prompt, image_path, model=fallback_model, timeout=timeout)
                    except Exception:
                        continue
            raise RuntimeError(f"Todos los modelos fallaron para imagen: {exc}")