from .gemini_service import ask_gemini
# Ejemplo de cómo llamar a la función con el nombre de modelo corregido
respuesta = ask_gemini("Escribe un poema", model="gemini-2.0-flash")
print(respuesta)