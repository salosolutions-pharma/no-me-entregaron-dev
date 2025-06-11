import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pip_processor import PIPProcessor

# Ruta de la carpeta con las imágenes
folder_path = r"G:\Mi unidad\No me entregaron\Formulas Medicas"

# Simulamos un session_id cualquiera
session_id = "gemini-2.0-flash"

# Crear instancia del procesador
processor = PIPProcessor()

# Procesar todas las imágenes y guardar los resultados en una lista
extensiones_validas = {".png", ".jpg", ".jpeg"}
resultados = [
    processor.process_image(os.path.join(folder_path, filename), session_id)
    for filename in os.listdir(folder_path)
    if os.path.splitext(filename)[1].lower() in extensiones_validas
]


print(resultados)