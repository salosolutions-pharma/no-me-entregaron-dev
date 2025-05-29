import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pip_processor import PIPProcessor


# Ruta de imagen local (ajusta esto con el path real en tu máquina)
image_path = r"G:\Mi unidad\No me entregaron\Formulas Medicas\Formula Carolina Salazar.jpg"

# Simulamos un session_id cualquiera
session_id = "demo-session-002"

# Crear instancia del procesador
processor = PIPProcessor()

# Ejecutar el procesamiento
resultado = processor.process_image(image_path, session_id)

# Mostrar el resultado
print("\n--- Resultado ---")
print(resultado)
