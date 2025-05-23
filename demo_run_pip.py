from pip_processor import PIPProcessor

# Ruta de imagen local (ajusta esto con el path real en tu máquina)
image_path = r"C:\Users\salos\Downloads\photo_4963257537629630187_y.jpg"

# Simulamos un session_id cualquiera
session_id = "demo-session-001"

# Crear instancia del procesador
processor = PIPProcessor()

# Ejecutar el procesamiento
resultado = processor.process_image(image_path, session_id)

# Mostrar el resultado
print("\n--- Resultado ---")
print(resultado)
