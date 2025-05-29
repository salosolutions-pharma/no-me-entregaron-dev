import sys
import os
import json
import re
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configurar paths
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)

def usar_openai_directo(prompt: str, image_path: str) -> str:
    """
    Usar OpenAI directamente sin pasar por LLMCore
    """
    from llm_core.openai_service import ask_openai_image
    
    print("🤖 Usando OpenAI directamente...")
    return ask_openai_image(prompt, image_path)

def leer_prompt():
    """Leer prompt para análisis de fórmulas"""
    prompt_path = Path(parent_dir) / "processor_image_prescription" / "prompt_PIP.txt"
    
    if prompt_path.exists():
        return prompt_path.read_text(encoding="utf-8")
    
    # Prompt simplificado
    return """
Eres un asistente que analiza fórmulas médicas.

Analiza esta imagen de fórmula médica y extrae la información del paciente.
Extrae especialmente la EPS del paciente.

Devuelve SOLO un JSON con esta estructura:
{
  "datos": {
    "tipo_documento": "...",
    "numero_documento": "...",
    "paciente": "...",
    "eps": "...",
    "ips": "...",
    "doctor": "...",
    "regimen": "...",
    "ciudad": "...",
    "fecha_atencion": "...",
    "telefono": [],
    "direccion": "...",
    "diagnostico": "...",
    "medicamentos": []
  }
}
"""

def extraer_json(respuesta: str) -> dict:
    """Extraer JSON de la respuesta"""
    # Buscar JSON entre code fences
    code_fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", respuesta, re.DOTALL)
    if code_fence:
        json_text = code_fence.group(1)
    else:
        # Buscar JSON sin code fence
        json_match = re.search(r'\{.*\}', respuesta, re.DOTALL)
        if json_match:
            json_text = json_match.group()
        else:
            json_text = respuesta.strip()
    
    try:
        datos = json.loads(json_text)
        return datos.get("datos", datos)
    except json.JSONDecodeError as e:
        print(f"❌ Error parseando JSON: {e}")
        print(f"📄 Respuesta: {respuesta}")
        return {}

def test_completo_openai(image_path: str):
    """Test completo usando OpenAI directamente"""
    print(f"\n🏥 TEST CON OPENAI DIRECTO: {image_path}")
    print("=" * 60)
    
    # Verificar imagen
    if not Path(image_path).exists():
        print(f"❌ Imagen no existe: {image_path}")
        return
    
    try:
        # Paso 1: Extraer datos con OpenAI
        prompt = leer_prompt()
        print("📝 Prompt cargado")
        
        respuesta = usar_openai_directo(prompt, image_path)
        
        print("📄 Respuesta de OpenAI:")
        print("-" * 50)
        print(respuesta)
        print("-" * 50)
        
        # Paso 2: Parsear JSON
        datos = extraer_json(respuesta)
        
        if not datos:
            print("❌ No se pudieron extraer datos")
            return
        
        # Mostrar datos extraídos
        print("\n📋 DATOS EXTRAÍDOS:")
        print("-" * 30)
        for campo, valor in datos.items():
            if valor and valor != [] and valor != "":
                print(f"   📌 {campo}: {valor}")
        
        # Paso 3: Parsear EPS
        eps_cruda = datos.get("eps")
        
        if not eps_cruda:
            print("\n⚠️ No se encontró EPS en los datos")
            return
        
        print(f"\n🎯 PARSEANDO EPS: '{eps_cruda}'")
        print("-" * 40)
        
        # Importar parser EPS
        from parser import crear_parser
        parser = crear_parser()
        resultado = parser.parsear(eps_cruda)
        
        # Mostrar resultado
        status = "✅" if resultado['entidad_estandarizada'] else "❌"
        print(f"   {status} EPS Original: '{eps_cruda}'")
        print(f"   🎯 EPS Estandarizada: {resultado['entidad_estandarizada']}")
        print(f"   🔧 Método: {resultado['metodo_usado']}")
        
        # Resumen final
        print(f"\n🎉 RESULTADO FINAL:")
        print(f"   👤 Paciente: {datos.get('paciente', 'N/A')}")
        print(f"   🆔 Documento: {datos.get('tipo_documento', '')}{datos.get('numero_documento', '')}")
        print(f"   🏥 EPS: {eps_cruda} → {resultado['entidad_estandarizada']}")
        print(f"   🏥 IPS: {datos.get('ips', 'N/A')}")
        print(f"   👨‍⚕️ Doctor: {datos.get('doctor', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Función principal"""
    print("🧪 TEST PARSER EPS + OPENAI DIRECTO")
    print("=" * 50)
    
    # Verificar configuración
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ OPENAI_API_KEY no configurado")
        return
    
    print(f"✅ OpenAI API Key: {'*' * 10}{api_key[-10:]}")
    
    # Solicitar imagen
    image_path = input("\n📸 Ruta de la imagen de fórmula médica: ").strip()
    image_path = image_path.strip('"').strip("'")
    
    if not image_path:
        print("❌ No se proporcionó imagen")
        return
    
    test_completo_openai(image_path)

if __name__ == "__main__":
    main()