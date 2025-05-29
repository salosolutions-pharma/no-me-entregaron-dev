import os
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from google.cloud import bigquery
from openai import OpenAI

# Cargar variables de entorno
load_dotenv()


class EPSParserError(Exception):
    """Excepción para errores del parser de EPS."""
    pass


class EPSParser:
    """Parser para normalizar nombres de entidades de salud colombianas."""
    
    def __init__(self) -> None:
        """Inicializa el parser con configuración desde variables de entorno."""
        # BigQuery
        self.project_id = self._get_env('PROJECT_ID')
        self.dataset_id = os.getenv('MOTOR_IDENTIDADES_DATASET', 'motor_identidades')
        self.table_id = os.getenv('EPS_TABLE', 'eps')
        self.client = bigquery.Client(project=self.project_id)
        
        # OpenAI
        self.openai_client = OpenAI(api_key=self._get_env('OPENAI_API_KEY'))
        self.openai_model = os.getenv('OPENAI_DEFAULT_MODEL', 'gpt-4o-mini')
        
        # Cargar entidades
        self.entidades_ref = self._load_entities()
    
    def _get_env(self, key: str) -> str:
        """Obtiene variable de entorno requerida."""
        value = os.getenv(key)
        if not value:
            raise EPSParserError(f"Variable requerida no configurada: {key}")
        return value
    
    def _load_entities(self) -> List[Dict[str, str]]:
        """Carga entidades desde BigQuery."""
        query = f"""
        SELECT tipo_entidad, entidad_estandarizado, razon_social, alias
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        """
        
        try:
            results = self.client.query(query).result()
            return [
                {
                    'tipo_entidad': row.tipo_entidad,
                    'entidad_estandarizado': row.entidad_estandarizado,
                    'razon_social': row.razon_social,
                    'alias': row.alias or ""
                }
                for row in results
            ]
        except Exception as e:
            raise EPSParserError(f"Error cargando entidades: {e}") from e
    
    def _quick_match(self, nombre: str) -> Optional[str]:
        """Búsqueda rápida por coincidencia directa."""
        nombre_clean = nombre.upper().strip()
        
        # Coincidencia exacta
        for entidad in self.entidades_ref:
            if entidad['entidad_estandarizado'].upper() == nombre_clean:
                return entidad['entidad_estandarizado']
        
        # Coincidencia por contención
        for entidad in self.entidades_ref:
            entidad_upper = entidad['entidad_estandarizado'].upper()
            if len(entidad_upper) > 3 and (entidad_upper in nombre_clean or nombre_clean in entidad_upper):
                return entidad['entidad_estandarizado']
        
        # Búsqueda en alias
        for entidad in self.entidades_ref:
            if entidad['alias']:
                aliases = [alias.strip().upper() for alias in entidad['alias'].split(';')]
                for alias in aliases:
                    if len(alias) > 3 and (alias in nombre_clean or nombre_clean in alias):
                        return entidad['entidad_estandarizado']
        
        return None
    
    def _openai_match(self, nombre: str) -> Optional[str]:
        """Búsqueda usando OpenAI para casos complejos."""
        entidades_lista = [e['entidad_estandarizado'] for e in self.entidades_ref]
        
        prompt = f"""Identifica la entidad de salud colombiana correspondiente a: "{nombre}"

Entidades disponibles: {', '.join(entidades_lista)}

Responde SOLO con el nombre exacto de la entidad o "NO_ENCONTRADO"."""
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.1
            )
            
            resultado = response.choices[0].message.content.strip()
            return resultado if resultado in entidades_lista else None
            
        except Exception:
            return None
    
    def parsear(self, nombre: str) -> Dict[str, Any]:
        """
        Parsea un nombre de EPS.
        
        Args:
            nombre: Nombre a parsear.
            
        Returns:
            Dict con nombre_original, entidad_estandarizada, metodo_usado.
        """
        if not nombre or not nombre.strip():
            return {
                "nombre_original": nombre,
                "entidad_estandarizada": None,
                "metodo_usado": "entrada_vacia"
            }
        
        # Intentar coincidencia rápida
        resultado = self._quick_match(nombre)
        if resultado:
            return {
                "nombre_original": nombre,
                "entidad_estandarizada": resultado,
                "metodo_usado": "coincidencia_rapida"
            }
        
        # Usar OpenAI
        resultado = self._openai_match(nombre)
        return {
            "nombre_original": nombre,
            "entidad_estandarizada": resultado,
            "metodo_usado": "openai" if resultado else "no_encontrado"
        }


def crear_parser() -> EPSParser:
    """Crea instancia del parser."""
    return EPSParser()


def parsear_eps(nombre: str) -> Optional[str]:
    """
    Parsea un nombre de EPS y devuelve solo la entidad estandarizada.
    
    Args:
        nombre: Nombre de EPS a parsear.
        
    Returns:
        Nombre estandarizado o None.
    """
    parser = crear_parser()
    resultado = parser.parsear(nombre)
    return resultado['entidad_estandarizada']