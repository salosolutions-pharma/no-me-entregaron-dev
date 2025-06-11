import os
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from google.cloud import bigquery
from openai import OpenAI
from google.api_core.exceptions import GoogleAPIError

# Cargar variables de entorno
load_dotenv()

# Configuración de logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class EPSParserError(Exception):
    """Excepción para errores específicos del parser de EPS."""
    pass

class EPSParser:
    """
    Parser para normalizar nombres de entidades de salud colombianas (EPS/IPS).
    Utiliza una base de datos de entidades en BigQuery y, si es necesario,
    un modelo de lenguaje grande (LLM) de OpenAI para la normalización.
    """
    
    def __init__(self) -> None:
        """
        Inicializa el parser con la configuración necesaria desde variables de entorno
        y carga las entidades de referencia desde BigQuery.
        
        Raises:
            EPSParserError: Si alguna variable de entorno requerida no está configurada,
                            o si hay un error al cargar las entidades de BigQuery.
        """
        # Configuración de BigQuery
        self.project_id = self._get_required_env('PROJECT_ID')
        self.dataset_id = os.getenv('MOTOR_IDENTIDADES_DATASET', 'motor_identidades')
        self.table_id = os.getenv('EPS_TABLE', 'eps')
        
        try:
            self.bq_client = bigquery.Client(project=self.project_id)
            logging.info(f"Conectado a BigQuery Project: {self.project_id}, Dataset: {self.dataset_id}, Table: {self.table_id}")
        except Exception as e:
            logger.exception("Error al inicializar el cliente de BigQuery.")
            raise EPSParserError(f"Fallo al inicializar cliente de BigQuery: {e}") from e
        
        # Configuración de OpenAI
        self.openai_api_key = self._get_required_env('OPENAI_API_KEY')
        self.openai_model = os.getenv('OPENAI_DEFAULT_MODEL', 'gpt-4o-mini')
        
        try:
            self.openai_client = OpenAI(api_key=self.openai_api_key)
            logging.info(f"Cliente de OpenAI inicializado con modelo: {self.openai_model}")
        except Exception as e:
            logger.exception("Error al inicializar el cliente de OpenAI.")
            raise EPSParserError(f"Fallo al inicializar cliente de OpenAI: {e}") from e
        
        # Cargar entidades de referencia
        self.reference_entities = self._load_reference_entities()
    
    def _get_required_env(self, key: str) -> str:
        """
        Obtiene una variable de entorno requerida.
        
        Args:
            key: El nombre de la variable de entorno.
            
        Returns:
            El valor de la variable de entorno.
            
        Raises:
            EPSParserError: Si la variable de entorno no está configurada.
        """
        value = os.getenv(key)
        if not value:
            logger.critical(f"Variable de entorno requerida no configurada: {key}")
            raise EPSParserError(f"Variable requerida no configurada: {key}")
        return value
    
    def _load_reference_entities(self) -> List[Dict[str, str]]:
        """
        Carga las entidades de salud de referencia (EPS/IPS) desde BigQuery.
        
        Returns:
            Una lista de diccionarios, cada uno representando una entidad con
            'tipo_entidad', 'entidad_estandarizado', 'razon_social', y 'alias'.
            
        Raises:
            EPSParserError: Si hay un error al consultar BigQuery.
        """
        query = f"""
        SELECT tipo_entidad, entidad_estandarizado, razon_social, alias
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        """
        
        try:
            results = self.bq_client.query(query).result()
            entities = []
            for row in results:
                entities.append({
                    'tipo_entidad': row.tipo_entidad,
                    'entidad_estandarizado': row.entidad_estandarizado,
                    'razon_social': row.razon_social,
                    'alias': row.alias or "" # Asegurar que 'alias' sea una cadena vacía si es None
                })
            logger.info(f"Cargadas {len(entities)} entidades de referencia desde BigQuery.")
            return entities
        except GoogleAPIError as e:
            logger.exception("Error de BigQuery al cargar entidades de referencia.")
            raise EPSParserError(f"Error cargando entidades desde BigQuery: {e}") from e
        except Exception as e:
            logger.exception("Error inesperado al cargar entidades de referencia.")
            raise EPSParserError(f"Error inesperado cargando entidades: {e}") from e
    
    def _perform_quick_match(self, raw_name: str) -> Optional[str]:
        """
        Realiza una búsqueda rápida de coincidencia para el nombre de una entidad.
        Prioriza la coincidencia exacta, luego la contención, y finalmente los alias.
        
        Args:
            raw_name: El nombre de la entidad a normalizar.
            
        Returns:
            El nombre estandarizado de la entidad si se encuentra una coincidencia,
            o None si no se encuentra.
        """
        cleaned_name = raw_name.upper().strip()
        
        # 1. Coincidencia exacta con el nombre estandarizado
        for entity in self.reference_entities:
            if entity['entidad_estandarizado'].upper() == cleaned_name:
                logger.debug(f"Coincidencia exacta encontrada para '{raw_name}': {entity['entidad_estandarizado']}")
                return entity['entidad_estandarizado']
        
        # 2. Coincidencia por contención (nombre estandarizado dentro del nombre limpio o viceversa)
        for entity in self.reference_entities:
            standardized_name_upper = entity['entidad_estandarizado'].upper()
            # Se requiere un mínimo de 3 caracteres para evitar falsos positivos con cadenas cortas
            if len(standardized_name_upper) > 3 and (standardized_name_upper in cleaned_name or cleaned_name in standardized_name_upper):
                logger.debug(f"Coincidencia por contención encontrada para '{raw_name}': {entity['entidad_estandarizado']}")
                return entity['entidad_estandarizado']
        
        # 3. Búsqueda en alias (contención en los alias)
        for entity in self.reference_entities:
            if entity['alias']:
                # Divide los alias por ';' y limpia espacios
                aliases = [alias.strip().upper() for alias in entity['alias'].split(';') if alias.strip()]
                for alias in aliases:
                    if len(alias) > 3 and (alias in cleaned_name or cleaned_name in alias):
                        logger.debug(f"Coincidencia por alias encontrada para '{raw_name}': {entity['entidad_estandarizado']}")
                        return entity['entidad_estandarizado']
        
        logger.debug(f"No se encontró coincidencia rápida para '{raw_name}'.")
        return None
    
    def _perform_openai_match(self, raw_name: str) -> Optional[str]:
        """
        Realiza una búsqueda de coincidencia utilizando el modelo de lenguaje de OpenAI
        para casos donde la coincidencia directa no es suficiente.
        
        Args:
            raw_name: El nombre de la entidad a normalizar.
            
        Returns:
            El nombre estandarizado de la entidad si OpenAI lo identifica correctamente
            de la lista de entidades disponibles, o None si no lo hace o hay un error.
        """
        # Crear una lista de nombres de entidades estandarizados para el prompt
        standardized_entity_names = [e['entidad_estandarizado'] for e in self.reference_entities]
        
        prompt_message = f"""
        Dada la siguiente lista de entidades de salud colombianas:
        {', '.join(standardized_entity_names)}

        Por favor, identifica la entidad que mejor corresponde al nombre: "{raw_name}"
        Responde SOLO con el nombre EXACTO de la entidad estandarizada si la encuentras en la lista,
        o con la frase "NO_ENCONTRADO" si no hay una correspondencia clara.
        """
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt_message}],
                max_tokens=50, # Suficientes tokens para una respuesta corta
                temperature=0.1 # Baja temperatura para respuestas determinísticas
            )
            
            # Extraer y limpiar la respuesta del modelo
            model_output = response.choices[0].message.content.strip()
            
            # Verificar si la respuesta del modelo es una de nuestras entidades estandarizadas
            if model_output in standardized_entity_names:
                logger.info(f"OpenAI identificó '{raw_name}' como '{model_output}'.")
                return model_output
            
            logger.info(f"OpenAI no identificó una entidad clara para '{raw_name}'. Respuesta: '{model_output}'")
            return None # O si el modelo respondió "NO_ENCONTRADO"
            
        except Exception as e:
            logger.error(f"Error al usar OpenAI para normalizar '{raw_name}': {e}", exc_info=True)
            return None
    
    def parse_eps_name(self, raw_name: str) -> Dict[str, Any]:
        """
        Normaliza un nombre de entidad de salud (EPS/IPS) utilizando un enfoque de dos pasos:
        primero una búsqueda rápida local, luego un LLM (OpenAI) para casos complejos.
        
        Args:
            raw_name: El nombre crudo de la entidad de salud a parsear.
            
        Returns:
            Un diccionario que contiene:
            - "original_name": El nombre de entrada.
            - "standardized_entity": El nombre estandarizado encontrado, o None si no se pudo normalizar.
            - "method_used": El método que se usó ("empty_input", "quick_match", "openai", "not_found").
        """
        if not raw_name or not raw_name.strip():
            logger.info("Entrada vacía para el parsing de EPS.")
            return {
                "original_name": raw_name,
                "standardized_entity": None,
                "method_used": "empty_input"
            }
        
        # 1. Intentar coincidencia rápida (basada en BigQuery)
        standardized_result = self._perform_quick_match(raw_name)
        if standardized_result:
            return {
                "original_name": raw_name,
                "standardized_entity": standardized_result,
                "method_used": "quick_match"
            }
        
        # 2. Si la coincidencia rápida falla, usar OpenAI
        standardized_result = self._perform_openai_match(raw_name)
        return {
            "original_name": raw_name,
            "standardized_entity": standardized_result,
            "method_used": "openai" if standardized_result else "not_found"
        }

# --- Funciones auxiliares para el uso externo del módulo ---
def create_eps_parser() -> EPSParser:
    """
    Crea y devuelve una nueva instancia de EPSParser.
    
    Raises:
        EPSParserError: Si hay un error al inicializar el parser.
    """
    return EPSParser()

def get_standardized_eps(raw_name: str) -> Optional[str]:
    """
    Función de conveniencia para parsear un nombre de EPS y devolver
    directamente solo la entidad estandarizada.
    
    Args:
        raw_name: Nombre de la EPS a normalizar.
        
    Returns:
        El nombre estandarizado de la EPS, o None si no se puede normalizar.
    """
    parser = create_eps_parser()
    result = parser.parse_eps_name(raw_name)
    return result['standardized_entity']