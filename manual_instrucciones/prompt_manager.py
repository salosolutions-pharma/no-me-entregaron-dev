import os
import logging
from typing import Optional, Dict

from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

load_dotenv()

logger = logging.getLogger(__name__)


class PromptManagerError(Exception):
    """Excepción personalizada para errores en PromptManager."""


class PromptManager:
    """Gestiona la recuperación dinámica de prompts de una tabla de BigQuery."""

    def __init__(self) -> None:
        """Inicializa PromptManager configurando el cliente de BigQuery y definiendo la tabla de prompts."""
        self.project_id = os.getenv("PROJECT_ID")
        self.dataset_id = os.getenv("BIGQUERY_DATASET_ID", "NME_dev")
        self.table_id = os.getenv("BIGQUERY_PROMPTS_TABLE_ID", "manual_instrucciones")

        if not self.project_id:
            logger.critical("La variable de entorno 'PROJECT_ID' no está configurada para PromptManager.")
            raise PromptManagerError("PROJECT_ID no configurado.")

        self.table_reference = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        try:
            self.bq_client = bigquery.Client(project=self.project_id)
            logger.info(f"PromptManager conectado a la tabla: {self.table_reference}")
        except Exception as exc:
            logger.exception("Fallo al inicializar el cliente de BigQuery en PromptManager.")
            raise PromptManagerError(f"Fallo al inicializar el cliente de BigQuery: {exc}") from exc

    def get_prompt_by_module_and_function(self, modulo: str, funcionalidad: str) -> Optional[str]:
        """
        Recupera un prompt específico por módulo y funcionalidad.
        
        Args:
            modulo: Nombre del módulo (ej: 'CLAIM', 'BYC', 'PIP', 'DATA')
            funcionalidad: Nombre de la funcionalidad (ej: 'reclamacion_eps', 'consentimiento')
            
        Returns:
            El texto del prompt o None si no se encuentra
        """
        query = f"""
            SELECT prompt_text
            FROM `{self.table_reference}`
            WHERE modulo = @modulo AND funcionalidad = @funcionalidad
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("modulo", "STRING", modulo),
                bigquery.ScalarQueryParameter("funcionalidad", "STRING", funcionalidad),
            ]
        )

        try:
            results = self.bq_client.query(query, job_config=job_config).result()

            for row in results:
                logger.info(f"Prompt encontrado para {modulo}.{funcionalidad} (longitud: {len(row.prompt_text)} caracteres)")
                return row.prompt_text

            logger.warning(f"No se encontró ningún prompt para {modulo}.{funcionalidad}")
            return None

        except GoogleAPIError as exc:
            logger.error(f"Error de BigQuery al consultar el prompt para {modulo}.{funcionalidad}: {exc}")
            return None
        except Exception as exc:
            logger.error(f"Error inesperado al consultar el prompt en PromptManager: {exc}")
            return None

    def get_prompt_by_keyword(self, keyword: str) -> Optional[str]:
        """
        Método legacy para compatibilidad con código existente.
        Mapea keywords antiguos a la nueva estructura modulo.funcionalidad.
        
        Args:
            keyword: Keyword antiguo (ej: 'BYC', 'PIP', 'CLAIM', 'RECLAMACION_EPS')
            
        Returns:
            El texto del prompt o None si no se encuentra
        """
        # Mapeo de keywords antiguos a nueva estructura
        mapping = {
            "BYC": ("BYC", "consentimiento"),
            "PIP": ("PIP", "extraccion_data"), 
            "CLAIM": ("DATA", "recoleccion_campos"),
            "RECLAMACION_EPS": ("CLAIM", "reclamacion_eps"),
            "RECLAMACION_SUPERSALUD": ("CLAIM", "reclamacion_supersalud"),
            "TUTELA": ("CLAIM", "tutela")
        }
        
        if keyword in mapping:
            modulo, funcionalidad = mapping[keyword]
            logger.info(f"Mapeando keyword '{keyword}' a {modulo}.{funcionalidad}")
            return self.get_prompt_by_module_and_function(modulo, funcionalidad)
        
        logger.warning(f"Keyword '{keyword}' no encontrado en el mapeo legacy")
        return None

    def get_all_prompts(self) -> Dict[str, Dict[str, str]]:
        """
        Recupera todos los prompts disponibles organizados por módulo y funcionalidad.
        
        Returns:
            Dict anidado: {modulo: {funcionalidad: prompt_text}}
        """
        query = f"""
            SELECT modulo, funcionalidad, prompt_text
            FROM `{self.table_reference}`
            ORDER BY modulo, funcionalidad
        """

        try:
            results = self.bq_client.query(query).result()
            prompts = {}

            for row in results:
                modulo = row.modulo
                funcionalidad = row.funcionalidad
                
                if modulo not in prompts:
                    prompts[modulo] = {}
                
                prompts[modulo][funcionalidad] = row.prompt_text

            logger.info(f"Cargados prompts de {len(prompts)} módulos desde BigQuery.")
            return prompts

        except GoogleAPIError as exc:
            logger.error(f"Error de BigQuery al obtener todos los prompts: {exc}")
            return {}
        except Exception as exc:
            logger.error(f"Error inesperado al obtener todos los prompts: {exc}")
            return {}

    def list_available_modules(self) -> Dict[str, list]:
        """
        Lista todos los módulos y sus funcionalidades disponibles.
        
        Returns:
            Dict: {modulo: [lista_de_funcionalidades]}
        """
        query = f"""
            SELECT modulo, funcionalidad
            FROM `{self.table_reference}`
            ORDER BY modulo, funcionalidad
        """

        try:
            results = self.bq_client.query(query).result()
            modules = {}

            for row in results:
                modulo = row.modulo
                funcionalidad = row.funcionalidad
                
                if modulo not in modules:
                    modules[modulo] = []
                
                modules[modulo].append(funcionalidad)

            logger.info(f"Módulos disponibles: {list(modules.keys())}")
            return modules

        except Exception as exc:
            logger.error(f"Error al listar módulos disponibles: {exc}")
            return {}


# Instancia global
prompt_manager: Optional[PromptManager] = None

try:
    prompt_manager = PromptManager()
    logger.info("PromptManager instanciado correctamente.")
except PromptManagerError as e:
    logger.critical(f"Error al inicializar prompt_manager: {e}")
    prompt_manager = None
except Exception as e:
    logger.critical(f"Error inesperado al inicializar prompt_manager: {e}")
    prompt_manager = None