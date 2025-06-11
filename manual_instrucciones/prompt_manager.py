import os
import logging
from typing import Optional, Dict

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PromptManagerError(Exception):
    """Excepción personalizada para errores en PromptManager."""


class PromptManager:
    """
    Gestiona la recuperación dinámica de prompts de una tabla de BigQuery.
    """

    def __init__(self) -> None:
        """
        Inicializa PromptManager configurando el cliente de BigQuery
        y definiendo la tabla de prompts.
        """
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

    def get_prompt_by_keyword(self, modulo: str) -> Optional[str]:
        """
        Recupera un prompt de la tabla basándose en el módulo especificado.

        Args:
            modulo (str): Nombre del módulo (BYC, PIP, etc.)

        Returns:
            Optional[str]: Texto del prompt si se encuentra, None en caso contrario.
        """
        query = f"""
            SELECT prompt_text
            FROM `{self.table_reference}`
            WHERE modulo = @modulo
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("modulo", "STRING", modulo),
            ]
        )

        try:
            results = self.bq_client.query(query, job_config=job_config).result()

            for row in results:
                logger.info(f"Prompt encontrado para el módulo '{modulo}' (longitud: {len(row.prompt_text)} caracteres)")
                return row.prompt_text

            logger.warning(f"No se encontró ningún prompt para el módulo '{modulo}'")
            return None

        except GoogleAPIError as exc:
            logger.error(f"Error de BigQuery al consultar el prompt para el módulo '{modulo}': {exc}")
            return None
        except Exception as exc:
            logger.error(f"Error inesperado al consultar el prompt en PromptManager: {exc}")
            return None

    def get_all_prompts(self) -> Dict[str, str]:
        """
        Recupera todos los prompts disponibles.

        Returns:
            dict: Diccionario con los módulos como claves y los prompts como valores.
        """
        query = f"""
            SELECT modulo, prompt_text
            FROM `{self.table_reference}`
        """

        try:
            results = self.bq_client.query(query).result()
            prompts = {}

            for row in results:
                prompts[row.modulo] = row.prompt_text

            logger.info(f"Cargados {len(prompts)} prompts desde BigQuery.")
            return prompts

        except GoogleAPIError as exc:
            logger.error(f"Error de BigQuery al obtener todos los prompts: {exc}")
            return {}
        except Exception as exc:
            logger.error(f"Error inesperado al obtener todos los prompts: {exc}")
            return {}


prompt_manager: Optional[PromptManager] = None

try:
    prompt_manager = PromptManager()
except PromptManagerError as e:
    logger.critical(f"Error al inicializar prompt_manager: {e}")
    prompt_manager = None
except Exception as e:
    logger.critical(f"Error inesperado al inicializar prompt_manager: {e}")
    prompt_manager = None