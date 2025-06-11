import os
import logging
from typing import Optional

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
    Gestiona la obtención dinámica de prompts desde una tabla de BigQuery.

    Este manager realiza consultas en tiempo real a BigQuery para obtener
    prompts según módulo y palabra clave, garantizando siempre la versión más actualizada.
    """

    def __init__(self) -> None:
        """
        Inicializa el PromptManager configurando el cliente de BigQuery
        y definiendo la tabla de prompts.
        """
        self.project_id = os.getenv("PROJECT_ID")
        self.dataset_id = os.getenv("BIGQUERY_DATASET_ID", "NME_dev")
        self.table_id = os.getenv("BIGQUERY_PROMPTS_TABLE_ID", "manual_instrucciones")

        if not self.project_id:
            logger.critical(
                "La variable de entorno 'PROJECT_ID' no está configurada para PromptManager."
            )
            raise PromptManagerError("PROJECT_ID no configurado.")

        self.table_reference = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        try:
            self.bq_client = bigquery.Client(project=self.project_id)
            logger.info(f"PromptManager conectado a la tabla: {self.table_reference}")
        except Exception as exc:
            logger.exception("Fallo al inicializar cliente BigQuery en PromptManager.")
            raise PromptManagerError(
                f"Fallo inicializando cliente BigQuery: {exc}"
            ) from exc

    def get_prompt_by_keyword(
        self, modulo: str
    ) -> Optional[str]:
        """
        Obtiene un prompt de la tabla buscando el primer texto que contenga la palabra clave
        dentro de un módulo específico. La búsqueda es case-insensitive y se realiza
        directamente en BigQuery para garantizar frescura.

        Args:
            modulo (str): Nombre del módulo donde buscar el prompt.
            keyword (str): Palabra clave para buscar dentro del texto del prompt.

        Returns:
            Optional[str]: Texto del prompt si se encuentra, None si no.
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
            print(results)
            for row in results:
                return row.prompt_text
            logger.warning(
                f"No se encontró prompt para módulo '{modulo}'"
            )
            return None
        except GoogleAPIError as exc:
            logger.error(
                f"Error de BigQuery al consultar prompt para módulo '{modulo}: {exc}"
            )
            return None
        except Exception as exc:
            logger.error(
                f"Error inesperado al consultar prompt en PromptManager: {exc}"
            )
            return None


# Instancia global de PromptManager
prompt_manager: Optional[PromptManager] = None
try:
    prompt_manager = PromptManager()
except PromptManagerError as e:
    logger.critical(f"Error inicializando prompt_manager: {e}")
    prompt_manager = None
