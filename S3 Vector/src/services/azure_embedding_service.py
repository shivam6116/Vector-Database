import logging
from openai import AzureOpenAI
from src.config import settings

logger = logging.getLogger(__name__)


class AzureEmbeddingService:
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=settings.api_key,
            api_version=settings.api_version,
            azure_endpoint=settings.endpoint,
        )
        self.model = settings.embedding_model

    def get_embedding(self, text: str) -> list[float]:
        """
        Generate an embedding for the given text using Azure OpenAI.
        """
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=[text]
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}", exc_info=True)
            return []
