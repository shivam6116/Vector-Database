import logging
import boto3
from src.services.azure_embedding_service import AzureEmbeddingService
from src.config import settings

logger = logging.getLogger(__name__)


class S3VectorService(AzureEmbeddingService):
    def __init__(self):
        super().__init__()

        self.s3vectors = boto3.client(
            's3vectors',
            aws_access_key_id=settings.aws_user_access_key,
            aws_secret_access_key=settings.aws_user_secret_key,
            region_name=settings.s3_region
        )
        self.s3_bucket = settings.s3_bucket
        self.index_name = settings.s3_vector_index

    def store_vectors(self, vector_data: list[dict]):
        """
        Store vectors in S3 Vector Index.
        """
        vectors = []
        for item in vector_data:
            embedding = self.get_embedding(item['text'])
            if embedding:
                vectors.append({
                    "key": item['key'],
                    "data": {"float32": embedding},
                    "metadata": item['metadata']
                })

        if not vectors:
            logger.warning("No valid vectors to store")
            return None

        try:
            response = self.s3vectors.put_vectors(
                vectorBucketName=self.s3_bucket,
                indexName=self.index_name,
                vectors=vectors
            )
            logger.info("Vectors stored successfully")
            return response
        except Exception as e:
            logger.error(f"Vector storage failed: {e}", exc_info=True)
            return None
