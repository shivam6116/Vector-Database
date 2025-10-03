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



    def delete_all_vectors(self, verbose: bool = False) -> int:
        """
        Delete all vectors from the S3 Vector Index.

        Args:
            verbose (bool): If True, logs each deleted key.

        Returns:
            int: Number of vectors deleted.
        """
        num_vectors = 0
        next_token = None

        try:
            while True:
                kwargs = {
                    "vectorBucketName": self.s3_bucket,
                    "indexName": self.index_name,
                    "returnMetadata": True,
                }
                if next_token:
                    kwargs["nextToken"] = next_token

                response = self.s3vectors.list_vectors(**kwargs)

                keys = [vector["key"] for vector in response.get("vectors", [])]
                if keys:
                    self.s3vectors.delete_vectors(
                        vectorBucketName=self.s3_bucket,
                        indexName=self.index_name,
                        keys=keys,
                    )
                    num_vectors += len(keys)
                    if verbose:
                        for key in keys:
                            logger.info(f"Deleted vector with key: {key}")

                next_token = response.get("nextToken")
                if not next_token:
                    break

            logger.info(f"Deleted {num_vectors} vectors from index {self.index_name}.")
            return num_vectors

        except Exception as e:
            logger.error(f"Failed to delete vectors: {e}", exc_info=True)
            return 0
        
        
    def query_vector_index(self,query_text: str, top_k: int = 5, return_metadata: bool = True):
        """Query S3 Vector Index using embedding of a given text."""
        embedding = self.get_embedding(query_text)
        if not embedding:
            return {"error": "Failed to generate embedding"}

        try:
            response = self.s3vectors.query_vectors(
                vectorBucketName=settings.s3_bucket,
                indexName=settings.s3_vector_index,
                queryVector={"float32": embedding},
                topK=top_k,
                returnDistance=True,
                returnMetadata=return_metadata,
            )
            return response.get("vectors", [])
        except Exception as e:
            return {"error": str(e)}
