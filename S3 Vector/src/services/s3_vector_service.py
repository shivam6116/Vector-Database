import logging
import boto3
from time import sleep
import numpy as np
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


    def batch_store_vectors(self, vector_data: list[dict], batch_size: int = 100, retries: int = 3):
        """Store vectors in batches with retry logic."""
        total = len(vector_data)
        for start in range(0, total, batch_size):
            batch = vector_data[start:start+batch_size]
            for attempt in range(retries):
                try:
                    response = self.store_vectors(batch)
                    if response:
                        break
                except Exception as e:
                    logger.warning(f"Retry {attempt+1}/{retries} failed: {e}")
                    sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to store batch starting at index {start}")


    def update_vector(self, key: str, new_text: str, new_metadata: dict):
        """Update an existing vector by key."""
        embedding = self.get_embedding(new_text)
        if not embedding:
            logger.warning(f"Failed to get embedding for key {key}")
            return None
        try:
            response = self.s3vectors.put_vectors(
                vectorBucketName=self.s3_bucket,
                indexName=self.index_name,
                vectors=[{
                    "key": key,
                    "data": {"float32": embedding},
                    "metadata": new_metadata
                }]
            )
            logger.info(f"Vector with key {key} updated successfully")
            return response
        except Exception as e:
            logger.error(f"Failed to update vector {key}: {e}", exc_info=True)
            return None


    def get_vector_by_key(self, key: str, return_metadata: bool = True):
        try:
            response = self.s3vectors.get_vectors(
                vectorBucketName=self.s3_bucket,
                indexName=self.index_name,
                keys=[key],
                returnData=True,
                returnMetadata=return_metadata
            )
            vectors = response.get('vectors', [])
            if not vectors:
                logger.info(f"No vector found with key {key}")
                return None
            return vectors[0]
        except Exception as e:
            logger.error(f"Failed to get vector by key {key}: {e}", exc_info=True)
            return None


    def count_vectors(self):
        count = 0
        next_token = None
        try:
            while True:
                kwargs = {
                    "vectorBucketName": self.s3_bucket,
                    "indexName": self.index_name,
                    "returnMetadata": False,
                    "returnData": False,
                }
                if next_token:
                    kwargs["nextToken"] = next_token
                response = self.s3vectors.list_vectors(**kwargs)
                count += len(response.get("vectors", []))
                next_token = response.get("nextToken")
                if not next_token:
                    break
            return count
        except Exception as e:
            logger.error(f"Failed to count vectors: {e}", exc_info=True)
            return 0


    def filtered_query(self, query_text: str, filter_expression: dict, top_k: int = 5):
        embedding = self.get_embedding(query_text)
        if not embedding:
            return {"error": "Failed to generate embedding"}
        try:
            response = self.s3vectors.query_vectors(
                vectorBucketName=self.s3_bucket,
                indexName=self.index_name,
                queryVector={"float32": embedding},
                topK=top_k,
                returnDistance=True,
                returnMetadata=True,
                filter=filter_expression
            )
            return response.get("vectors", [])
        except Exception as e:
            return {"error": str(e)}


    def delete_all_vectors(self, verbose: bool = False) -> int:
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


    def query_vector_index(self, query_text: str, top_k: int = 5, return_metadata: bool = True):
        embedding = self.get_embedding(query_text)
        if not embedding:
            return {"error": "Failed to generate embedding"}
        try:
            response = self.s3vectors.query_vectors(
                vectorBucketName=self.s3_bucket,
                indexName=self.index_name,
                queryVector={"float32": embedding},
                topK=top_k,
                returnDistance=True,
                returnMetadata=return_metadata,
            )
            return response.get("vectors", [])
        except Exception as e:
            return {"error": str(e)}


    def update_metadata(self, key: str, new_metadata: dict):
        """Update only the metadata for a vector key without changing embedding."""
        try:
            vector = self.get_vector_by_key(key)
            if not vector:
                logger.warning(f"Vector with key {key} not found")
                return None
            # Update metadata and re-put the vector
            embedding_data = vector.get('data')
            response = self.s3vectors.put_vectors(
                vectorBucketName=self.s3_bucket,
                indexName=self.index_name,
                vectors=[{
                    "key": key,
                    "data": embedding_data,
                    "metadata": new_metadata
                }]
            )
            logger.info(f"Metadata updated for vector key {key}")
            return response
        except Exception as e:
            logger.error(f"Failed to update metadata for key {key}: {e}", exc_info=True)
            return None


    # Placeholder for index management - requires AWS CLI or SDK support beyond boto3 base client
    def create_index(self, index_name: str, dimension: int, distance_metric: str):
        """
        Create a new vector index.
        AWS SDK for Python does not support index creation directly,
        typically done via AWS Console or CLI.
        """
        logger.info("Index creation should be done via AWS Console or CLI.")


    def delete_index(self, index_name: str):
        """
        Delete a vector index.
        AWS SDK for Python does not support index deletion directly,
        typically done via AWS Console or CLI.
        """
        logger.info("Index deletion should be done via AWS Console or CLI.")


    @staticmethod
    def calculate_distance(vec1: list[float], vec2: list[float], method: str = "cosine") -> float:
        """
        Calculate distance/similarity between two vectors.
        Supported methods: cosine, euclidean
        """
        v1 = np.array(vec1)
        v2 = np.array(vec2)
        if method == "cosine":
            cos_sim = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
            return float(cos_sim)
        elif method == "euclidean":
            dist = np.linalg.norm(v1 - v2)
            return float(dist)
        else:
            raise ValueError(f"Unsupported distance method: {method}")
