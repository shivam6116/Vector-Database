import logging

from time import sleep
import numpy as np
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from src.services.azure_embedding_service import AzureEmbeddingService
from src.config import settings





logger = logging.getLogger(__name__)

class MongoDBVectorService(AzureEmbeddingService):
    def __init__(self, connection_string: str, db_name: str, collection_name: str):
        super().__init__()
        self.client = MongoClient(connection_string)
        self.collection = self.client[db_name][collection_name]


    def store_vectors(self, vector_data: list[dict]):
        """
        Insert or update vectors in MongoDB.
        Each item should be a dict with 'key', 'text', and 'metadata' fields.
        """
        operations = []
        for item in vector_data:
            embedding = self.get_embedding(item['text'])
            if embedding:
                operations.append(
                    UpdateOne(
                        {'key': item['key']},
                        {'$set': {
                            'embedding': embedding,
                            'metadata': item.get('metadata', {}),
                        }},
                        upsert=True
                    )
                )
        if not operations:
            logger.warning("No valid vectors to store")
            return None
        try:
            result = self.collection.bulk_write(operations)
            logger.info(f"Stored {result.upserted_count + result.modified_count} vectors")
            return result
        except PyMongoError as e:
            logger.error(f"Vector storage failed: {e}", exc_info=True)
            return None

    def batch_store_vectors(self, vector_data: list[dict], batch_size: int = 100, retries: int = 3):
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
        embedding = self.get_embedding(new_text)
        if not embedding:
            logger.warning(f"Failed to get embedding for key {key}")
            return None
        try:
            result = self.collection.update_one(
                {'key': key},
                {'$set': {'embedding': embedding, 'metadata': new_metadata}},
                upsert=True
            )
            logger.info(f"Vector with key {key} updated successfully")
            return result
        except PyMongoError as e:
            logger.error(f"Failed to update vector {key}: {e}", exc_info=True)
            return None

    def get_vector_by_key(self, key: str, return_metadata: bool = True):
        projection = {'embedding': 1}
        if return_metadata:
            projection['metadata'] = 1
        doc = self.collection.find_one({'key': key}, projection=projection)
        if not doc:
            logger.info(f"No vector found with key {key}")
            return None
        return doc

    def count_vectors(self):
        try:
            return self.collection.count_documents({})
        except PyMongoError as e:
            logger.error(f"Failed to count vectors: {e}", exc_info=True)
            return 0

    def filtered_query(self, query_text: str, filter_expression: dict, top_k: int = 5):
        embedding = self.get_embedding(query_text)
        if not embedding:
            return {"error": "Failed to generate embedding"}

        pipeline = [
            {"$match": filter_expression},
            {
                "$search": {
                    "index": "your_vector_search_index_name",  # replace with your Atlas vector search index name
                    "knnBeta": {
                        "vector": embedding,
                        "path": "embedding",
                        "k": top_k
                    }
                }
            },
            {"$project": {"embedding": 0}}  # exclude embedding from results
        ]
        try:
            results = list(self.collection.aggregate(pipeline))
            return results
        except PyMongoError as e:
            logger.error(f"Filtered query failed: {e}", exc_info=True)
            return {"error": str(e)}

    def delete_all_vectors(self, verbose: bool = False) -> int:
        try:
            result = self.collection.delete_many({})
            count = result.deleted_count
            logger.info(f"Deleted {count} vectors")
            return count
        except PyMongoError as e:
            logger.error(f"Failed to delete vectors: {e}", exc_info=True)
            return 0

    def query_vector_index(self, query_text: str, top_k: int = 5):
        embedding = self.get_embedding(query_text)
        if not embedding:
            return {"error": "Failed to generate embedding"}

        pipeline = [
            {
                "$search": {
                    "index": "your_vector_search_index_name",  # replace with your Atlas vector search index
                    "knnBeta": {
                        "vector": embedding,
                        "path": "embedding",
                        "k": top_k
                    }
                }
            },
            {"$project": {"embedding": 0}}  # exclude embedding in results
        ]

        try:
            results = list(self.collection.aggregate(pipeline))
            return results
        except PyMongoError as e:
            logger.error(f"Query vector index failed: {e}", exc_info=True)
            return {"error": str(e)}

    def update_metadata(self, key: str, new_metadata: dict):
        try:
            result = self.collection.update_one({'key': key}, {'$set': {'metadata': new_metadata}})
            if result.matched_count == 0:
                logger.warning(f"No vector found with key {key} to update metadata")
                return None
            logger.info(f"Metadata updated for vector key {key}")
            return result
        except PyMongoError as e:
            logger.error(f"Failed to update metadata for key {key}: {e}", exc_info=True)
            return None

    @staticmethod
    def calculate_distance(vec1: list[float], vec2: list[float], method: str = "cosine") -> float:
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


