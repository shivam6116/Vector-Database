import json
import logging
from src.services.mongo_vector_service import MongoDBVectorService

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("="*5, "Running main.py.", "="*5)

    vector_service = MongoDBVectorService()

    movie_data = [
        {
            "key": "Star Wars",
            "text": "Star Wars: A farm boy joins rebels to fight an evil empire in space",
            "metadata": {"genre": "scifi"}
        },
        {
            "key": "Jurassic Park",
            "text": "Jurassic Park: Scientists create dinosaurs in a theme park that goes wrong",
            "metadata": {"genre": "scifi"}
        },
        {
            "key": "Finding Nemo",
            "text": "Finding Nemo: A father fish searches the ocean to find his lost son",
            "metadata": {"genre": "family"}
        }
    ]

    connection_string = ""
    # mongo_service = MongoDBVectorService(connection_string, "your_db", "your_collection")


    vector_service.batch_store_vectors(movie_data)
    results = vector_service.query_vector_index("search query here", top_k=3)
    print(results)