import logging
from src.services.s3_vector_service import S3VectorService


logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    vector_service = S3VectorService()
    
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
    
    result = vector_service.store_vectors(movie_data)
    print("Storage result:", result)
