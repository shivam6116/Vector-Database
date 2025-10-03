import json
import logging
from src.services.s3_vector_service import S3VectorService

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("="*5, "Running main.py.", "="*5)

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

    # Store vector data
    # result = vector_service.store_vectors(movie_data)
    # print("Storage result:", result)

    # Query vectors
    # query = "A farm boy joins rebels to fight an evil empire in space"
    # results = vector_service.query_vector_index(query, top_k=3)
    # print("Query results:")
    # print(json.dumps(results, indent=2))

    # Get vector by key
    # star_wars_vector = vector_service.get_vector_by_key("Star Wars")
    # print("Get vector by key (Star Wars):")
    # print(json.dumps(star_wars_vector, indent=2))

    # Update a vector's text and metadata
    # update_response = vector_service.update_vector(
    #     key="Star Wars",
    #     new_text="Star Wars: A young hero joins a rebellion to overthrow a dark empire",
    #     new_metadata={"genre": "scifi", "updated": True}
    # )
    # print("Update vector response:")
    # print(update_response)

    # Update only metadata
    # metadata_update_response = vector_service.update_metadata(
    #     key="Jurassic Park",
    #     new_metadata={"genre": "sci-fi", "classic": True}
    # )
    # print("Metadata update response:")
    # print(metadata_update_response)

    # Count vectors in index
    # count = vector_service.count_vectors()
    # print(f"Total vectors in index: {count}")

    # Example of filtered query based on metadata (if supported)
    '''https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-metadata-filtering.html'''
    filter_expr = {"genre": "family"}
    filtered_results = vector_service.filtered_query(
        query_text="Ocean search for lost fish",
        filter_expression=filter_expr,
        top_k=2
    )
    print("Filtered query results:")
    print(json.dumps(filtered_results, indent=2))

    # Delete all vectors
    # deleted_count = vector_service.delete_all_vectors(verbose=True)
    # print(f"Deleted {deleted_count} vectors from the index.")
