
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    api_key: str
    api_version: str
    endpoint: str
    embedding_model: str
    aws_user_access_key: str
    aws_user_secret_key: str
    s3_region: str
    s3_bucket: str
    s3_vector_index: str

    class Config:
        env_file = ".env"

settings = Settings()
print("="*5,"Config.py Loaded.","="*5)
