
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    api_key: str
    api_version: str
    endpoint: str
    embedding_model: str


    class Config:
        env_file = ".env"

settings = Settings()
print("="*5,"Config.py Loaded.","="*5)
