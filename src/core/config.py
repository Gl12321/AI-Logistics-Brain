import os
from functools import lru_cache
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "admin"
    DB_PASSWORD: str = "1234"
    DB_NAME: str = "ai_finacial_bd"

    NEO4J_HOST: str = "localhost"
    NEO4J_PORT: int = 7687
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "12345678"

    DATA_RAW_DIR: Path = BASE_DIR / "data" / "raw"
    GRAPH_SEED_DIR: Path = BASE_DIR / "data" / "seed"
    MODELS_DIR: Path = BASE_DIR / "models"

    MODELS: dict = {
        "embedder": {
            "repo_id": "thenlper/gte-large",
            "dimension": 1024,
            "device": "cpu",
            "cache_path": MODELS_DIR / "embedder"
        },

        "summarizer": {
            "repo_id": "Qwen/Qwen2.5-32B-Instruct-GGUF",
            "file_name": "qwen2.5-32b-instruct-q4_k_m.gguf",
            "context_window": 32768,
            "temperature": 0.1,
            "device": "cuda",
            "tensor_split": [15, 15],
            "cache_path": MODELS_DIR / "summarizer"
        }
    }

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding='utf-8',
        extra="ignore"
    )

    @property
    def db_url_async(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()