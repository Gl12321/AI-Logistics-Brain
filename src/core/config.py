import os
from functools import lru_cache
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    GRAPH_HOST: str = Field(default="localhost")
    GRAPH_PORT: int = Field(default=6379)

    DATA_RAW_DIR: Path = BASE_DIR / "data" / "raw"
    GRAPH_SEED_DIR: Path = BASE_DIR / "data" / "seed"

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        extra="ignore"
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()