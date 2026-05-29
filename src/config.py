"""Application configuration."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    kafka_bootstrap_servers: str = "localhost:9092"
    postgres_dsn: str = "postgresql+asyncpg://aikb:aikb_secret@localhost:5432/aikb"
    qdrant_url: str = "http://localhost:6333"
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "neo4j_secret"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "aikb"
    minio_secret_key: str = "aikb_secret"
    cors_origins: list[str] = ["*"]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
