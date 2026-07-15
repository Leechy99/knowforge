"""Application configuration."""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = Field(default="ai-kb-consumers", min_length=1)
    kafka_dlq_topic: str = Field(default="dlq-events", min_length=1)
    postgres_dsn: str = "postgresql+asyncpg://aikb:aikb_secret@localhost:5432/aikb"
    qdrant_url: str = "http://localhost:6333"
    qdrant_collection_name: str = Field(default="ai_knowledge_base", min_length=1)
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "neo4j_secret"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "aikb"
    minio_secret_key: str = "aikb_secret"
    minio_bucket_name: str = Field(default="knowforge", min_length=1)
    embedding_model: str = Field(default="BAAI/bge-large-zh-v1.5", min_length=1)
    embedding_dimension: int = Field(default=1024, gt=0)
    embedding_batch_size: int = Field(default=32, gt=0, le=1024)
    embedding_device: str = Field(default="cpu", min_length=1)
    health_check_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    cors_origins: list[str] = ["*"]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
