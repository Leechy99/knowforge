"""Tests for canonical runtime configuration."""

import pytest
from pydantic import ValidationError

from src.config import Settings


def test_settings_exposes_consistent_embedding_defaults() -> None:
    settings = Settings(_env_file=None)

    assert settings.embedding_model == "BAAI/bge-large-zh-v1.5"
    assert settings.embedding_dimension == 1024
    assert settings.qdrant_collection_name == "ai_knowledge_base"


def test_settings_reads_canonical_environment_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql+asyncpg://u:p@db:5432/kf")
    monkeypatch.setenv("EMBEDDING_DIMENSION", "768")
    monkeypatch.setenv("QDRANT_COLLECTION_NAME", "knowledge")

    settings = Settings(_env_file=None)

    assert settings.postgres_dsn.endswith("/kf")
    assert settings.embedding_dimension == 768
    assert settings.qdrant_collection_name == "knowledge"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("embedding_dimension", 0),
        ("embedding_batch_size", 0),
        ("qdrant_collection_name", ""),
        ("minio_bucket_name", ""),
    ],
)
def test_settings_rejects_invalid_runtime_values(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})
