"""Unified Content Schema for AI Knowledge Base.

This module defines the core data structures for representing documents,
chunks, entities, and their relationships across all supported content types.
"""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


class SourceType(StrEnum):
    """Origin of the content ingestion."""

    FILE = "file"
    CRAWL = "crawl"
    DB = "db"


class ContentType(StrEnum):
    """Type of content being processed."""

    ARTICLE = "article"
    CODE = "code"
    DOC = "doc"
    SOCIAL = "social"


class ContentMetadata(BaseModel):
    """Metadata associated with a content document."""

    model_config = ConfigDict(frozen=True)

    source_type: SourceType
    content_type: ContentType
    source_url: str | None = None
    source_path: str | None = None
    filename: str | None = None
    file_size: int | None = None
    mime_type: str | None = None
    title: str | None = None
    description: str | None = None
    author: str | None = None
    created_at: datetime | None = None
    modified_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)
    custom_fields: dict[str, str] = Field(default_factory=dict)


class ContentChunk(BaseModel):
    """A chunk/segment of content extracted from a document."""

    model_config = ConfigDict(frozen=True)

    chunk_id: UUID = Field(default_factory=uuid4)
    document_id: UUID
    chunk_index: int
    content: str
    start_char: int
    end_char: int
    token_count: int | None = None


class ContentEntity(BaseModel):
    """An entity (person, organization, concept) extracted from content."""

    model_config = ConfigDict(frozen=True)

    entity_id: UUID = Field(default_factory=uuid4)
    document_id: UUID
    entity_type: str
    entity_value: str
    summary: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    metadata: dict[str, str] = Field(default_factory=dict)


class ContentRelation(BaseModel):
    """A relationship between two entities or documents."""

    model_config = ConfigDict(frozen=True)

    relation_id: UUID = Field(default_factory=uuid4)
    source_id: UUID
    target_id: UUID
    relation_type: str
    properties: dict[str, str] = Field(default_factory=dict)


class ContentVectors(BaseModel):
    """Vector embeddings for semantic search."""

    model_config = ConfigDict(frozen=True)

    vector_id: UUID = Field(default_factory=uuid4)
    chunk_id: UUID
    vector: list[float]
    model_name: str
    dimensions: int

    @field_validator("vector")
    @classmethod
    def validate_vector_dimensions(cls, v: list[float]) -> list[float]:
        """Ensure vector is not empty."""
        if not v:
            raise ValueError("Vector cannot be empty")
        return v


class ContentBody(BaseModel):
    """The main body content of a document."""

    model_config = ConfigDict(frozen=True)

    body_id: UUID = Field(default_factory=uuid4)
    document_id: UUID
    content: str
    raw_content: str | None = None
    language: str | None = None


class ContentDocument(BaseModel):
    """Complete content document with all associated data."""

    model_config = ConfigDict(frozen=True)

    document_id: UUID = Field(default_factory=uuid4)
    metadata: ContentMetadata
    body: ContentBody
    chunks: list[ContentChunk] = Field(default_factory=list)
    entities: list[ContentEntity] = Field(default_factory=list)
    relations: list[ContentRelation] = Field(default_factory=list)
    vectors: list[ContentVectors] = Field(default_factory=list)

    @field_validator("chunks", "entities", "relations", "vectors")
    @classmethod
    def validate_lists_not_empty_for_processed(
        cls, v: list[Any], info: ValidationInfo
    ) -> list[Any]:
        """Warn if lists are empty for processed documents."""
        return v


# Type aliases for cleaner imports
DocumentId = Annotated[UUID, "document identifier"]
ChunkId = Annotated[UUID, "chunk identifier"]
EntityId = Annotated[UUID, "entity identifier"]
VectorId = Annotated[UUID, "vector identifier"]
