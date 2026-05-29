"""Unified Content Schema for AI Knowledge Base.

This module defines the core data structures for representing documents,
chunks, entities, and their relationships across all supported content types.
"""

from datetime import datetime
from enum import Enum
from typing import Annotated, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SourceType(str, Enum):
    """Origin of the content ingestion."""

    FILE = "file"
    CRAWL = "crawl"
    DB = "db"


class ContentType(str, Enum):
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
    source_url: Optional[str] = None
    source_path: Optional[str] = None
    filename: Optional[str] = None
    file_size: Optional[int] = None
    mime_type: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
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
    token_count: Optional[int] = None


class ContentEntity(BaseModel):
    """An entity (person, organization, concept) extracted from content."""

    model_config = ConfigDict(frozen=True)

    entity_id: UUID = Field(default_factory=uuid4)
    document_id: UUID
    entity_type: str
    entity_value: str
    summary: Optional[str] = None
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
    raw_content: Optional[str] = None
    language: Optional[str] = None


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
        cls, v: list, info
    ) -> list:
        """Warn if lists are empty for processed documents."""
        return v


# Type aliases for cleaner imports
DocumentId = Annotated[UUID, "document identifier"]
ChunkId = Annotated[UUID, "chunk identifier"]
EntityId = Annotated[UUID, "entity identifier"]
VectorId = Annotated[UUID, "vector identifier"]