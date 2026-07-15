"""Schema definitions for the AI Knowledge Base."""

from src.schemas.content import (
    ContentBody,
    ContentChunk,
    ContentDocument,
    ContentEntity,
    ContentMetadata,
    ContentRelation,
    ContentType,
    ContentVectors,
    SourceType,
)
from src.schemas.events import EventType, KafkaEvent

__all__ = [
    # Content Schema
    "SourceType",
    "ContentType",
    "ContentMetadata",
    "ContentChunk",
    "ContentEntity",
    "ContentRelation",
    "ContentVectors",
    "ContentBody",
    "ContentDocument",
    # Event Schema
    "EventType",
    "KafkaEvent",
]
