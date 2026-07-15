"""Kafka Event Schema for AI Knowledge Base.

This module defines event structures for Kafka message payloads
used in async document processing pipeline.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from src.utils.time import utc_now


class EventType(StrEnum):
    """Types of events emitted during document processing."""

    DOCUMENT_INGESTED = "document.ingested"
    DOCUMENT_PROCESSED = "document.processed"
    DOCUMENT_FAILED = "document.failed"
    DOCUMENT_EXPORTED = "document.exported"


class KafkaEvent(BaseModel):
    """Kafka event message envelope."""

    model_config = ConfigDict(frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    timestamp: datetime = Field(default_factory=utc_now)
    version: str = "1.0"
    source: str = "knowforge"

    # Document reference
    document_id: UUID | None = None
    correlation_id: str | None = None

    # Event-specific payload
    payload: dict[str, Any] = Field(default_factory=dict)

    # Error information (for failed events)
    error: dict[str, Any] | None = None

    @property
    def is_error(self) -> bool:
        """Check if this is an error event."""
        return self.event_type == EventType.DOCUMENT_FAILED or self.error is not None

    def with_error(self, error_type: str, error_message: str) -> "KafkaEvent":
        """Create a new event with error information."""
        return KafkaEvent(
            event_id=self.event_id,
            event_type=self.event_type,
            timestamp=self.timestamp,
            version=self.version,
            source=self.source,
            document_id=self.document_id,
            correlation_id=self.correlation_id,
            payload=self.payload,
            error={"type": error_type, "message": error_message},
        )


class DocumentIngestedPayload(BaseModel):
    """Payload for document ingested event."""

    file_path: str
    file_name: str
    file_size: int
    mime_type: str
    source_type: str


class DocumentProcessedPayload(BaseModel):
    """Payload for document processed event."""

    chunk_count: int
    entity_count: int
    relation_count: int
    vector_dimensions: int
    processing_time_ms: int


class DocumentFailedPayload(BaseModel):
    """Payload for document failed event."""

    error_type: str
    error_message: str
    stage: str
    retryable: bool = False


class DocumentExportedPayload(BaseModel):
    """Payload for document exported event."""

    export_format: str
    export_path: str
    export_size: int
