"""Unit tests for schema definitions."""

from datetime import datetime
from uuid import UUID

import pytest

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
from src.schemas.events import (
    DocumentExportedPayload,
    DocumentFailedPayload,
    DocumentIngestedPayload,
    DocumentProcessedPayload,
    EventType,
    KafkaEvent,
)


class TestContentSchemas:
    """Tests for unified content schemas."""

    def test_source_type_enum_values(self):
        """Verify SourceType enum values."""
        assert SourceType.FILE.value == "file"
        assert SourceType.CRAWL.value == "crawl"
        assert SourceType.DB.value == "db"

    def test_content_type_enum_values(self):
        """Verify ContentType enum values."""
        assert ContentType.ARTICLE.value == "article"
        assert ContentType.CODE.value == "code"
        assert ContentType.DOC.value == "doc"
        assert ContentType.SOCIAL.value == "social"

    def test_content_metadata_creation(self):
        """Test ContentMetadata model creation."""
        metadata = ContentMetadata(
            source_type=SourceType.FILE,
            content_type=ContentType.DOC,
            filename="test.pdf",
            title="Test Document",
            author="Test Author",
            tags=["test", "document"],
        )
        assert metadata.source_type == SourceType.FILE
        assert metadata.content_type == ContentType.DOC
        assert metadata.filename == "test.pdf"
        assert metadata.title == "Test Document"
        assert metadata.author == "Test Author"
        assert metadata.tags == ["test", "document"]
        assert metadata.custom_fields == {}

    def test_content_metadata_immutable(self):
        """Verify ContentMetadata is immutable (frozen)."""
        metadata = ContentMetadata(
            source_type=SourceType.FILE,
            content_type=ContentType.DOC,
        )
        with pytest.raises(Exception):  # pydantic ValidationError
            metadata.filename = "changed.pdf"

    def test_content_chunk_creation(self):
        """Test ContentChunk model creation."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")
        chunk = ContentChunk(
            document_id=doc_id,
            chunk_index=0,
            content="This is a test chunk.",
            start_char=0,
            end_char=20,
            token_count=5,
        )
        assert chunk.document_id == doc_id
        assert chunk.chunk_index == 0
        assert chunk.content == "This is a test chunk."
        assert chunk.token_count == 5
        assert chunk.chunk_id is not None

    def test_content_entity_creation(self):
        """Test ContentEntity model creation."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")
        entity = ContentEntity(
            document_id=doc_id,
            entity_type="PERSON",
            entity_value="John Doe",
            confidence=0.95,
        )
        assert entity.entity_type == "PERSON"
        assert entity.entity_value == "John Doe"
        assert entity.confidence == 0.95
        assert entity.entity_id is not None

    def test_content_entity_confidence_bounds(self):
        """Test ContentEntity confidence is bounded 0-1."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")
        # Valid confidence
        entity = ContentEntity(
            document_id=doc_id,
            entity_type="PERSON",
            entity_value="John Doe",
            confidence=0.0,
        )
        assert entity.confidence == 0.0

        entity = ContentEntity(
            document_id=doc_id,
            entity_type="PERSON",
            entity_value="John Doe",
            confidence=1.0,
        )
        assert entity.confidence == 1.0

    def test_content_relation_creation(self):
        """Test ContentRelation model creation."""
        source_id = UUID("12345678-1234-5678-1234-567812345678")
        target_id = UUID("87654321-4321-8765-4321-876543218765")
        relation = ContentRelation(
            source_id=source_id,
            target_id=target_id,
            relation_type="AUTHORED_BY",
            properties={"weight": "1.0"},
        )
        assert relation.source_id == source_id
        assert relation.target_id == target_id
        assert relation.relation_type == "AUTHORED_BY"

    def test_content_vectors_creation(self):
        """Test ContentVectors model creation."""
        chunk_id = UUID("12345678-1234-5678-1234-567812345678")
        vector_data = ContentVectors(
            chunk_id=chunk_id,
            vector=[0.1, 0.2, 0.3, 0.4],
            model_name="all-MiniLM-L6-v2",
            dimensions=384,
        )
        assert vector_data.chunk_id == chunk_id
        assert vector_data.vector == [0.1, 0.2, 0.3, 0.4]
        assert vector_data.model_name == "all-MiniLM-L6-v2"

    def test_content_vectors_empty_rejected(self):
        """Test ContentVectors rejects empty vectors."""
        chunk_id = UUID("12345678-1234-5678-1234-567812345678")
        with pytest.raises(ValueError):
            ContentVectors(
                chunk_id=chunk_id,
                vector=[],
                model_name="all-MiniLM-L6-v2",
                dimensions=384,
            )

    def test_content_body_creation(self):
        """Test ContentBody model creation."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")
        body = ContentBody(
            document_id=doc_id,
            content="This is the main content.",
            raw_content="<p>This is the main content.</p>",
            language="en",
        )
        assert body.document_id == doc_id
        assert body.content == "This is the main content."
        assert body.raw_content == "<p>This is the main content.</p>"
        assert body.language == "en"

    def test_content_document_complete(self):
        """Test ContentDocument with all components."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")
        metadata = ContentMetadata(
            source_type=SourceType.FILE,
            content_type=ContentType.DOC,
            filename="test.pdf",
        )
        body = ContentBody(
            document_id=doc_id,
            content="Test content body",
        )
        chunk = ContentChunk(
            document_id=doc_id,
            chunk_index=0,
            content="Test content body",
            start_char=0,
            end_char=17,
        )
        entity = ContentEntity(
            document_id=doc_id,
            entity_type="TEST",
            entity_value="TestEntity",
            confidence=0.9,
        )

        doc = ContentDocument(
            document_id=doc_id,
            metadata=metadata,
            body=body,
            chunks=[chunk],
            entities=[entity],
        )
        assert doc.document_id == doc_id
        assert len(doc.chunks) == 1
        assert len(doc.entities) == 1
        assert doc.metadata.filename == "test.pdf"


class TestEventSchemas:
    """Tests for Kafka event schemas."""

    def test_event_type_enum_values(self):
        """Verify EventType enum values."""
        assert EventType.DOCUMENT_INGESTED.value == "document.ingested"
        assert EventType.DOCUMENT_PROCESSED.value == "document.processed"
        assert EventType.DOCUMENT_FAILED.value == "document.failed"
        assert EventType.DOCUMENT_EXPORTED.value == "document.exported"

    def test_kafka_event_creation(self):
        """Test KafkaEvent model creation."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")
        event = KafkaEvent(
            event_type=EventType.DOCUMENT_INGESTED,
            document_id=doc_id,
            payload={"file_path": "/test/file.pdf"},
        )
        assert event.event_type == EventType.DOCUMENT_INGESTED
        assert event.document_id == doc_id
        assert event.payload["file_path"] == "/test/file.pdf"
        assert event.event_id is not None
        assert event.timestamp is not None

    def test_kafka_event_is_error(self):
        """Test KafkaEvent is_error property."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")

        ingested_event = KafkaEvent(
            event_type=EventType.DOCUMENT_INGESTED,
            document_id=doc_id,
        )
        assert not ingested_event.is_error

        failed_event = KafkaEvent(
            event_type=EventType.DOCUMENT_FAILED,
            document_id=doc_id,
        )
        assert failed_event.is_error

    def test_kafka_event_with_error(self):
        """Test KafkaEvent with_error method."""
        doc_id = UUID("12345678-1234-5678-1234-567812345678")
        event = KafkaEvent(
            event_type=EventType.DOCUMENT_PROCESSED,
            document_id=doc_id,
        )
        error_event = event.with_error("ParseError", "Failed to parse PDF")
        assert error_event.is_error
        assert error_event.error is not None
        assert error_event.error["type"] == "ParseError"
        assert error_event.error["message"] == "Failed to parse PDF"

    def test_document_ingested_payload(self):
        """Test DocumentIngestedPayload schema."""
        payload = DocumentIngestedPayload(
            file_path="/docs/test.pdf",
            file_name="test.pdf",
            file_size=1024,
            mime_type="application/pdf",
            source_type="file",
        )
        assert payload.file_name == "test.pdf"
        assert payload.file_size == 1024

    def test_document_processed_payload(self):
        """Test DocumentProcessedPayload schema."""
        payload = DocumentProcessedPayload(
            chunk_count=10,
            entity_count=5,
            relation_count=3,
            vector_dimensions=384,
            processing_time_ms=1500,
        )
        assert payload.chunk_count == 10
        assert payload.entity_count == 5
        assert payload.processing_time_ms == 1500

    def test_document_failed_payload(self):
        """Test DocumentFailedPayload schema."""
        payload = DocumentFailedPayload(
            error_type="ParseError",
            error_message="Unable to extract text",
            stage="extraction",
            retryable=True,
        )
        assert payload.error_type == "ParseError"
        assert payload.retryable is True

    def test_document_exported_payload(self):
        """Test DocumentExportedPayload schema."""
        payload = DocumentExportedPayload(
            export_format="json",
            export_path="/exports/doc.json",
            export_size=2048,
        )
        assert payload.export_format == "json"
        assert payload.export_size == 2048

    def test_kafka_event_version_default(self):
        """Test KafkaEvent has default version."""
        event = KafkaEvent(event_type=EventType.DOCUMENT_INGESTED)
        assert event.version == "1.0"

    def test_kafka_event_source_default(self):
        """Test KafkaEvent has default source."""
        event = KafkaEvent(event_type=EventType.DOCUMENT_INGESTED)
        assert event.source == "knowforge"

    def test_kafka_event_correlation_id(self):
        """Test KafkaEvent correlation_id handling."""
        event = KafkaEvent(
            event_type=EventType.DOCUMENT_PROCESSED,
            correlation_id="corr-123-abc",
        )
        assert event.correlation_id == "corr-123-abc"

    def test_kafka_event_timestamp_default(self):
        """Test KafkaEvent timestamp is auto-generated."""
        before = datetime.utcnow()
        event = KafkaEvent(event_type=EventType.DOCUMENT_INGESTED)
        after = datetime.utcnow()
        assert before <= event.timestamp <= after
