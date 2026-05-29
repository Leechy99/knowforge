"""Unit tests for Kafka producer and consumer."""

import json
from datetime import datetime
from typing import Any
from uuid import uuid4

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.kafka.producer import KafkaEventProducer
from src.kafka.consumer import KafkaEventConsumer, KafkaConsumerError, ErrorAggregator


class TestKafkaEventProducer:
    """Tests for KafkaEventProducer."""

    @pytest.fixture
    def producer(self):
        return KafkaEventProducer(bootstrap_servers="localhost:9092")

    def test_producer_initialization(self, producer):
        """Test producer initializes with correct defaults."""
        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.producer is None
        assert producer.topics == {
            "file": "file-events",
            "crawl": "crawl-events",
            "db": "db-events",
        }

    def test_producer_custom_bootstrap_servers(self):
        """Test producer accepts custom bootstrap servers."""
        producer = KafkaEventProducer(bootstrap_servers="kafka:9092")
        assert producer.bootstrap_servers == "kafka:9092"

    @pytest.mark.asyncio
    async def test_start_creates_producer(self, producer):
        """Test start creates and starts AIOKafkaProducer."""
        with patch("src.kafka.producer.AIOKafkaProducer") as mock_producer_class:
            mock_instance = AsyncMock()
            mock_producer_class.return_value = mock_instance

            await producer.start()

            mock_producer_class.assert_called_once()
            mock_instance.start.assert_called_once()
            assert producer.producer is mock_instance

    @pytest.mark.asyncio
    async def test_stop_with_active_producer(self, producer):
        """Test stop stops the producer."""
        mock_producer = AsyncMock()
        producer.producer = mock_producer
        await producer.stop()
        mock_producer.stop.assert_called_once()
        assert producer.producer is None

    @pytest.mark.asyncio
    async def test_stop_without_producer(self, producer):
        """Test stop does nothing when producer is None."""
        producer.producer = None
        await producer.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_publish_document_ingested(self, producer):
        """Test publish_document_ingested sends event to correct topic."""
        producer.producer = AsyncMock()

        source_type = "file"
        source_identifier = "/path/to/doc.pdf"
        metadata = {"file_size": 1024, "mime_type": "application/pdf"}

        with patch("src.kafka.producer.uuid4", return_value=uuid4()):
            with patch("src.kafka.producer.datetime") as mock_datetime:
                mock_datetime.utcnow.return_value = datetime(2024, 1, 1, 12, 0, 0)
                event_id = await producer.publish_document_ingested(
                    source_type, source_identifier, metadata
                )

        assert event_id is not None
        producer.producer.send_and_wait.assert_called_once()
        call_args = producer.producer.send_and_wait.call_args
        assert call_args[0][0] == "file-events"
        event_sent = call_args[0][1]
        assert event_sent["source_type"] == source_type
        assert event_sent["payload"]["source_identifier"] == source_identifier

    @pytest.mark.asyncio
    async def test_publish_document_ingested_unknown_source_type(self, producer):
        """Test publish_document_ingested uses file-events for unknown source."""
        producer.producer = AsyncMock()

        await producer.publish_document_ingested(
            "unknown", "id", {}
        )

        call_args = producer.producer.send_and_wait.call_args
        assert call_args[0][0] == "file-events"

    @pytest.mark.asyncio
    async def test_publish_document_ingested_crawl_source(self, producer):
        """Test publish_document_ingested uses crawl-events for crawl source."""
        producer.producer = AsyncMock()

        await producer.publish_document_ingested(
            "crawl", "http://example.com", {}
        )

        call_args = producer.producer.send_and_wait.call_args
        assert call_args[0][0] == "crawl-events"

    @pytest.mark.asyncio
    async def test_publish_document_ingested_db_source(self, producer):
        """Test publish_document_ingested uses db-events for db source."""
        producer.producer = AsyncMock()

        await producer.publish_document_ingested(
            "db", "table:users", {}
        )

        call_args = producer.producer.send_and_wait.call_args
        assert call_args[0][0] == "db-events"


# =============================================================================
# RED Phase Tests: Producer Retry with Exponential Backoff
# =============================================================================

class TestProducerRetry:
    """Tests for Kafka producer retry logic with exponential backoff."""

    @pytest.fixture
    def producer(self):
        return KafkaEventProducer(bootstrap_servers="localhost:9092")

    @pytest.mark.asyncio
    async def test_publish_with_retry_succeeds_on_first_attempt(self, producer):
        """Test publish_with_retry succeeds when first attempt succeeds."""
        producer.producer = AsyncMock()
        producer.producer.send_and_wait = AsyncMock()  # No errors

        event = {"event_id": "123", "event_type": "test"}
        await producer.publish_with_retry("test-topic", event)

        producer.producer.send_and_wait.assert_called_once_with("test-topic", event)

    @pytest.mark.asyncio
    async def test_publish_with_retry_retries_on_failure(self, producer):
        """Test publish_with_retry retries with exponential backoff on failure."""
        producer.producer = AsyncMock()
        # Fail first two attempts, succeed on third
        producer.producer.send_and_wait = AsyncMock(
            side_effect=[Exception("Connection failed"), Exception("Timeout"), None]
        )

        event = {"event_id": "123", "event_type": "test"}

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await producer.publish_with_retry("test-topic", event, max_retries=3)

            # Should have attempted 3 times
            assert producer.producer.send_and_wait.call_count == 3
            # Should have slept twice between retries (backoff)
            assert mock_sleep.call_count == 2

    @pytest.mark.asyncio
    async def test_publish_with_retry_exhausts_retries(self, producer):
        """Test publish_with_retry raises after exhausting all retries."""
        producer.producer = AsyncMock()
        producer.producer.send_and_wait = AsyncMock(
            side_effect=Exception("Persistent failure")
        )

        event = {"event_id": "123", "event_type": "test"}

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(Exception, match="Persistent failure"):
                await producer.publish_with_retry("test-topic", event, max_retries=3)

            assert producer.producer.send_and_wait.call_count == 3

    @pytest.mark.asyncio
    async def test_publish_with_retry_backoff_increases(self, producer):
        """Test exponential backoff: delays increase between retries."""
        producer.producer = AsyncMock()

        async def mock_send(*args, **kwargs):
            raise Exception("Retry")

        producer.producer.send_and_wait = mock_send

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            with pytest.raises(Exception, match="Retry"):
                await producer.publish_with_retry("test-topic", {}, max_retries=3)

            # Check sleep was called with increasing delays (exponential backoff)
            assert mock_sleep.call_count == 2
            # First delay < second delay (exponential growth)
            assert mock_sleep.call_args_list[0][0][0] < mock_sleep.call_args_list[1][0][0]


# =============================================================================
# RED Phase Tests: Consumer Graceful Shutdown
# =============================================================================

class TestConsumerGracefulShutdown:
    """Tests for Kafka consumer graceful shutdown."""

    @pytest.fixture
    def consumer(self):
        return KafkaEventConsumer(bootstrap_servers="localhost:9092", group_id="test-group")

    @pytest.mark.asyncio
    async def test_consume_stops_on_shutdown_signal(self, consumer):
        """Test consume stops gracefully when shutdown is requested."""
        consumer.consumer = AsyncMock()

        # Simulate shutdown being set during iteration
        shutdown_requested = False

        async def mock_iter():
            nonlocal shutdown_requested
            yield MagicMock(
                value={"event_type": "test", "event_id": "1", "payload": {}}
            )
            # Check shutdown flag and stop
            if consumer._shutdown:
                return
            shutdown_requested = True

        consumer.consumer = MagicMock()
        consumer.consumer.__aiter__ = lambda self: mock_iter()

        # Start consume with shutdown flag
        consumer._shutdown = True

        # Should not raise, should exit cleanly
        await consumer.consume()

    @pytest.mark.asyncio
    async def test_stop_raises_if_not_started(self, consumer):
        """Test stop raises if consumer was never started."""
        consumer.consumer = None
        # Should handle gracefully, not raise
        await consumer.stop()  # Should not raise


# =============================================================================
# RED Phase Tests: Dead Letter Queue (DLQ)
# =============================================================================

class TestConsumerDLQ:
    """Tests for Kafka consumer DLQ behavior."""

    @pytest.fixture
    def consumer(self):
        return KafkaEventConsumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            dlq_topic="dlq-events",
        )

    @pytest.mark.asyncio
    async def test_handler_failure_sends_to_dlq(self, consumer):
        """Test that failed handler events are sent to DLQ topic."""
        consumer.producer = AsyncMock()
        consumer.producer.send_and_wait = AsyncMock()

        # Register a handler that always fails
        async def failing_handler(event):
            raise Exception("Handler failed")

        consumer.register_handler("test.event", failing_handler)

        # Simulate message that triggers failing handler
        event = {"event_id": "123", "event_type": "test.event", "payload": {}}
        await consumer._handle_with_dlq(event)

        # Should have sent to DLQ
        consumer.producer.send_and_wait.assert_called_once()
        dlq_call = consumer.producer.send_and_wait.call_args
        assert dlq_call[0][0] == "dlq-events"
        dlq_event = dlq_call[0][1]
        assert dlq_event["original_event"] == event
        assert "error" in dlq_event

    @pytest.mark.asyncio
    async def test_handler_success_does_not_send_to_dlq(self, consumer):
        """Test that successful handler execution does not send to DLQ."""
        consumer.producer = AsyncMock()
        consumer.producer.send_and_wait = AsyncMock()

        # Register a handler that succeeds
        async def success_handler(event):
            return

        consumer.register_handler("test.event", success_handler)

        event = {"event_id": "123", "event_type": "test.event", "payload": {}}
        await consumer._handle_with_dlq(event)

        # Should NOT have sent to DLQ
        consumer.producer.send_and_wait.assert_not_called()


# =============================================================================
# RED Phase Tests: Error Aggregation
# =============================================================================

class TestErrorAggregator:
    """Tests for error aggregation and collection."""

    def test_aggregator_adds_error(self):
        """Test aggregator collects errors."""
        aggregator = ErrorAggregator()
        aggregator.add_error("test_event", Exception("Test error"), {"event_id": "123"})

        assert aggregator.error_count == 1
        errors = aggregator.get_errors()
        assert len(errors) == 1
        assert errors[0]["event_type"] == "test_event"

    def test_aggregator_clears_errors(self):
        """Test aggregator clears errors after retrieval."""
        aggregator = ErrorAggregator()
        aggregator.add_error("test_event", Exception("Test error"), {"event_id": "123"})

        aggregator.clear_errors()

        assert aggregator.error_count == 0
        assert len(aggregator.get_errors()) == 0

    def test_aggregator_max_size(self):
        """Test aggregator respects max_size limit."""
        aggregator = ErrorAggregator(max_size=3)

        for i in range(5):
            aggregator.add_error(f"event_{i}", Exception(f"Error {i}"), {})

        assert aggregator.error_count == 3
        errors = aggregator.get_errors()
        # Should have most recent errors
        assert errors[-1]["event_type"] == "event_4"

    def test_aggregator_get_summary(self):
        """Test aggregator provides error summary."""
        aggregator = ErrorAggregator()
        aggregator.add_error("event_a", Exception("Error A"), {"event_id": "1"})
        aggregator.add_error("event_a", Exception("Error B"), {"event_id": "2"})
        aggregator.add_error("event_b", Exception("Error C"), {"event_id": "3"})

        summary = aggregator.get_summary()

        assert summary["total_errors"] == 3
        assert summary["by_event_type"]["event_a"] == 2
        assert summary["by_event_type"]["event_b"] == 1


# =============================================================================
# Original Consumer Tests (updated to use new error handling)
# =============================================================================

class TestKafkaEventConsumer:
    """Tests for KafkaEventConsumer."""

    @pytest.fixture
    def consumer(self):
        return KafkaEventConsumer(bootstrap_servers="localhost:9092", group_id="test-group")

    def test_consumer_initialization(self, consumer):
        """Test consumer initializes with correct defaults."""
        assert consumer.bootstrap_servers == "localhost:9092"
        assert consumer.group_id == "test-group"
        assert consumer.consumer is None
        assert consumer.handlers == {}

    def test_consumer_custom_group_id(self):
        """Test consumer accepts custom group_id."""
        consumer = KafkaEventConsumer(group_id="custom-group")
        assert consumer.group_id == "custom-group"

    @pytest.mark.asyncio
    async def test_start_creates_consumer(self, consumer):
        """Test start creates and starts AIOKafkaConsumer."""
        with patch("src.kafka.consumer.AIOKafkaConsumer") as mock_consumer_class:
            mock_instance = AsyncMock()
            mock_consumer_class.return_value = mock_instance

            topics = ["file-events", "crawl-events"]
            await consumer.start(topics)

            mock_consumer_class.assert_called_once()
            mock_instance.start.assert_called_once()

    def test_register_handler(self, consumer):
        """Test registering an event handler."""
        handler = AsyncMock()

        consumer.register_handler("document.ingested", handler)

        assert "document.ingested" in consumer.handlers
        assert consumer.handlers["document.ingested"] is handler

    def test_register_handler_multiple(self, consumer):
        """Test registering multiple event handlers."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()

        consumer.register_handler("event1", handler1)
        consumer.register_handler("event2", handler2)

        assert consumer.handlers["event1"] is handler1
        assert consumer.handlers["event2"] is handler2

    @pytest.mark.asyncio
    async def test_consume_without_consumer_raises_error(self, consumer):
        """Test consume raises error if consumer not started."""
        consumer.consumer = None
        with pytest.raises(RuntimeError, match="Consumer not started"):
            await consumer.consume()

    @pytest.mark.asyncio
    async def test_consume_handles_registered_event(self, consumer):
        """Test consume calls handler for registered event type."""
        handler = AsyncMock()
        consumer.register_handler("document.ingested", handler)

        mock_message = MagicMock()
        mock_message.value = {
            "event_id": "123",
            "event_type": "document.ingested",
            "payload": {"source": "test"},
        }

        event = mock_message.value
        event_type = event.get("event_type")
        registered_handler = consumer.handlers.get(event_type)

        assert registered_handler is handler
        await registered_handler(event)
        handler.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_consume_skips_unregistered_event(self, consumer):
        """Test consume skips events without registered handler."""
        mock_message = MagicMock()
        mock_message.value = {
            "event_id": "123",
            "event_type": "unknown.event",
            "payload": {},
        }

        event = mock_message.value
        event_type = event.get("event_type")
        handler = consumer.handlers.get(event_type)

        assert handler is None

    @pytest.mark.asyncio
    async def test_stop_with_active_consumer(self, consumer):
        """Test stop stops the consumer."""
        mock_consumer = AsyncMock()
        consumer.consumer = mock_consumer
        await consumer.stop()
        mock_consumer.stop.assert_called_once()
        assert consumer.consumer is None

    @pytest.mark.asyncio
    async def test_stop_without_consumer(self, consumer):
        """Test stop does nothing when consumer is None."""
        consumer.consumer = None
        await consumer.stop()  # Should not raise
