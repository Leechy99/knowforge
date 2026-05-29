"""
Kafka Consumer - Consume events from Kafka with parallel processing pipeline
"""
import json
import logging
from collections import deque
from datetime import datetime
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)

EventHandler = Callable[[dict[str, Any]], Awaitable[Any]]


class KafkaConsumerError(Exception):
    """Raised for Kafka consumer failures."""


class ErrorAggregator:
    """Bounded in-memory collection of consumer errors."""

    def __init__(self, max_size: int = 100):
        self._errors: deque[dict[str, Any]] = deque(maxlen=max_size)

    @property
    def error_count(self) -> int:
        return len(self._errors)

    def add_error(
        self,
        event_type: str,
        error: Exception,
        event: dict[str, Any] | None = None,
    ) -> None:
        self._errors.append(
            {
                "event_type": event_type,
                "error": str(error),
                "error_type": type(error).__name__,
                "event": event or {},
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

    def get_errors(self) -> list[dict[str, Any]]:
        return list(self._errors)

    def clear_errors(self) -> None:
        self._errors.clear()

    def get_summary(self) -> dict[str, Any]:
        by_event_type: dict[str, int] = {}
        for error in self._errors:
            event_type = error["event_type"]
            by_event_type[event_type] = by_event_type.get(event_type, 0) + 1
        return {
            "total_errors": len(self._errors),
            "by_event_type": by_event_type,
        }


class _NoopCoordinator:
    async def start_all(self) -> None:
        return None

    async def stop_all(self) -> None:
        return None

    async def route_event(self, event: dict[str, Any]) -> None:
        return None


def _build_default_coordinator() -> Any:
    try:
        from src.processors.parallel.coordinator import PipelineCoordinator

        return PipelineCoordinator()
    except ModuleNotFoundError as exc:
        logger.warning("Pipeline coordinator unavailable: %s", exc)
        return _NoopCoordinator()


class KafkaEventConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "ai-kb-consumers",
        dlq_topic: str = "dlq-events",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.dlq_topic = dlq_topic
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: Any | None = None
        self.coordinator = _build_default_coordinator()
        self.handlers: dict[str, EventHandler] = {}
        self.error_aggregator = ErrorAggregator()
        self._running = False
        self._shutdown = False

    def register_handler(self, event_type: str, handler: EventHandler) -> None:
        self.handlers[event_type] = handler

    async def start(self, topics: list[str]):
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
        )
        await self.consumer.start()
        await self.coordinator.start_all()
        self._running = True
        self._shutdown = False

    async def stop(self):
        self._running = False
        self._shutdown = True
        await self.coordinator.stop_all()
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None

    async def _handle_with_dlq(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type", "")
        handler = self.handlers.get(event_type)

        try:
            if handler:
                await handler(event)
            else:
                await self.coordinator.route_event(event)
        except Exception as exc:
            self.error_aggregator.add_error(event_type, exc, event)
            if self.producer:
                await self.producer.send_and_wait(
                    self.dlq_topic,
                    {
                        "original_event": event,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )
            else:
                logger.exception("Error handling event %s", event_type)

    async def consume(self):
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        async for message in self.consumer:
            if self._shutdown:
                break
            event = message.value
            await self._handle_with_dlq(event)
