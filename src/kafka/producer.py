"""
Kafka Producer - Publish events to Kafka
"""
import asyncio
import json
from enum import StrEnum
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaProducer

from src.schemas.events import EventType
from src.utils.time import utc_now


class KafkaTopic(StrEnum):
    """Kafka topic names."""

    FILE = "file-events"
    CRAWL = "crawl-events"
    DB = "db-events"

    @classmethod
    def for_source(cls, source_type: str) -> "KafkaTopic":
        return getattr(cls, source_type.upper(), cls.FILE)


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer: AIOKafkaProducer | None = None
        self.topics = {
            "file": KafkaTopic.FILE.value,
            "crawl": KafkaTopic.CRAWL.value,
            "db": KafkaTopic.DB.value,
        }

    async def start(self) -> None:
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
        )
        await self.producer.start()

    async def stop(self) -> None:
        if self.producer:
            await self.producer.stop()
            self.producer = None

    async def publish_with_retry(
        self,
        topic: str,
        event: dict[str, Any],
        max_retries: int = 3,
        initial_backoff: float = 0.5,
    ) -> None:
        if not self.producer:
            raise RuntimeError("Producer not started")

        last_error: Exception | None = None
        for attempt in range(max_retries):
            try:
                await self.producer.send_and_wait(topic, event)
                return
            except Exception as exc:
                last_error = exc
                if attempt == max_retries - 1:
                    break
                await asyncio.sleep(initial_backoff * (2**attempt))

        if last_error:
            raise last_error

    async def publish_document_ingested(
        self,
        source_type: str,
        source_identifier: str,
        metadata: dict[str, Any],
    ) -> str:
        event = {
            "event_id": str(uuid4()),
            "event_type": EventType.DOCUMENT_INGESTED.value,
            "timestamp": utc_now().isoformat(),
            "source_type": source_type,
            "payload": {
                "source_identifier": source_identifier,
                "metadata": metadata,
            },
        }
        topic = KafkaTopic.for_source(source_type)
        await self.publish_with_retry(topic.value, event)
        return str(event["event_id"])
