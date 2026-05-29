"""Kafka integration for AI Knowledge Base."""

from src.kafka.producer import KafkaEventProducer
from src.kafka.consumer import KafkaEventConsumer

__all__ = ["KafkaEventProducer", "KafkaEventConsumer"]