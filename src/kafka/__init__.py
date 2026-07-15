"""Kafka integration for AI Knowledge Base."""

from src.kafka.consumer import KafkaEventConsumer
from src.kafka.producer import KafkaEventProducer

__all__ = ["KafkaEventProducer", "KafkaEventConsumer"]
