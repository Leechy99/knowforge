"""Processor Node - Base class for parallel processor workers"""
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass, field
from typing import Any
import asyncio
import time


class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class NodeMetrics:
    processed_count: int = 0
    error_count: int = 0
    avg_process_time: float = 0.0


class ProcessorNode(ABC):
    """Base class for processor nodes in the parallel pipeline."""

    def __init__(
        self,
        name: str,
        max_retries: int = 3,
        base_delay: float = 1.0,
        backoff_factor: float = 2.0,
    ):
        self.name = name
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self._metrics = {"processed": 0, "errors": 0, "total_time": 0.0}
        self._running = False

    @property
    def health(self) -> HealthStatus:
        """Return current health status based on error rate."""
        if self._metrics["errors"] > 10:
            return HealthStatus.UNHEALTHY
        elif self._metrics["errors"] > 5:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    @property
    def metrics(self) -> NodeMetrics:
        """Return current metrics."""
        count = self._metrics["processed"]
        avg = self._metrics["total_time"] / count if count > 0 else 0.0
        return NodeMetrics(
            processed_count=count,
            error_count=self._metrics["errors"],
            avg_process_time=avg,
        )

    async def start(self) -> None:
        """Start the processor node."""
        self._running = True

    async def stop(self) -> None:
        """Stop the processor node gracefully."""
        self._running = False

    async def process_with_retry(self, data: dict[str, Any]) -> dict[str, Any]:
        """Process data with exponential backoff retry."""
        last_error = None
        for attempt in range(self.max_retries):
            try:
                start_time = time.perf_counter()
                result = await self.process(data)
                elapsed = time.perf_counter() - start_time
                self._metrics["processed"] += 1
                self._metrics["total_time"] += elapsed
                return result
            except Exception as e:
                last_error = e
                self._metrics["errors"] += 1
                if attempt < self.max_retries - 1:
                    delay = self.base_delay * (self.backoff_factor ** attempt)
                    await asyncio.sleep(delay)
        raise last_error

    @abstractmethod
    async def process(self, data: dict[str, Any]) -> dict[str, Any]:
        """Process the data. Must be implemented by subclasses."""
        pass