"""Task Queue - Thread-safe priority queue with backpressure"""
import asyncio
import hashlib
from collections import deque
from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class Priority(IntEnum):
    HIGH = 0
    NORMAL = 1
    LOW = 2


@dataclass
class Task:
    priority: Priority
    data: dict[str, Any]
    content_hash: str | None = None
    task_id: str | None = None


@dataclass
class QueueStats:
    enqueued: int = 0
    dequeued: int = 0
    dropped: int = 0


class TaskQueue:
    """Thread-safe task queue with priority and backpressure."""

    def __init__(
        self,
        max_size: int = 1000,
        enable_dedup: bool = True,
    ):
        self.max_size = max_size
        self.enable_dedup = enable_dedup
        self._queues: dict[Priority, deque[Task]] = {
            Priority.HIGH: deque(),
            Priority.NORMAL: deque(),
            Priority.LOW: deque(),
        }
        self._seen_hashes: set[str] = set()
        self._stats = QueueStats()
        self._lock = asyncio.Lock()

    def _compute_hash(self, data: dict[str, Any]) -> str:
        """Compute content hash for deduplication."""
        content = str(data.get("content", "")) + str(data.get("source_id", ""))
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    async def put(self, data: dict[str, Any], priority: Priority = Priority.NORMAL) -> bool:
        """Add task to queue. Returns False if dropped (duplicate or full)."""
        async with self._lock:
            if self.enable_dedup:
                content_hash = self._compute_hash(data)
                if content_hash in self._seen_hashes:
                    self._stats.dropped += 1
                    return False
                self._seen_hashes.add(content_hash)

            total_size = sum(len(q) for q in self._queues.values())
            if total_size >= self.max_size:
                self._stats.dropped += 1
                return False

            task = Task(priority=priority, data=data, content_hash=content_hash if self.enable_dedup else None)
            self._queues[priority].append(task)
            self._stats.enqueued += 1
            return True

    async def get(self) -> Task | None:
        """Get highest priority task. Returns None if empty."""
        async with self._lock:
            for priority in [Priority.HIGH, Priority.NORMAL, Priority.LOW]:
                if self._queues[priority]:
                    task = self._queues[priority].popleft()
                    self._stats.dequeued += 1
                    if task.content_hash and task.content_hash in self._seen_hashes:
                        self._seen_hashes.discard(task.content_hash)
                    return task
            return None

    def size(self) -> int:
        """Return total number of tasks."""
        return sum(len(q) for q in self._queues.values())

    def is_full(self) -> bool:
        """Return True if queue is at max capacity."""
        return self.size() >= self.max_size

    @property
    def stats(self) -> QueueStats:
        """Return queue statistics."""
        return self._stats
