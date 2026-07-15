"""Worker Pool - Manages concurrent worker execution"""
import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from types import TracebackType
from typing import Any

from src.utils.time import utc_now


class WorkerStatus(Enum):
    IDLE = "idle"
    BUSY = "busy"
    STOPPED = "stopped"


@dataclass(frozen=True)
class WorkerInfo:
    worker_id: int
    status: WorkerStatus
    tasks_completed: int = 0
    tasks_failed: int = 0


@dataclass(frozen=True)
class WorkerFailure:
    worker_id: int
    error_type: str
    message: str
    failed_at: datetime


class WorkerPool:
    """Manages a pool of concurrent workers."""

    def __init__(
        self,
        max_workers: int = 4,
        name: str = "WorkerPool",
        max_failures: int = 100,
    ):
        self.max_workers = max_workers
        self.name = name
        self._workers: list[asyncio.Task[None]] = []
        self._tasks: asyncio.Queue[
            tuple[Callable[..., Awaitable[Any]], tuple[Any, ...], dict[str, Any]]
        ] = asyncio.Queue()
        self._worker_infos: list[WorkerInfo] = []
        self._shutdown = False
        self._worker_count = 0
        self._failures: deque[WorkerFailure] = deque(maxlen=max_failures)

    async def __aenter__(self) -> "WorkerPool":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.shutdown()

    async def start(self) -> None:
        """Start all workers."""
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._workers.append(worker)
            self._worker_infos.append(
                WorkerInfo(worker_id=i, status=WorkerStatus.IDLE)
            )

    async def _worker_loop(self, worker_id: int) -> None:
        """Worker coroutine that processes tasks."""
        while not self._shutdown:
            try:
                task_data = await asyncio.wait_for(
                    self._tasks.get(), timeout=1.0
                )
                func, args, kwargs = task_data
                current_info = self._worker_infos[worker_id]
                self._worker_infos[worker_id] = WorkerInfo(
                    worker_id=worker_id,
                    status=WorkerStatus.BUSY,
                    tasks_completed=current_info.tasks_completed,
                    tasks_failed=current_info.tasks_failed,
                )
                try:
                    await func(*args, **kwargs)
                except Exception as exc:
                    self._failures.append(
                        WorkerFailure(
                            worker_id=worker_id,
                            error_type=type(exc).__name__,
                            message=str(exc),
                            failed_at=utc_now(),
                        )
                    )
                    current_info = self._worker_infos[worker_id]
                    self._worker_infos[worker_id] = WorkerInfo(
                        worker_id=worker_id,
                        status=WorkerStatus.IDLE,
                        tasks_completed=current_info.tasks_completed,
                        tasks_failed=current_info.tasks_failed + 1,
                    )
                else:
                    current_info = self._worker_infos[worker_id]
                    self._worker_infos[worker_id] = WorkerInfo(
                        worker_id=worker_id,
                        status=WorkerStatus.IDLE,
                        tasks_completed=current_info.tasks_completed + 1,
                        tasks_failed=current_info.tasks_failed,
                    )
                finally:
                    self._tasks.task_done()
            except TimeoutError:
                continue

    async def submit(
        self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> None:
        """Submit a task to the pool."""
        await self._tasks.put((func, args, kwargs))

    def get_workers(self) -> list[WorkerInfo]:
        """Return status of all workers."""
        return list(self._worker_infos)

    def get_failures(self) -> list[WorkerFailure]:
        """Return a snapshot of recent task failures."""
        return list(self._failures)

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the pool gracefully."""
        self._shutdown = True
        await asyncio.wait(self._workers, timeout=timeout)
