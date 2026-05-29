"""Worker Pool - Manages concurrent worker execution"""
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
import asyncio
from enum import Enum


class WorkerStatus(Enum):
    IDLE = "idle"
    BUSY = "busy"
    STOPPED = "stopped"


@dataclass(frozen=True)
class WorkerInfo:
    worker_id: int
    status: WorkerStatus
    tasks_completed: int = 0


class WorkerPool:
    """Manages a pool of concurrent workers."""

    def __init__(
        self,
        max_workers: int = 4,
        name: str = "WorkerPool",
    ):
        self.max_workers = max_workers
        self.name = name
        self._workers: list[asyncio.Task[None]] = []
        self._tasks: asyncio.Queue[tuple[Callable, tuple, dict]] = asyncio.Queue()
        self._worker_infos: list[WorkerInfo] = []
        self._shutdown = False
        self._worker_count = 0

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
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
                )
                try:
                    await func(*args, **kwargs)
                except Exception:
                    pass
                finally:
                    current_info = self._worker_infos[worker_id]
                    self._worker_infos[worker_id] = WorkerInfo(
                        worker_id=worker_id,
                        status=WorkerStatus.IDLE,
                        tasks_completed=current_info.tasks_completed + 1,
                    )
                    self._tasks.task_done()
            except asyncio.TimeoutError:
                continue

    async def submit(
        self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> None:
        """Submit a task to the pool."""
        await self._tasks.put((func, args, kwargs))

    def get_workers(self) -> list[WorkerInfo]:
        """Return status of all workers."""
        return list(self._worker_infos)

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the pool gracefully."""
        self._shutdown = True
        await asyncio.wait(self._workers, timeout=timeout)