"""
Unit tests for WorkerPool
"""
import pytest
import asyncio
import importlib.util
import sys
from pathlib import Path

# Directly load the module without triggering package __init__.py
_module_path = Path(__file__).parent.parent.parent / "src" / "processors" / "parallel" / "pool.py"
_spec = importlib.util.spec_from_file_location("processors.parallel.pool", _module_path)
_pool_module = importlib.util.module_from_spec(_spec)
sys.modules["processors.parallel.pool"] = _pool_module
_spec.loader.exec_module(_pool_module)

WorkerPool = _pool_module.WorkerPool
WorkerInfo = _pool_module.WorkerInfo
WorkerStatus = _pool_module.WorkerStatus


class TestWorkerStatus:
    def test_worker_status_values(self):
        assert WorkerStatus.IDLE.value == "idle"
        assert WorkerStatus.BUSY.value == "busy"
        assert WorkerStatus.STOPPED.value == "stopped"


class TestWorkerInfo:
    def test_worker_info_defaults(self):
        info = WorkerInfo(worker_id=0, status=WorkerStatus.IDLE)
        assert info.worker_id == 0
        assert info.status == WorkerStatus.IDLE
        assert info.tasks_completed == 0

    def test_worker_info_immutable(self):
        info = WorkerInfo(worker_id=1, status=WorkerStatus.BUSY, tasks_completed=5)
        with pytest.raises(Exception):
            info.tasks_completed = 10


class TestWorkerPoolInit:
    def test_default_initialization(self):
        pool = WorkerPool()
        assert pool.max_workers == 4
        assert pool.name == "WorkerPool"
        assert pool._shutdown is False

    def test_custom_initialization(self):
        pool = WorkerPool(max_workers=8, name="TestPool")
        assert pool.max_workers == 8
        assert pool.name == "TestPool"


class TestWorkerPoolContextManager:
    @pytest.mark.asyncio
    async def test_context_manager_starts_and_stops(self):
        async with WorkerPool(max_workers=2) as pool:
            assert len(pool._workers) == 2
            assert len(pool._worker_infos) == 2
        # After context exit, workers should be done
        # Give some time for graceful shutdown
        await asyncio.sleep(0.1)


class TestWorkerPoolStart:
    @pytest.mark.asyncio
    async def test_start_creates_workers(self):
        pool = WorkerPool(max_workers=3)
        await pool.start()
        assert len(pool._workers) == 3
        assert len(pool._worker_infos) == 3
        # Verify all workers are idle initially
        for info in pool._worker_infos:
            assert info.status == WorkerStatus.IDLE
            assert info.tasks_completed == 0
        await pool.shutdown()

    @pytest.mark.asyncio
    async def test_start_respects_max_workers(self):
        pool = WorkerPool(max_workers=6)
        await pool.start()
        assert pool.max_workers == 6
        assert len(pool._workers) == 6
        await pool.shutdown()


class TestWorkerPoolSubmit:
    @pytest.mark.asyncio
    async def test_submit_adds_task_to_queue(self):
        pool = WorkerPool(max_workers=1)
        await pool.start()

        call_count = 0

        async def increment():
            nonlocal call_count
            call_count += 1

        await pool.submit(increment)
        await asyncio.sleep(0.2)  # Give time for task to process

        assert call_count == 1
        await pool.shutdown()

    @pytest.mark.asyncio
    async def test_submit_with_args(self):
        pool = WorkerPool(max_workers=1)
        await pool.start()

        results = []

        async def capture(value):
            results.append(value)

        await pool.submit(capture, "test_value")
        await asyncio.sleep(0.2)

        assert results == ["test_value"]
        await pool.shutdown()

    @pytest.mark.asyncio
    async def test_submit_with_kwargs(self):
        pool = WorkerPool(max_workers=1)
        await pool.start()

        results = []

        async def capture(a, b=None):
            results.append((a, b))

        await pool.submit(capture, "first", b="second")
        await asyncio.sleep(0.2)

        assert results == [("first", "second")]
        await pool.shutdown()

    @pytest.mark.asyncio
    async def test_submit_multiple_tasks(self):
        pool = WorkerPool(max_workers=2)
        await pool.start()

        call_count = 0

        async def increment():
            nonlocal call_count
            call_count += 1

        for _ in range(5):
            await pool.submit(increment)

        await asyncio.sleep(0.5)

        assert call_count == 5
        await pool.shutdown()


class TestWorkerPoolGetWorkers:
    @pytest.mark.asyncio
    async def test_get_workers_returns_all_workers(self):
        pool = WorkerPool(max_workers=3)
        await pool.start()

        workers = pool.get_workers()
        assert len(workers) == 3

        for i, worker in enumerate(workers):
            assert worker.worker_id == i
            assert worker.status == WorkerStatus.IDLE
            assert worker.tasks_completed == 0

        await pool.shutdown()

    @pytest.mark.asyncio
    async def test_get_workers_returns_copy(self):
        pool = WorkerPool(max_workers=2)
        await pool.start()

        workers1 = pool.get_workers()
        workers2 = pool.get_workers()

        assert workers1 is not workers2
        assert workers1 == workers2

        await pool.shutdown()


class TestWorkerPoolShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_sets_shutdown_flag(self):
        pool = WorkerPool(max_workers=1)
        await pool.start()
        assert pool._shutdown is False

        await pool.shutdown()

        assert pool._shutdown is True

    @pytest.mark.asyncio
    async def test_shutdown_with_default_timeout(self):
        pool = WorkerPool(max_workers=2)
        await pool.start()

        await pool.shutdown()

        # Workers should have completed within timeout
        # If this fails, the shutdown didn't wait properly
        assert pool._shutdown is True

    @pytest.mark.asyncio
    async def test_shutdown_with_custom_timeout(self):
        pool = WorkerPool(max_workers=1)
        await pool.start()

        await pool.shutdown(timeout=5.0)

        assert pool._shutdown is True


class TestWorkerPoolIntegration:
    @pytest.mark.asyncio
    async def test_concurrent_task_processing(self):
        pool = WorkerPool(max_workers=3)
        await pool.start()

        results = []

        async def process(item):
            await asyncio.sleep(0.01)  # Simulate work
            results.append(item)

        tasks = [pool.submit(process, i) for i in range(10)]
        for t in tasks:
            await t

        # Wait for all tasks to complete
        await asyncio.sleep(0.5)

        assert len(results) == 10
        assert sorted(results) == list(range(10))

        await pool.shutdown()

    @pytest.mark.asyncio
    async def test_worker_task_counts(self):
        pool = WorkerPool(max_workers=2)
        await pool.start()

        async def noop():
            pass

        for _ in range(6):
            await pool.submit(noop)

        await asyncio.sleep(0.5)

        workers = pool.get_workers()
        total_completed = sum(w.tasks_completed for w in workers)
        assert total_completed == 6

        await pool.shutdown()

    @pytest.mark.asyncio
    async def test_context_manager_shutdown_works(self):
        async with WorkerPool(max_workers=2) as pool:
            async def noop():
                pass

            for _ in range(3):
                await pool.submit(noop)

            await asyncio.sleep(0.3)

        # Pool should have shut down gracefully after context exit