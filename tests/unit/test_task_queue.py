"""
Unit tests for TaskQueue
"""
import pytest
import importlib.util
import sys
from pathlib import Path

# Directly load the module without triggering package __init__.py
_module_path = Path(__file__).parent.parent.parent / "src" / "processors" / "parallel" / "queue.py"
_spec = importlib.util.spec_from_file_location("processors.parallel.queue", _module_path)
_queue_module = importlib.util.module_from_spec(_spec)
sys.modules["processors.parallel.queue"] = _queue_module
_spec.loader.exec_module(_queue_module)

Priority = _queue_module.Priority
Task = _queue_module.Task
QueueStats = _queue_module.QueueStats
TaskQueue = _queue_module.TaskQueue


class TestPriority:
    def test_priority_values(self):
        assert Priority.HIGH == 0
        assert Priority.NORMAL == 1
        assert Priority.LOW == 2

    def test_priority_ordering(self):
        assert Priority.HIGH < Priority.NORMAL < Priority.LOW


class TestTask:
    def test_task_creation(self):
        task = Task(priority=Priority.NORMAL, data={"key": "value"})
        assert task.priority == Priority.NORMAL
        assert task.data == {"key": "value"}
        assert task.content_hash is None
        assert task.task_id is None

    def test_task_with_hash(self):
        task = Task(priority=Priority.HIGH, data={"content": "test"}, content_hash="abc123")
        assert task.content_hash == "abc123"


class TestQueueStats:
    def test_stats_defaults(self):
        stats = QueueStats()
        assert stats.enqueued == 0
        assert stats.dequeued == 0
        assert stats.dropped == 0

    def test_stats_mutable(self):
        stats = QueueStats()
        stats.enqueued = 5
        stats.dequeued = 3
        stats.dropped = 2
        assert stats.enqueued == 5
        assert stats.dequeued == 3
        assert stats.dropped == 2


class TestTaskQueueInit:
    def test_default_initialization(self):
        queue = TaskQueue()
        assert queue.max_size == 1000
        assert queue.enable_dedup is True
        assert queue.size() == 0
        assert queue.is_full() is False

    def test_custom_max_size(self):
        queue = TaskQueue(max_size=100)
        assert queue.max_size == 100
        assert queue.size() == 0

    def test_dedup_disabled(self):
        queue = TaskQueue(enable_dedup=False)
        assert queue.enable_dedup is False


class TestTaskQueuePut:
    @pytest.mark.asyncio
    async def test_put_increments_size(self):
        queue = TaskQueue()
        result = await queue.put({"content": "test1"}, Priority.NORMAL)
        assert result is True
        assert queue.size() == 1

    @pytest.mark.asyncio
    async def test_put_multiple_tasks(self):
        queue = TaskQueue()
        await queue.put({"content": "test1"}, Priority.NORMAL)
        await queue.put({"content": "test2"}, Priority.NORMAL)
        await queue.put({"content": "test3"}, Priority.LOW)
        assert queue.size() == 3

    @pytest.mark.asyncio
    async def test_put_updates_stats(self):
        queue = TaskQueue()
        await queue.put({"content": "test1"}, Priority.NORMAL)
        assert queue.stats.enqueued == 1
        assert queue.stats.dequeued == 0
        assert queue.stats.dropped == 0

    @pytest.mark.asyncio
    async def test_put_with_dedup_duplicate(self):
        queue = TaskQueue(enable_dedup=True)
        await queue.put({"content": "same", "source_id": "src1"}, Priority.NORMAL)
        result = await queue.put({"content": "same", "source_id": "src1"}, Priority.NORMAL)
        assert result is False
        assert queue.size() == 1
        assert queue.stats.dropped == 1

    @pytest.mark.asyncio
    async def test_put_without_dedup_allows_duplicates(self):
        queue = TaskQueue(enable_dedup=False)
        await queue.put({"content": "same", "source_id": "src1"}, Priority.NORMAL)
        result = await queue.put({"content": "same", "source_id": "src1"}, Priority.NORMAL)
        assert result is True
        assert queue.size() == 2

    @pytest.mark.asyncio
    async def test_put_at_max_size_returns_false(self):
        queue = TaskQueue(max_size=2)
        await queue.put({"content": "test1"}, Priority.NORMAL)
        await queue.put({"content": "test2"}, Priority.NORMAL)
        result = await queue.put({"content": "test3"}, Priority.NORMAL)
        assert result is False
        assert queue.size() == 2
        assert queue.stats.dropped == 1

    @pytest.mark.asyncio
    async def test_put_high_priority_task(self):
        queue = TaskQueue()
        await queue.put({"content": "low"}, Priority.LOW)
        await queue.put({"content": "high"}, Priority.HIGH)
        await queue.put({"content": "normal"}, Priority.NORMAL)
        assert queue.size() == 3


class TestTaskQueueGet:
    @pytest.mark.asyncio
    async def test_get_returns_high_priority_first(self):
        queue = TaskQueue()
        await queue.put({"content": "low"}, Priority.LOW)
        await queue.put({"content": "normal"}, Priority.NORMAL)
        await queue.put({"content": "high"}, Priority.HIGH)

        task = await queue.get()
        assert task is not None
        assert task.data["content"] == "high"

    @pytest.mark.asyncio
    async def test_get_returns_normal_before_low(self):
        queue = TaskQueue()
        await queue.put({"content": "low"}, Priority.LOW)
        await queue.put({"content": "normal"}, Priority.NORMAL)

        task = await queue.get()
        assert task is not None
        assert task.data["content"] == "normal"

    @pytest.mark.asyncio
    async def test_get_returns_none_when_empty(self):
        queue = TaskQueue()
        task = await queue.get()
        assert task is None

    @pytest.mark.asyncio
    async def test_get_updates_stats(self):
        queue = TaskQueue()
        await queue.put({"content": "test"}, Priority.NORMAL)
        await queue.get()
        assert queue.stats.dequeued == 1

    @pytest.mark.asyncio
    async def test_get_removes_task_from_queue(self):
        queue = TaskQueue()
        await queue.put({"content": "test"}, Priority.NORMAL)
        assert queue.size() == 1
        await queue.get()
        assert queue.size() == 0

    @pytest.mark.asyncio
    async def test_get_removes_hash_from_seen(self):
        queue = TaskQueue(enable_dedup=True)
        await queue.put({"content": "test", "source_id": "src1"}, Priority.NORMAL)
        assert queue.size() == 1
        await queue.get()
        assert queue.size() == 0
        # After getting, same task can be re-added
        result = await queue.put({"content": "test", "source_id": "src1"}, Priority.NORMAL)
        assert result is True


class TestTaskQueueSize:
    @pytest.mark.asyncio
    async def test_size_empty_queue(self):
        queue = TaskQueue()
        assert queue.size() == 0

    @pytest.mark.asyncio
    async def test_size_after_puts(self):
        queue = TaskQueue()
        await queue.put({"content": "1"}, Priority.NORMAL)
        await queue.put({"content": "2"}, Priority.HIGH)
        await queue.put({"content": "3"}, Priority.LOW)
        assert queue.size() == 3

    @pytest.mark.asyncio
    async def test_size_after_get(self):
        queue = TaskQueue()
        await queue.put({"content": "1"}, Priority.NORMAL)
        await queue.put({"content": "2"}, Priority.NORMAL)
        await queue.get()
        assert queue.size() == 1


class TestTaskQueueIsFull:
    @pytest.mark.asyncio
    async def test_is_full_false_when_empty(self):
        queue = TaskQueue(max_size=5)
        assert queue.is_full() is False

    @pytest.mark.asyncio
    async def test_is_full_true_at_capacity(self):
        queue = TaskQueue(max_size=2)
        await queue.put({"content": "1"}, Priority.NORMAL)
        await queue.put({"content": "2"}, Priority.NORMAL)
        assert queue.is_full() is True

    @pytest.mark.asyncio
    async def test_is_full_false_below_capacity(self):
        queue = TaskQueue(max_size=5)
        await queue.put({"content": "1"}, Priority.NORMAL)
        await queue.put({"content": "2"}, Priority.NORMAL)
        assert queue.is_full() is False


class TestTaskQueueStats:
    @pytest.mark.asyncio
    async def test_stats_initial_state(self):
        queue = TaskQueue()
        stats = queue.stats
        assert stats.enqueued == 0
        assert stats.dequeued == 0
        assert stats.dropped == 0

    @pytest.mark.asyncio
    async def test_stats_after_operations(self):
        queue = TaskQueue()
        await queue.put({"content": "1"}, Priority.NORMAL)
        await queue.put({"content": "2"}, Priority.NORMAL)
        await queue.put({"content": "3"}, Priority.NORMAL)
        await queue.get()
        await queue.get()
        # 1 still in queue
        await queue.put({"content": "4"}, Priority.NORMAL)
        # 1 left in queue now

        stats = queue.stats
        assert stats.enqueued == 4
        assert stats.dequeued == 2
        assert stats.dropped == 0

    @pytest.mark.asyncio
    async def test_stats_with_dropped_duplicates(self):
        queue = TaskQueue(enable_dedup=True)
        await queue.put({"content": "same", "source_id": "src"}, Priority.NORMAL)
        await queue.put({"content": "same", "source_id": "src"}, Priority.NORMAL)
        await queue.put({"content": "different"}, Priority.NORMAL)

        stats = queue.stats
        assert stats.enqueued == 2
        assert stats.dropped == 1


class TestTaskQueueDeduplication:
    @pytest.mark.asyncio
    async def test_different_content_not_deduplicated(self):
        queue = TaskQueue(enable_dedup=True)
        await queue.put({"content": "content1", "source_id": "src1"}, Priority.NORMAL)
        result = await queue.put({"content": "content2", "source_id": "src1"}, Priority.NORMAL)
        assert result is True

    @pytest.mark.asyncio
    async def test_different_source_id_not_deduplicated(self):
        queue = TaskQueue(enable_dedup=True)
        await queue.put({"content": "same", "source_id": "src1"}, Priority.NORMAL)
        result = await queue.put({"content": "same", "source_id": "src2"}, Priority.NORMAL)
        assert result is True

    @pytest.mark.asyncio
    async def test_hash_computed_correctly(self):
        queue = TaskQueue()
        data = {"content": "test", "source_id": "src"}
        hash1 = queue._compute_hash(data)
        hash2 = queue._compute_hash(data)
        assert hash1 == hash2
        assert len(hash1) == 16


class TestTaskQueueFIFOWithinPriority:
    @pytest.mark.asyncio
    async def test_fifo_within_same_priority(self):
        queue = TaskQueue()
        await queue.put({"content": "first"}, Priority.NORMAL)
        await queue.put({"content": "second"}, Priority.NORMAL)
        await queue.put({"content": "third"}, Priority.NORMAL)

        task1 = await queue.get()
        task2 = await queue.get()
        task3 = await queue.get()

        assert task1.data["content"] == "first"
        assert task2.data["content"] == "second"
        assert task3.data["content"] == "third"