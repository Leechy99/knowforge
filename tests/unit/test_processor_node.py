"""
Unit tests for ProcessorNode base class
"""
import importlib.util
import sys
from pathlib import Path

import pytest

# Directly load the module without triggering package __init__.py
_module_path = Path(__file__).parent.parent.parent / "src" / "processors" / "parallel" / "node.py"
_spec = importlib.util.spec_from_file_location("processors.parallel.node", _module_path)
_node_module = importlib.util.module_from_spec(_spec)
sys.modules["processors.parallel.node"] = _node_module
_spec.loader.exec_module(_node_module)

ProcessorNode = _node_module.ProcessorNode
NodeMetrics = _node_module.NodeMetrics
HealthStatus = _node_module.HealthStatus


class DummyProcessor(ProcessorNode):
    """Concrete implementation of ProcessorNode for testing."""

    def __init__(self, name: str = "test", max_retries: int = 3, **kwargs):
        super().__init__(name, max_retries, **kwargs)
        self.process_calls = []

    async def process(self, data: dict) -> dict:
        self.process_calls.append(data)
        if data.get("fail"):
            raise ValueError(data.get("error_msg", "Test error"))
        return {"result": "processed", **data}


class TestNodeMetrics:
    def test_metrics_defaults(self):
        metrics = NodeMetrics()
        assert metrics.processed_count == 0
        assert metrics.error_count == 0
        assert metrics.avg_process_time == 0.0

    def test_metrics_immutable(self):
        metrics = NodeMetrics(processed_count=5, error_count=2, avg_process_time=0.1)
        with pytest.raises(Exception):
            metrics.processed_count = 10


class TestHealthStatus:
    def test_health_status_values(self):
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"


class TestProcessorNodeInit:
    def test_default_initialization(self):
        node = DummyProcessor("test_node")
        assert node.name == "test_node"
        assert node.max_retries == 3
        assert node.base_delay == 1.0
        assert node.backoff_factor == 2.0
        assert node._running is False

    def test_custom_initialization(self):
        node = DummyProcessor("custom", max_retries=5, base_delay=2.0, backoff_factor=3.0)
        assert node.max_retries == 5
        assert node.base_delay == 2.0
        assert node.backoff_factor == 3.0


class TestProcessorNodeHealth:
    def test_health_healthy_when_no_errors(self):
        node = DummyProcessor("healthy_node")
        assert node.health == HealthStatus.HEALTHY

    def test_health_degraded_at_6_errors(self):
        node = DummyProcessor("degraded_node")
        node._metrics["errors"] = 6
        assert node.health == HealthStatus.DEGRADED

    def test_health_unhealthy_at_11_errors(self):
        node = DummyProcessor("unhealthy_node")
        node._metrics["errors"] = 11
        assert node.health == HealthStatus.UNHEALTHY


class TestProcessorNodeMetrics:
    def test_metrics_initial_state(self):
        node = DummyProcessor("metrics_node")
        metrics = node.metrics
        assert isinstance(metrics, NodeMetrics)
        assert metrics.processed_count == 0
        assert metrics.error_count == 0
        assert metrics.avg_process_time == 0.0

    def test_metrics_after_successful_process(self):
        node = DummyProcessor("metrics_node")
        node._metrics["processed"] = 5
        node._metrics["total_time"] = 0.5
        metrics = node.metrics
        assert metrics.processed_count == 5
        assert metrics.error_count == 0
        assert metrics.avg_process_time == 0.1


class TestProcessorNodeLifecycle:
    @pytest.mark.asyncio
    async def test_start_sets_running_true(self):
        node = DummyProcessor("lifecycle_node")
        assert node._running is False
        await node.start()
        assert node._running is True

    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self):
        node = DummyProcessor("lifecycle_node")
        await node.start()
        assert node._running is True
        await node.stop()
        assert node._running is False


class TestProcessorNodeProcess:
    @pytest.mark.asyncio
    async def test_process_returns_result(self):
        node = DummyProcessor("process_node")
        result = await node.process({"key": "value"})
        assert result == {"result": "processed", "key": "value"}
        assert len(node.process_calls) == 1

    @pytest.mark.asyncio
    async def test_process_raises_exception(self):
        node = DummyProcessor("process_node")
        with pytest.raises(ValueError):
            await node.process({"fail": True})


class TestProcessWithRetry:
    @pytest.mark.asyncio
    async def test_retry_success_first_attempt(self):
        node = DummyProcessor("retry_node", max_retries=3)
        result = await node.process_with_retry({"data": "test"})
        assert result == {"result": "processed", "data": "test"}
        assert node.metrics.processed_count == 1
        assert node.metrics.error_count == 0

    @pytest.mark.asyncio
    async def test_retry_success_after_failure(self):
        node = DummyProcessor("retry_node", max_retries=3, base_delay=0.01)
        # First call fails, second succeeds
        call_count = 0

        async def mock_process(data):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("First attempt fails")
            return {"result": "processed", **data}

        # Replace process with mock that fails on first call
        node.process = mock_process
        result = await node.process_with_retry({"data": "test"})
        assert result == {"result": "processed", "data": "test"}
        assert node.metrics.processed_count == 1
        assert node.metrics.error_count == 1

    @pytest.mark.asyncio
    async def test_retry_exhausted_all_attempts(self):
        node = DummyProcessor("retry_node", max_retries=3, base_delay=0.01)
        with pytest.raises(ValueError) as exc_info:
            await node.process_with_retry({"fail": True, "error_msg": "Always fails"})
        assert str(exc_info.value) == "Always fails"
        assert node.metrics.processed_count == 0
        assert node.metrics.error_count == 3

    @pytest.mark.asyncio
    async def test_retry_updates_avg_process_time(self):
        node = DummyProcessor("retry_node", max_retries=3)
        await node.process_with_retry({"data": "test"})
        assert node.metrics.avg_process_time > 0

    @pytest.mark.asyncio
    async def test_retry_max_attempts_respected(self):
        node = DummyProcessor("retry_node", max_retries=2, base_delay=0.01)

        # Create a callable class that tracks attempts but always fails
        class AlwaysFails:
            def __init__(self):
                self.attempts = 0
            async def __call__(self, data):
                self.attempts += 1
                raise ValueError("Always fails")

        always_fails = AlwaysFails()
        node.process = always_fails  # type: ignore
        with pytest.raises(ValueError):
            await node.process_with_retry({"fail": True})
        # Should have exactly 2 errors (2 attempts with max_retries=2)
        assert node.metrics.error_count == 2


class TestProcessorNodeAbstract:
    def test_cannot_instantiate_abstract_class(self):
        with pytest.raises(TypeError) as exc_info:
            ProcessorNode("abstract_node")
        assert "abstract" in str(exc_info.value).lower()

    def test_abstract_method_enforced(self):
        """Verify that subclasses must implement process method."""
        class IncompleteProcessor(ProcessorNode):
            pass

        with pytest.raises(TypeError) as exc_info:
            IncompleteProcessor("incomplete")
        assert "abstract" in str(exc_info.value).lower()
