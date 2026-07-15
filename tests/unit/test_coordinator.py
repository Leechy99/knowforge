"""
Unit tests for PipelineCoordinator
"""
import importlib.util
import sys
from pathlib import Path

import pytest

# Directly load the module without triggering package __init__.py
_node_path = Path(__file__).parent.parent.parent / "src" / "processors" / "parallel" / "node.py"
_spec = importlib.util.spec_from_file_location("processors.parallel.node", _node_path)
_node_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_node_module)
# Register under both possible import paths
sys.modules["processors.parallel.node"] = _node_module
sys.modules["src.processors.parallel.node"] = _node_module

ProcessorNode = _node_module.ProcessorNode
NodeMetrics = _node_module.NodeMetrics
HealthStatus = _node_module.HealthStatus

# Now load coordinator directly
_coord_path = Path(__file__).parent.parent.parent / "src" / "processors" / "parallel" / "coordinator.py"
_spec = importlib.util.spec_from_file_location("processors.parallel.coordinator", _coord_path)
_coord_module = importlib.util.module_from_spec(_spec)
sys.modules["processors.parallel.coordinator"] = _coord_module
sys.modules["src.processors.parallel.coordinator"] = _coord_module
_spec.loader.exec_module(_coord_module)

PipelineCoordinator = _coord_module.PipelineCoordinator
PipelineStats = _coord_module.PipelineStats


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


class TestPipelineCoordinatorInit:
    def test_default_initialization(self):
        coordinator = PipelineCoordinator()
        assert coordinator.name == "PipelineCoordinator"
        assert coordinator._running is False
        assert coordinator._stats["processed"] == 0
        assert coordinator._stats["errors"] == 0

    def test_custom_name(self):
        coordinator = PipelineCoordinator("CustomCoordinator")
        assert coordinator.name == "CustomCoordinator"


class TestNodeRegistration:
    def test_register_node_single_event_type(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node")
        coordinator.register_node(node, ["file_events"])

        assert "parser_node" in coordinator.nodes
        assert "file_events" in coordinator._routing_table
        assert coordinator._routing_table["file_events"] == "parser_node"

    def test_register_node_multiple_event_types(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node")
        coordinator.register_node(node, ["file_events", "crawl_events"])

        assert "parser_node" in coordinator.nodes
        assert coordinator._routing_table["file_events"] == "parser_node"
        assert coordinator._routing_table["crawl_events"] == "parser_node"

    def test_register_multiple_nodes_different_event_types(self):
        coordinator = PipelineCoordinator()
        node1 = DummyProcessor("parser_node")
        node2 = DummyProcessor("cleaner_node")

        coordinator.register_node(node1, ["file_events"])
        coordinator.register_node(node2, ["crawl_events"])

        assert coordinator._routing_table["file_events"] == "parser_node"
        assert coordinator._routing_table["crawl_events"] == "cleaner_node"

    def test_unregister_node(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node")
        coordinator.register_node(node, ["file_events", "crawl_events"])
        coordinator.unregister_node("parser_node")

        assert "parser_node" not in coordinator.nodes
        assert "file_events" not in coordinator._routing_table
        assert "crawl_events" not in coordinator._routing_table

    def test_unregister_nonexistent_node(self):
        coordinator = PipelineCoordinator()
        # Should not raise
        coordinator.unregister_node("nonexistent")


class TestPipelineLifecycle:
    @pytest.mark.asyncio
    async def test_start_all_sets_running_true(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("test_node")
        coordinator.register_node(node, ["test_events"])

        assert coordinator.is_running is False
        await coordinator.start_all()
        assert coordinator.is_running is True
        assert node._running is True

    @pytest.mark.asyncio
    async def test_stop_all_sets_running_false(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("test_node")
        coordinator.register_node(node, ["test_events"])
        await coordinator.start_all()

        await coordinator.stop_all()

        assert coordinator.is_running is False
        assert node._running is False

    @pytest.mark.asyncio
    async def test_start_all_starts_multiple_nodes(self):
        coordinator = PipelineCoordinator()
        node1 = DummyProcessor("node1")
        node2 = DummyProcessor("node2")
        coordinator.register_node(node1, ["event1"])
        coordinator.register_node(node2, ["event2"])

        await coordinator.start_all()

        assert node1._running is True
        assert node2._running is True


class TestEventRouting:
    @pytest.mark.asyncio
    async def test_route_event_to_registered_node(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node")
        coordinator.register_node(node, ["file_events"])
        await coordinator.start_all()

        event = {"event_type": "file_events", "data": "test"}
        result = await coordinator.route_event(event)

        assert result is not None
        assert result["result"] == "processed"
        assert len(node.process_calls) == 1

    @pytest.mark.asyncio
    async def test_route_event_returns_none_when_not_running(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node")
        coordinator.register_node(node, ["file_events"])

        # Not started - should return None
        event = {"event_type": "file_events", "data": "test"}
        result = await coordinator.route_event(event)

        assert result is None

    @pytest.mark.asyncio
    async def test_route_event_returns_none_for_unknown_event_type(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node")
        coordinator.register_node(node, ["file_events"])
        await coordinator.start_all()

        event = {"event_type": "unknown_events", "data": "test"}
        result = await coordinator.route_event(event)

        assert result is None

    @pytest.mark.asyncio
    async def test_route_event_updates_stats_on_success(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node")
        coordinator.register_node(node, ["file_events"])
        await coordinator.start_all()

        await coordinator.route_event({"event_type": "file_events", "data": "test"})
        stats = coordinator.get_pipeline_stats()

        assert stats.total_processed == 1
        assert stats.total_errors == 0

    @pytest.mark.asyncio
    async def test_route_event_updates_stats_on_error(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("parser_node", max_retries=1, base_delay=0.01)
        coordinator.register_node(node, ["file_events"])
        await coordinator.start_all()

        await coordinator.route_event({"event_type": "file_events", "fail": True})
        stats = coordinator.get_pipeline_stats()

        assert stats.total_processed == 0
        assert stats.total_errors == 1


class TestHealthMonitoring:
    def test_get_health_status_empty(self):
        coordinator = PipelineCoordinator()
        status = coordinator.get_health_status()

        assert status == {}

    def test_get_health_status_returns_node_health(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("healthy_node")
        coordinator.register_node(node, ["test_events"])

        status = coordinator.get_health_status()
        assert status["healthy_node"] == HealthStatus.HEALTHY

    def test_get_health_status_with_degraded_node(self):
        coordinator = PipelineCoordinator()
        node = DummyProcessor("degraded_node")
        node._metrics["errors"] = 6
        coordinator.register_node(node, ["test_events"])

        status = coordinator.get_health_status()
        assert status["degraded_node"] == HealthStatus.DEGRADED


class TestPipelineStats:
    def test_get_pipeline_stats_initial(self):
        coordinator = PipelineCoordinator()
        stats = coordinator.get_pipeline_stats()

        assert isinstance(stats, PipelineStats)
        assert stats.total_processed == 0
        assert stats.total_errors == 0
        assert stats.active_nodes == 0

    def test_get_pipeline_stats_active_nodes_count(self):
        coordinator = PipelineCoordinator()
        node1 = DummyProcessor("healthy_node")
        node2 = DummyProcessor("unhealthy_node")
        node2._metrics["errors"] = 15

        coordinator.register_node(node1, ["event1"])
        coordinator.register_node(node2, ["event2"])

        stats = coordinator.get_pipeline_stats()
        assert stats.active_nodes == 1  # Only healthy_node is active


class TestPipelineStatsDataclass:
    def test_stats_defaults(self):
        stats = PipelineStats()
        assert stats.total_processed == 0
        assert stats.total_errors == 0
        assert stats.active_nodes == 0
        assert stats.avg_throughput == 0.0

    def test_stats_immutable(self):
        stats = PipelineStats(total_processed=10, total_errors=2, active_nodes=3)
        with pytest.raises(Exception):
            stats.total_processed = 20


class TestCoordinatorProperties:
    def test_nodes_property_returns_list(self):
        coordinator = PipelineCoordinator()
        node1 = DummyProcessor("node1")
        node2 = DummyProcessor("node2")

        coordinator.register_node(node1, ["event1"])
        coordinator.register_node(node2, ["event2"])

        nodes = coordinator.nodes
        assert isinstance(nodes, list)
        assert "node1" in nodes
        assert "node2" in nodes

    def test_is_running_initial_false(self):
        coordinator = PipelineCoordinator()
        assert coordinator.is_running is False
