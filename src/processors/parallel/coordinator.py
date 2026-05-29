"""Pipeline Coordinator - Central orchestrator for parallel processing"""
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import asyncio

if TYPE_CHECKING:
    from src.processors.parallel.node import ProcessorNode, NodeMetrics, HealthStatus
else:
    # Avoid triggering full package import to prevent missing dependency errors
    import sys
    from importlib import import_module
    _node_module = import_module("processors.parallel.node")
    sys.modules["src.processors.parallel.node"] = _node_module
    ProcessorNode = _node_module.ProcessorNode
    NodeMetrics = _node_module.NodeMetrics
    HealthStatus = _node_module.HealthStatus


@dataclass(frozen=True)
class PipelineStats:
    """Aggregated statistics for the entire pipeline."""

    total_processed: int = 0
    total_errors: int = 0
    active_nodes: int = 0
    avg_throughput: float = 0.0


class PipelineCoordinator:
    """Central coordinator for the parallel processing pipeline."""

    def __init__(self, name: str = "PipelineCoordinator"):
        self.name = name
        self._nodes: dict[str, ProcessorNode] = {}
        self._routing_table: dict[str, str] = {}
        self._running = False
        self._stats = {"processed": 0, "errors": 0}

    def register_node(self, node: ProcessorNode, event_types: list[str]) -> None:
        """Register a processor node and associate it with event types."""
        self._nodes[node.name] = node
        for et in event_types:
            self._routing_table[et] = node.name

    def unregister_node(self, node_name: str) -> None:
        """Unregister a node."""
        if node_name in self._nodes:
            del self._nodes[node_name]
        for et, name in list(self._routing_table.items()):
            if name == node_name:
                del self._routing_table[et]

    async def start_all(self) -> None:
        """Start all registered nodes."""
        self._running = True
        await asyncio.gather(*[node.start() for node in self._nodes.values()])

    async def stop_all(self, timeout: float = 30.0) -> None:
        """Gracefully stop all nodes."""
        self._running = False
        await asyncio.gather(
            *[node.stop() for node in self._nodes.values()], return_exceptions=True
        )

    async def route_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """Route event to appropriate node and return result."""
        if not self._running:
            return None

        event_type = event.get("event_type", "")
        node_name = self._routing_table.get(event_type)

        if not node_name or node_name not in self._nodes:
            return None

        node = self._nodes[node_name]
        try:
            result = await node.process_with_retry(event)
            self._stats["processed"] += 1
            return result
        except Exception:
            self._stats["errors"] += 1
            return None

    def get_health_status(self) -> dict[str, HealthStatus]:
        """Return health status of all nodes."""
        return {name: node.health for name, node in self._nodes.items()}

    def get_pipeline_stats(self) -> PipelineStats:
        """Return aggregated pipeline statistics."""
        active = sum(1 for n in self._nodes.values() if n.health != HealthStatus.UNHEALTHY)
        return PipelineStats(
            total_processed=self._stats["processed"],
            total_errors=self._stats["errors"],
            active_nodes=active,
        )

    @property
    def nodes(self) -> list[str]:
        """Return list of registered node names."""
        return list(self._nodes.keys())

    @property
    def is_running(self) -> bool:
        """Return True if pipeline is running."""
        return self._running