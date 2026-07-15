"""Tests for dependency health probing."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.api.health import probe_dependencies


def _app_with_clients(postgres, qdrant, neo4j):
    return SimpleNamespace(
        state=SimpleNamespace(
            postgres_client=postgres,
            vector_store=qdrant,
            neo4j_client=neo4j,
        )
    )


@pytest.mark.asyncio
async def test_probe_dependencies_reports_all_ready() -> None:
    app = _app_with_clients(
        SimpleNamespace(health_check=AsyncMock(return_value=None)),
        SimpleNamespace(health_check=MagicMock(return_value=None)),
        SimpleNamespace(health_check=AsyncMock(return_value=None)),
    )

    result = await probe_dependencies(app, timeout_seconds=0.5)

    assert result == {
        "postgres": "ready",
        "qdrant": "ready",
        "neo4j": "ready",
    }


@pytest.mark.asyncio
async def test_probe_dependencies_sanitizes_failure() -> None:
    app = _app_with_clients(
        SimpleNamespace(
            health_check=AsyncMock(side_effect=RuntimeError("secret password"))
        ),
        SimpleNamespace(health_check=MagicMock(return_value=None)),
        SimpleNamespace(health_check=AsyncMock(return_value=None)),
    )

    result = await probe_dependencies(app, timeout_seconds=0.5)

    assert result["postgres"] == "unavailable"
    assert "secret" not in repr(result)


@pytest.mark.asyncio
async def test_probe_dependencies_marks_timeout_unavailable() -> None:
    async def slow_probe() -> None:
        await asyncio.sleep(0.1)

    app = _app_with_clients(
        SimpleNamespace(health_check=slow_probe),
        SimpleNamespace(health_check=MagicMock(return_value=None)),
        SimpleNamespace(health_check=AsyncMock(return_value=None)),
    )

    result = await probe_dependencies(app, timeout_seconds=0.01)

    assert result["postgres"] == "unavailable"
