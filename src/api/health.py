"""Liveness and dependency-aware readiness endpoints."""

import asyncio
import inspect
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse


router = APIRouter(prefix="/health")


async def _run_health_check(client: Any, timeout_seconds: float) -> str:
    async def invoke() -> None:
        health_check = client.health_check
        is_async_callable = inspect.iscoroutinefunction(health_check) or (
            inspect.iscoroutinefunction(getattr(health_check, "__call__", None))
        )
        if is_async_callable:
            result = health_check()
        else:
            result = await asyncio.to_thread(health_check)
        if inspect.isawaitable(result):
            await result

    try:
        await asyncio.wait_for(invoke(), timeout=timeout_seconds)
    except Exception:
        return "unavailable"
    return "ready"


async def probe_dependencies(
    app: Any,
    timeout_seconds: float,
) -> dict[str, str]:
    """Probe required dependencies without exposing implementation errors."""
    names_and_clients = (
        ("postgres", getattr(app.state, "postgres_client", None)),
        ("qdrant", getattr(app.state, "vector_store", None)),
        ("neo4j", getattr(app.state, "neo4j_client", None)),
    )
    statuses = await asyncio.gather(
        *(
            _run_health_check(client, timeout_seconds)
            if client is not None and hasattr(client, "health_check")
            else asyncio.sleep(0, result="unavailable")
            for _, client in names_and_clients
        )
    )
    return {
        name: status
        for (name, _), status in zip(names_and_clients, statuses, strict=True)
    }


@router.get("/live")
async def liveness_check() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/ready")
async def readiness_check(request: Request) -> JSONResponse:
    timeout_seconds = request.app.state.settings.health_check_timeout_seconds
    dependencies = await probe_dependencies(request.app, timeout_seconds)
    is_ready = all(status == "ready" for status in dependencies.values())
    return JSONResponse(
        status_code=200 if is_ready else 503,
        content={
            "status": "ready" if is_ready else "not_ready",
            "dependencies": dependencies,
        },
    )
