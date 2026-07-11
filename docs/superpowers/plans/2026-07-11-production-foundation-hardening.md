# KnowForge Production Foundation Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add production-grade configuration consistency, health probes, bounded API inputs, observable worker failures, precise MinIO errors, aware UTC timestamps, and CI gates without external services or API response breakage.

**Architecture:** `Settings` remains the composition root and feeds explicit values into runtime clients. A focused health module probes injected application-state clients and returns sanitized readiness results. Reliability changes stay local to the worker pool and MinIO adapter, while timezone handling is standardized across schema, learning, storage, connector, and Kafka boundaries.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic v2, SQLAlchemy asyncio, Qdrant client, Neo4j async driver, boto3/botocore, pytest, Ruff, MyPy, GitHub Actions.

## Global Constraints

- Do not add an external LLM provider or alter the current placeholder RAG answer behavior.
- Do not require PostgreSQL, Qdrant, Neo4j, MinIO, Kafka, or Docker for unit tests.
- Preserve `/health` and all existing successful API response structures.
- Invalid bounded inputs use FastAPI's standard HTTP 422 response.
- Readiness failures return sanitized HTTP 503 responses without raw connection details.
- Follow red-green-refactor for every production behavior change.
- Run focused tests after each task and the complete quality gate at the end.

---

## File Structure

- `src/config.py`: canonical runtime configuration and validated limits.
- `src/main.py`: application composition and health route registration.
- `src/api/health.py`: liveness/readiness endpoints and isolated dependency probes.
- `src/api/qa.py`, `src/api/search.py`, `src/api/failures.py`, `src/api/export.py`: request boundary validation only.
- `src/storage/postgres_client.py`, `src/storage/qdrant_client.py`, `src/storage/neo4j_client.py`: explicit configuration and health probe methods.
- `src/storage/minio_client.py`: configurable bucket and precise missing-bucket handling.
- `src/processors/parallel/pool.py`: immutable worker failure records and correct metrics.
- `src/utils/time.py`: single aware-UTC clock helper.
- Timestamp consumers under `src/schemas`, `src/learning`, `src/kafka`, `src/connectors`, and `src/storage`: use the shared UTC helper.
- `tests/unit/test_config.py`, `tests/unit/test_health.py`: new focused tests.
- Existing API, worker, storage, schema, learning, connector, and Kafka tests: regression and boundary coverage.
- `.github/workflows/ci.yml`: infrastructure-free quality gate.
- `docker/.env.example`, `README.md`, `pyproject.toml`: configuration and tooling alignment.

---

### Task 1: Canonical, Validated Runtime Configuration

**Files:**
- Create: `tests/unit/test_config.py`
- Modify: `src/config.py`
- Modify: `src/main.py`
- Modify: `src/processors/vectorizer.py`
- Modify: `src/storage/qdrant_client.py`
- Modify: `src/storage/minio_client.py`
- Modify: `docker/.env.example`
- Modify: `README.md`

**Interfaces:**
- Produces: `Settings.embedding_model: str`, `embedding_dimension: int`, `embedding_batch_size: int`, `embedding_device: str`, `qdrant_collection_name: str`, `minio_bucket_name: str`, `kafka_consumer_group: str`, `kafka_dlq_topic: str`, `health_check_timeout_seconds: float`.
- Produces: `ContentVectorizer(model_name, dimension, batch_size, device)` and `QdrantVectorStore(url, dimension, collection_name)` configured by `lifespan`.
- Produces: `MinioFileStore(..., bucket_name: str = "knowforge")` compatibility constructor.

- [ ] **Step 1: Write failing configuration tests**

Create `tests/unit/test_config.py`:

```python
import pytest
from pydantic import ValidationError

from src.config import Settings


def test_settings_exposes_consistent_embedding_defaults() -> None:
    settings = Settings(_env_file=None)
    assert settings.embedding_model == "BAAI/bge-large-zh-v1.5"
    assert settings.embedding_dimension == 1024
    assert settings.qdrant_collection_name == "ai_knowledge_base"


def test_settings_reads_canonical_environment_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql+asyncpg://u:p@db:5432/kf")
    monkeypatch.setenv("EMBEDDING_DIMENSION", "768")
    monkeypatch.setenv("QDRANT_COLLECTION_NAME", "knowledge")
    settings = Settings(_env_file=None)
    assert settings.postgres_dsn.endswith("/kf")
    assert settings.embedding_dimension == 768
    assert settings.qdrant_collection_name == "knowledge"


@pytest.mark.parametrize(
    ("field", "value"),
    [("embedding_dimension", 0), ("embedding_batch_size", 0),
     ("qdrant_collection_name", ""), ("minio_bucket_name", "")],
)
def test_settings_rejects_invalid_runtime_values(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `uv run pytest tests/unit/test_config.py -q`

Expected: failures for missing settings fields and missing validation.

- [ ] **Step 3: Implement validated settings**

In `src/config.py`, use `Field` constraints and explicit defaults:

```python
from pydantic import Field

embedding_model: str = Field(default="BAAI/bge-large-zh-v1.5", min_length=1)
embedding_dimension: int = Field(default=1024, gt=0)
embedding_batch_size: int = Field(default=32, gt=0, le=1024)
embedding_device: str = Field(default="cpu", min_length=1)
qdrant_collection_name: str = Field(default="ai_knowledge_base", min_length=1)
minio_bucket_name: str = Field(default="knowforge", min_length=1)
kafka_consumer_group: str = Field(default="ai-kb-consumers", min_length=1)
kafka_dlq_topic: str = Field(default="dlq-events", min_length=1)
health_check_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
```

Update `QdrantVectorStore` to store `self.collection_name` from its constructor and replace every `COLLECTION_NAME` operation with that instance field. Update `MinioFileStore` in the same way for its bucket. In `src/main.py`, construct the vectorizer and vector store from the same `Settings` values.

- [ ] **Step 4: Align environment documentation**

Replace disconnected host fragments in `docker/.env.example` with `POSTGRES_DSN`, `QDRANT_URL`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSION`, `QDRANT_COLLECTION_NAME`, and `MINIO_BUCKET_NAME`. Set the documented model/dimension pair to BGE large Chinese/1024. Update the README configuration section to name these canonical variables.

- [ ] **Step 5: Verify GREEN and regressions**

Run: `uv run pytest tests/unit/test_config.py tests/api/test_routes.py tests/unit/test_storage_clients.py -q`

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```text
git add src/config.py src/main.py src/processors/vectorizer.py src/storage/qdrant_client.py src/storage/minio_client.py docker/.env.example README.md tests/unit/test_config.py
git commit -m "feat: centralize runtime configuration"
```

---

### Task 2: Bounded API Inputs

**Files:**
- Modify: `tests/api/test_routes.py`
- Modify: `src/api/qa.py`
- Modify: `src/api/search.py`
- Modify: `src/api/failures.py`
- Modify: `src/api/export.py`

**Interfaces:**
- Produces: `QAMode = Literal["vector", "graph", "hybrid"]`.
- Consumes: existing route/service signatures without changing successful response models.
- Produces: HTTP 422 for empty/oversized strings, invalid modes, limits outside 1..100, and more than 100 export IDs.

- [ ] **Step 1: Write failing boundary tests**

Add parameterized tests to `tests/api/test_routes.py`:

```python
@pytest.mark.parametrize(
    "payload",
    [
        {"question": ""},
        {"question": "x" * 8001},
        {"question": "ok", "mode": "invalid"},
        {"question": "ok", "limit": 0},
        {"question": "ok", "limit": 101},
    ],
)
def test_qa_rejects_invalid_bounds(client, payload):
    assert client.post("/api/v1/qa", json=payload).status_code == 422


@pytest.mark.parametrize(
    "payload",
    [{"query": ""}, {"query": "x" * 8001}, {"query": "ok", "limit": 0},
     {"query": "ok", "limit": 101}],
)
def test_search_rejects_invalid_bounds(client, payload):
    assert client.post("/api/v1/search", json=payload).status_code == 422


def test_failure_list_rejects_invalid_limit(client):
    assert client.get("/api/v1/failures?limit=0").status_code == 422
    assert client.get("/api/v1/failures?limit=101").status_code == 422


def test_export_rejects_more_than_100_ids(client):
    ids = ",".join(f"doc-{index}" for index in range(101))
    assert client.get(f"/api/v1/export?ids={ids}").status_code == 422


def test_export_rejects_oversized_query(client):
    assert client.get("/api/v1/export", params={"query": "x" * 8001}).status_code == 422
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/api/test_routes.py -q`

Expected: the new cases reach services or return non-422 responses.

- [ ] **Step 3: Add minimal Pydantic and FastAPI constraints**

Use `Annotated`, `StringConstraints`, `Field`, `Literal`, and `Query`:

```python
BoundedText = Annotated[str, StringConstraints(min_length=1, max_length=8_000)]
QAMode = Literal["vector", "graph", "hybrid"]

class QARequest(BaseModel):
    question: BoundedText
    filters: dict[str, Any] | None = None
    mode: QAMode = "hybrid"
    limit: int = Field(default=10, ge=1, le=100)
```

Apply the same bounded text and limit to search. Declare failure limit as `Query(20, ge=1, le=100)`. Declare export query as `Query(None, max_length=8_000)` and validate parsed non-empty ID count before loading documents; raise `HTTPException(422, "At most 100 document IDs are allowed")` when exceeded.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/api/test_routes.py -q`

Expected: all API tests pass with existing successful bodies unchanged.

- [ ] **Step 5: Commit**

```text
git add src/api/qa.py src/api/search.py src/api/failures.py src/api/export.py tests/api/test_routes.py
git commit -m "feat: validate API request bounds"
```

---

### Task 3: Liveness and Dependency-Aware Readiness

**Files:**
- Create: `src/api/health.py`
- Create: `tests/unit/test_health.py`
- Modify: `src/main.py`
- Modify: `src/api/__init__.py`
- Modify: `src/storage/postgres_client.py`
- Modify: `src/storage/qdrant_client.py`
- Modify: `src/storage/neo4j_client.py`
- Modify: `tests/api/test_routes.py`

**Interfaces:**
- Produces: `PostgresClient.health_check() -> Awaitable[None]`.
- Produces: `QdrantVectorStore.health_check() -> None`.
- Produces: `Neo4jGraphStore.health_check() -> Awaitable[None]`.
- Produces: `probe_dependencies(app: FastAPI, timeout_seconds: float) -> Awaitable[dict[str, str]]`.
- Produces: `GET /health/live` and `GET /health/ready`.

- [ ] **Step 1: Write failing health tests**

Create `tests/unit/test_health.py` with fake clients:

```python
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.api.health import probe_dependencies


@pytest.mark.asyncio
async def test_probe_dependencies_reports_all_ready() -> None:
    app = SimpleNamespace(state=SimpleNamespace(
        postgres_client=SimpleNamespace(health_check=AsyncMock(return_value=None)),
        vector_store=SimpleNamespace(health_check=MagicMock(return_value=None)),
        neo4j_client=SimpleNamespace(health_check=AsyncMock(return_value=None)),
    ))
    assert await probe_dependencies(app, 0.5) == {
        "postgres": "ready", "qdrant": "ready", "neo4j": "ready"
    }


@pytest.mark.asyncio
async def test_probe_dependencies_sanitizes_failure() -> None:
    app = SimpleNamespace(state=SimpleNamespace(
        postgres_client=SimpleNamespace(
            health_check=AsyncMock(side_effect=RuntimeError("secret password"))
        ),
        vector_store=SimpleNamespace(health_check=MagicMock(return_value=None)),
        neo4j_client=SimpleNamespace(health_check=AsyncMock(return_value=None)),
    ))
    result = await probe_dependencies(app, 0.5)
    assert result["postgres"] == "unavailable"
    assert "secret" not in repr(result)
```

Add API assertions for `/health/live` returning 200 and readiness returning 200/503 when app-state health methods succeed/fail.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/unit/test_health.py tests/api/test_routes.py -q`

Expected: import failure for `src.api.health` and missing routes.

- [ ] **Step 3: Implement storage probes**

Implement PostgreSQL with `async with self.engine.connect() as connection: await connection.execute(text("SELECT 1"))`; Qdrant with `self.client.get_collections()`; Neo4j with `await self.driver.verify_connectivity()`.

- [ ] **Step 4: Implement isolated readiness probing**

In `src/api/health.py`, normalize sync/async calls using `inspect.isawaitable`, execute the synchronous Qdrant probe with `asyncio.to_thread`, wrap each probe with `asyncio.wait_for`, and convert every exception/timeout to the stable string `unavailable`. Return dependency keys in a stable order.

The readiness endpoint returns:

```python
body = {"status": "ready" if ready else "not_ready", "dependencies": dependencies}
return JSONResponse(status_code=200 if ready else 503, content=body)
```

Register the router without a version prefix and retain the existing `/health` route.

- [ ] **Step 5: Verify GREEN**

Run: `uv run pytest tests/unit/test_health.py tests/api/test_routes.py tests/unit/test_storage_clients.py -q`

Expected: all selected tests pass and no raw exception string appears in responses.

- [ ] **Step 6: Commit**

```text
git add src/api/health.py src/api/__init__.py src/main.py src/storage/postgres_client.py src/storage/qdrant_client.py src/storage/neo4j_client.py tests/unit/test_health.py tests/api/test_routes.py tests/unit/test_storage_clients.py
git commit -m "feat: add readiness health probes"
```

---

### Task 4: Observable Worker Failures and Precise MinIO Errors

**Files:**
- Modify: `tests/unit/test_worker_pool.py`
- Modify: `tests/unit/test_storage_clients.py`
- Modify: `src/processors/parallel/pool.py`
- Modify: `src/storage/minio_client.py`

**Interfaces:**
- Produces: frozen `WorkerFailure(worker_id: int, error_type: str, message: str, failed_at: datetime)`.
- Produces: `WorkerInfo.tasks_failed: int = 0` in addition to `tasks_completed`.
- Produces: `WorkerPool.get_failures() -> list[WorkerFailure]` as a snapshot.
- Produces: `WorkerPool(max_failures: int = 100)` bounded failure retention.
- Produces: `MinioFileStore.ensure_bucket()` creates only for S3 error code `404`, `NoSuchBucket`, or `NotFound`.

- [ ] **Step 1: Write failing worker tests**

Add to `tests/unit/test_worker_pool.py`:

```python
@pytest.mark.asyncio
async def test_failed_task_is_recorded_and_later_task_runs():
    pool = WorkerPool(max_workers=1)
    await pool.start()
    ran = []

    async def fail():
        raise ValueError("bad task")

    async def succeed():
        ran.append(True)

    await pool.submit(fail)
    await pool.submit(succeed)
    await pool._tasks.join()
    failures = pool.get_failures()
    assert ran == [True]
    assert len(failures) == 1
    assert failures[0].error_type == "ValueError"
    assert sum(worker.tasks_failed for worker in pool.get_workers()) == 1
    assert sum(worker.tasks_completed for worker in pool.get_workers()) == 1
    await pool.shutdown()


@pytest.mark.asyncio
async def test_failure_buffer_is_bounded():
    pool = WorkerPool(max_workers=1, max_failures=2)
    await pool.start()
    async def fail(index):
        raise RuntimeError(str(index))
    for index in range(3):
        await pool.submit(fail, index)
    await pool._tasks.join()
    assert [failure.message for failure in pool.get_failures()] == ["1", "2"]
    await pool.shutdown()
```

- [ ] **Step 2: Write failing MinIO tests**

Use `botocore.exceptions.ClientError` in `tests/unit/test_storage_clients.py`:

```python
def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "HeadBucket")

def test_ensure_bucket_creates_only_when_missing():
    store = MinioFileStore(bucket_name="docs")
    store.client = MagicMock()
    store.client.head_bucket.side_effect = client_error("404")
    store.ensure_bucket()
    store.client.create_bucket.assert_called_once_with(Bucket="docs")

def test_ensure_bucket_propagates_permission_error():
    store = MinioFileStore(bucket_name="docs")
    store.client = MagicMock()
    store.client.head_bucket.side_effect = client_error("AccessDenied")
    with pytest.raises(ClientError):
        store.ensure_bucket()
    store.client.create_bucket.assert_not_called()
```

- [ ] **Step 3: Verify RED**

Run: `uv run pytest tests/unit/test_worker_pool.py tests/unit/test_storage_clients.py -q`

Expected: missing failure API/metric and permission errors incorrectly creating buckets.

- [ ] **Step 4: Implement minimal reliability changes**

Use a `deque[WorkerFailure](maxlen=max_failures)`, create `WorkerFailure` and `WorkerInfo` as frozen dataclasses, and update exactly one of `tasks_completed` or `tasks_failed` per task. Keep `task_done()` in `finally`. Return `list(self._failures)` from `get_failures()`.

In MinIO, catch only `ClientError`, inspect `error.response["Error"]["Code"]`, create for the three missing codes, and re-raise all other codes unchanged.

- [ ] **Step 5: Verify GREEN**

Run: `uv run pytest tests/unit/test_worker_pool.py tests/unit/test_storage_clients.py -q`

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```text
git add src/processors/parallel/pool.py src/storage/minio_client.py tests/unit/test_worker_pool.py tests/unit/test_storage_clients.py
git commit -m "fix: expose worker and storage failures"
```

---

### Task 5: Timezone-Aware UTC Timestamps

**Files:**
- Create: `src/utils/time.py`
- Modify: `src/schemas/content.py`
- Modify: `src/schemas/events.py`
- Modify: `src/learning/failure_tracker.py`
- Modify: `src/learning/strategy_store.py`
- Modify: `src/kafka/producer.py`
- Modify: `src/kafka/consumer.py`
- Modify: `src/connectors/github_connector.py`
- Modify: `src/storage/postgres_client.py`
- Modify: `tests/unit/test_schemas.py`
- Modify: `tests/unit/test_learning.py`
- Modify: `tests/unit/test_kafka.py`
- Modify: `tests/unit/test_connectors.py`
- Modify: `tests/unit/test_storage_clients.py`

**Interfaces:**
- Produces: `utc_now() -> datetime`, always returning `datetime.now(UTC)`.
- Preserves serialized ISO 8601 timestamp strings while adding the explicit `+00:00` offset.

- [ ] **Step 1: Write failing timezone assertions**

Update timestamp tests to assert awareness:

```python
from datetime import UTC

def test_kafka_event_timestamp_is_aware_utc():
    event = KafkaEvent(event_type=EventType.DOCUMENT_INGESTED)
    assert event.timestamp.tzinfo is UTC
    assert event.timestamp.utcoffset().total_seconds() == 0

def test_processing_failure_timestamps_are_aware_utc():
    failure = FailureTracker().record_failure(
        "file", "doc", FailureType.PARSE_ERROR, "failed"
    )
    assert failure.created_at.tzinfo is UTC
    assert failure.updated_at.tzinfo is UTC
```

Update connector and producer expectations to accept/require a `+00:00` suffix rather than naive ISO strings.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/unit/test_schemas.py tests/unit/test_learning.py tests/unit/test_kafka.py tests/unit/test_connectors.py tests/unit/test_storage_clients.py -q`

Expected: assertions show `tzinfo is None` in current defaults.

- [ ] **Step 3: Add the shared clock and migrate production consumers**

Create:

```python
from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)
```

Replace production `datetime.utcnow` defaults and calls with `utc_now`. Use `DateTime(timezone=True)` in touched SQLAlchemy mappings. Remove now-unused `datetime` imports. Ensure TTL comparisons use aware values on both sides.

- [ ] **Step 4: Verify GREEN and warnings**

Run: `uv run pytest tests/unit/test_schemas.py tests/unit/test_learning.py tests/unit/test_kafka.py tests/unit/test_connectors.py tests/unit/test_storage_clients.py -q`

Expected: all selected tests pass with no `datetime.utcnow()` warning originating from `src/`.

- [ ] **Step 5: Commit**

```text
git add src/utils/time.py src/schemas src/learning src/kafka src/connectors/github_connector.py src/storage/postgres_client.py tests/unit/test_schemas.py tests/unit/test_learning.py tests/unit/test_kafka.py tests/unit/test_connectors.py tests/unit/test_storage_clients.py
git commit -m "refactor: use timezone-aware UTC timestamps"
```

---

### Task 6: Quality Debt Cleanup and CI Gate

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `pyproject.toml`
- Modify: lint/type-affected files under `src/` and `tests/`
- Modify: `README.md`

**Interfaces:**
- Produces: a GitHub Actions workflow running the same three local acceptance commands.
- Produces: current Ruff configuration under `[tool.ruff.lint]` rather than deprecated top-level keys.
- Does not intentionally change runtime behavior beyond Tasks 1-5.

- [ ] **Step 1: Capture the current failing quality gates**

Run:

```text
uv run ruff check src tests
uv run mypy src
```

Expected: Ruff and MyPy fail before cleanup. Save the categories in the task notes; do not weaken rules to make them disappear.

- [ ] **Step 2: Modernize Ruff configuration**

Move `select`, `ignore`, and `per-file-ignores` beneath `[tool.ruff.lint]`. Keep the existing rule set `E`, `F`, `I`, `N`, `W`, `UP` and the existing `E501` exception.

- [ ] **Step 3: Apply safe mechanical fixes and review the diff**

Run: `uv run ruff check src tests --fix`

Then inspect `git diff`. Accept import sorting, unused imports, newline fixes, modern optional syntax, `StrEnum`, and builtin `TimeoutError` updates. Manually repair behavioral findings such as unused prompt construction only when removal preserves the current placeholder RAG behavior.

- [ ] **Step 4: Resolve remaining MyPy findings without broad ignores**

Run `uv run mypy src`, fix signatures and third-party boundaries with narrow protocols or casts, and add a targeted per-module override only if a dependency genuinely lacks usable type information. Do not set `ignore_missing_imports = true` globally and do not introduce `Any` where an existing precise type is available.

- [ ] **Step 5: Add CI workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - run: python -m pip install --upgrade pip
      - run: python -m pip install -e ".[dev]"
      - run: ruff check src tests
      - run: mypy src
      - run: pytest tests -q
```

- [ ] **Step 6: Update README status**

Replace the stale `334 passed` note with a non-numeric statement that the test suite is enforced by CI. Document `/health/live` and `/health/ready` and the canonical configuration names from Task 1.

- [ ] **Step 7: Run the complete verification gate**

Run:

```text
uv run ruff check src tests
uv run mypy src
uv run pytest tests -q
uv run pytest --cov=src --cov-report=term-missing tests
git diff --check
```

Expected: Ruff exits 0, MyPy exits 0, all tests pass, coverage is at least 80%, and `git diff --check` reports no whitespace errors.

- [ ] **Step 8: Commit**

```text
git add .github/workflows/ci.yml pyproject.toml README.md src tests
git commit -m "ci: enforce project quality gates"
```

---

## Final Review Checklist

- [ ] Compare every acceptance criterion in `docs/superpowers/specs/2026-07-11-production-foundation-hardening-design.md` with a passing test or quality command.
- [ ] Confirm `git status --short` contains no unrelated or accidental files.
- [ ] Confirm no credentials, raw DSNs, model downloads, or generated caches are staged.
- [ ] Review the final diff specifically for API response compatibility and startup behavior.
- [ ] Report the exact test, Ruff, MyPy, and coverage results to the user.
