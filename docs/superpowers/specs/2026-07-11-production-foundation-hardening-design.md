# KnowForge Production Foundation Hardening Design

## Objective

Harden KnowForge's production foundation without introducing an external LLM,
changing the main API response shapes, or requiring Docker-backed services in the
unit test suite. The change must leave the existing behavior covered, add explicit
failure behavior for new safeguards, and keep the full test suite passing.

## Scope

This iteration includes:

- making `src.config.Settings` the single source of truth for runtime service,
  embedding, collection, bucket, and Kafka configuration;
- aligning `docker/.env.example` with the names and defaults actually consumed by
  the application;
- adding liveness and dependency-aware readiness health endpoints while retaining
  the existing `/health` compatibility endpoint;
- bounding and validating QA, search, failure-list, and export request inputs;
- making worker-task failures observable instead of silently swallowing them;
- narrowing MinIO bucket-creation error handling to the missing-bucket case;
- replacing naive UTC timestamps in production code with timezone-aware UTC values;
- adding a continuous-integration workflow for linting, type checking, and tests;
- resolving production-code lint and type errors touched or exposed by this work.

This iteration explicitly excludes:

- an LLM provider or real answer generation;
- authentication, authorization, tenant isolation, and rate limiting;
- Alembic migrations and persistence changes to the failure tracker;
- connector scheduling and complete Kafka worker orchestration;
- distributed transactions or an outbox across PostgreSQL, Qdrant, Neo4j, and
  MinIO;
- Docker-backed integration tests.

## Configuration Design

`Settings` owns every value needed during application construction. In addition to
the current connection settings, it exposes the embedding model name, embedding
dimension, Qdrant collection name, MinIO bucket name, and Kafka consumer settings.
Application construction passes those values into clients rather than relying on
client-level hard-coded production defaults.

`docker/.env.example` uses the exact environment variable names accepted by
`Settings`. Composite values such as `POSTGRES_DSN` and `QDRANT_URL` are documented
as such; disconnected `POSTGRES_HOST`-style variables are removed unless the
application consumes them. The example uses the BGE large Chinese model and a
1024-dimensional collection consistently.

Settings validation rejects non-positive embedding dimensions, empty collection or
bucket names, and invalid application limits at startup. This catches incompatible
configuration before the service accepts traffic.

## Health Endpoint Design

The application provides three endpoints:

- `GET /health` remains a lightweight compatibility liveness response with the
  existing `{"status": "healthy"}` body;
- `GET /health/live` confirms that the API process is running and returns HTTP 200;
- `GET /health/ready` runs bounded probes for PostgreSQL, Qdrant, and Neo4j and
  returns a per-dependency result.

Readiness returns HTTP 200 with overall status `ready` only when every required
dependency succeeds. It returns HTTP 503 with overall status `not_ready` when any
probe fails or times out. A failed probe is represented by a stable public status,
not by a raw exception or credential-bearing connection string.

Health probing is isolated behind small async-compatible probe functions. Tests can
inject fake clients and do not need live infrastructure. Synchronous third-party
health methods are moved off the event loop when necessary.

## API Validation Design

Pydantic request models and FastAPI query constraints enforce these bounds:

- QA question: 1 to 8,000 characters;
- search query: 1 to 8,000 characters;
- QA mode: only `vector`, `graph`, or `hybrid`;
- QA and search result limits: 1 to 100;
- failure list limit: 1 to 100;
- export IDs: at most 100 non-empty IDs;
- export query: at most 8,000 characters.

Existing successful response shapes remain unchanged. Invalid requests use
FastAPI's standard HTTP 422 validation response. Dependency failures discovered by
readiness use HTTP 503. This iteration does not add a new global error envelope.

## Worker Failure Design

`WorkerPool` continues processing subsequent tasks after an individual task fails,
but it no longer discards the exception. The pool records a bounded immutable
failure entry containing the worker identifier, exception type, public message, and
timestamp. Callers can inspect failures through a read-only snapshot method.

A failed task is counted separately from a completed task. Queue bookkeeping still
calls `task_done()` exactly once. The pool does not automatically retry tasks in this
iteration because retry policy belongs to the processor node and Kafka pipeline.

## MinIO Error Handling Design

`ensure_bucket()` creates the bucket only when `head_bucket` reports the S3
not-found condition. Authentication, authorization, network, and unexpected service
errors propagate to the caller. This prevents misconfiguration from being mistaken
for a missing bucket.

The bucket name becomes an instance configuration value supplied by `Settings`.
Existing default construction remains supported for compatibility.

## Time Handling Design

Production timestamps use `datetime.now(UTC)` and remain timezone-aware through
Pydantic models, Kafka payload creation, failure tracking, strategy tracking, and
database record conversion. Comparisons only combine aware timestamps. Serialized
values use ISO 8601 and therefore include the UTC offset.

Database columns touched by this iteration are configured as timezone-aware where
the ORM mapping permits it. A database migration is outside scope, so this change
does not claim to alter an already-deployed schema.

## Continuous Integration Design

A GitHub Actions workflow runs on pushes and pull requests using Python 3.11. It
installs the development dependency group from the project metadata and executes:

1. `ruff check src tests`;
2. `mypy src`;
3. `pytest tests -q`.

The workflow uses no external service containers. Checks must use dependency mocks
already present in the unit tests. Production-code lint issues are corrected in this
iteration. Purely mechanical test-file lint cleanup may be included when required to
make the declared CI command pass.

## Testing Strategy

Every behavior change follows red-green-refactor:

- settings tests cover environment-name alignment and invalid values;
- API tests cover valid boundaries and HTTP 422 responses outside them;
- health tests cover all-ready, individual dependency failure, timeout, and absence
  of sensitive error details;
- worker-pool tests prove failures are recorded, task accounting remains correct,
  and later tasks still run;
- MinIO tests distinguish a missing bucket from permission and network failures;
- time tests assert timezone-aware values and serialized UTC offsets;
- the complete existing test suite is run after focused tests;
- Ruff, MyPy, and the full test suite are the final acceptance checks.

## Compatibility and Rollout

The existing `/health` endpoint and successful QA, search, failure, and export
response structures remain compatible. Invalid inputs that previously passed
through may now receive HTTP 422. Configuration deployments must rename variables
to the canonical `Settings` names shown in the updated environment example.

The change is safe to roll out before the ingestion and real-RAG iterations because
it does not require an external model API, a database schema migration, or a live
Kafka consumer.

## Acceptance Criteria

- `Settings` and `docker/.env.example` agree on every documented runtime variable.
- Embedding model and Qdrant vector dimension cannot silently diverge in default
  configuration.
- Liveness remains available even if dependencies are down.
- Readiness returns HTTP 503 and identifies failed dependencies without leaking raw
  connection details.
- Oversized, empty, or out-of-range API inputs receive HTTP 422.
- Worker exceptions are observable and never silently counted as successful work.
- MinIO creates a bucket only for a verified not-found response.
- Production code no longer emits naive-UTC deprecation warnings in covered paths.
- Ruff, MyPy, and all tests pass in CI without infrastructure containers.
