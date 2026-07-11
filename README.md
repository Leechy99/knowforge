# KnowForge

KnowForge is an AI-ready knowledge ingestion and retrieval service. It collects content
from files, web pages, GitHub repositories, and databases; cleans and chunks that content;
stores it across vector, graph, metadata, and object stores; and exposes APIs for RAG,
vector search, graph queries, failure feedback, and export.

The project is built with FastAPI and is designed as a practical backend foundation for
knowledge-base and retrieval-augmented generation systems.

## Features

- Multi-source ingestion: file watcher, GitHub connector, crawler, and database connector
- Processing pipeline: parsing, cleaning, deduplication, chunking, and embedding
- Storage integrations: PostgreSQL, Qdrant, Neo4j, and MinIO
- Query APIs: RAG QA, vector search, graph query, and document export
- Failure feedback loop: track processing failures and apply user feedback
- Export formats: Markdown, JSON, and graph-oriented JSON
- Parallel processing primitives for worker pools, queues, and processor nodes

## Architecture

```text
Sources
  -> Kafka event bus
  -> Processors: parser -> cleaner -> deduplicator -> chunker -> vectorizer
  -> Storage: PostgreSQL + Qdrant + Neo4j + MinIO
  -> FastAPI query and management APIs
```

## Quick Start

### 1. Create an environment

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -e ".[dev]"
```

On Linux or macOS:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

### 2. Start infrastructure

```bash
cd docker
docker compose up -d
```

This starts Kafka, PostgreSQL, Qdrant, Neo4j, and MinIO for local development.

### 3. Configure environment

```bash
cp docker/.env.example .env
```

Edit `.env` as needed for your local services.

The canonical runtime variables are `POSTGRES_DSN`, `QDRANT_URL`,
`QDRANT_COLLECTION_NAME`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`,
`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET_NAME`,
`EMBEDDING_MODEL`, and `EMBEDDING_DIMENSION`. Keep the embedding model and vector
dimension aligned; the default BGE large Chinese model uses 1024 dimensions.

### 4. Run tests

```bash
pytest tests/ -q
```

### 5. Start the API

```bash
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

Then open:

```text
http://localhost:8000/docs
```

On application startup, `src.main` wires the runtime query services into FastAPI
state:

- `qa_service`: `RAGQA` backed by Qdrant, PostgreSQL, Neo4j, and the lazy BGE
  vectorizer
- `search_service`: `VectorSearch` backed by Qdrant and the same vectorizer
- `document_service`: `DocumentService`, a query-layer adapter backed by
  `PostgresClient`, used by export routes to load documents by ID, list recent
  documents, or search document text/source URLs

With local infrastructure running, `/api/v1/qa`, `/api/v1/search`, and
`/api/v1/export` are available immediately after a fresh API boot instead of requiring
manual test-time service injection.

## API Overview

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Health check |
| `POST` | `/api/v1/qa` | RAG-style question answering |
| `POST` | `/api/v1/search` | Vector similarity search |
| `GET` | `/api/v1/export` | Export documents |
| `GET` | `/api/v1/failures` | List processing failures |
| `POST` | `/api/v1/failures/{id}/feedback` | Submit failure feedback |

`/api/v1/export` accepts `format=markdown|json|graph`, optional comma-separated
`ids`, or a `query` parameter. When no IDs or query are provided, it exports the latest
documents from PostgreSQL.

## Development

Useful commands:

```bash
pytest tests/ -q
ruff check src tests
mypy src
```

Current test status in this workspace:

```text
334 passed
```

`ruff` and `mypy` are enabled but still surface existing cleanup/type-debt items. The
runtime test suite is the current source of truth for the first development phase.

## Project Status

KnowForge is early-stage software. The current codebase provides a working service
skeleton, storage/query adapters, processor components, and tests. Production deployment
still needs hardening around configuration, authentication, observability, migrations, and
long-running connector orchestration.

## License

MIT License. See [LICENSE](LICENSE).
