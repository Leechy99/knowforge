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

## API Overview

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Health check |
| `POST` | `/api/v1/qa` | RAG-style question answering |
| `POST` | `/api/v1/search` | Vector similarity search |
| `GET` | `/api/v1/export` | Export documents |
| `GET` | `/api/v1/failures` | List processing failures |
| `POST` | `/api/v1/failures/{id}/feedback` | Submit failure feedback |

## Development

Useful commands:

```bash
pytest tests/ -q
ruff check src tests
mypy src
```

Current test status in this workspace:

```text
330 passed
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
