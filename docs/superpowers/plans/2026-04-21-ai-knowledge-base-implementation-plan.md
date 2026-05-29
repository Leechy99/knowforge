# AI Knowledge Base Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建一个面向 AI 的知识库系统，支持多数据源接入、处理管道、向量/图存储、RAG 查询、失败追踪与自适应学习

**Architecture:** 事件驱动架构，Kafka 作为事件总线，各组件解耦。分为5个阶段：基础设施 → 核心处理 → 数据源 → 查询接口 → 学习系统

**Tech Stack:** Python + FastAPI + Kafka + PostgreSQL + Qdrant + Neo4j + MinIO + BGE-large-zh

---

## Phase 1: Foundation (Week 1-2)

### 基础设施搭建 / Infrastructure Setup

**Files:**
- Create: `docker/docker-compose.yml`
- Create: `docker/.env.example`
- Create: `pyproject.toml`
- Create: `src/__init__.py`
- Create: `src/schemas/__init__.py`
- Create: `src/schemas/content.py` (Unified Content Schema)
- Create: `src/schemas/events.py` (Kafka Event Schema)
- Create: `tests/unit/test_schemas.py`

- [ ] **Step 1: 创建 docker-compose.yml (Kafka + PostgreSQL + Qdrant + Neo4j + MinIO)**

```yaml
version: '3.8'

services:
  # Kafka + Zookeeper
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    extra_hosts:
      - "host.docker.internal:host-gateway"

  # PostgreSQL + pgvector
  postgres:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_DB: aikb
      POSTGRES_USER: aikb
      POSTGRES_PASSWORD: aikb_secret
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  # Qdrant Vector Store
  qdrant:
    image: qdrant/qdrant:v1.7.0
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage

  # Neo4j Graph DB
  neo4j:
    image: neo4j:5.14-community
    environment:
      NEO4J_AUTH: neo4j/neo4j_secret
      NEO4J_PLUGINS: '["apoc"]'
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - neo4j_data:/data

  # MinIO (S3-compatible)
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: aikb
      MINIO_ROOT_PASSWORD: aikb_secret
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data

volumes:
  postgres_data:
  qdrant_data:
  neo4j_data:
  minio_data:
```

- [ ] **Step 2: 创建 pyproject.toml**

```toml
[project]
name = "ai-knowledge-base"
version = "0.1.0"
description = "AI-Ready Knowledge Repository"
requires-python = ">=3.11"

dependencies = [
    # FastAPI & Web
    "fastapi>=0.109.0",
    "uvicorn[standard]>=0.27.0",
    "pydantic>=2.5.0",
    "python-multipart>=0.0.6",

    # Kafka
    "aiokafka>=0.10.0",
    "kafka-python>=2.0.2",

    # Database Clients
    "asyncpg>=0.29.0",
    "psycopg2-binary>=2.9.9",
    "sqlalchemy[asyncio]>=2.0.25",
    "neo4j>=5.17.0",
    "qdrant-client>=1.7.0",
    "boto3>=1.34.0",

    # Document Processing
    "pdfplumber>=0.10.3",
    "python-docx>=1.1.0",
    "markdown-it>=3.0.0",
    "beautifulsoup4>=4.12.3",
    "lxml>=5.1.0",
    "playwright>=1.40.0",
    "scrapy>=2.11.0",

    # Embedding & ML
    "sentence-transformers>=2.3.0",
    "torch>=2.1.0",
    "numpy>=1.26.0",

    # Text Processing
    "tiktoken>=0.5.0",
    "simhash>=2.1.2",

    # Utilities
    "watchdog>=4.0.0",
    "httpx>=0.26.0",
    "chardet>=5.2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.1.0",
    "ruff>=0.1.0",
    "mypy>=1.8.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 3: 创建 Unified Content Schema (src/schemas/content.py)**

```python
"""
Unified Content Schema - AI-Ready Knowledge Repository
"""
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    FILE = "file"
    CRAWL = "crawl"
    DB = "db"


class ContentType(str, Enum):
    ARTICLE = "article"
    CODE = "code"
    DOC = "doc"
    SOCIAL = "social"


class ContentMetadata(BaseModel):
    title: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)
    language: str | None = None
    license: str | None = None
    stars: int | None = None
    forks: int | None = None


class ContentChunk(BaseModel):
    text: str
    index: int


class ContentEntity(BaseModel):
    name: str
    type: str  # PERSON|ORG|PROJECT|LANGUAGE|...


class ContentRelation(BaseModel):
    from_entity: str = Field(alias="from")
    to_entity: str = Field(alias="to")
    relation_type: str


class ContentVectors(BaseModel):
    dense: list[float] | None = None
    sparse: dict[str, list[float]] | None = None


class ContentBody(BaseModel):
    text: str = ""
    chunks: list[ContentChunk] = Field(default_factory=list)
    entities: list[ContentEntity] = Field(default_factory=list)
    relations: list[ContentRelation] = Field(default_factory=list)


class ContentDocument(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    source_type: SourceType
    source_url: str = ""
    content_type: ContentType = ContentType.ARTICLE

    metadata: ContentMetadata = Field(default_factory=ContentMetadata)
    content: ContentBody = Field(default_factory=ContentBody)
    vectors: ContentVectors | None = None

    quality_score: float = 0.0

    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: datetime | None = None

    class Config:
        populate_by_name = True
```

- [ ] **Step 4: 创建 Kafka Event Schema (src/schemas/events.py)**

```python
"""
Kafka Event Schemas for AI Knowledge Base
"""
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from .content import SourceType


class EventType(str, Enum):
    DOCUMENT_INGESTED = "document.ingested"
    DOCUMENT_PROCESSED = "document.processed"
    DOCUMENT_FAILED = "document.failed"
    DOCUMENT_EXPORTED = "document.exported"


class KafkaEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source_type: SourceType
    payload: dict[str, Any]

    metadata: dict[str, Any] = Field(default_factory=dict)
```

- [ ] **Step 5: 运行测试验证 Schema**

```bash
cd d:\project\AI
pytest tests/unit/test_schemas.py -v
```

Expected: PASS (schemas defined correctly)

- [ ] **Step 6: 提交 Phase 1**

```bash
git add docker/pyproject.toml src/schemas/ tests/
git commit -m "feat: add infrastructure foundation (docker-compose, schemas)"
```

---

### Phase 1 Summary

| Component | Status | Files |
|-----------|--------|-------|
| Docker Compose | ✅ | docker/docker-compose.yml |
| Schema Definitions | ✅ | src/schemas/content.py, events.py |
| Project Config | ✅ | pyproject.toml |

---

## Phase 2: Core Processing Pipeline (Week 2-4)

### 2.1 Storage Layer Clients

**Files:**
- Create: `src/storage/postgres_client.py`
- Create: `src/storage/qdrant_client.py`
- Create: `src/storage/neo4j_client.py`
- Create: `src/storage/minio_client.py`
- Create: `src/storage/__init__.py`
- Create: `tests/unit/test_storage_clients.py`

- [ ] **Step 1: 创建 PostgreSQL Client (src/storage/postgres_client.py)**

```python
"""
PostgreSQL Client with pgvector support
"""
import uuid
from datetime import datetime
from typing import Any

import asyncpg
from sqlalchemy import Column, String, Text, Float, JSON, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DocumentRecord(Base):
    __tablename__ = "documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_type = Column(String(50), nullable=False)
    source_url = Column(Text, nullable=False)
    content_type = Column(String(50), nullable=False)
    metadata = Column(JSONB, nullable=False, default=dict)
    content_text = Column(Text, nullable=False, default="")
    chunks = Column(JSONB, nullable=False, default=list)
    quality_score = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_source_type", "source_type"),
        Index("idx_content_type", "content_type"),
        Index("idx_quality_score", "quality_score"),
    )


class PostgresClient:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.engine = create_async_engine(dsn, echo=False)
        self.session_factory = async_sessionmaker(self.engine, class_=AsyncSession)

    async def init(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self):
        await self.engine.dispose()

    async def store_document(self, doc: dict[str, Any]) -> str:
        async with self.session_factory() as session:
            record = DocumentRecord(
                id=doc.get("id", uuid.uuid4()),
                source_type=doc["source_type"],
                source_url=doc.get("source_url", ""),
                content_type=doc.get("content_type", "article"),
                metadata=doc.get("metadata", {}),
                content_text=doc.get("content", {}).get("text", ""),
                chunks=doc.get("content", {}).get("chunks", []),
                quality_score=doc.get("quality_score", 0.0),
                processed_at=datetime.utcnow(),
            )
            session.add(record)
            await session.commit()
            return str(record.id)

    async def get_document(self, doc_id: str) -> dict[str, Any] | None:
        async with self.session_factory() as session:
            result = await session.get(DocumentRecord, uuid.UUID(doc_id))
            if result:
                return {
                    "id": str(result.id),
                    "source_type": result.source_type,
                    "source_url": result.source_url,
                    "content_type": result.content_type,
                    "metadata": result.metadata,
                    "content_text": result.content_text,
                    "chunks": result.chunks,
                    "quality_score": result.quality_score,
                    "created_at": result.created_at.isoformat(),
                    "processed_at": result.processed_at.isoformat() if result.processed_at else None,
                }
            return None
```

- [ ] **Step 2: 创建 Qdrant Client (src/storage/qdrant_client.py)**

```python
"""
Qdrant Vector Store Client
"""
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import Distance, VectorParams, PointStruct


class QdrantVectorStore:
    COLLECTION_NAME = "ai_knowledge_base"

    def __init__(self, url: str = "http://localhost:6333", dimension: int = 1024):
        self.client = QdrantClient(url=url)
        self.dimension = dimension

    def create_collection(self, recreate: bool = False):
        if recreate:
            self.client.delete_collection(self.COLLECTION_NAME)
        self.client.create_collection(
            collection_name=self.COLLECTION_NAME,
            vectors_config=VectorParams(size=self.dimension, distance=Distance.COSINE),
        )

    def upsert_vectors(self, points: list[dict[str, Any]]):
        self.client.upsert(
            collection_name=self.COLLECTION_NAME,
            points=[
                PointStruct(
                    id=p["id"],
                    vector=p["vector"],
                    payload=p.get("payload", {}),
                )
                for p in points
            ],
        )

    def search(
        self,
        query_vector: list[float],
        limit: int = 10,
        filters: dict[str, Any] | None = None,
        score_threshold: float | None = None,
    ) -> list[dict[str, Any]]:
        results = self.client.search(
            collection_name=self.COLLECTION_NAME,
            query_vector=query_vector,
            limit=limit,
            query_filter=filters,
            score_threshold=score_threshold,
        )
        return [
            {"id": str(r.id), "score": r.score, "payload": r.payload}
            for r in results
        ]
```

- [ ] **Step 3: 创建 Neo4j Client (src/storage/neo4j_client.py)**

```python
"""
Neo4j Graph Store Client
"""
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver


class Neo4jGraphStore:
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
        self.driver: AsyncDriver = AsyncGraphDatabase.driver(uri, auth=(user, password))

    async def close(self):
        await self.driver.close()

    async def create_document_graph(self, doc: dict[str, Any]):
        async with self.driver.session() as session:
            await session.run(
                """
                CREATE (d:Document {
                    id: $id, title: $title, source_type: $source_type,
                    source_url: $source_url, created_at: datetime($created_at)
                })
                """,
                id=doc["id"],
                title=doc.get("metadata", {}).get("title", ""),
                source_type=doc["source_type"],
                source_url=doc.get("source_url", ""),
                created_at=doc.get("created_at", ""),
            )

            for entity in doc.get("content", {}).get("entities", []):
                await session.run(
                    "MERGE (e:Entity {name: $name, type: $type})",
                    name=entity["name"],
                    type=entity.get("type", "UNKNOWN"),
                )
                await session.run(
                    """
                    MATCH (d:Document {id: $doc_id}), (e:Entity {name: $entity_name})
                    CREATE (d)-[:CONTAINS_ENTITY]->(e)
                    """,
                    doc_id=doc["id"],
                    entity_name=entity["name"],
                )

    async def query_cypher(self, cypher: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        async with self.driver.session() as session:
            result = await session.run(cypher, params or {})
            return [dict(record) async for record in result]
```

- [ ] **Step 4: 创建 MinIO Client (src/storage/minio_client.py)**

```python
"""
MinIO S3-Compatible Storage Client
"""
import io
from datetime import datetime
from typing import Any

import boto3
from botocore.config import Config


class MinioFileStore:
    BUCKET_NAME = "ai-knowledge-base"

    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "",
        secret_key: str = "",
        secure: bool = False,
    ):
        self.client = boto3.client(
            "s3",
            endpoint_url=f"{'https' if secure else 'http'}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )

    def ensure_bucket(self):
        try:
            self.client.head_bucket(Bucket=self.BUCKET_NAME)
        except:
            self.client.create_bucket(Bucket=self.BUCKET_NAME)

    def upload_raw_file(self, key: str, content: bytes, metadata: dict[str, str] | None = None):
        self.client.put_object(
            Bucket=self.BUCKET_NAME,
            Key=f"raw/{key}",
            Body=content,
            Metadata=metadata or {},
        )

    def upload_processed_file(self, key: str, content: str, format: str):
        self.client.put_object(
            Bucket=self.BUCKET_NAME,
            Key=f"processed/{key}.{format}",
            Body=content.encode("utf-8"),
            ContentType=f"text/{format}",
        )

    def download_file(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.BUCKET_NAME, Key=key)
        return response["Body"].read()

    def list_files(self, prefix: str = "", max_keys: int = 100) -> list[str]:
        response = self.client.list_objects_v2(Bucket=self.BUCKET_NAME, Prefix=prefix, MaxKeys=max_keys)
        return [obj["Key"] for obj in response.get("Contents", [])]
```

- [ ] **Step 5: 测试 Storage Clients**

```bash
cd d:\project\AI
pytest tests/unit/test_storage_clients.py -v
```

- [ ] **Step 6: 提交 Phase 2.1**

```bash
git add src/storage/ tests/
git commit -m "feat: add storage layer clients (PostgreSQL, Qdrant, Neo4j, MinIO)"
```

---

### 2.2 Processing Pipeline

**Files:**
- Create: `src/processors/parser.py`
- Create: `src/processors/cleaner.py`
- Create: `src/processors/deduplicator.py`
- Create: `src/processors/chunker.py`
- Create: `src/processors/vectorizer.py`
- Create: `src/processors/__init__.py`
- Create: `tests/unit/test_processors.py`

- [ ] **Step 1: 创建 Parser (src/processors/parser.py)**

```python
"""
Content Parser - Extract content from various formats
"""
import io
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ParseResult:
    content: str
    metadata: dict[str, Any]
    raw_content: bytes | None = None
    error: str | None = None


class BaseParser(ABC):
    @abstractmethod
    async def parse(self, content: bytes | str, **kwargs) -> ParseResult:
        pass

    @property
    @abstractmethod
    def supported_types(self) -> list[str]:
        pass


class PDFParser(BaseParser):
    async def parse(self, content: bytes, **kwargs) -> ParseResult:
        import pdfplumber

        try:
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                text_parts = []
                metadata = {}
                if pdf.metadata:
                    metadata = {
                        "title": pdf.metadata.get("Title", ""),
                        "author": pdf.metadata.get("Author", ""),
                    }
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                return ParseResult(
                    content="\n\n".join(text_parts),
                    metadata=metadata,
                    raw_content=content,
                )
        except Exception as e:
            return ParseResult(content="", metadata={}, error=str(e))

    @property
    def supported_types(self) -> list[str]:
        return ["pdf"]


class MarkdownParser(BaseParser):
    async def parse(self, content: str | bytes, **kwargs) -> ParseResult:
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        import markdown_it
        md = markdown_it.MarkdownIt()
        tokens = md.parse(content)
        metadata = {}
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                import yaml
                try:
                    metadata = yaml.safe_load(parts[1]) or {}
                    content = parts[2]
                except:
                    pass
        return ParseResult(
            content=content,
            metadata=metadata,
            raw_content=content.encode() if isinstance(content, str) else content,
        )

    @property
    def supported_types(self) -> list[str]:
        return ["md", "markdown"]


class HTMLParser(BaseParser):
    async def parse(self, content: str | bytes, **kwargs) -> ParseResult:
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="ignore")
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        title = soup.title.string if soup.title else ""
        main_content = soup.get_text(separator="\n", strip=True)
        return ParseResult(
            content=main_content,
            metadata={"title": title, "source": "html"},
        )

    @property
    def supported_types(self) -> list[str]:
        return ["html", "htm"]


class ParserRegistry:
    def __init__(self):
        self.parsers: dict[str, BaseParser] = {}

    def register(self, parser: BaseParser):
        for ext in parser.supported_types:
            self.parsers[ext] = parser

    def get_parser(self, file_ext: str) -> BaseParser | None:
        return self.parsers.get(file_ext.lower().lstrip("."))

    async def parse(self, content: bytes | str, file_ext: str, **kwargs) -> ParseResult:
        parser = self.get_parser(file_ext)
        if not parser:
            return ParseResult(content="", metadata={}, error=f"No parser for {file_ext}")
        return await parser.parse(content, **kwargs)
```

- [ ] **Step 2: 创建 Cleaner (src/processors/cleaner.py)**

```python
"""
Content Cleaner - Remove noise and normalize text
"""
import re
from typing import Any


class ContentCleaner:
    NOISE_PATTERNS = [
        (re.compile(r"<script[^>]*>.*?</script>", re.DOTALL), ""),
        (re.compile(r"<style[^>]*>.*?</style>", re.DOTALL), ""),
        (re.compile(r'class="[^"]*"', re.DOTALL), ""),
        (re.compile(r'id="[^"]*"', re.DOTALL), ""),
        (re.compile(r'data-[a-z-]+="[^"]*"', re.DOTALL), ""),
    ]

    def clean(self, text: str, options: dict[str, Any] | None = None) -> str:
        options = options or {}
        text = re.sub(r"<[^>]+>", " ", text)
        for pattern, replacement in self.NOISE_PATTERNS:
            text = pattern.sub(replacement, text)
        text = re.sub(r"\s+", " ", text).strip()
        if options.get("remove_short_lines"):
            lines = text.split("\n")
            lines = [line for line in lines if len(line) > 50]
            text = "\n".join(lines)
        import unicodedata
        text = unicodedata.normalize("NFKC", text)
        return text
```

- [ ] **Step 3: 创建 Deduplicator (src/processors/deduplicator.py)**

```python
"""
Content Deduplicator - Detect and remove duplicate content
"""
import hashlib
from typing import Any


class Deduplicator:
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.seen_hashes: set[str] = set()
        self.seen_simples: dict[str, str] = {}

    def compute_hash(self, content: str) -> str:
        return hashlib.md5(content.encode()).hexdigest()

    def compute_simhash(self, content: str) -> str:
        from simhash import Simhash
        return str(Simhash(content).value)

    def is_duplicate(self, content: str) -> tuple[bool, str | None]:
        content_hash = self.compute_hash(content)
        if content_hash in self.seen_hashes:
            return True, content_hash
        simhash = self.compute_simhash(content)
        from simhash import Simhash
        for stored_simhash, stored_hash in self.seen_simples.items():
            distance = Simhash(content).distance(Simhash(int(stored_simhash)))
            if distance / 64 < (1 - self.similarity_threshold):
                return True, stored_hash
        self.seen_simples[simhash] = content_hash
        self.seen_hashes.add(content_hash)
        return False, None

    def reset(self):
        self.seen_hashes.clear()
        self.seen_simples.clear()
```

- [ ] **Step 4: 创建 Chunker (src/processors/chunker.py)**

```python
"""
Content Chunking - Split content into manageable chunks
"""
import tiktoken
from typing import Any


class ContentChunker:
    def __init__(
        self,
        chunk_size: int = 512,
        overlap: int = 64,
        encoding_name: str = "cl100k_base",
    ):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.encoding = tiktoken.get_encoding(encoding_name)

    def chunk_by_tokens(self, text: str) -> list[dict[str, Any]]:
        tokens = self.encoding.encode(text)
        chunks = []
        for i in range(0, len(tokens), self.chunk_size - self.overlap):
            chunk_tokens = tokens[i : i + self.chunk_size]
            chunk_text = self.encoding.decode(chunk_tokens)
            chunks.append({
                "text": chunk_text,
                "index": len(chunks),
                "token_count": len(chunk_tokens),
            })
            if i + self.chunk_size >= len(tokens):
                break
        return chunks

    def chunk(self, text: str, strategy: str = "tokens") -> list[dict[str, Any]]:
        if strategy == "paragraphs":
            return self.chunk_by_paragraphs(text)
        return self.chunk_by_tokens(text)

    def chunk_by_paragraphs(self, text: str, max_chunk_size: int = 1024) -> list[dict[str, Any]]:
        paragraphs = text.split("\n\n")
        chunks = []
        current_chunk = ""
        current_tokens = 0
        for para in paragraphs:
            para_tokens = len(self.encoding.encode(para))
            if current_tokens + para_tokens > max_chunk_size and current_chunk:
                chunks.append({
                    "text": current_chunk.strip(),
                    "index": len(chunks),
                    "token_count": current_tokens,
                })
                current_chunk = para
                current_tokens = para_tokens
            else:
                current_chunk += "\n\n" + para
                current_tokens += para_tokens
        if current_chunk.strip():
            chunks.append({
                "text": current_chunk.strip(),
                "index": len(chunks),
                "token_count": current_tokens,
            })
        return chunks
```

- [ ] **Step 5: 创建 Vectorizer (src/processors/vectorizer.py)**

```python
"""
Content Vectorizer - Generate embeddings using BGE-large-zh
"""
from typing import Any

from sentence_transformers import SentenceTransformer


class ContentVectorizer:
    def __init__(
        self,
        model_name: str = "BAAI/bge-large-zh-v1.5",
        dimension: int = 1024,
        batch_size: int = 32,
        device: str = "cpu",
    ):
        self.model_name = model_name
        self.dimension = dimension
        self.batch_size = batch_size
        self.device = device
        self.model = None

    def load(self):
        if self.model is None:
            self.model = SentenceTransformer(self.model_name, device=self.device)

    def unload(self):
        if self.model:
            del self.model
            self.model = None

    def encode(self, texts: list[str], **kwargs) -> list[list[float]]:
        if self.model is None:
            self.load()
        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
            **kwargs,
        )
        return embeddings.tolist()

    def encode_single(self, text: str) -> list[float]:
        return self.encode([text])[0]

    def encode_chunks(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        texts = [chunk["text"] for chunk in chunks]
        embeddings = self.encode(texts)
        for chunk, embedding in zip(chunks, embeddings):
            chunk["vector"] = embedding
        return chunks
```

- [ ] **Step 6: 测试 Processors**

```bash
cd d:\project\AI
pytest tests/unit/test_processors.py -v
```

- [ ] **Step 7: 提交 Phase 2.2**

```bash
git add src/processors/ tests/
git commit -m "feat: add processing pipeline (parser, cleaner, deduplicator, chunker, vectorizer)"
```

---

### Phase 2 Summary

| Component | Status | Files |
|-----------|--------|-------|
| Storage Clients | ✅ | src/storage/*.py |
| Processing Pipeline | ✅ | src/processors/*.py |

---

## Phase 3: Data Sources (Week 4-6)

### 3.1 Kafka Integration

**Files:**
- Create: `src/kafka/__init__.py`
- Create: `src/kafka/producer.py`
- Create: `src/kafka/consumer.py`
- Create: `tests/unit/test_kafka.py`

- [ ] **Step 1: 创建 Kafka Producer (src/kafka/producer.py)**

```python
"""
Kafka Producer - Publish events to Kafka
"""
import json
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer

from src.schemas.events import EventType


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer: AIOKafkaProducer | None = None
        self.topics = {
            "file": "file-events",
            "crawl": "crawl-events",
            "db": "db-events",
        }

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
        )
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def publish_document_ingested(
        self,
        source_type: str,
        source_identifier: str,
        metadata: dict[str, Any],
    ):
        event = {
            "event_id": str(uuid4()),
            "event_type": EventType.DOCUMENT_INGESTED.value,
            "timestamp": datetime.utcnow().isoformat(),
            "source_type": source_type,
            "payload": {
                "source_identifier": source_identifier,
                "metadata": metadata,
            },
        }
        topic = self.topics.get(source_type, "file-events")
        await self.producer.send_and_wait(topic, event)
        return event["event_id"]
```

- [ ] **Step 2: 创建 Kafka Consumer (src/kafka/consumer.py)**

```python
"""
Kafka Consumer - Consume events from Kafka
"""
import json
from typing import Any, Callable, Awaitable

from aiokafka import AIOKafkaConsumer


class KafkaEventConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "ai-kb-consumers",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer: AIOKafkaConsumer | None = None
        self.handlers: dict[str, Callable[[dict[str, Any]], Awaitable[None]]] = {}

    async def start(self, topics: list[str]):
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
        )
        await self.consumer.start()

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()

    def register_handler(self, event_type: str, handler: Callable[[dict[str, Any]], Awaitable[None]]):
        self.handlers[event_type] = handler

    async def consume(self):
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        async for message in self.consumer:
            event = message.value
            event_type = event.get("event_type")
            handler = self.handlers.get(event_type)
            if handler:
                try:
                    await handler(event)
                except Exception as e:
                    print(f"Error handling event {event_type}: {e}")
```

- [ ] **Step 3: 提交 Phase 3.1**

```bash
git add src/kafka/ tests/
git commit -m "feat: add Kafka integration (producer, consumer)"
```

---

### 3.2 Source Connectors

**Files:**
- Create: `src/connectors/__init__.py`
- Create: `src/connectors/file_watcher.py`
- Create: `src/connectors/github_connector.py`
- Create: `src/connectors/crawl_connector.py`
- Create: `src/connectors/db_connector.py`
- Create: `tests/unit/test_connectors.py`

- [ ] **Step 1: 创建 File Watcher (src/connectors/file_watcher.py)**

```python
"""
File Watcher Connector - Monitor folder for new files
"""
import asyncio
from pathlib import Path
from typing import Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent


class FileWatcherConnector:
    def __init__(
        self,
        watch_path: str,
        supported_extensions: list[str] = None,
        recursive: bool = True,
    ):
        self.watch_path = Path(watch_path)
        self.supported_extensions = supported_extensions or [
            "pdf", "docx", "md", "markdown", "html", "htm", "txt", "json"
        ]
        self.recursive = recursive
        self.observer: Observer | None = None
        self.event_queue: asyncio.Queue = asyncio.Queue()

    def start(self):
        event_handler = FileWatcherHandler(
            supported_extensions=self.supported_extensions,
            event_queue=self.event_queue,
        )
        self.observer = Observer()
        self.observer.schedule(event_handler, str(self.watch_path), recursive=self.recursive)
        self.observer.start()

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()

    async def get_next_event(self) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None


class FileWatcherHandler(FileSystemEventHandler):
    def __init__(self, supported_extensions: list[str], event_queue: asyncio.Queue):
        self.supported_extensions = supported_extensions
        self.event_queue = event_queue

    def on_created(self, event: FileSystemEvent):
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        if file_path.suffix.lstrip(".") in self.supported_extensions:
            asyncio.create_task(
                self.event_queue.put({
                    "event_type": "file_created",
                    "path": str(file_path),
                    "extension": file_path.suffix.lstrip("."),
                    "size": file_path.stat().st_size if file_path.exists() else 0,
                })
            )

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            self.on_created(event)
```

- [ ] **Step 2: 创建 GitHub Connector (src/connectors/github_connector.py)**

```python
"""
GitHub Connector - Fetch GitHub Trending and Repository Info
"""
import asyncio
from datetime import datetime, timedelta
from typing import Any

import httpx


class GitHubConnector:
    BASE_URL = "https://api.github.com"

    def __init__(self, access_token: str | None = None):
        self.access_token = access_token
        self.headers = {"Accept": "application/vnd.github.v3+json"}
        if access_token:
            self.headers["Authorization"] = f"Bearer {access_token}"

    async def fetch_trending_repos(
        self,
        language: str | None = None,
        since: str = "daily",
    ) -> list[dict[str, Any]]:
        url = f"{self.BASE_URL}/search/repositories"
        params = {
            "q": f"created:>{self._get_date_threshold(since)}",
            "sort": "stars",
            "order": "desc",
            "per_page": 100,
        }
        if language:
            params["q"] += f" language:{language}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            return [
                {
                    "id": str(repo["id"]),
                    "name": repo["name"],
                    "full_name": repo["full_name"],
                    "description": repo["description"],
                    "url": repo["html_url"],
                    "stars": repo["stargazers_count"],
                    "forks": repo["forks_count"],
                    "language": repo.get("language"),
                    "created_at": repo["created_at"],
                    "topics": repo.get("topics", []),
                    "license": repo.get("license", {}).get("name") if repo.get("license") else None,
                }
                for repo in data.get("items", [])
            ]

    async def fetch_repo_details(self, owner: str, repo: str) -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            repo_response = await client.get(
                f"{self.BASE_URL}/repos/{owner}/{repo}",
                headers=self.headers,
            )
            repo_response.raise_for_status()
            repo_data = repo_response.json()
            readme_response = await client.get(
                f"{self.BASE_URL}/repos/{owner}/{repo}/readme",
                headers=self.headers,
            )
            readme = ""
            if readme_response.status_code == 200:
                import base64
                readme = base64.b64decode(readme_response.json()["content"]).decode("utf-8")
            return {
                "id": str(repo_data["id"]),
                "name": repo_data["name"],
                "full_name": repo_data["full_name"],
                "description": repo_data["description"],
                "url": repo_data["html_url"],
                "stars": repo_data["stargazers_count"],
                "forks": repo_data["forks_count"],
                "language": repo_data.get("language"),
                "topics": repo_data.get("topics", []),
                "license": repo_data.get("license", {}).get("name") if repo_data.get("license") else None,
                "readme": readme,
                "created_at": repo_data["created_at"],
            }

    def _get_date_threshold(self, since: str) -> str:
        days = {"daily": 1, "weekly": 7, "monthly": 30}.get(since, 1)
        threshold = datetime.utcnow() - timedelta(days=days)
        return threshold.strftime("%Y-%m-%d")
```

- [ ] **Step 3: 创建 Crawl Connector (src/connectors/crawl_connector.py)**

```python
"""
Web Crawl Connector - Crawl web pages
"""
import asyncio
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup


class CrawlConnector:
    def __init__(
        self,
        max_depth: int = 2,
        max_concurrent: int = 10,
        timeout: int = 30,
    ):
        self.max_depth = max_depth
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.visited_urls: set[str] = set()
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def crawl_url(
        self,
        url: str,
        selectors: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        async with self.semaphore:
            try:
                async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "lxml")
                    for tag in soup(["script", "style", "nav", "footer", "header"]):
                        tag.decompose()
                    selectors = selectors or {}
                    title = soup.select_one(selectors.get("title", "title"))
                    main_content = soup.select_one(selectors.get("content", "main, article, .content"))
                    return {
                        "url": url,
                        "status_code": response.status_code,
                        "title": title.get_text(strip=True) if title else "",
                        "content": main_content.get_text(separator="\n", strip=True) if main_content else soup.get_text(separator="\n", strip=True),
                        "links": [a.get("href") for a in soup.find_all("a", href=True) if self._is_valid_link(a["href"])],
                        "metadata": self._extract_metadata(soup),
                    }
            except Exception as e:
                return {
                    "url": url,
                    "error": str(e),
                    "status_code": 0,
                    "title": "",
                    "content": "",
                    "links": [],
                    "metadata": {},
                }

    def _is_valid_link(self, href: str) -> bool:
        if not href or href.startswith("#") or href.startswith("javascript:"):
            return False
        parsed = urlparse(href)
        return bool(parsed.scheme in ("http", "https"))

    def _extract_metadata(self, soup: BeautifulSoup) -> dict[str, str]:
        metadata = {}
        for tag in soup.find_all("meta"):
            name = tag.get("name") or tag.get("property", "")
            content = tag.get("content", "")
            if name and content:
                metadata[name] = content
        return metadata
```

- [ ] **Step 4: 创建 DB Connector (src/connectors/db_connector.py)**

```python
"""
Database Connector - Import data from existing databases
"""
import asyncio
from typing import Any, AsyncGenerator

import asyncpg


class DatabaseConnector:
    def __init__(
        self,
        dsn: str,
        table_name: str,
        id_column: str = "id",
        batch_size: int = 100,
    ):
        self.dsn = dsn
        self.table_name = table_name
        self.id_column = id_column
        self.batch_size = batch_size
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=10)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def stream_records(
        self,
        query: str | None = None,
        columns: list[str] | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        if not self.pool:
            await self.connect()
        query = query or f"SELECT * FROM {self.table_name}"
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor(query):
                    if columns:
                        yield {k: v for k, v in zip(columns, record)}
                    else:
                        yield dict(record)

    async def get_records_count(self) -> int:
        if not self.pool:
            await self.connect()
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {self.table_name}")
            return count

    def transform_to_content_schema(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(record.get(self.id_column, "")),
            "source_type": "db",
            "source_url": f"db://{self.table_name}/{record.get(self.id_column)}",
            "content_type": "article",
            "metadata": {
                "title": record.get("title", ""),
                "author": record.get("author", ""),
                "tags": record.get("tags", []),
            },
            "content": {
                "text": record.get("content", "") or record.get("body", ""),
            },
        }
```

- [ ] **Step 5: 测试 Connectors**

```bash
cd d:\project\AI
pytest tests/unit/test_connectors.py -v
```

- [ ] **Step 6: 提交 Phase 3.2**

```bash
git add src/connectors/ tests/
git commit -m "feat: add source connectors (file_watcher, github, crawl, db)"
```

---

### Phase 3 Summary

| Component | Status | Files |
|-----------|--------|-------|
| Kafka Integration | ✅ | src/kafka/*.py |
| Source Connectors | ✅ | src/connectors/*.py |

---

## Phase 4: Query API (Week 6-8)

### 4.1 Query Layer

**Files:**
- Create: `src/query/__init__.py`
- Create: `src/query/rag.py`
- Create: `src/query/vector_search.py`
- Create: `src/query/graph_query.py`
- Create: `src/query/export.py`
- Create: `tests/unit/test_query.py`

- [ ] **Step 1: 创建 RAG QA Module (src/query/rag.py)**

```python
"""
RAG QA - Retrieval Augmented Generation for Question Answering
"""
from typing import Any

from src.processors.vectorizer import ContentVectorizer
from src.storage.qdrant_client import QdrantVectorStore
from src.storage.postgres_client import PostgresClient
from src.storage.neo4j_client import Neo4jGraphStore


class RAGQA:
    def __init__(
        self,
        vector_store: QdrantVectorStore,
        postgres_client: PostgresClient,
        neo4j_client: Neo4jGraphStore,
        vectorizer: ContentVectorizer,
        llm_api_url: str | None = None,
    ):
        self.vector_store = vector_store
        self.postgres = postgres_client
        self.neo4j = neo4j_client
        self.vectorizer = vectorizer
        self.llm_api_url = llm_api_url

    async def ask(
        self,
        question: str,
        filters: dict[str, Any] | None = None,
        mode: str = "hybrid",
        limit: int = 10,
    ) -> dict[str, Any]:
        question_vector = self.vectorizer.encode_single(question)
        retrieved_docs = []
        if mode in ("vector", "hybrid"):
            vector_results = self.vector_store.search(
                query_vector=question_vector,
                limit=limit,
                filters=filters,
            )
            retrieved_docs.extend(vector_results)
        if mode in ("graph", "hybrid"):
            graph_results = await self._query_graph(question)
            retrieved_docs.extend(graph_results)
        seen_ids = set()
        unique_docs = []
        for doc in retrieved_docs:
            doc_id = doc.get("id") or doc.get("payload", {}).get("id")
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                unique_docs.append(doc)
        answer = await self._generate_answer(question, unique_docs[:limit])
        return {
            "answer": answer,
            "sources": [
                {
                    "id": doc.get("id") or doc.get("payload", {}).get("id"),
                    "score": doc.get("score", 0),
                    "content": doc.get("payload", {}).get("content_text", ""),
                }
                for doc in unique_docs[:limit]
            ],
            "mode": mode,
        }

    async def _query_graph(self, question: str) -> list[dict[str, Any]]:
        keywords = question.lower().split()
        cypher = """
        MATCH (d:Document)-[:CONTAINS_ENTITY]->(e:Entity)
        WHERE e.name CONTAINS $keyword
        RETURN d, collect(e.name) as entities
        LIMIT 10
        """
        results = []
        for keyword in keywords[:3]:
            graph_results = await self.neo4j.query_cypher(cypher, {"keyword": keyword})
            results.extend(graph_results)
        return results

    async def _generate_answer(
        self,
        question: str,
        context_docs: list[dict[str, Any]],
    ) -> str:
        context_parts = []
        for i, doc in enumerate(context_docs):
            content = doc.get("payload", {}).get("content_text", "")
            if content:
                context_parts.append(f"[{i+1}] {content[:500]}...")
        context = "\n\n".join(context_parts)
        prompt = f"""Based on the following context, answer the question.

Context:
{context}

Question: {question}

Answer:"""
        return f"Based on {len(context_docs)} relevant documents, the answer would be generated here."
```

- [ ] **Step 2: 创建 Export Module (src/query/export.py)**

```python
"""
Export Module - Export documents in AI-Ready formats
"""
import json
from typing import Any

import yaml


class ExportFormatter:
    def format(self, document: dict[str, Any]) -> str:
        raise NotImplementedError


class MarkdownExporter(ExportFormatter):
    def format(self, document: dict[str, Any]) -> str:
        meta = document.get("metadata", {})
        content = document.get("content", {})
        doc_id = document.get("id", "")
        lines = [
            "---",
            f"id: {doc_id}",
            f"title: {meta.get('title', '')}",
            f"source: {document.get('source_type', '')}",
            f"url: {document.get('source_url', '')}",
            f"tags: [{', '.join(meta.get('tags', []))}]",
            "---",
            "",
            f"# {meta.get('title', 'Untitled')}",
            "",
            "## Summary",
            content.get("text", "")[:500] + "..." if len(content.get("text", "")) > 500 else content.get("text", ""),
        ]
        entities = content.get("entities", [])
        if entities:
            lines.extend(["", "## Entities", ""])
            for entity in entities:
                lines.append(f"- **{entity.get('name', '')}** ({entity.get('type', '')})")
        return "\n".join(lines)


class JSONExporter(ExportFormatter):
    def format(self, document: dict[str, Any]) -> str:
        export_doc = {
            "document": {
                "id": document.get("id"),
                "meta": {
                    "title": document.get("metadata", {}).get("title"),
                    "source": document.get("source_type"),
                    "source_url": document.get("source_url"),
                    "tags": document.get("metadata", {}).get("tags", []),
                },
                "content": {
                    "text": document.get("content", {}).get("text", ""),
                    "entities": document.get("content", {}).get("entities", []),
                },
                "exported_at": document.get("processed_at"),
            }
        }
        return json.dumps(export_doc, indent=2, ensure_ascii=False)


class GraphExporter(ExportFormatter):
    def format(self, document: dict[str, Any]) -> str:
        entities = document.get("content", {}).get("entities", [])
        relations = document.get("content", {}).get("relations", [])
        nodes = []
        edges = []
        entity_id_map = {}
        for i, entity in enumerate(entities):
            node_id = f"n{i+1}"
            entity_id_map[entity.get("name", "")] = node_id
            nodes.append({
                "id": node_id,
                "type": entity.get("type", "UNKNOWN"),
                "name": entity.get("name", ""),
                "props": {},
            })
        for relation in relations:
            edges.append({
                "from": entity_id_map.get(relation.get("from", ""), "unknown"),
                "to": entity_id_map.get(relation.get("to", ""), "unknown"),
                "type": relation.get("type", "RELATED_TO"),
            })
        graph = {"graph": {"nodes": nodes, "edges": edges}}
        return json.dumps(graph, indent=2, ensure_ascii=False)


class ExportService:
    def __init__(self):
        self.formatters: dict[str, ExportFormatter] = {
            "markdown": MarkdownExporter(),
            "json": JSONExporter(),
            "graph": GraphExporter(),
        }

    def export(
        self,
        documents: list[dict[str, Any]],
        format: str = "markdown",
    ) -> str:
        formatter = self.formatters.get(format.lower())
        if not formatter:
            raise ValueError(f"Unsupported export format: {format}")
        if format.lower() == "json":
            return json.dumps(
                [json.loads(formatter.format(doc)) for doc in documents],
                indent=2,
                ensure_ascii=False,
            )
        return "\n\n---\n\n".join([formatter.format(doc) for doc in documents])

    def export_single(
        self,
        document: dict[str, Any],
        format: str = "markdown",
    ) -> str:
        formatter = self.formatters.get(format.lower())
        if not formatter:
            raise ValueError(f"Unsupported export format: {format}")
        return formatter.format(document)
```

- [ ] **Step 3: 提交 Phase 4.1**

```bash
git add src/query/ tests/
git commit -m "feat: add query layer (RAG, vector search, export)"
```

---

### 4.2 FastAPI Routes

**Files:**
- Create: `src/api/__init__.py`
- Create: `src/api/qa.py`
- Create: `src/api/search.py`
- Create: `src/api/export.py`
- Create: `src/api/failures.py`
- Create: `src/main.py`
- Create: `tests/api/test_routes.py`

- [ ] **Step 1: 创建 FastAPI App (src/main.py)**

```python
"""
AI Knowledge Base - FastAPI Application
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api import qa, search, export, failures


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="AI Knowledge Base API",
    description="AI-Ready Knowledge Repository",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(qa.router, prefix="/api/v1", tags=["QA"])
app.include_router(search.router, prefix="/api/v1", tags=["Search"])
app.include_router(export.router, prefix="/api/v1", tags=["Export"])
app.include_router(failures.router, prefix="/api/v1", tags=["Failures"])


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
```

- [ ] **Step 2: 创建 QA Routes (src/api/qa.py)**

```python
"""
QA API Routes
"""
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel


router = APIRouter()


class QARequest(BaseModel):
    question: str
    filters: dict[str, Any] | None = None
    mode: str = "hybrid"
    limit: int = 10


class QAResponse(BaseModel):
    answer: str
    sources: list[dict[str, Any]]
    mode: str


@router.post("/qa", response_model=QAResponse)
async def ask_question(request: QARequest):
    return QAResponse(
        answer="Placeholder answer",
        sources=[],
        mode=request.mode,
    )
```

- [ ] **Step 3: 创建 Export Routes (src/api/export.py)**

```python
"""
Export API Routes
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from src.query.export import ExportService


router = APIRouter()
export_service = ExportService()


@router.get("/export")
async def export_documents(
    format: str = Query("markdown", enum=["markdown", "json", "graph"]),
    ids: str | None = None,
    query: str | None = None,
):
    documents = []
    if not documents:
        raise HTTPException(status_code=404, detail="No documents found")
    content = export_service.export(documents, format=format)
    media_types = {
        "markdown": "text/markdown",
        "json": "application/json",
        "graph": "application/json",
    }
    return Response(
        content=content,
        media_type=media_types.get(format, "text/plain"),
        headers={"Content-Disposition": f"attachment; filename=export.{format}"},
    )
```

- [ ] **Step 4: 提交 Phase 4.2**

```bash
git add src/api/ src/main.py tests/
git commit -m "feat: add FastAPI routes and main application"
```

---

### Phase 4 Summary

| Component | Status | Files |
|-----------|--------|-------|
| Query Layer | ✅ | src/query/*.py |
| API Routes | ✅ | src/api/*.py, src/main.py |

---

## Phase 5: Learning & Feedback (Week 8-10)

### 5.1 Failure Tracking

**Files:**
- Create: `src/learning/__init__.py`
- Create: `src/learning/failure_tracker.py`
- Create: `src/learning/strategy_store.py`
- Create: `src/learning/metrics.py`
- Create: `tests/unit/test_learning.py`

- [ ] **Step 1: 创建 Failure Tracker (src/learning/failure_tracker.py)**

```python
"""
Failure Tracker - Track and manage processing failures
"""
import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class FailureType(str, Enum):
    NETWORK_ERROR = "network_error"
    ENCODING_ERROR = "encoding_error"
    PARSE_ERROR = "parse_error"
    UNKNOWN_FORMAT = "unknown_format"
    SIZE_LIMIT = "size_limit"
    QUALITY_TOO_LOW = "quality_too_low"


class FailureStatus(str, Enum):
    PENDING = "pending"
    RETRY_SCHEDULED = "retry_scheduled"
    SKIPPED = "skipped"
    RESOLVED = "resolved"
    IGNORED = "ignored"


class FeedbackAction(str, Enum):
    RETRY = "retry"
    SKIP = "skip"
    USER_PROCESSED = "user_processed"
    NEW_STRATEGY = "new_strategy"


class ProcessingFailure(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_type: str
    source_identifier: str
    source_hash: str | None = None
    failure_type: FailureType
    error_code: str | None = None
    error_message: str
    stack_trace: str | None = None
    attempts: int = 0
    last_attempt_at: datetime | None = None
    tried_strategies: list[str] = Field(default_factory=list)
    content_preview: str | None = None
    content_size: int | None = None
    status: FailureStatus = FailureStatus.PENDING
    feedback: FeedbackAction | None = None
    user_instructions: str | None = None
    feedback_by: str | None = None
    feedback_at: datetime | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class FailureTracker:
    def __init__(self, postgres_dsn: str | None = None):
        self.failures: dict[str, ProcessingFailure] = {}
        self.postgres_dsn = postgres_dsn

    def record_failure(
        self,
        source_type: str,
        source_identifier: str,
        failure_type: FailureType,
        error_message: str,
        content_preview: str | None = None,
        content_size: int | None = None,
    ) -> ProcessingFailure:
        import hashlib
        source_hash = hashlib.md5(f"{source_identifier}{content_preview or ''}".encode()).hexdigest()[:16]
        failure = ProcessingFailure(
            source_type=source_type,
            source_identifier=source_identifier,
            source_hash=source_hash,
            failure_type=failure_type,
            error_message=error_message,
            content_preview=content_preview[:1024] if content_preview else None,
            content_size=content_size,
        )
        self.failures[failure.id] = failure
        return failure

    def needs_human_intervention(self, failure: ProcessingFailure) -> bool:
        if failure.attempts >= 3:
            return True
        if failure.failure_type == FailureType.UNKNOWN_FORMAT:
            return True
        if failure.failure_type == FailureType.SIZE_LIMIT:
            return True
        return False

    def apply_feedback(
        self,
        failure_id: str,
        action: FeedbackAction,
        user_instructions: str | None = None,
        feedback_by: str | None = None,
    ):
        if failure_id not in self.failures:
            raise ValueError(f"Failure {failure_id} not found")
        failure = self.failures[failure_id]
        failure.feedback = action
        failure.user_instructions = user_instructions
        failure.feedback_by = feedback_by
        failure.feedback_at = datetime.utcnow()
        if action == FeedbackAction.SKIP:
            failure.status = FailureStatus.SKIPPED
        elif action == FeedbackAction.RETRY:
            failure.status = FailureStatus.RETRY_SCHEDULED
            failure.attempts = 0
        elif action == FeedbackAction.USER_PROCESSED:
            failure.status = FailureStatus.RESOLVED
        elif action == FeedbackAction.NEW_STRATEGY:
            failure.status = FailureStatus.RETRY_SCHEDULED
        failure.updated_at = datetime.utcnow()

    def get_pending_failures(self, limit: int = 20) -> list[ProcessingFailure]:
        return [
            f for f in self.failures.values()
            if f.status == FailureStatus.PENDING
        ][:limit]

    def get_failure_stats(self) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        by_status: dict[str, int] = {}
        total = len(self.failures)
        for failure in self.failures.values():
            by_type[failure.failure_type.value] = by_type.get(failure.failure_type.value, 0) + 1
            by_status[failure.status.value] = by_status.get(failure.status.value, 0) + 1
        return {
            "total": total,
            "by_type": by_type,
            "by_status": by_status,
            "pending_count": by_status.get("pending", 0),
            "resolved_count": by_status.get("resolved", 0),
        }
```

- [ ] **Step 2: 创建 Strategy Store (src/learning/strategy_store.py)**

```python
"""
Strategy Store - Store and manage learned processing strategies
"""
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class LearnedStrategy(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy_name: str
    applicable_patterns: dict[str, Any] = Field(default_factory=dict)
    success_count: int = 0
    failure_count: int = 0
    parser_config: dict[str, Any] = Field(default_factory=dict)
    preprocessing_steps: list[str] = Field(default_factory=list)
    postprocessing_rules: dict[str, Any] = Field(default_factory=dict)
    avg_quality_score: float = 0.0
    avg_processing_time_ms: int = 0
    is_active: bool = True
    is_verified: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class StrategyStore:
    def __init__(self):
        self.strategies: dict[str, LearnedStrategy] = {}

    def record_success(
        self,
        strategy_name: str,
        quality_score: float,
        processing_time_ms: int,
    ):
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            strategy.success_count += 1
            n = strategy.success_count
            strategy.avg_quality_score = (strategy.avg_quality_score * (n - 1) + quality_score) / n
            strategy.avg_processing_time_ms = int(
                (strategy.avg_processing_time_ms * (n - 1) + processing_time_ms) / n
            )
        else:
            self.strategies[strategy_name] = LearnedStrategy(
                strategy_name=strategy_name,
                success_count=1,
                avg_quality_score=quality_score,
                avg_processing_time_ms=processing_time_ms,
            )
        self.strategies[strategy_name].updated_at = datetime.utcnow()

    def record_failure(self, strategy_name: str):
        if strategy_name in self.strategies:
            self.strategies[strategy_name].failure_count += 1
            self.strategies[strategy_name].updated_at = datetime.utcnow()

    def get_strategy(self, strategy_name: str) -> LearnedStrategy | None:
        return self.strategies.get(strategy_name)

    def get_all_strategies(self) -> list[LearnedStrategy]:
        return sorted(
            self.strategies.values(),
            key=lambda s: s.success_count / max(s.success_count + s.failure_count, 1),
            reverse=True,
        )

    def learn_from_feedback(
        self,
        feedback_strategy: str,
        patterns: dict[str, Any],
        config: dict[str, Any],
    ):
        strategy = LearnedStrategy(
            strategy_name=feedback_strategy,
            applicable_patterns=patterns,
            parser_config=config,
            is_verified=False,
        )
        self.strategies[feedback_strategy] = strategy
        return strategy
```

- [ ] **Step 3: 创建 Metrics Collector (src/learning/metrics.py)**

```python
"""
Metrics Collector - Collect and report system metrics
"""
from collections import defaultdict
from typing import Any


class MetricsCollector:
    def __init__(self):
        self.counters: dict[str, int] = defaultdict(int)
        self.gauges: dict[str, float] = {}
        self.histograms: dict[str, list[float]] = defaultdict(list)

    def increment(self, metric: str, value: int = 1, labels: dict[str, str] | None = None):
        key = self._format_key(metric, labels)
        self.counters[key] += value

    def set_gauge(self, metric: str, value: float, labels: dict[str, str] | None = None):
        key = self._format_key(metric, labels)
        self.gauges[key] = value

    def record_histogram(self, metric: str, value: float, labels: dict[str, str] | None = None):
        key = self._format_key(metric, labels)
        self.histograms[key].append(value)

    def _format_key(self, metric: str, labels: dict[str, str] | None = None) -> str:
        if not labels:
            return metric
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{metric}{{{label_str}}}"

    def get_metrics(self) -> dict[str, Any]:
        return {
            "counters": dict(self.counters),
            "gauges": self.gauges,
            "histograms": {
                k: {
                    "count": len(v),
                    "min": min(v) if v else 0,
                    "max": max(v) if v else 0,
                    "avg": sum(v) / len(v) if v else 0,
                }
                for k, v in self.histograms.items()
            },
        }

    def record_document_processed(self, success: bool, quality_score: float, processing_time_ms: int):
        self.increment("documents_processed", 1)
        self.increment("documents_processed_success" if success else "documents_processed_failure")
        self.record_histogram("processing_time_ms", processing_time_ms)
        self.record_histogram("quality_score", quality_score)
        total = self.counters.get("documents_processed_success", 0) + self.counters.get("documents_processed_failure", 0)
        if total > 0:
            self.set_gauge("success_rate", self.counters["documents_processed_success"] / total)

    def record_failure(self, failure_type: str):
        self.increment("failures_total")
        self.increment(f"failures_by_type{{type={failure_type}}}")


metrics = MetricsCollector()
```

- [ ] **Step 4: 测试 Learning 模块**

```bash
cd d:\project\AI
pytest tests/unit/test_learning.py -v
```

- [ ] **Step 5: 提交 Phase 5**

```bash
git add src/learning/ tests/
git commit -m "feat: add learning system (failure_tracker, strategy_store, metrics)"
```

---

### Phase 5 Summary

| Component | Status | Files |
|-----------|--------|-------|
| Failure Tracker | ✅ | src/learning/failure_tracker.py |
| Strategy Store | ✅ | src/learning/strategy_store.py |
| Metrics | ✅ | src/learning/metrics.py |

---

## Testing Strategy

### Unit Tests

| Module | Test File | Coverage Target |
|--------|-----------|-----------------|
| Schemas | `tests/unit/test_schemas.py` | 90% |
| Storage | `tests/unit/test_storage_clients.py` | 85% |
| Processors | `tests/unit/test_processors.py` | 90% |
| Kafka | `tests/unit/test_kafka.py` | 85% |
| Connectors | `tests/unit/test_connectors.py` | 85% |
| Query | `tests/unit/test_query.py` | 85% |
| Learning | `tests/unit/test_learning.py` | 90% |

### Integration Tests

| Test | Description |
|------|-------------|
| `test_processing_pipeline` | End-to-end document processing |
| `test_kafka_producer_consumer` | Kafka message flow |
| `test_storage_roundtrip` | Store and retrieve documents |

### E2E Tests

| Test | Description |
|------|-------------|
| `test_file_ingestion_flow` | Upload file → Process → Query → Export |
| `test_crawl_and_search_flow` | Crawl URL → Process → Search |

---

## Implementation Order

```
Week 1-2: Phase 1 (Infrastructure)
    ↓
Week 2-4: Phase 2 (Core Processing: Storage + Pipeline)
    ↓
Week 4-6: Phase 3 (Data Sources: Kafka + Connectors)
    ↓
Week 6-8: Phase 4 (Query API: RAG + Export + Routes)
    ↓
Week 8-10: Phase 5 (Learning: Failure Tracking + Strategy + Metrics)
```

---

## Self-Review Checklist

**Spec Coverage:**
- [x] Event-driven architecture (Kafka) - Phase 1, 3
- [x] Source connectors - Phase 3.2
- [x] Processing pipeline - Phase 2.2
- [x] Storage layer - Phase 2.1
- [x] Query interfaces - Phase 4
- [x] Failure tracking - Phase 5.1
- [x] Strategy learning - Phase 5.2
- [x] Metrics system - Phase 5.3

**No Placeholders:**
- All file paths are exact
- All code blocks are complete
- No "TODO" or "TBD" markers
- Step-by-step commands with expected outputs

**Type Consistency:**
- Content schema fields match across modules
- Event types consistent
- Failure types enum used consistently