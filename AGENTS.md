# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

---

## 项目概述 / Project Overview

KnowForge is a FastAPI-based data processing system that ingests content from multiple sources (files, web crawlers, GitHub, databases), cleans and transforms it into AI-friendly formats, and provides query APIs (RAG QA, vector search, knowledge graph queries) with support for exporting to Markdown, JSON, and Knowledge Graph formats.

KnowForge 是一个基于 FastAPI 的数据处理系统，从多个来源（文件、Web 爬虫、GitHub、数据库）摄取内容，清洗并转换为 AI 友好的格式，提供查询 API（RAG 问答、向量搜索、知识图谱查询），支持导出为 Markdown、JSON 和知识图谱格式。

---

## 常用命令 / Common Commands

```bash
# Start infrastructure (Kafka, PostgreSQL, Qdrant, Neo4j, MinIO)
# 启动基础设施（Kafka、PostgreSQL、Qdrant、Neo4j、MinIO）
cd docker && docker-compose up -d

# Install dependencies / 安装依赖
pip install -e .

# Run all tests / 运行所有测试
pytest tests/ -v

# Run tests with coverage / 运行测试并查看覆盖率
pytest --cov=src --cov-report=term-missing tests/

# Run a specific test file / 运行特定测试文件
pytest tests/unit/test_schemas.py -v

# Lint code / 代码检查
ruff check src/

# Type check / 类型检查
mypy src/

# Start the API server / 启动 API 服务器
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

---

## 架构 / Architecture

### Data Flow / 数据流

```
Sources (FileWatcher, GitHub, Crawler, DB) → Kafka Event Bus → Processors (Parser → Cleaner → Dedup → Chunk → Embed) → Storage (Qdrant, Neo4j, PostgreSQL, MinIO) → Query APIs
```

### Key Modules / 核心模块

| Module | Purpose / 用途 |
|--------|---------------|
| `src/connectors/` | Source ingestion (file watching, GitHub API, web scraping, DB polling) / 源数据摄取（文件监控、GitHub API、Web 爬虫、数据库轮询） |
| `src/kafka/` | Event bus producer/consumer for decoupling sources from processors / 事件总线生产者和消费者，解耦数据源与处理器 |
| `src/processors/` | Pipeline stages: parser, cleaner, deduplicator, chunker, vectorizer / 处理管道阶段：解析器、清洗器、去重器、分块器、向量化器 |
| `src/storage/` | Storage clients: PostgreSQL, Qdrant (vector), Neo4j (graph), MinIO (files) / 存储客户端：PostgreSQL、Qdrant（向量）、Neo4j（图）、MinIO（文件） |
| `src/query/` | Query layer: RAG QA, vector search, graph query, export formatters / 查询层：RAG 问答、向量搜索、图查询、导出格式化器 |
| `src/learning/` | Failure tracking, strategy store, and metrics collection / 失败追踪、策略存储和指标收集 |
| `src/api/` | FastAPI route handlers / FastAPI 路由处理器 |

### Storage Layer / 存储层

- **Qdrant** (port 6333): Vector storage with BGE-large-zh embeddings for similarity search / 使用 BGE-large-zh 嵌入的向量存储，用于相似性搜索
- **Neo4j** (port 7687): Knowledge graph for entity-relation queries (Cypher) / 知识图谱，用于实体关系查询（Cypher）
- **PostgreSQL** (port 5432): Metadata storage with pgvector for hybrid search / 元数据存储，使用 pgvector 进行混合搜索
- **MinIO** (port 9000): S3-compatible file storage for raw/exported files / S3 兼容的文件存储

### Event Topics / 事件主题

- `file-events`: Local file ingestion / 本地文件摄取
- `crawl-events`: Web crawler results / Web 爬虫结果
- `db-events`: Database connector imports / 数据库连接器导入

---

## 配置 / Configuration

Environment variables are defined in `docker/.env.example`. Copy to `.env` and configure:

环境变量定义在 `docker/.env.example`。复制到 `.env` 并配置：

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker (default: localhost:9092) / Kafka 代理（默认：localhost:9092）
- `POSTGRES_*`: PostgreSQL connection / PostgreSQL 连接
- `QDRANT_*`: Qdrant connection / Qdrant 连接
- `NEO4J_*`: Neo4j connection / Neo4j 连接
- `MINIO_*`: MinIO connection / MinIO 连接

---

## API 端点 / API Endpoints

### Query APIs / 查询 API

| Method | Endpoint | Description / 描述 |
|--------|----------|-------------------|
| POST | `/api/v1/qa` | RAG conversational question answering / RAG 对话问答 |
| POST | `/api/v1/search` | Vector similarity search / 向量相似性搜索 |
| POST | `/api/v1/graph/query` | Knowledge graph query (Cypher) / 知识图谱查询（Cypher） |
| GET | `/api/v1/export` | Export documents / 导出文档 |

### Management APIs / 管理 API

| Method | Endpoint | Description / 描述 |
|--------|----------|-------------------|
| GET | `/api/v1/failures` | List processing failures / 列出处理失败 |
| POST | `/api/v1/failures/{id}/feedback` | Submit failure feedback / 提交失败反馈 |
| GET | `/api/v1/metrics` | System metrics / 系统指标 |
| GET | `/health` | Health check / 健康检查 |

---

## 失败追踪与学习 / Failure Tracking & Learning

The `src/learning/` module implements a self-healing feedback loop:

`src/learning/` 模块实现了自愈反馈循环：

- Failures are classified by type: network_error, encoding_error, parse_error, unknown_format, size_limit, quality_too_low
- 失败按类型分类：network_error、encoding_error、parse_error、unknown_format、size_limit、quality_too_low
- After 3+ attempts or specific failure types, human intervention is flagged
- 3次以上重试或特定失败类型会标记为需要人工干预
- Users provide feedback via `/api/v1/failures/{id}/feedback` with actions: retry, skip, user_processed, new_strategy
- 用户可通过 `/api/v1/failures/{id}/feedback` 提供反馈，动作包括：retry、skip、user_processed、new_strategy
- Learned strategies are stored in PostgreSQL for future processing attempts
- 学习到的策略存储在 PostgreSQL 中用于后续处理尝试

---

## 代码标准 / Code Standards

- Follow PEP 8 with type annotations on all function signatures / 遵循 PEP 8，所有函数签名添加类型注解
- Use `dataclass(frozen=True)` for immutable DTOs / 使用 `dataclass(frozen=True)` 定义不可变 DTO
- Use Pydantic v2 for schema validation / 使用 Pydantic v2 进行模式验证
- Immutability: prefer creating new objects over mutating existing ones / 不可变性：优先创建新对象而非修改现有对象
- Error handling: handle explicitly, never silently swallow / 错误处理：显式处理，绝不静默吞掉

---

## 数据模式 / Data Schema

### Unified Content Document / 统一内容文档

The core data structure is defined in `src/schemas/content.py`:

核心数据结构定义在 `src/schemas/content.py`：

```python
ContentDocument = {
    id: str,                    # UUID v4
    source_type: str,           # "file" | "crawl" | "db"
    source_url: str,            # Original path or URL / 原始路径或URL
    content_type: str,          # "article" | "code" | "doc" | "social"
    metadata: {
        title: str,
        author: str | None,
        published_at: datetime | None,
        tags: list[str],
        language: str           # "zh" | "en"
    },
    content: {
        text: str,              # Plain text content / 纯文本内容
        chunks: list[{text: str, index: int}]
    },
    vectors: {
        dense: list[float]      # BGE-large-zh embeddings
    },
    quality_score: float
}
```

### Kafka Events / Kafka 事件

Events are defined in `src/schemas/events.py`. Connectors send events to the following topics:

事件定义在 `src/schemas/events.py`。各连接器向以下 topic 发送事件：

| Connector | Topic | Event Type |
|-----------|-------|------------|
| FileWatcher | `file-events` | FileEvent |
| CrawlConnector | `crawl-events` | CrawlEvent |
| DBConnector | `db-events` | DBEvent |

---

## 定时任务 / Scheduled Tasks

Scheduled tasks are defined in each connector:

定时任务定义在各连接器中：

| Task | Schedule | Description / 描述 |
|------|----------|-------------------|
| GitHub Trending | `0 */6 * * *` | Fetch every 6 hours / 每6小时获取一次 |
| AI News | `*/30 * * * *` | Every 30 minutes / 每30分钟抓取 |
| DB Sync | `0 2 * * *` | Daily at 2 AM / 每天凌晨2点 |
| Cleanup | `0 3 * * *` | Daily at 3 AM / 每天凌晨3点清理 |

---

## 失败类型 / Failure Types

The `src/learning/` module tracks the following failure types:

`src/learning/` 模块追踪以下失败类型：

| Type | Description / 描述 |
|------|-------------------|
| `network_error` | Network connection failed / 网络连接失败 |
| `encoding_error` | Character encoding issue / 字符编码问题 |
| `parse_error` | Parsing failed / 解析失败 |
| `unknown_format` | Unknown file format / 未知文件格式 |
| `size_limit` | Exceeded size limit / 超出大小限制 |
| `quality_too_low` | Content quality below threshold / 内容质量不达标 |

Feedback actions / 反馈动作：`retry`、`skip`、`user_processed`、`new_strategy`
