# AI Knowledge Base (AI-Ready Knowledge Repository)

**Date:** 2026-04-21
**Status:** Approved for Implementation
**Version:** 1.0

---

## 1. Overview

### Purpose

构建一个面向 AI 的知识库系统：接收来自文件、爬虫、数据库等多种数据源，将数据清洗、去噪、转化为 AI 可高效消费的干净格式，并提供多种查询接口（对话、搜索、API）。

### Scope

| 维度 | 范围 |
|------|------|
| 用途 | 研究分析 + 个人知识管理 + AI 数据准备 |
| 数据源 | 本地文件、网页爬虫（定时）、已有数据库/知识图谱 |
| 存储 | 多格式输出（Markdown、JSON、图结构），支持导出 |
| 查询 | 对话问答 + 结构化筛选 + API |
| 规模 | 百万级文档，支持分布式扩展 |
| 部署 | 本地优先 → 阿里云 |

---

## 2. Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         System Architecture                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Sources                         Event Bus              Consumers      │
│  ────────                        ─────────              ─────────      │
│  • File Watcher                  Kafka                  • Parser       │
│  • GitHub Trending               (事件总线)              • Crawler     │
│  • Web Scraper                  ┌─────────────┐          • DB Import   │
│  • DB/KG Connector              │ file-events │          └──────┬──────┘
│                                 │crawl-events │                 │
│  Schedule                       │ db-events   │                 │
│  ────────                       └──────┬──────┘                 │
│  • GitHub Trending (6h)              │                         ▼
│  • AI News (30min)              ┌────▼──────────────────────────────┐  │
│  • DB Sync (daily)              │         Content Schema            │  │
│                                │       (统一中间格式)               │  │
│                                └────┬───────────┬───────────┬───────┘  │
│                                    │           │           │          │
│                              ┌─────▼─┐   ┌─────▼─┐   ┌────▼────┐     │
│                              │Vector-│   │ KG    │   │ File    │     │
│                              │izer   │   │Builder│   │ Cache   │     │
│                              └───┬───┘   └───┬───┘   └────────┘     │
│                                  │           │                      │
│  ┌──────────────────────────────┼───────────┼──────────────────────┤
│                                  ▼           ▼                      │
│  Storage Layer                                                    │
│  ─────────────                                                    │
│  • PostgreSQL (元数据 + pgvector)                                  │
│  • Qdrant (向量存储)                                              │
│  • Neo4j (图存储)                                                 │
│  • MinIO (文件存储)                                               │
│                                  │                                   │
│                                  ▼                                   │
│  Query Layer                                                      │
│  ──────────                                                      │
│  • RAG QA API                                                    │
│  • Vector Search API                                             │
│  • Graph Query API                                               │
│  • Export API                                                    │
│  • WebSocket (实时)                                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **单一职责** - 每个模块只做一件事
2. **事件驱动** - 各组件通过 Kafka 解耦
3. **可扩展** - 消费者可并行扩展
4. **自学习** - 失败反馈循环，持续优化
5. **可观测** - 完整的指标追踪体系

---

## 3. Components

### 3.1 Source Connectors

| 数据源 | Connector | 说明 |
|--------|-----------|------|
| 本地文件 | `FileWatcher` | 监控文件夹，新文件自动入队 |
| PDF | `PDFConnector` | pdfplumber 提取文本+元数据 |
| Word | `DocxConnector` | python-docx 全支持 |
| Markdown | `MarkdownConnector` | 解析 frontmatter 元数据 |
| HTML | `HTMLConnector` | BeautifulSoup + Selector |
| GitHub | `GitHubConnector` | API 抓取 Trending + 详情 |
| 网页爬虫 | `CrawlConnector` | Scrapy 爬虫，支持 Playwright JS 渲染 |
| 数据库 | `DBConnector` | PostgreSQL/MySQL → Schema 转换 |
| 知识图谱 | `KGConnector` | Neo4j/图数据库导出 |

### 3.2 Processing Pipeline

```
Raw Input → Parse → Clean → Deduplicate → Chunk → Embed → Store
              │       │         │          │       │
           解析器   清洗器   去重检测     分块器   向量化
```

| Stage | Description |
|-------|-------------|
| **Parse** | 不同格式转成统一 Schema |
| **Clean** | 去 CSS/JS/广告、净化 HTML、标准化 whitespace |
| **Deduplicate** | SimHash 或 MinHash 去重 |
| **Chunk** | 按语义/长度分块（512-1024 tokens） |
| **Embed** | BGE-large-zh 生成向量 |

### 3.3 Storage Layer

| 存储 | 引擎 | 用途 |
|------|------|------|
| 向量库 | Qdrant | 高效相似性搜索，支持过滤条件 |
| 图数据库 | Neo4j | 实体关系查询，知识推理 |
| 关系数据库 | PostgreSQL + pgvector | 元数据、结构化+向量混合查询 |
| 文件存储 | MinIO (S3) | 原始文件、清洗后文件 |

### 3.4 Query API

```python
# RAG 问答
POST /api/v1/qa
{
  "question": "最近有哪些热门的开源项目？",
  "filters": {"time_range": "7d", "language": "zh"},
  "mode": "hybrid"
}

# 向量搜索
POST /api/v1/search
{
  "query": "AI agent framework",
  "limit": 20,
  "filters": {"source_type": "github", "stars": {"$gt": 1000}}
}

# 知识图谱查询
POST /api/v1/graph/query
{
  "cypher": "MATCH (p:Person)-[:CREATED]->(o:Project) WHERE o.stars > 1000 RETURN p,o"
}

# 导出 API
GET /api/v1/export?format=markdown&ids=id1,id2
GET /api/v1/export?format=json&query=AI&filters={...}
```

### 3.5 Scheduled Tasks

| 任务 | 调度 | 说明 |
|------|------|------|
| GitHub Trending | `0 */6 * * *` (每6小时) | 抓取热门项目 |
| AI 新闻 | `*/30 * * * *` (每30分钟) | 监控 AI 新闻源 |
| 增量同步 | `0 2 * * *` (每天凌晨) | DB/KG 增量导入 |
| 清理/去重 | `0 3 * * *` (每天凌晨) | 过期数据清理 |

---

## 4. Unified Content Schema

```json
{
  "id": "uuid-v4",
  "source_type": "file|crawl|db",
  "source_url": "原始路径或URL",
  "content_type": "article|code|doc|social|...",

  "metadata": {
    "title": "文档标题",
    "author": "作者",
    "published_at": "发布时间",
    "tags": ["tag1", "tag2"],
    "language": "zh|en",
    "license": "开源协议",
    "stars": 1234,
    "forks": 567
  },

  "content": {
    "text": "纯文本内容（去噪后）",
    "chunks": [
      {"text": "chunk1", "index": 0},
      {"text": "chunk2", "index": 1}
    ],
    "entities": [
      {"name": "实体", "type": "PERSON|ORG|..."}
    ],
    "relations": [
      {"from": "实体A", "to": "实体B", "type": "RELATION_TYPE"}
    ]
  },

  "vectors": {
    "dense": [0.123, -0.456, ...],
    "sparse": {"indices": [0, 5], "values": [0.9, 0.8]}
  },

  "quality_score": 0.85,
  "created_at": "2024-01-01T00:00:00Z",
  "processed_at": "2024-01-01T00:01:00Z"
}
```

---

## 5. AI-Ready Export Formats

### 5.1 Markdown Format

```markdown
---
id: doc-uuid-123
title: LangChain v0.3 发布
source: github
url: https://github.com/langchain-ai/langchain
published_at: 2024-07-01
tags: [AI, LLM, Framework]
stars: 8500
language: en
---

# LangChain v0.3 Released

## Summary
Major improvements in LangChain v0.3...

## Key Features
- **Memory Module**: Enhanced conversation memory
- **Agent Improvements**: Faster agent initialization

## Metadata
| Field | Value |
|-------|-------|
| Version | 0.3.0 |
| GitHub Stars | 8500 |
```

### 5.2 JSON Format

```json
{
  "document": {
    "id": "doc-uuid-123",
    "meta": {
      "title": "LangChain v0.3 发布",
      "source": "github",
      "source_url": "https://github.com/langchain-ai/langchain",
      "published_at": "2024-07-01T00:00:00Z",
      "tags": ["AI", "LLM", "Framework"],
      "language": "en",
      "stars": 8500
    },
    "content": {
      "summary": "Major improvements...",
      "sections": [
        {"heading": "Key Features", "level": 2, "bullets": ["..."]}
      ],
      "entities": [
        {"name": "LangChain", "type": "PROJECT"},
        {"name": "Python", "type": "LANGUAGE"}
      ]
    },
    "created_at": "2024-07-01T12:00:00Z",
    "exported_at": "2024-07-02T08:00:00Z"
  }
}
```

### 5.3 Knowledge Graph Format

```json
{
  "graph": {
    "nodes": [
      {"id": "n1", "type": "Project", "name": "LangChain", "props": {"stars": 8500}},
      {"id": "n2", "type": "Language", "name": "Python", "props": {"version": "3.10+"}}
    ],
    "edges": [
      {"from": "n1", "to": "n2", "type": "WRITTEN_IN"}
    ]
  }
}
```

---

## 6. Failure Tracking & Adaptive Learning

### 6.1 Failure Registry Schema

```sql
CREATE TABLE processing_failures (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    source_type VARCHAR(50),
    source_identifier TEXT,
    source_hash VARCHAR(64),

    failure_type VARCHAR(50),
    error_code VARCHAR(100),
    error_message TEXT,
    stack_trace TEXT,

    attempts INT DEFAULT 0,
    last_attempt_at TIMESTAMP,
    tried_strategies JSONB,

    content_preview TEXT,
    content_size BIGINT,

    status VARCHAR(20) DEFAULT 'pending',
    feedback VARCHAR(50),
    user_instructions TEXT,
    feedback_by VARCHAR(100),
    feedback_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE learned_strategies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    strategy_name VARCHAR(100),
    applicable_patterns JSONB,
    success_count INT DEFAULT 0,
    failure_count INT DEFAULT 0,

    parser_config JSONB,
    preprocessing_steps JSONB,
    postprocessing_rules JSONB,

    avg_quality_score FLOAT,
    avg_processing_time_ms INT,

    is_active BOOLEAN DEFAULT true,
    is_verified BOOLEAN DEFAULT false,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### 6.2 Failure Classification

| Type | Description | Auto-Retry |
|------|-------------|------------|
| `network_error` | 网络超时、连接失败 | Yes (3次) |
| `encoding_error` | 编码检测失败 | Yes (5次) |
| `parse_error` | 解析器失败 | Conditional |
| `unknown_format` | 未知文件格式 | No (needs human) |
| `size_limit` | 超过大小限制 | No |
| `quality_too_low` | 质量分数过低 | No |

### 6.3 User Feedback Actions

| Action | Description |
|--------|-------------|
| `retry` | 按用户指引重新处理 |
| `skip` | 跳过此文档 |
| `user_processed` | 用户已自行处理，提供结果 |
| `new_strategy` | 提供新的处理策略 |

---

## 7. Metrics & Evolution Tracking

### 7.1 Metric Categories

| Category | Metrics |
|----------|---------|
| **规模与吞吐量** | total_documents, processing_rate, storage_size |
| **处理效率** | avg_processing_time_ms, retry_rate, queue_depth |
| **质量分布** | quality_score_distribution, by_dimension |
| **策略进化** | strategy_success_rate, new_strategies_learned |
| **失败模式** | failure_by_type, failure_by_source, similar_groups |
| **自愈能力** | self_healing_rate, first_try_success_rate |
| **成本分析** | compute_cost, storage_cost, cost_per_doc |
| **用户参与度** | feedback_count, avg_response_time, adoption_rate |

### 7.2 Key Ratios

- **成功率** = successful / total_processed
- **自愈率** = auto_resolved / total_failures
- **用户介入率** = needs_human / total_failures
- **策略有效性提升** = current_success_rate - 30d_ago_success_rate

---

## 8. Technology Stack

| Layer | Technology | Rationale |
|-------|------------|------------|
| Event Bus | Apache Kafka | 百万级吞吐，持久化，消费者组 |
| Data Processing | Python | 灵活处理各种格式 |
| Vector Store | Qdrant | Rust实现，高性能，支持过滤 |
| Relational DB | PostgreSQL + pgvector | 结构化+向量混合查询 |
| Graph Store | Neo4j | 知识图谱，Cypher 查询 |
| File Storage | MinIO | 本地S3兼容，迁移云无缝 |
| Web Scraper | Scrapy + Playwright | 静态+动态页面双支持 |
| Scheduler | APScheduler + Cron | 轻量定时任务 |
| API | FastAPI | 异步，高性能，自动文档 |
| Embedding | BGE-large-zh (本地) | 中文优化，可离线 |

---

## 9. File Structure

```
ai-knowledge-base/
├── src/
│   ├── connectors/           # Source connectors
│   │   ├── file_watcher.py
│   │   ├── pdf_connector.py
│   │   ├── github_connector.py
│   │   ├── crawl_connector.py
│   │   └── db_connector.py
│   │
│   ├── processors/           # Processing pipeline
│   │   ├── parser.py
│   │   ├── cleaner.py
│   │   ├── deduplicator.py
│   │   ├── chunker.py
│   │   └── vectorizer.py
│   │
│   ├── storage/              # Storage layer
│   │   ├── qdrant_client.py
│   │   ├── neo4j_client.py
│   │   ├── postgres_client.py
│   │   └── minio_client.py
│   │
│   ├── query/                # Query API
│   │   ├── rag.py
│   │   ├── vector_search.py
│   │   ├── graph_query.py
│   │   └── export.py
│   │
│   ├── learning/             # Feedback & learning
│   │   ├── failure_tracker.py
│   │   ├── strategy_learner.py
│   │   └── strategy_store.py
│   │
│   └── api/                  # FastAPI routes
│       ├── qa.py
│       ├── search.py
│       ├── graph.py
│       ├── export.py
│       └── failures.py
│
├── kafka/                    # Kafka topics & config
├── schemas/                  # Schema definitions
├── tests/                    # Tests
├── docs/                     # Documentation
└── docker/                   # Docker compose
```

---

## 10. Extensibility Points

1. **New Source**: Add new connector in `connectors/`, publish to Kafka
2. **New Storage**: Add new consumer in `processors/`, write to new store
3. **New Export Format**: Add formatter in `query/export.py`
4. **New Strategy**: Add to `learned_strategies` table, update weights
5. **New Metric**: Add to metrics collector, update dashboard

---

## 11. Open Questions

None - design approved for implementation.