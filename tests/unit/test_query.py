"""
Unit tests for query layer - RAG and Export modules
"""
import json
from unittest.mock import AsyncMock, MagicMock

import pytest


class TestRAGQA:
    """Tests for RAG QA module"""

    @pytest.fixture
    def mock_vector_store(self):
        store = MagicMock()
        store.search.return_value = [
            {"id": "doc-1", "score": 0.95, "payload": {"content_text": "Sample document content"}}
        ]
        return store

    @pytest.fixture
    def mock_postgres(self):
        return MagicMock()

    @pytest.fixture
    def mock_neo4j(self):
        client = AsyncMock()
        client.query_cypher.return_value = [
            {"d": {"id": "doc-1", "title": "Test Doc"}, "entities": ["Entity1"]}
        ]
        return client

    @pytest.fixture
    def mock_vectorizer(self):
        vectorizer = MagicMock()
        vectorizer.encode_single.return_value = [0.1] * 1024
        return vectorizer

    @pytest.fixture
    def rag_qa(self, mock_vector_store, mock_postgres, mock_neo4j, mock_vectorizer):
        from src.query.rag import RAGQA
        return RAGQA(
            vector_store=mock_vector_store,
            postgres_client=mock_postgres,
            neo4j_client=mock_neo4j,
            vectorizer=mock_vectorizer,
        )

    @pytest.mark.asyncio
    async def test_ask_vector_mode(self, rag_qa, mock_vector_store, mock_vectorizer):
        """Test RAG ask in vector-only mode"""
        result = await rag_qa.ask("What is AI?", mode="vector", limit=5)

        assert "answer" in result
        assert "sources" in result
        assert result["mode"] == "vector"
        mock_vectorizer.encode_single.assert_called_once_with("What is AI?")
        mock_vector_store.search.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_hybrid_mode(self, rag_qa, mock_vector_store, mock_neo4j, mock_vectorizer):
        """Test RAG ask in hybrid mode"""
        result = await rag_qa.ask("What is AI?", mode="hybrid", limit=5)

        assert "answer" in result
        assert "sources" in result
        assert result["mode"] == "hybrid"
        mock_vector_store.search.assert_called_once()
        mock_neo4j.query_cypher.assert_called()

    @pytest.mark.asyncio
    async def test_ask_graph_mode(self, rag_qa, mock_vector_store, mock_neo4j, mock_vectorizer):
        """Test RAG ask in graph-only mode"""
        result = await rag_qa.ask("What is AI?", mode="graph", limit=5)

        assert "answer" in result
        assert "sources" in result
        assert result["mode"] == "graph"
        mock_vector_store.search.assert_not_called()
        mock_neo4j.query_cypher.assert_called()

    @pytest.mark.asyncio
    async def test_ask_deduplicates_sources(self, rag_qa, mock_vector_store, mock_vectorizer):
        """Test that duplicate sources are removed"""
        mock_vector_store.search.return_value = [
            {"id": "doc-1", "score": 0.95, "payload": {"content_text": "Content A"}},
            {"id": "doc-1", "score": 0.90, "payload": {"content_text": "Content A"}},
            {"id": "doc-2", "score": 0.85, "payload": {"content_text": "Content B"}},
        ]
        result = await rag_qa.ask("What is AI?", mode="vector")

        source_ids = [s["id"] for s in result["sources"]]
        assert len(source_ids) == 2
        assert "doc-1" in source_ids
        assert "doc-2" in source_ids

    @pytest.mark.asyncio
    async def test_ask_with_filters(self, rag_qa, mock_vector_store, mock_vectorizer):
        """Test RAG ask with filters"""
        filters = {"must": [{"key": "source_type", "match": {"value": "article"}}]}
        await rag_qa.ask("What is AI?", filters=filters, mode="vector")

        call_args = mock_vector_store.search.call_args
        assert call_args.kwargs["filters"] == filters


class TestVectorSearch:
    """Tests for Vector Search module"""

    @pytest.fixture
    def mock_vector_store(self):
        store = MagicMock()
        store.search.return_value = [
            {"id": "doc-1", "score": 0.95, "payload": {"text": "Sample"}},
            {"id": "doc-2", "score": 0.85, "payload": {"text": "Another"}},
        ]
        return store

    @pytest.fixture
    def mock_vectorizer(self):
        vectorizer = MagicMock()
        vectorizer.encode_single.return_value = [0.1] * 1024
        vectorizer.encode.return_value = [[0.1] * 1024, [0.2] * 1024]
        return vectorizer

    @pytest.fixture
    def vector_search(self, mock_vector_store, mock_vectorizer):
        from src.query.vector_search import VectorSearch
        return VectorSearch(
            vector_store=mock_vector_store,
            vectorizer=mock_vectorizer,
        )

    def test_search(self, vector_search, mock_vector_store, mock_vectorizer):
        """Test basic vector search"""
        results = vector_search.search("What is AI?", limit=10)

        assert len(results) == 2
        mock_vectorizer.encode_single.assert_called_once_with("What is AI?")
        mock_vector_store.search.assert_called_once()

    def test_search_with_filters(self, vector_search, mock_vector_store, mock_vectorizer):
        """Test vector search with filters"""
        filters = {"must": [{"key": "doc_id", "match": {"value": "doc-1"}}]}
        score_threshold = 0.5

        vector_search.search(
            "What is AI?",
            filters=filters,
            score_threshold=score_threshold,
        )

        call_args = mock_vector_store.search.call_args
        assert call_args.kwargs["filters"] == filters
        assert call_args.kwargs["score_threshold"] == score_threshold

    def test_batch_search(self, vector_search, mock_vector_store, mock_vectorizer):
        """Test batch vector search"""
        queries = ["Query 1", "Query 2"]
        results = vector_search.batch_search(queries, limit=5)

        assert len(results) == 2
        mock_vectorizer.encode.assert_called_once_with(queries)
        assert mock_vector_store.search.call_count == 2

    def test_search_by_vector(self, vector_search, mock_vector_store):
        """Test search by pre-computed vector"""
        vector = [0.1] * 1024
        results = vector_search.search_by_vector(vector, limit=10)

        assert len(results) == 2
        mock_vector_store.search.assert_called_once_with(
            query_vector=vector,
            limit=10,
            filters=None,
            score_threshold=None,
        )


class TestDocumentService:
    """Tests for query-facing document access."""

    @pytest.fixture
    def mock_postgres(self):
        return AsyncMock()

    @pytest.fixture
    def document_service(self, mock_postgres):
        from src.query.document_service import DocumentService

        return DocumentService(postgres_client=mock_postgres)

    @pytest.mark.asyncio
    async def test_get_document_delegates_to_postgres(self, document_service, mock_postgres):
        mock_postgres.get_document.return_value = {"id": "doc-1"}

        result = await document_service.get_document("doc-1")

        assert result == {"id": "doc-1"}
        mock_postgres.get_document.assert_awaited_once_with("doc-1")

    @pytest.mark.asyncio
    async def test_get_documents_delegates_to_postgres(self, document_service, mock_postgres):
        mock_postgres.get_documents.return_value = [{"id": "doc-1"}]

        result = await document_service.get_documents(["doc-1"])

        assert result == [{"id": "doc-1"}]
        mock_postgres.get_documents.assert_awaited_once_with(["doc-1"])

    @pytest.mark.asyncio
    async def test_list_documents_delegates_to_postgres(self, document_service, mock_postgres):
        mock_postgres.list_documents.return_value = [{"id": "doc-1"}]

        result = await document_service.list_documents(limit=5)

        assert result == [{"id": "doc-1"}]
        mock_postgres.list_documents.assert_awaited_once_with(limit=5)

    @pytest.mark.asyncio
    async def test_search_documents_delegates_to_postgres(self, document_service, mock_postgres):
        mock_postgres.search_documents.return_value = [{"id": "doc-1"}]

        result = await document_service.search_documents("ai", limit=5)

        assert result == [{"id": "doc-1"}]
        mock_postgres.search_documents.assert_awaited_once_with("ai", limit=5)


class TestGraphQuery:
    """Tests for Graph Query module"""

    @pytest.fixture
    def mock_neo4j(self):
        client = AsyncMock()
        return client

    @pytest.fixture
    def graph_query(self, mock_neo4j):
        from src.query.graph_query import GraphQuery
        return GraphQuery(neo4j_client=mock_neo4j)

    @pytest.mark.asyncio
    async def test_find_entities(self, graph_query, mock_neo4j):
        """Test finding entities by type"""
        mock_neo4j.query_cypher.return_value = [
            {"name": "Entity1", "type": "PERSON"},
            {"name": "Entity2", "type": "PERSON"},
        ]

        results = await graph_query.find_entities(entity_type="PERSON", limit=50)

        assert len(results) == 2
        mock_neo4j.query_cypher.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_entities_all_types(self, graph_query, mock_neo4j):
        """Test finding all entities without type filter"""
        mock_neo4j.query_cypher.return_value = [
            {"name": "Entity1", "type": "PERSON"},
            {"name": "Org1", "type": "ORGANIZATION"},
        ]

        results = await graph_query.find_entities(limit=100)

        assert len(results) == 2
        mock_neo4j.query_cypher.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_documents_by_entity(self, graph_query, mock_neo4j):
        """Test finding documents containing an entity"""
        mock_neo4j.query_cypher.return_value = [
            {"id": "doc-1", "title": "Test Doc", "source_type": "article"}
        ]

        results = await graph_query.find_documents_by_entity("Entity1")

        assert len(results) == 1
        assert results[0]["id"] == "doc-1"

    @pytest.mark.asyncio
    async def test_find_related_entities(self, graph_query, mock_neo4j):
        """Test finding related entities"""
        mock_neo4j.query_cypher.return_value = [
            {"source": "Entity1", "target": "Entity2", "distance": 1}
        ]

        results = await graph_query.find_related_entities("Entity1", max_depth=2)

        assert len(results) == 1
        assert results[0]["source"] == "Entity1"
        assert results[0]["target"] == "Entity2"

    @pytest.mark.asyncio
    async def test_get_document_graph(self, graph_query, mock_neo4j):
        """Test getting full document graph"""
        mock_neo4j.query_cypher.side_effect = [
            [{"name": "Entity1", "type": "PERSON"}, {"name": "Entity2", "type": "ORG"}],
            [{"from": "Entity1", "to": "Entity2", "type": "RELATED"}],
        ]

        result = await graph_query.get_document_graph("doc-123")

        assert result["doc_id"] == "doc-123"
        assert len(result["entities"]) == 2
        assert len(result["relations"]) == 1


class TestExportService:
    """Tests for Export Service"""

    @pytest.fixture
    def sample_document(self):
        return {
            "id": "doc-123",
            "source_type": "article",
            "source_url": "https://example.com/article",
            "metadata": {
                "title": "Test Article",
                "tags": ["AI", "ML"],
            },
            "content": {
                "text": "This is the article content about artificial intelligence.",
                "entities": [
                    {"name": "AI", "type": "CONCEPT"},
                    {"name": "ML", "type": "CONCEPT"},
                ],
                "relations": [
                    {"from": "AI", "to": "ML", "type": "RELATED_TO"},
                ],
            },
            "processed_at": "2024-01-01T00:00:00",
        }

    @pytest.fixture
    def export_service(self):
        from src.query.export import ExportService
        return ExportService()

    def test_markdown_exporter(self, export_service, sample_document):
        """Test exporting document as Markdown"""
        result = export_service.export_single(sample_document, format="markdown")

        assert "---" in result
        assert "Test Article" in result
        assert "source: article" in result
        assert "**AI** (CONCEPT)" in result

    def test_json_exporter(self, export_service, sample_document):
        """Test exporting document as JSON"""
        result = export_service.export_single(sample_document, format="json")

        assert '"id": "doc-123"' in result
        assert '"title": "Test Article"' in result
        assert "AI" in result

    def test_graph_exporter(self, export_service, sample_document):
        """Test exporting document as Graph JSON"""
        result = export_service.export_single(sample_document, format="graph")

        assert '"nodes"' in result
        assert '"edges"' in result
        assert "AI" in result

    def test_export_multiple_documents_markdown(self, export_service, sample_document):
        """Test exporting multiple documents as Markdown"""
        docs = [sample_document, sample_document]
        result = export_service.export(docs, format="markdown")

        assert result.count("---") >= 4

    def test_export_multiple_documents_json(self, export_service, sample_document):
        """Test exporting multiple documents as JSON"""
        docs = [sample_document, sample_document]
        result = export_service.export(docs, format="json")

        assert result.count("doc-123") == 2

    def test_export_unsupported_format_raises(self, export_service, sample_document):
        """Test that unsupported format raises ValueError"""
        with pytest.raises(ValueError, match="Unsupported export format"):
            export_service.export_single(sample_document, format="xml")

    def test_export_service_has_all_formatters(self, export_service):
        """Test ExportService has all expected formatters"""
        assert "markdown" in export_service.formatters
        assert "json" in export_service.formatters
        assert "graph" in export_service.formatters


class TestMarkdownExporter:
    """Tests for Markdown Exporter"""

    def test_format_short_content(self):
        """Test Markdown format with short content (no truncation)"""
        from src.query.export import MarkdownExporter

        exporter = MarkdownExporter()
        doc = {
            "id": "doc-1",
            "metadata": {"title": "Short Doc", "tags": []},
            "content": {"text": "Short content", "entities": []},
        }
        result = exporter.format(doc)

        assert "Short content" in result
        assert "..." not in result

    def test_format_long_content_truncates(self):
        """Test Markdown format truncates long content"""
        from src.query.export import MarkdownExporter

        exporter = MarkdownExporter()
        doc = {
            "id": "doc-1",
            "metadata": {"title": "Long Doc", "tags": []},
            "content": {"text": "x" * 600, "entities": []},
        }
        result = exporter.format(doc)

        assert "..." in result
        assert len(result.split("## Summary")[1].split("...")[0].strip()) == 500

    def test_format_with_entities(self):
        """Test Markdown format includes entities"""
        from src.query.export import MarkdownExporter

        exporter = MarkdownExporter()
        doc = {
            "id": "doc-1",
            "metadata": {"title": "Entity Doc", "tags": []},
            "content": {
                "text": "Content",
                "entities": [
                    {"name": "Entity1", "type": "PERSON"},
                    {"name": "Entity2", "type": "ORG"},
                ],
            },
        }
        result = exporter.format(doc)

        assert "## Entities" in result
        assert "**Entity1** (PERSON)" in result
        assert "**Entity2** (ORG)" in result


class TestJSONExporter:
    """Tests for JSON Exporter"""

    def test_format_structure(self):
        """Test JSON export has correct structure"""
        from src.query.export import JSONExporter

        exporter = JSONExporter()
        doc = {
            "id": "doc-1",
            "metadata": {"title": "Test"},
            "content": {"text": "Content"},
        }
        result = exporter.format(doc)
        parsed = json.loads(result)

        assert "document" in parsed
        assert parsed["document"]["id"] == "doc-1"
        assert "meta" in parsed["document"]
        assert "content" in parsed["document"]


class TestGraphExporter:
    """Tests for Graph Exporter"""

    def test_format_creates_nodes_and_edges(self):
        """Test Graph export creates proper node/edge structure"""
        from src.query.export import GraphExporter

        exporter = GraphExporter()
        doc = {
            "id": "doc-1",
            "content": {
                "entities": [
                    {"name": "A", "type": "TYPE_A"},
                    {"name": "B", "type": "TYPE_B"},
                ],
                "relations": [
                    {"from": "A", "to": "B", "type": "RELATED"},
                ],
            },
        }
        result = exporter.format(doc)
        parsed = json.loads(result)

        assert "graph" in parsed
        assert len(parsed["graph"]["nodes"]) == 2
        assert len(parsed["graph"]["edges"]) == 1

    def test_format_handles_missing_entity_refs(self):
        """Test Graph export handles missing entity references gracefully"""
        from src.query.export import GraphExporter

        exporter = GraphExporter()
        doc = {
            "id": "doc-1",
            "content": {
                "entities": [{"name": "A", "type": "TYPE_A"}],
                "relations": [
                    {"from": "A", "to": "Unknown", "type": "RELATED"},
                ],
            },
        }
        result = exporter.format(doc)
        parsed = json.loads(result)

        assert len(parsed["graph"]["edges"]) == 1
        assert parsed["graph"]["edges"][0]["to"] == "unknown"
