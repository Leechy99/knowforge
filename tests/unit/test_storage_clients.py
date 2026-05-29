"""
Unit tests for storage layer clients
"""
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestPostgresClient:
    """Tests for PostgreSQL client"""

    @pytest.fixture
    def sample_document(self):
        return {
            "id": str(uuid.uuid4()),
            "source_type": "article",
            "source_url": "https://example.com/article",
            "content_type": "article",
            "metadata": {"title": "Test Article", "author": "Test Author"},
            "content": {
                "text": "This is the article content.",
                "chunks": [{"text": "chunk1", "index": 0}],
            },
            "quality_score": 0.95,
        }

    @pytest.mark.asyncio
    async def test_store_document(self, sample_document):
        """Test storing a document in PostgreSQL"""
        from src.storage.postgres_client import PostgresClient

        with patch("src.storage.postgres_client.create_async_engine") as mock_engine:
            with patch("src.storage.postgres_client.async_sessionmaker") as mock_sessionmaker:
                mock_engine.return_value = MagicMock()
                mock_sessionmaker.return_value = MagicMock()

                client = PostgresClient("postgresql://localhost/test")

                mock_session = AsyncMock()
                mock_session_factory = MagicMock(return_value=mock_session)
                mock_sessionmaker.return_value = mock_session_factory

                mock_session.commit = AsyncMock()

                client.session_factory = mock_session_factory

                result = await client.store_document(sample_document)

                assert result is not None
                mock_session.add.assert_called_once()
                mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_document(self, sample_document):
        """Test retrieving a document from PostgreSQL"""
        from src.storage.postgres_client import PostgresClient, DocumentRecord

        with patch("src.storage.postgres_client.create_async_engine") as mock_engine:
            with patch("src.storage.postgres_client.async_sessionmaker") as mock_sessionmaker:
                mock_engine.return_value = MagicMock()
                mock_sessionmaker.return_value = MagicMock()

                client = PostgresClient("postgresql://localhost/test")

                mock_record = MagicMock(spec=DocumentRecord)
                mock_record.id = uuid.uuid4()
                mock_record.source_type = "article"
                mock_record.source_url = "https://example.com/article"
                mock_record.content_type = "article"
                mock_record.metadata = {"title": "Test"}
                mock_record.content_text = "content"
                mock_record.chunks = []
                mock_record.quality_score = 0.5
                mock_record.created_at = datetime.utcnow()
                mock_record.processed_at = datetime.utcnow()

                mock_session = AsyncMock()
                mock_session.get = AsyncMock(return_value=mock_record)
                mock_session_factory = MagicMock(return_value=mock_session)
                mock_sessionmaker.return_value = mock_session_factory

                client.session_factory = mock_session_factory

                result = await client.get_document(str(mock_record.id))

                assert result is not None
                assert result["source_type"] == "article"
                assert result["content_type"] == "article"


class TestQdrantVectorStore:
    """Tests for Qdrant vector store client"""

    @pytest.fixture
    def sample_points(self):
        return [
            {
                "id": "vec-1",
                "vector": [0.1] * 1024,
                "payload": {"doc_id": "doc-1", "text": "sample text"},
            },
            {
                "id": "vec-2",
                "vector": [0.2] * 1024,
                "payload": {"doc_id": "doc-2", "text": "another text"},
            },
        ]

    def test_create_collection(self):
        """Test creating a Qdrant collection"""
        from src.storage.qdrant_client import QdrantVectorStore

        with patch("src.storage.qdrant_client.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            store = QdrantVectorStore(url="http://localhost:6333", dimension=1024)
            store.create_collection(recreate=False)

            mock_client.create_collection.assert_called_once()

    def test_create_collection_recreate(self):
        """Test recreating a Qdrant collection"""
        from src.storage.qdrant_client import QdrantVectorStore

        with patch("src.storage.qdrant_client.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            store = QdrantVectorStore(url="http://localhost:6333", dimension=1024)
            store.create_collection(recreate=True)

            mock_client.delete_collection.assert_called_once()
            mock_client.create_collection.assert_called_once()

    def test_upsert_vectors(self, sample_points):
        """Test upserting vectors to Qdrant"""
        from src.storage.qdrant_client import QdrantVectorStore

        with patch("src.storage.qdrant_client.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            store = QdrantVectorStore(url="http://localhost:6333", dimension=1024)
            store.upsert_vectors(sample_points)

            mock_client.upsert.assert_called_once()
            call_args = mock_client.upsert.call_args
            assert call_args.kwargs["collection_name"] == "ai_knowledge_base"
            assert len(call_args.kwargs["points"]) == 2

    def test_search(self):
        """Test searching vectors in Qdrant"""
        from src.storage.qdrant_client import QdrantVectorStore

        with patch("src.storage.qdrant_client.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            mock_result = MagicMock()
            mock_result.id = "vec-1"
            mock_result.score = 0.95
            mock_result.payload = {"doc_id": "doc-1"}
            mock_client.search.return_value = [mock_result]

            store = QdrantVectorStore(url="http://localhost:6333", dimension=1024)
            results = store.search(query_vector=[0.1] * 1024, limit=10)

            assert len(results) == 1
            assert results[0]["id"] == "vec-1"
            assert results[0]["score"] == 0.95
            mock_client.search.assert_called_once()

    def test_search_with_filters(self):
        """Test searching with filters"""
        from src.storage.qdrant_client import QdrantVectorStore

        with patch("src.storage.qdrant_client.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.search.return_value = []

            store = QdrantVectorStore(url="http://localhost:6333", dimension=1024)
            filters = {"must": [{"key": "doc_id", "match": {"value": "doc-1"}}]}
            store.search(query_vector=[0.1] * 1024, filters=filters, score_threshold=0.5)

            call_args = mock_client.search.call_args
            assert call_args.kwargs["query_filter"] == filters
            assert call_args.kwargs["score_threshold"] == 0.5


class TestNeo4jGraphStore:
    """Tests for Neo4j graph store client"""

    @pytest.fixture
    def sample_document(self):
        return {
            "id": "doc-123",
            "source_type": "article",
            "source_url": "https://example.com/article",
            "metadata": {"title": "Test Article"},
            "created_at": "2024-01-01T00:00:00",
            "content": {
                "entities": [
                    {"name": "Entity1", "type": "PERSON"},
                    {"name": "Entity2", "type": "ORGANIZATION"},
                ]
            },
        }

    @pytest.mark.asyncio
    async def test_create_document_graph(self, sample_document):
        """Test creating a document graph in Neo4j"""
        from src.storage.neo4j_client import Neo4jGraphStore

        with patch("src.storage.neo4j_client.AsyncGraphDatabase") as mock_driver_class:
            mock_driver = MagicMock()
            mock_driver_class.driver.return_value = mock_driver

            mock_session = AsyncMock()
            mock_session.run = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_driver.session.return_value = mock_session

            store = Neo4jGraphStore(uri="bolt://localhost:7687", user="neo4j", password="password")
            await store.create_document_graph(sample_document)

            assert mock_session.run.call_count == 4

    @pytest.mark.asyncio
    async def test_query_cypher(self):
        """Test executing a Cypher query"""
        from src.storage.neo4j_client import Neo4jGraphStore

        with patch("src.storage.neo4j_client.AsyncGraphDatabase") as mock_driver_class:
            mock_driver = MagicMock()
            mock_driver_class.driver.return_value = mock_driver

            mock_session = AsyncMock()
            mock_result = AsyncMock()
            mock_result.__aiter__ = lambda self: iter([{"n": {"name": "Test"}}])
            mock_session.run = AsyncMock(return_value=mock_result)
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_driver.session.return_value = mock_session

            store = Neo4jGraphStore(uri="bolt://localhost:7687", user="neo4j", password="password")
            results = await store.query_cypher("MATCH (n) RETURN n")

            assert len(results) == 1
            assert results[0]["n"]["name"] == "Test"

    @pytest.mark.asyncio
    async def test_close(self):
        """Test closing the Neo4j driver"""
        from src.storage.neo4j_client import Neo4jGraphStore

        with patch("src.storage.neo4j_client.AsyncGraphDatabase") as mock_driver_class:
            mock_driver = MagicMock()
            mock_driver_class.driver.return_value = mock_driver

            store = Neo4jGraphStore(uri="bolt://localhost:7687", user="neo4j", password="password")
            await store.close()

            mock_driver.close.assert_called_once()


class TestMinioFileStore:
    """Tests for MinIO file store client"""

    def test_ensure_bucket_exists(self):
        """Test ensuring bucket exists when it does"""
        from src.storage.minio_client import MinioFileStore

        with patch("src.storage.minio_client.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_client.head_bucket.return_value = {}
            mock_boto3.client.return_value = mock_client

            store = MinioFileStore(endpoint="localhost:9000", access_key="key", secret_key="secret")
            store.ensure_bucket()

            mock_client.head_bucket.assert_called_once_with(Bucket="knowforge")
            mock_client.create_bucket.assert_not_called()

    def test_ensure_bucket_creates_if_not_exists(self):
        """Test creating bucket if it doesn't exist"""
        from src.storage.minio_client import MinioFileStore

        with patch("src.storage.minio_client.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_client.head_bucket.side_effect = Exception("Not found")
            mock_boto3.client.return_value = mock_client

            store = MinioFileStore(endpoint="localhost:9000", access_key="key", secret_key="secret")
            store.ensure_bucket()

            mock_client.create_bucket.assert_called_once_with(Bucket="knowforge")

    def test_upload_raw_file(self):
        """Test uploading a raw file"""
        from src.storage.minio_client import MinioFileStore

        with patch("src.storage.minio_client.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.client.return_value = mock_client

            store = MinioFileStore(endpoint="localhost:9000", access_key="key", secret_key="secret")
            store.upload_raw_file(
                key="test.pdf",
                content=b"file content",
                metadata={"content-type": "application/pdf"},
            )

            mock_client.put_object.assert_called_once()
            call_args = mock_client.put_object.call_args
            assert call_args.kwargs["Bucket"] == "knowforge"
            assert call_args.kwargs["Key"] == "raw/test.pdf"
            assert call_args.kwargs["Body"] == b"file content"

    def test_upload_processed_file(self):
        """Test uploading a processed file"""
        from src.storage.minio_client import MinioFileStore

        with patch("src.storage.minio_client.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.client.return_value = mock_client

            store = MinioFileStore(endpoint="localhost:9000", access_key="key", secret_key="secret")
            store.upload_processed_file(key="doc-123", content="processed content", format="txt")

            mock_client.put_object.assert_called_once()
            call_args = mock_client.put_object.call_args
            assert call_args.kwargs["Key"] == "processed/doc-123.txt"
            assert call_args.kwargs["ContentType"] == "text/txt"

    def test_download_file(self):
        """Test downloading a file"""
        from src.storage.minio_client import MinioFileStore

        with patch("src.storage.minio_client.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_response = {"Body": MagicMock(read=MagicMock(return_value=b"file content"))}
            mock_client.get_object.return_value = mock_response
            mock_boto3.client.return_value = mock_client

            store = MinioFileStore(endpoint="localhost:9000", access_key="key", secret_key="secret")
            result = store.download_file("raw/test.pdf")

            assert result == b"file content"
            mock_client.get_object.assert_called_once_with(Bucket="knowforge", Key="raw/test.pdf")

    def test_list_files(self):
        """Test listing files with prefix"""
        from src.storage.minio_client import MinioFileStore

        with patch("src.storage.minio_client.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_client.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "raw/doc1.pdf"},
                    {"Key": "raw/doc2.pdf"},
                    {"Key": "processed/doc1.txt"},
                ]
            }
            mock_boto3.client.return_value = mock_client

            store = MinioFileStore(endpoint="localhost:9000", access_key="key", secret_key="secret")
            result = store.list_files(prefix="raw/", max_keys=100)

            assert len(result) == 3
            assert "raw/doc1.pdf" in result
            mock_client.list_objects_v2.assert_called_once_with(
                Bucket="knowforge", Prefix="raw/", MaxKeys=100
            )

    def test_list_files_empty(self):
        """Test listing files when bucket is empty"""
        from src.storage.minio_client import MinioFileStore

        with patch("src.storage.minio_client.boto3") as mock_boto3:
            mock_client = MagicMock()
            mock_client.list_objects_v2.return_value = {}
            mock_boto3.client.return_value = mock_client

            store = MinioFileStore(endpoint="localhost:9000", access_key="key", secret_key="secret")
            result = store.list_files()

            assert result == []
