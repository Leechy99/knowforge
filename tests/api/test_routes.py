"""
API Routes Tests
"""
import pytest
from fastapi.testclient import TestClient

from src.learning.failure_tracker import FailureTracker, FailureType
from src.main import app
from src.query.document_service import DocumentService
from src.query.rag import RAGQA
from src.query.vector_search import VectorSearch


def _clear_state() -> None:
    for name in (
        "postgres_client",
        "vector_store",
        "neo4j_client",
        "vectorizer",
        "qa_service",
        "search_service",
        "document_service",
        "failure_tracker",
    ):
        if hasattr(app.state, name):
            delattr(app.state, name)


@pytest.fixture
def client():
    _clear_state()
    with TestClient(app) as test_client:
        yield test_client
    _clear_state()


SAMPLE_DOCUMENT = {
    "id": "doc-1",
    "source_type": "file",
    "source_url": "/tmp/doc.md",
    "metadata": {"title": "Doc 1", "tags": ["ai"]},
    "content": {"text": "Document text"},
}


class TestHealthEndpoint:
    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestAppStartup:
    def test_lifespan_initializes_query_and_document_services(self, client):
        assert isinstance(app.state.qa_service, RAGQA)
        assert isinstance(app.state.search_service, VectorSearch)
        assert isinstance(app.state.document_service, DocumentService)
        assert hasattr(app.state.document_service, "get_document")
        assert hasattr(app.state.document_service, "get_documents")
        assert hasattr(app.state.document_service, "list_documents")
        assert hasattr(app.state.document_service, "search_documents")


class TestQARoutes:
    def test_ask_question_uses_startup_service(self, client):
        app.state.vectorizer.encode_single = lambda text: [0.1] * 1024
        app.state.vector_store.search = lambda **kwargs: [
            {"id": "doc-1", "score": 0.9, "payload": {"content_text": "AI content"}}
        ]
        app.state.neo4j_client.query_cypher = AsyncQuery([])

        response = client.post(
            "/api/v1/qa",
            json={"question": "What is AI?"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "Based on 1 relevant documents, the answer would be generated here."
        assert data["sources"] == [{"id": "doc-1", "score": 0.9, "content": "AI content"}]
        assert data["mode"] == "hybrid"

    def test_ask_question_with_mode_uses_startup_service(self, client):
        app.state.vectorizer.encode_single = lambda text: [0.1] * 1024
        app.state.vector_store.search = lambda **kwargs: [
            {"id": "doc-1", "score": 0.9, "payload": {"content_text": "AI content"}}
        ]

        response = client.post(
            "/api/v1/qa",
            json={"question": "What is AI?", "mode": "vector"},
        )

        assert response.status_code == 200
        assert response.json()["mode"] == "vector"

    def test_ask_question_without_service_returns_503(self, client):
        delattr(app.state, "qa_service")

        response = client.post(
            "/api/v1/qa",
            json={"question": "What is AI?"},
        )

        assert response.status_code == 503
        assert response.json()["detail"] == "QA service unavailable"

    def test_ask_question_missing_question(self, client):
        response = client.post(
            "/api/v1/qa",
            json={},
        )
        assert response.status_code == 422


class TestSearchRoutes:
    def test_search_uses_startup_service(self, client):
        app.state.vectorizer.encode_single = lambda text: [0.1] * 1024
        app.state.vector_store.search = lambda **kwargs: [
            {"id": "doc-1", "score": 0.95, "payload": {"query": "test query"}}
        ][: kwargs["limit"]]

        response = client.post(
            "/api/v1/search",
            json={"query": "test query"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["results"] == [
            {"id": "doc-1", "score": 0.95, "payload": {"query": "test query"}}
        ]
        assert data["total"] == 1

    def test_search_without_service_returns_503(self, client):
        delattr(app.state, "search_service")

        response = client.post(
            "/api/v1/search",
            json={"query": "test query"},
        )

        assert response.status_code == 503
        assert response.json()["detail"] == "Search service unavailable"

    def test_search_missing_query(self, client):
        response = client.post(
            "/api/v1/search",
            json={},
        )
        assert response.status_code == 422


class TestExportRoutes:
    def test_export_no_documents(self, client):
        app.state.postgres_client.get_documents = AsyncQuery([])

        response = client.get("/api/v1/export?ids=missing")

        assert response.status_code == 404
        assert response.json()["detail"] == "No documents found"

    def test_export_without_service_returns_503(self, client):
        delattr(app.state, "document_service")

        response = client.get("/api/v1/export")

        assert response.status_code == 503
        assert response.json()["detail"] == "Document service unavailable"

    def test_export_by_ids(self, client):
        app.state.postgres_client.get_documents = AsyncQuery([SAMPLE_DOCUMENT])

        response = client.get("/api/v1/export?ids=doc-1")

        assert response.status_code == 200
        assert "Document text" in response.text

    def test_export_format_query(self, client):
        app.state.postgres_client.search_documents = AsyncQuery([SAMPLE_DOCUMENT])

        response = client.get("/api/v1/export?format=json&query=ai")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")


class TestFailureRoutes:
    def test_list_failures_empty(self, client):
        response = client.get("/api/v1/failures")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_failures_with_limit(self, client):
        tracker = FailureTracker()
        tracker.record_failure(
            "file",
            "/tmp/a.txt",
            FailureType.PARSE_ERROR,
            "parse failed",
        )
        app.state.failure_tracker = tracker

        response = client.get("/api/v1/failures?limit=10")

        assert response.status_code == 200
        assert len(response.json()) == 1

    def test_get_failure_not_found(self, client):
        response = client.get("/api/v1/failures/test-id-123")
        assert response.status_code == 404
        assert response.json()["detail"] == "Failure not found"

    def test_submit_feedback_updates_tracker(self, client):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            "file",
            "/tmp/a.txt",
            FailureType.PARSE_ERROR,
            "parse failed",
        )
        app.state.failure_tracker = tracker

        response = client.post(
            f"/api/v1/failures/{failure.id}/feedback",
            json={"action": "retry"},
        )

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        assert tracker.failures[failure.id].status.value == "retry_scheduled"

    def test_submit_feedback_not_found(self, client):
        response = client.post(
            "/api/v1/failures/test-id-123/feedback",
            json={"action": "retry"},
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Failure not found"


class TestAPIRoutesPresence:
    def test_all_routers_registered(self, client):
        app.state.vectorizer.encode_single = lambda text: [0.1] * 1024
        app.state.vector_store.search = lambda **kwargs: [
            {"id": "doc-1", "score": 0.95, "payload": {"content_text": "content"}}
        ]
        app.state.neo4j_client.query_cypher = AsyncQuery([])
        app.state.postgres_client.list_documents = AsyncQuery([SAMPLE_DOCUMENT])

        response = client.post("/api/v1/qa", json={"question": "test"})
        assert response.status_code == 200

        response = client.post("/api/v1/search", json={"query": "test"})
        assert response.status_code == 200

        response = client.get("/api/v1/export")
        assert response.status_code == 200

        response = client.get("/api/v1/failures")
        assert response.status_code == 200


class AsyncQuery:
    def __init__(self, result):
        self.result = result

    async def __call__(self, *args, **kwargs):
        return self.result
