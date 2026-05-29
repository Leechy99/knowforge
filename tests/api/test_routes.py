"""
API Routes Tests
"""
import pytest
from fastapi.testclient import TestClient

from src.learning.failure_tracker import FailureTracker, FailureType
from src.main import app


def _clear_state() -> None:
    for name in (
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


class FakeQAService:
    async def ask(self, question, filters=None, mode="hybrid", limit=10):
        return {
            "answer": f"Answer for {question}",
            "sources": [{"id": "doc-1", "score": 0.9}],
            "mode": mode,
        }


class FakeSearchService:
    def search(self, query, limit=20, filters=None):
        return [{"id": "doc-1", "query": query, "score": 0.95}][:limit]


class FakeDocumentService:
    def __init__(self):
        self.documents = {
            "doc-1": {
                "id": "doc-1",
                "source_type": "file",
                "source_url": "/tmp/doc.md",
                "metadata": {"title": "Doc 1", "tags": ["ai"]},
                "content": {"text": "Document text"},
            }
        }

    def get_documents(self, ids):
        return [self.documents[doc_id] for doc_id in ids if doc_id in self.documents]

    def search_documents(self, query):
        return list(self.documents.values()) if query else []

    def list_documents(self):
        return list(self.documents.values())


class TestHealthEndpoint:
    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestQARoutes:
    def test_ask_question_calls_service(self, client):
        app.state.qa_service = FakeQAService()

        response = client.post(
            "/api/v1/qa",
            json={"question": "What is AI?"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "Answer for What is AI?"
        assert data["sources"] == [{"id": "doc-1", "score": 0.9}]
        assert data["mode"] == "hybrid"

    def test_ask_question_with_mode(self, client):
        app.state.qa_service = FakeQAService()

        response = client.post(
            "/api/v1/qa",
            json={"question": "What is AI?", "mode": "vector"},
        )

        assert response.status_code == 200
        assert response.json()["mode"] == "vector"

    def test_ask_question_without_service_returns_503(self, client):
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
    def test_search_calls_service(self, client):
        app.state.search_service = FakeSearchService()

        response = client.post(
            "/api/v1/search",
            json={"query": "test query"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["results"] == [{"id": "doc-1", "query": "test query", "score": 0.95}]
        assert data["total"] == 1

    def test_search_without_service_returns_503(self, client):
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
        app.state.document_service = FakeDocumentService()

        response = client.get("/api/v1/export?ids=missing")

        assert response.status_code == 404
        assert response.json()["detail"] == "No documents found"

    def test_export_without_service_returns_503(self, client):
        response = client.get("/api/v1/export")

        assert response.status_code == 503
        assert response.json()["detail"] == "Document service unavailable"

    def test_export_by_ids(self, client):
        app.state.document_service = FakeDocumentService()

        response = client.get("/api/v1/export?ids=doc-1")

        assert response.status_code == 200
        assert "Document text" in response.text

    def test_export_format_query(self, client):
        app.state.document_service = FakeDocumentService()

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
        response = client.post("/api/v1/qa", json={"question": "test"})
        assert response.status_code == 503

        response = client.post("/api/v1/search", json={"query": "test"})
        assert response.status_code == 503

        response = client.get("/api/v1/export")
        assert response.status_code == 503

        response = client.get("/api/v1/failures")
        assert response.status_code == 200
