"""
Unit tests for Source Connectors
"""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.connectors.file_watcher import FileWatcherConnector, FileWatcherHandler
from src.connectors.github_connector import GitHubConnector
from src.connectors.crawl_connector import CrawlConnector
from src.connectors.db_connector import DatabaseConnector


class TestFileWatcherConnector:
    """Tests for FileWatcherConnector"""

    def test_init_default_extensions(self):
        connector = FileWatcherConnector(watch_path="/tmp")
        assert connector.watch_path == Path("/tmp")
        assert connector.supported_extensions == [
            "pdf", "docx", "md", "markdown", "html", "htm", "txt", "json"
        ]
        assert connector.recursive is True
        assert connector.observer is None

    def test_init_custom_extensions(self):
        connector = FileWatcherConnector(
            watch_path="/tmp",
            supported_extensions=["pdf", "md"],
            recursive=False
        )
        assert connector.supported_extensions == ["pdf", "md"]
        assert connector.recursive is False

    @pytest.mark.asyncio
    async def test_get_next_event_timeout(self):
        connector = FileWatcherConnector(watch_path="/tmp")
        connector.event_queue = asyncio.Queue()
        result = await connector.get_next_event()
        assert result is None

    @pytest.mark.asyncio
    async def test_get_next_event_returns_event(self):
        connector = FileWatcherConnector(watch_path="/tmp")
        connector.event_queue = asyncio.Queue()
        test_event = {"event_type": "file_created", "path": "/tmp/test.md"}
        await connector.event_queue.put(test_event)
        result = await connector.get_next_event()
        assert result == test_event

    def test_start_creates_observer(self):
        connector = FileWatcherConnector(watch_path="/tmp")
        with patch("src.connectors.file_watcher.Observer") as mock_observer_class:
            mock_observer = MagicMock()
            mock_observer_class.return_value = mock_observer
            connector.start()
            mock_observer.schedule.assert_called_once()
            mock_observer.start.assert_called_once()

    def test_stop_stops_observer(self):
        connector = FileWatcherConnector(watch_path="/tmp")
        mock_observer = MagicMock()
        connector.observer = mock_observer
        connector.stop()
        mock_observer.stop.assert_called_once()
        mock_observer.join.assert_called_once()

    def test_stop_when_no_observer(self):
        connector = FileWatcherConnector(watch_path="/tmp")
        connector.observer = None
        connector.stop()


class TestFileWatcherHandler:
    """Tests for FileWatcherHandler"""

    def test_on_created_ignores_directories(self):
        handler = FileWatcherHandler(supported_extensions=["md"], event_queue=asyncio.Queue())
        mock_event = MagicMock()
        mock_event.is_directory = True
        handler.on_created(mock_event)

    def test_on_created_ignores_unsupported_extensions(self):
        handler = FileWatcherHandler(supported_extensions=["md"], event_queue=asyncio.Queue())
        mock_event = MagicMock()
        mock_event.is_directory = False
        mock_event.src_path = "/tmp/test.txt"
        with patch("pathlib.Path") as mock_path:
            mock_path_instance = MagicMock()
            mock_path_instance.suffix.lstrip.return_value = "txt"
            mock_path.return_value = mock_path_instance
            handler.on_created(mock_event)

    def test_on_modified_calls_on_created(self):
        handler = FileWatcherHandler(supported_extensions=["md"], event_queue=asyncio.Queue())
        with patch.object(handler, "on_created") as mock_on_created:
            mock_event = MagicMock()
            mock_event.is_directory = False
            handler.on_modified(mock_event)
            mock_on_created.assert_called_once_with(mock_event)


class TestGitHubConnector:
    """Tests for GitHubConnector"""

    def test_init_without_token(self):
        connector = GitHubConnector()
        assert connector.access_token is None
        assert "Authorization" not in connector.headers

    def test_init_with_token(self):
        connector = GitHubConnector(access_token="test_token")
        assert connector.access_token == "test_token"
        assert connector.headers["Authorization"] == "Bearer test_token"

    def test_get_date_threshold_daily(self):
        connector = GitHubConnector()
        threshold = connector._get_date_threshold("daily")
        assert threshold is not None

    def test_get_date_threshold_weekly(self):
        connector = GitHubConnector()
        threshold = connector._get_date_threshold("weekly")
        assert threshold is not None

    def test_get_date_threshold_monthly(self):
        connector = GitHubConnector()
        threshold = connector._get_date_threshold("monthly")
        assert threshold is not None

    @pytest.mark.asyncio
    async def test_fetch_trending_repos(self):
        connector = GitHubConnector()
        mock_response_data = {
            "items": [
                {
                    "id": 123,
                    "name": "test-repo",
                    "full_name": "user/test-repo",
                    "description": "A test repo",
                    "html_url": "https://github.com/user/test-repo",
                    "stargazers_count": 100,
                    "forks_count": 50,
                    "language": "Python",
                    "created_at": "2024-01-01T00:00:00Z",
                    "topics": ["ai", "ml"],
                    "license": {"name": "MIT"},
                }
            ]
        }
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            repos = await connector.fetch_trending_repos()
            assert len(repos) == 1
            assert repos[0]["name"] == "test-repo"
            assert repos[0]["stars"] == 100
            assert repos[0]["language"] == "Python"
            assert repos[0]["topics"] == ["ai", "ml"]
            assert repos[0]["license"] == "MIT"

    @pytest.mark.asyncio
    async def test_fetch_repo_details(self):
        connector = GitHubConnector()
        mock_repo_data = {
            "id": 123,
            "name": "test-repo",
            "full_name": "user/test-repo",
            "description": "A test repo",
            "html_url": "https://github.com/user/test-repo",
            "stargazers_count": 100,
            "forks_count": 50,
            "language": "Python",
            "created_at": "2024-01-01T00:00:00Z",
            "topics": ["ai"],
            "license": {"name": "MIT"},
        }
        mock_readme_data = {"content": "Q29udGVudA=="}  # base64 for "Content"

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_repo_response = MagicMock()
            mock_repo_response.json.return_value = mock_repo_data
            mock_repo_response.raise_for_status = MagicMock()
            mock_readme_response = MagicMock()
            mock_readme_response.status_code = 200
            mock_readme_response.json.return_value = mock_readme_data

            mock_client.get.side_effect = [mock_repo_response, mock_readme_response]
            mock_client_class.return_value.__aenter__.return_value = mock_client

            details = await connector.fetch_repo_details("user", "test-repo")
            assert details["name"] == "test-repo"
            assert details["stars"] == 100
            assert details["readme"] == "Content"


class TestCrawlConnector:
    """Tests for CrawlConnector"""

    def test_init_default_values(self):
        connector = CrawlConnector()
        assert connector.max_depth == 2
        assert connector.max_concurrent == 10
        assert connector.timeout == 30
        assert connector.visited_urls == set()

    def test_init_custom_values(self):
        connector = CrawlConnector(max_depth=5, max_concurrent=20, timeout=60)
        assert connector.max_depth == 5
        assert connector.max_concurrent == 20
        assert connector.timeout == 60

    def test_is_valid_link_valid(self):
        connector = CrawlConnector()
        assert connector._is_valid_link("https://example.com") is True
        assert connector._is_valid_link("http://example.com") is True

    def test_is_valid_link_invalid(self):
        connector = CrawlConnector()
        assert connector._is_valid_link("") is False
        assert connector._is_valid_link("#anchor") is False
        assert connector._is_valid_link("javascript:void(0)") is False

    def test_extract_metadata(self):
        connector = CrawlConnector()
        html = """
        <html>
            <meta name="description" content="Test description">
            <meta property="og:title" content="Test title">
        </html>
        """
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        metadata = connector._extract_metadata(soup)
        assert metadata["description"] == "Test description"
        assert metadata["og:title"] == "Test title"

    @pytest.mark.asyncio
    async def test_crawl_url_success(self):
        connector = CrawlConnector()
        html = """
        <html>
            <title>Test Page</title>
            <main><p>Content here</p></main>
            <a href="https://example.com">Link</a>
        </html>
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await connector.crawl_url("https://example.com")
            assert result["url"] == "https://example.com"
            assert result["status_code"] == 200
            assert result["title"] == "Test Page"
            assert "Content here" in result["content"]
            assert "https://example.com" in result["links"]

    @pytest.mark.asyncio
    async def test_crawl_url_error(self):
        connector = CrawlConnector()
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.side_effect = Exception("Network error")
            mock_client_class.return_value.__aenter__.return_value = mock_client

            result = await connector.crawl_url("https://example.com")
            assert result["url"] == "https://example.com"
            assert result["status_code"] == 0
            assert "error" in result


class TestDatabaseConnector:
    """Tests for DatabaseConnector"""

    def test_init_default_values(self):
        connector = DatabaseConnector(dsn="postgresql://localhost/test", table_name="articles")
        assert connector.dsn == "postgresql://localhost/test"
        assert connector.table_name == "articles"
        assert connector.id_column == "id"
        assert connector.batch_size == 100
        assert connector.pool is None

    def test_init_custom_values(self):
        connector = DatabaseConnector(
            dsn="postgresql://localhost/test",
            table_name="docs",
            id_column="doc_id",
            batch_size=50
        )
        assert connector.id_column == "doc_id"
        assert connector.batch_size == 50

    def test_transform_to_content_schema(self):
        connector = DatabaseConnector(
            dsn="postgresql://localhost/test",
            table_name="articles",
            id_column="article_id"
        )
        record = {
            "article_id": 123,
            "title": "Test Article",
            "author": "John Doe",
            "tags": ["ai", "ml"],
            "content": "This is the article content."
        }
        result = connector.transform_to_content_schema(record)
        assert result["id"] == "123"
        assert result["source_type"] == "db"
        assert result["source_url"] == "db://articles/123"
        assert result["metadata"]["title"] == "Test Article"
        assert result["metadata"]["author"] == "John Doe"
        assert result["content"]["text"] == "This is the article content."

    def test_transform_to_content_schema_with_body(self):
        connector = DatabaseConnector(dsn="postgresql://localhost/test", table_name="docs")
        record = {
            "id": 1,
            "title": "Doc",
            "body": "Document body content."
        }
        result = connector.transform_to_content_schema(record)
        assert result["content"]["text"] == "Document body content."

    @pytest.mark.asyncio
    async def test_get_records_count(self):
        connector = DatabaseConnector(dsn="postgresql://localhost/test", table_name="articles")
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = 42
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        connector.pool = mock_pool

        count = await connector.get_records_count()
        assert count == 42
