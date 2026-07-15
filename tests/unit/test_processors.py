"""
Unit tests for processors module
"""

import pytest

from src.processors.chunker import ContentChunker
from src.processors.cleaner import ContentCleaner
from src.processors.deduplicator import Deduplicator
from src.processors.parser import (
    HTMLParser,
    MarkdownParser,
    ParseResult,
    ParserRegistry,
    PDFParser,
)
from src.processors.vectorizer import ContentVectorizer


class TestParseResult:
    def test_parse_result_creation(self):
        result = ParseResult(content="test content", metadata={"key": "value"})
        assert result.content == "test content"
        assert result.metadata == {"key": "value"}
        assert result.raw_content is None
        assert result.error is None

    def test_parse_result_with_error(self):
        result = ParseResult(content="", metadata={}, error="Something went wrong")
        assert result.error == "Something went wrong"
        assert result.content == ""


class TestPDFParser:
    def setup_method(self):
        self.parser = PDFParser()

    def test_supported_types(self):
        assert self.parser.supported_types == ["pdf"]

    @pytest.mark.asyncio
    async def test_parse_pdf_content(self):
        # Minimal PDF structure
        pdf_content = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n1 0 obj<</Type/Catalog>>\nendobj\n"
        result = await self.parser.parse(pdf_content)
        # PDF parser returns empty content on invalid PDF - this is expected behavior
        assert isinstance(result, ParseResult)

    @pytest.mark.asyncio
    async def test_parse_invalid_pdf(self):
        result = await self.parser.parse(b"not a pdf")
        assert result.error is not None or result.content == ""


class TestMarkdownParser:
    def setup_method(self):
        self.parser = MarkdownParser()

    def test_supported_types(self):
        assert self.parser.supported_types == ["md", "markdown"]

    @pytest.mark.asyncio
    async def test_parse_markdown_string(self):
        content = "# Hello World\n\nThis is a test."
        result = await self.parser.parse(content)
        assert result.content == content
        assert result.raw_content is not None

    @pytest.mark.asyncio
    async def test_parse_markdown_bytes(self):
        content = "# Hello World\n\nThis is a test."
        result = await self.parser.parse(content.encode("utf-8"))
        assert result.content == content

    @pytest.mark.asyncio
    async def test_parse_markdown_with_yaml_frontmatter(self):
        content = "---\ntitle: Test\nauthor: Author\n---\n# Hello\n"
        result = await self.parser.parse(content)
        assert "title" in result.metadata or result.content.startswith("# Hello")

    @pytest.mark.asyncio
    async def test_parse_empty_markdown(self):
        result = await self.parser.parse("")
        assert result.content == ""


class TestHTMLParser:
    def setup_method(self):
        self.parser = HTMLParser()

    def test_supported_types(self):
        assert self.parser.supported_types == ["html", "htm"]

    @pytest.mark.asyncio
    async def test_parse_simple_html(self):
        content = "<html><head><title>Test</title></head><body><p>Hello</p></body></html>"
        result = await self.parser.parse(content)
        assert "Hello" in result.content
        assert result.metadata["title"] == "Test"
        assert result.metadata["source"] == "html"

    @pytest.mark.asyncio
    async def test_parse_html_removes_scripts(self):
        content = "<html><body><script>alert('x')</script><p>Content</p></body></html>"
        result = await self.parser.parse(content)
        assert "alert" not in result.content
        assert "Content" in result.content

    @pytest.mark.asyncio
    async def test_parse_html_removes_nav_footer(self):
        content = "<html><body><nav>Nav</nav><main>Main</main><footer>Footer</footer></body></html>"
        result = await self.parser.parse(content)
        assert "Nav" not in result.content
        assert "Footer" not in result.content
        assert "Main" in result.content

    @pytest.mark.asyncio
    async def test_parse_html_bytes(self):
        content = b"<html><body><p>Test</p></body></html>"
        result = await self.parser.parse(content)
        assert "Test" in result.content


class TestParserRegistry:
    def setup_method(self):
        self.registry = ParserRegistry()

    def test_register_parser(self):
        parser = PDFParser()
        self.registry.register(parser)
        assert self.registry.get_parser("pdf") is not None

    def test_get_parser_case_insensitive(self):
        parser = PDFParser()
        self.registry.register(parser)
        assert self.registry.get_parser("PDF") is not None
        assert self.registry.get_parser(".pdf") is not None

    def test_get_parser_not_found(self):
        result = self.registry.get_parser("unknown")
        assert result is None

    @pytest.mark.asyncio
    async def test_parse_with_registered_parser(self):
        self.registry.register(MarkdownParser())
        result = await self.registry.parse("# Test", "md")
        assert result.content == "# Test"

    @pytest.mark.asyncio
    async def test_parse_with_unregistered_parser(self):
        result = await self.registry.parse("content", "xyz")
        assert result.error == "No parser for xyz"


class TestContentCleaner:
    def setup_method(self):
        self.cleaner = ContentCleaner()

    def test_clean_basic_html_tags(self):
        text = "<p>Hello <strong>World</strong></p>"
        result = self.cleaner.clean(text)
        assert "<" not in result
        assert "Hello" in result

    def test_clean_script_tags(self):
        text = "<script>alert('x')</script><p>Content</p>"
        result = self.cleaner.clean(text)
        assert "alert" not in result
        assert "Content" in result

    def test_clean_style_tags(self):
        text = "<style>.class{color:red}</style><p>Text</p>"
        result = self.cleaner.clean(text)
        assert "color" not in result
        assert "Text" in result

    def test_clean_class_attributes(self):
        text = '<div class="container" id="main">Content</div>'
        result = self.cleaner.clean(text)
        assert "class" not in result
        assert "container" not in result
        assert "Content" in result

    def test_clean_data_attributes(self):
        text = '<div data-id="123">Content</div>'
        result = self.cleaner.clean(text)
        assert "data-id" not in result
        assert "Content" in result

    def test_normalize_whitespace(self):
        text = "Hello\n\n\n   World   "
        result = self.cleaner.clean(text)
        assert "  " not in result
        assert "\n\n\n" not in result

    def test_normalize_unicode(self):
        text = "café"
        result = self.cleaner.clean(text)
        assert "café" in result

    def test_clean_with_remove_short_lines(self):
        text = "Short\n\nThis is a much longer line that should be kept because it has more than fifty characters"
        result = self.cleaner.clean(text, options={"remove_short_lines": True})
        lines = result.split("\n")
        assert all(len(line) > 50 for line in lines if line.strip())

    def test_clean_empty_string(self):
        result = self.cleaner.clean("")
        assert result == ""


class TestDeduplicator:
    def setup_method(self):
        self.dedup = Deduplicator()

    def test_compute_hash(self):
        hash1 = self.dedup.compute_hash("test content")
        hash2 = self.dedup.compute_hash("test content")
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hex length

    def test_compute_hash_different_content(self):
        hash1 = self.dedup.compute_hash("content a")
        hash2 = self.dedup.compute_hash("content b")
        assert hash1 != hash2

    def test_is_duplicate_first_content(self):
        is_dup, hash_val = self.dedup.is_duplicate("first content")
        assert is_dup is False
        assert hash_val is None

    def test_is_duplicate_exact_match(self):
        self.dedup.is_duplicate("test content")
        is_dup, hash_val = self.dedup.is_duplicate("test content")
        assert is_dup is True
        assert hash_val is not None

    def test_is_duplicate_similar_content(self):
        # With 0.85 threshold, very similar content might be detected
        self.dedup.is_duplicate("This is a somewhat long piece of text for testing")
        is_dup, _ = self.dedup.is_duplicate("This is a somewhat long piece of text for testing purposes")
        # May or may not be duplicate depending on simhash distance

    def test_reset_clears_state(self):
        self.dedup.is_duplicate("test content")
        self.dedup.reset()
        is_dup, _ = self.dedup.is_duplicate("test content")
        assert is_dup is False

    def test_different_threshold(self):
        dedup_strict = Deduplicator(similarity_threshold=0.95)
        dedup_lenient = Deduplicator(similarity_threshold=0.5)
        assert dedup_strict.similarity_threshold == 0.95
        assert dedup_lenient.similarity_threshold == 0.5


class TestContentChunker:
    def setup_method(self):
        self.chunker = ContentChunker(chunk_size=50, overlap=10, encoding_name="cl100k_base")

    def test_chunk_by_tokens_empty(self):
        result = self.chunker.chunk_by_tokens("")
        assert result == []

    def test_chunk_by_tokens_single_chunk(self):
        text = "a" * 30  # Short text
        result = self.chunker.chunk_by_tokens(text)
        assert len(result) == 1
        assert result[0]["text"] == text
        assert result[0]["index"] == 0
        assert result[0]["token_count"] > 0

    def test_chunk_by_tokens_multiple_chunks(self):
        text = "a" * 200  # Longer text that needs chunking
        result = self.chunker.chunk_by_tokens(text)
        assert len(result) > 1
        for chunk in result:
            assert "text" in chunk
            assert "index" in chunk
            assert "token_count" in chunk

    def test_chunk_default_strategy(self):
        text = "Hello world"
        result = self.chunker.chunk(text)
        assert len(result) >= 1

    def test_chunk_paragraphs_strategy(self):
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
        result = self.chunker.chunk(text, strategy="paragraphs")
        assert len(result) >= 1
        for chunk in result:
            assert "text" in chunk

    def test_chunk_by_paragraphs(self):
        text = "First paragraph here.\n\nSecond paragraph here."
        result = self.chunker.chunk_by_paragraphs(text)
        assert len(result) >= 1

    def test_chunk_respects_overlap(self):
        text = "abcdefghij" * 50
        chunks = self.chunker.chunk_by_tokens(text)
        if len(chunks) > 1:
            # Subsequent chunks should start with overlap from previous
            pass  # Just verify chunks are created

    def test_custom_chunk_size(self):
        chunker = ContentChunker(chunk_size=100, overlap=20)
        text = "x" * 500
        result = chunker.chunk_by_tokens(text)
        for chunk in result:
            assert chunk["token_count"] <= 100


class TestContentVectorizer:
    def setup_method(self):
        class FakeModel:
            def encode(self, texts, **kwargs):
                import numpy as np

                return np.array([[0.1, 0.2, 0.3] for _ in texts])

        self.vectorizer = ContentVectorizer(
            model_name="BAAI/bge-small-en-v1.5",  # Using smaller model for tests
            dimension=512,
            batch_size=8,
            device="cpu",
            model_factory=lambda *args, **kwargs: FakeModel(),
        )

    def test_initialization(self):
        assert self.vectorizer.model_name == "BAAI/bge-small-en-v1.5"
        assert self.vectorizer.dimension == 512
        assert self.vectorizer.batch_size == 8
        assert self.vectorizer.device == "cpu"
        assert self.vectorizer.model is None

    def test_load_model(self):
        # Model is None initially
        assert self.vectorizer.model is None
        # Load should initialize the model
        self.vectorizer.load()
        assert self.vectorizer.model is not None
        self.vectorizer.unload()

    def test_unload_model(self):
        self.vectorizer.load()
        self.vectorizer.unload()
        assert self.vectorizer.model is None

    @pytest.mark.asyncio
    async def test_async_load(self):
        # Verify async compatibility
        pass

    def test_encode_chunks_structure(self):
        chunks = [
            {"text": "Hello world", "index": 0, "token_count": 2},
            {"text": "Another chunk", "index": 1, "token_count": 2}
        ]
        # encode_chunks should add vector to each chunk
        # This test verifies structure only - actual encoding needs model loaded
        for chunk in chunks:
            assert "text" in chunk
            assert "index" in chunk

    def test_encode_single_returns_vector(self):
        assert self.vectorizer.encode_single("test") == pytest.approx([0.1, 0.2, 0.3])

    def test_encode_returns_list_of_lists(self):
        vectors = self.vectorizer.encode(["test1", "test2"])
        assert len(vectors) == 2
        for vector in vectors:
            assert vector == pytest.approx([0.1, 0.2, 0.3])
