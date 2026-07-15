"""
Content Parser - Extract content from various formats
"""
import io
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import markdown_it

from src.utils.file_utils import normalize_extension


@dataclass
class ParseResult:
    content: str
    metadata: dict[str, Any]
    raw_content: bytes | None = None
    error: str | None = None


class BaseParser(ABC):
    @abstractmethod
    async def parse(self, content: bytes | str, **kwargs: Any) -> ParseResult:
        pass

    @property
    @abstractmethod
    def supported_types(self) -> list[str]:
        pass


class PDFParser(BaseParser):
    async def parse(self, content: bytes | str, **kwargs: Any) -> ParseResult:
        try:
            import pdfplumber

            if isinstance(content, str):
                content = content.encode()
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                text_parts: list[str] = []
                metadata: dict[str, Any] = {}
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


_markdown_parser = markdown_it.MarkdownIt()


class MarkdownParser(BaseParser):
    async def parse(self, content: str | bytes, **kwargs: Any) -> ParseResult:
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        _markdown_parser.parse(content)
        metadata: dict[str, Any] = {}
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                try:
                    import yaml

                    metadata = yaml.safe_load(parts[1]) or {}
                    content = parts[2].lstrip()
                except Exception:
                    content = parts[2].lstrip()
        return ParseResult(
            content=content,
            metadata=metadata,
            raw_content=content.encode() if isinstance(content, str) else content,
        )

    @property
    def supported_types(self) -> list[str]:
        return ["md", "markdown"]


_html_parser = "lxml"


class HTMLParser(BaseParser):
    async def parse(self, content: str | bytes, **kwargs: Any) -> ParseResult:
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="ignore")
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, _html_parser)
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
    def __init__(self) -> None:
        self.parsers: dict[str, BaseParser] = {}

    def register(self, parser: BaseParser) -> None:
        for ext in parser.supported_types:
            self.parsers[ext] = parser

    def get_parser(self, file_ext: str) -> BaseParser | None:
        return self.parsers.get(normalize_extension(file_ext))

    async def parse(
        self,
        content: bytes | str,
        file_ext: str,
        **kwargs: Any,
    ) -> ParseResult:
        parser = self.get_parser(file_ext)
        if not parser:
            return ParseResult(content="", metadata={}, error=f"No parser for {file_ext}")
        return await parser.parse(content, **kwargs)
