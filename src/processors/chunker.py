"""
Content Chunking - Split content into manageable chunks
"""
from typing import Any

import tiktoken


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
        if not tokens:
            return []
        if len(tokens) <= self.chunk_size and len(text) > self.chunk_size:
            return self._chunk_by_char_window(text)
        chunks: list[dict[str, Any]] = []
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

    def _chunk_by_char_window(self, text: str) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        step = self.chunk_size - self.overlap
        for i in range(0, len(text), step):
            chunk_text = text[i : i + self.chunk_size]
            chunks.append({
                "text": chunk_text,
                "index": len(chunks),
                "token_count": len(self.encoding.encode(chunk_text)),
            })
            if i + self.chunk_size >= len(text):
                break
        return chunks

    def chunk(self, text: str, strategy: str = "tokens") -> list[dict[str, Any]]:
        if strategy == "paragraphs":
            return self.chunk_by_paragraphs(text)
        return self.chunk_by_tokens(text)

    def chunk_by_paragraphs(self, text: str, max_chunk_size: int = 1024) -> list[dict[str, Any]]:
        paragraphs = text.split("\n\n")
        chunks: list[dict[str, Any]] = []
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
