"""
Content Vectorizer - Generate embeddings using BGE-large-zh
"""
from collections.abc import Callable
from typing import Any, cast


class ContentVectorizer:
    def __init__(
        self,
        model_name: str = "BAAI/bge-large-zh-v1.5",
        dimension: int = 1024,
        batch_size: int = 32,
        device: str = "cpu",
        model_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.model_name = model_name
        self.dimension = dimension
        self.batch_size = batch_size
        self.device = device
        self.model_factory = model_factory
        self.model: Any | None = None

    def load(self) -> None:
        if self.model is None:
            if self.model_factory is None:
                from sentence_transformers import SentenceTransformer

                self.model_factory = SentenceTransformer
            self.model = self.model_factory(self.model_name, device=self.device)

    def unload(self) -> None:
        if self.model:
            del self.model
            self.model = None

    def encode(self, texts: list[str], **kwargs: Any) -> list[list[float]]:
        if self.model is None:
            self.load()
        if self.model is None:
            raise RuntimeError("Embedding model failed to load")
        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
            **kwargs,
        )
        return cast(list[list[float]], embeddings.tolist())

    def encode_single(self, text: str) -> list[float]:
        return self.encode([text])[0]

    def encode_chunks(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        texts = [chunk["text"] for chunk in chunks]
        embeddings = self.encode(texts)
        for chunk, embedding in zip(chunks, embeddings):
            chunk["vector"] = embedding
        return chunks
