"""
Vector Search - Pure vector similarity search operations
"""
from typing import Any

from src.processors.vectorizer import ContentVectorizer
from src.storage.qdrant_client import QdrantVectorStore


class VectorSearch:
    def __init__(
        self,
        vector_store: QdrantVectorStore,
        vectorizer: ContentVectorizer,
    ):
        self.vector_store = vector_store
        self.vectorizer = vectorizer

    def search(
        self,
        query: str,
        limit: int = 10,
        filters: dict[str, Any] | None = None,
        score_threshold: float | None = None,
    ) -> list[dict[str, Any]]:
        query_vector = self.vectorizer.encode_single(query)
        return self.vector_store.search(
            query_vector=query_vector,
            limit=limit,
            filters=filters,
            score_threshold=score_threshold,
        )

    def batch_search(
        self,
        queries: list[str],
        limit: int = 10,
        filters: dict[str, Any] | None = None,
    ) -> list[list[dict[str, Any]]]:
        query_vectors = self.vectorizer.encode(queries)
        results = []
        for query_vector in query_vectors:
            search_results = self.vector_store.search(
                query_vector=query_vector,
                limit=limit,
                filters=filters,
            )
            results.append(search_results)
        return results

    def search_by_vector(
        self,
        vector: list[float],
        limit: int = 10,
        filters: dict[str, Any] | None = None,
        score_threshold: float | None = None,
    ) -> list[dict[str, Any]]:
        return self.vector_store.search(
            query_vector=vector,
            limit=limit,
            filters=filters,
            score_threshold=score_threshold,
        )
