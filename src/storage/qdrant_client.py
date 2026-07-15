"""
Qdrant Vector Store Client
"""
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams


class QdrantVectorStore:
    COLLECTION_NAME = "ai_knowledge_base"

    def __init__(
        self,
        url: str = "http://localhost:6333",
        dimension: int = 1024,
        collection_name: str = COLLECTION_NAME,
    ) -> None:
        self.client: Any = QdrantClient(url=url)
        self.dimension = dimension
        self.collection_name = collection_name

    def create_collection(self, recreate: bool = False) -> None:
        if recreate:
            self.client.delete_collection(self.collection_name)
        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=self.dimension, distance=Distance.COSINE),
        )

    def health_check(self) -> None:
        self.client.get_collections()

    def upsert_vectors(self, points: list[dict[str, Any]]) -> None:
        self.client.upsert(
            collection_name=self.collection_name,
            points=[
                PointStruct(
                    id=p["id"],
                    vector=p["vector"],
                    payload=p.get("payload", {}),
                )
                for p in points
            ],
        )

    def search(
        self,
        query_vector: list[float],
        limit: int = 10,
        filters: dict[str, Any] | None = None,
        score_threshold: float | None = None,
    ) -> list[dict[str, Any]]:
        results = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            limit=limit,
            query_filter=filters,
            score_threshold=score_threshold,
        )
        return [
            {"id": str(r.id), "score": r.score, "payload": r.payload}
            for r in results
        ]
