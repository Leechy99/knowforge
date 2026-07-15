"""
RAG QA - Retrieval Augmented Generation for Question Answering
"""
from typing import Any

from src.processors.vectorizer import ContentVectorizer
from src.storage.neo4j_client import Neo4jGraphStore
from src.storage.postgres_client import PostgresClient
from src.storage.qdrant_client import QdrantVectorStore


class RAGQA:
    def __init__(
        self,
        vector_store: QdrantVectorStore,
        postgres_client: PostgresClient,
        neo4j_client: Neo4jGraphStore,
        vectorizer: ContentVectorizer,
        llm_api_url: str | None = None,
    ):
        self.vector_store = vector_store
        self.postgres = postgres_client
        self.neo4j = neo4j_client
        self.vectorizer = vectorizer
        self.llm_api_url = llm_api_url

    async def ask(
        self,
        question: str,
        filters: dict[str, Any] | None = None,
        mode: str = "hybrid",
        limit: int = 10,
    ) -> dict[str, Any]:
        question_vector = self.vectorizer.encode_single(question)
        retrieved_docs = []
        if mode in ("vector", "hybrid"):
            vector_results = self.vector_store.search(
                query_vector=question_vector,
                limit=limit,
                filters=filters,
            )
            retrieved_docs.extend(vector_results)
        if mode in ("graph", "hybrid"):
            graph_results = await self._query_graph(question)
            retrieved_docs.extend(graph_results)
        seen_ids = set()
        unique_docs = []
        for doc in retrieved_docs:
            doc_id = doc.get("id") or doc.get("payload", {}).get("id")
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                unique_docs.append(doc)
        answer = await self._generate_answer(question, unique_docs[:limit])
        return {
            "answer": answer,
            "sources": [
                {
                    "id": doc.get("id") or doc.get("payload", {}).get("id"),
                    "score": doc.get("score", 0),
                    "content": doc.get("payload", {}).get("content_text", ""),
                }
                for doc in unique_docs[:limit]
            ],
            "mode": mode,
        }

    async def _query_graph(self, question: str) -> list[dict[str, Any]]:
        keywords = question.lower().split()
        cypher = """
        MATCH (d:Document)-[:CONTAINS_ENTITY]->(e:Entity)
        WHERE e.name CONTAINS $keyword
        RETURN d, collect(e.name) as entities
        LIMIT 10
        """
        results = []
        for keyword in keywords[:3]:
            graph_results = await self.neo4j.query_cypher(cypher, {"keyword": keyword})
            results.extend(graph_results)
        return results

    async def _generate_answer(
        self,
        question: str,
        context_docs: list[dict[str, Any]],
    ) -> str:
        return f"Based on {len(context_docs)} relevant documents, the answer would be generated here."
