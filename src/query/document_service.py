"""
Document Service - Query-facing document access adapter.
"""
from typing import Any

from src.storage.postgres_client import PostgresClient


class DocumentService:
    """Load documents for API use through the metadata storage client."""

    def __init__(self, postgres_client: PostgresClient):
        self.postgres = postgres_client

    async def get_document(self, doc_id: str) -> dict[str, Any] | None:
        return await self.postgres.get_document(doc_id)

    async def get_documents(self, doc_ids: list[str]) -> list[dict[str, Any]]:
        return await self.postgres.get_documents(doc_ids)

    async def list_documents(self, limit: int = 100) -> list[dict[str, Any]]:
        return await self.postgres.list_documents(limit=limit)

    async def search_documents(
        self,
        query: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return await self.postgres.search_documents(query, limit=limit)
