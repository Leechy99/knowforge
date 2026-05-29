"""
Database Connector - Import data from existing databases
"""
import asyncio
import inspect
from typing import Any, AsyncGenerator

import asyncpg


class DatabaseConnector:
    def __init__(
        self,
        dsn: str,
        table_name: str,
        id_column: str = "id",
        batch_size: int = 100,
    ):
        self.dsn = dsn
        self.table_name = table_name
        self.id_column = id_column
        self.batch_size = batch_size
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=10)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def stream_records(
        self,
        query: str | None = None,
        columns: list[str] | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        if not self.pool:
            await self.connect()
        query = query or f"SELECT * FROM {self.table_name}"
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor(query):
                    if columns:
                        yield {k: v for k, v in zip(columns, record)}
                    else:
                        yield dict(record)

    async def get_records_count(self) -> int:
        if not self.pool:
            await self.connect()
        acquired = self.pool.acquire()
        if not hasattr(acquired, "__aenter__") and inspect.isawaitable(acquired):
            acquired = await acquired
        async with acquired as conn:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {self.table_name}")
            return count

    def transform_to_content_schema(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(record.get(self.id_column, "")),
            "source_type": "db",
            "source_url": f"db://{self.table_name}/{record.get(self.id_column)}",
            "content_type": "article",
            "metadata": {
                "title": record.get("title", ""),
                "author": record.get("author", ""),
                "tags": record.get("tags", []),
            },
            "content": {
                "text": record.get("content", "") or record.get("body", ""),
            },
        }
