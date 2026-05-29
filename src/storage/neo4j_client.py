"""
Neo4j Graph Store Client
"""
import inspect
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver


class Neo4jGraphStore:
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
        self.driver: AsyncDriver = AsyncGraphDatabase.driver(uri, auth=(user, password))

    async def close(self):
        result = self.driver.close()
        if inspect.isawaitable(result):
            await result

    async def create_document_graph(self, doc: dict[str, Any]):
        entities = doc.get("content", {}).get("entities", [])
        async with self.driver.session() as session:
            await session.run(
                """
                CREATE (d:Document {
                    id: $id, title: $title, source_type: $source_type,
                    source_url: $source_url, created_at: datetime($created_at)
                })
                """,
                id=doc["id"],
                title=doc.get("metadata", {}).get("title", ""),
                source_type=doc["source_type"],
                source_url=doc.get("source_url", ""),
                created_at=doc.get("created_at", ""),
            )

            for entity in entities:
                await session.run(
                    """
                    MATCH (d:Document {id: $doc_id})
                    MERGE (e:Entity {name: $name, type: $type})
                    CREATE (d)-[:CONTAINS_ENTITY]->(e)
                    """,
                    doc_id=doc["id"],
                    name=entity["name"],
                    type=entity.get("type", "UNKNOWN"),
                )
            await session.run(
                "MATCH (d:Document {id: $doc_id}) RETURN d",
                doc_id=doc["id"],
            )

    async def query_cypher(self, cypher: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        async with self.driver.session() as session:
            result = await session.run(cypher, params or {})
            records = []
            async_iter = result.__aiter__()
            if hasattr(async_iter, "__anext__"):
                async for record in result:
                    records.append(dict(record))
            else:
                for record in async_iter:
                    records.append(dict(record))
            return records
