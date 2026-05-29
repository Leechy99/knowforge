"""
Graph Query - Neo4j graph database query operations
"""
from typing import Any

from src.storage.neo4j_client import Neo4jGraphStore


class GraphQuery:
    def __init__(self, neo4j_client: Neo4jGraphStore):
        self.neo4j = neo4j_client

    async def find_entities(
        self,
        entity_type: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if entity_type:
            cypher = """
            MATCH (e:Entity)
            WHERE e.type = $entity_type
            RETURN e.name as name, e.type as type
            LIMIT $limit
            """
            results = await self.neo4j.query_cypher(cypher, {"entity_type": entity_type, "limit": limit})
        else:
            cypher = """
            MATCH (e:Entity)
            RETURN e.name as name, e.type as type
            LIMIT $limit
            """
            results = await self.neo4j.query_cypher(cypher, {"limit": limit})
        return results

    async def find_documents_by_entity(
        self,
        entity_name: str,
    ) -> list[dict[str, Any]]:
        cypher = """
        MATCH (d:Document)-[:CONTAINS_ENTITY]->(e:Entity)
        WHERE e.name = $entity_name
        RETURN d.id as id, d.title as title, d.source_type as source_type
        LIMIT 50
        """
        results = await self.neo4j.query_cypher(cypher, {"entity_name": entity_name})
        return results

    async def find_related_entities(
        self,
        entity_name: str,
        max_depth: int = 2,
    ) -> list[dict[str, Any]]:
        cypher = f"""
        MATCH path = (e1:Entity {{name: $entity_name}})-[*1..{max_depth}]-(e2:Entity)
        WHERE e1 <> e2
        RETURN e1.name as source, e2.name as target, length(path) as distance
        LIMIT 100
        """
        results = await self.neo4j.query_cypher(cypher, {"entity_name": entity_name})
        return results

    async def get_document_graph(
        self,
        doc_id: str,
    ) -> dict[str, Any]:
        entities_cypher = """
        MATCH (d:Document {id: $doc_id})-[:CONTAINS_ENTITY]->(e:Entity)
        RETURN e.name as name, e.type as type
        """
        entities = await self.neo4j.query_cypher(entities_cypher, {"doc_id": doc_id})
        relations_cypher = """
        MATCH (d:Document {id: $doc_id})-[:CONTAINS_ENTITY]->(e1:Entity)
        MATCH (d:Document {id: $doc_id})-[:CONTAINS_ENTITY]->(e2:Entity)
        WHERE e1 <> e2
        RETURN e1.name as from, e2.name as to, "RELATED" as type
        LIMIT 50
        """
        relations = await self.neo4j.query_cypher(relations_cypher, {"doc_id": doc_id})
        return {
            "doc_id": doc_id,
            "entities": entities,
            "relations": relations,
        }
