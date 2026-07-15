"""
Export Module - Export documents in AI-Ready formats
"""
import json
from typing import Any


class ExportFormatter:
    def format(self, document: dict[str, Any]) -> str:
        raise NotImplementedError


class MarkdownExporter(ExportFormatter):
    def format(self, document: dict[str, Any]) -> str:
        meta = document.get("metadata", {})
        content = document.get("content", {})
        doc_id = document.get("id", "")
        lines = [
            "---",
            f"id: {doc_id}",
            f"title: {meta.get('title', '')}",
            f"source: {document.get('source_type', '')}",
            f"url: {document.get('source_url', '')}",
            f"tags: [{', '.join(meta.get('tags', []))}]",
            "---",
            "",
            f"# {meta.get('title', 'Untitled')}",
            "",
            "## Summary",
            content.get("text", "")[:500] + "..." if len(content.get("text", "")) > 500 else content.get("text", ""),
        ]
        entities = content.get("entities", [])
        if entities:
            lines.extend(["", "## Entities", ""])
            for entity in entities:
                lines.append(f"- **{entity.get('name', '')}** ({entity.get('type', '')})")
        return "\n".join(lines)


class JSONExporter(ExportFormatter):
    def format(self, document: dict[str, Any]) -> str:
        export_doc = {
            "document": {
                "id": document.get("id"),
                "meta": {
                    "title": document.get("metadata", {}).get("title"),
                    "source": document.get("source_type"),
                    "source_url": document.get("source_url"),
                    "tags": document.get("metadata", {}).get("tags", []),
                },
                "content": {
                    "text": document.get("content", {}).get("text", ""),
                    "entities": document.get("content", {}).get("entities", []),
                },
                "exported_at": document.get("processed_at"),
            }
        }
        return json.dumps(export_doc, indent=2, ensure_ascii=False)


class GraphExporter(ExportFormatter):
    def format(self, document: dict[str, Any]) -> str:
        entities = document.get("content", {}).get("entities", [])
        relations = document.get("content", {}).get("relations", [])
        nodes = []
        edges = []
        entity_id_map = {}
        for i, entity in enumerate(entities):
            node_id = f"n{i+1}"
            entity_id_map[entity.get("name", "")] = node_id
            nodes.append({
                "id": node_id,
                "type": entity.get("type", "UNKNOWN"),
                "name": entity.get("name", ""),
                "props": {},
            })
        for relation in relations:
            edges.append({
                "from": entity_id_map.get(relation.get("from", ""), "unknown"),
                "to": entity_id_map.get(relation.get("to", ""), "unknown"),
                "type": relation.get("type", "RELATED_TO"),
            })
        graph = {"graph": {"nodes": nodes, "edges": edges}}
        return json.dumps(graph, indent=2, ensure_ascii=False)


class ExportService:
    def __init__(self) -> None:
        self.formatters: dict[str, ExportFormatter] = {
            "markdown": MarkdownExporter(),
            "json": JSONExporter(),
            "graph": GraphExporter(),
        }

    def export(
        self,
        documents: list[dict[str, Any]],
        format: str = "markdown",
    ) -> str:
        formatter = self.formatters.get(format.lower())
        if not formatter:
            raise ValueError(f"Unsupported export format: {format}")
        if format.lower() == "json":
            return json.dumps(
                [json.loads(formatter.format(doc)) for doc in documents],
                indent=2,
                ensure_ascii=False,
            )
        return "\n\n---\n\n".join([formatter.format(doc) for doc in documents])

    def export_single(
        self,
        document: dict[str, Any],
        format: str = "markdown",
    ) -> str:
        formatter = self.formatters.get(format.lower())
        if not formatter:
            raise ValueError(f"Unsupported export format: {format}")
        return formatter.format(document)
