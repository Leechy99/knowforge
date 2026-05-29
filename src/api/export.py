"""
Export API Routes
"""
import inspect
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi import Request
from fastapi.responses import Response

from src.query.export import ExportService


router = APIRouter()
export_service = ExportService()


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _load_documents(
    document_service: Any,
    ids: str | None,
    query: str | None,
) -> list[dict[str, Any]]:
    if ids:
        doc_ids = [doc_id.strip() for doc_id in ids.split(",") if doc_id.strip()]
        if hasattr(document_service, "get_documents"):
            return await _maybe_await(document_service.get_documents(doc_ids))
        if hasattr(document_service, "get_document"):
            documents = [
                await _maybe_await(document_service.get_document(doc_id))
                for doc_id in doc_ids
            ]
            return [doc for doc in documents if doc]

    if query and hasattr(document_service, "search_documents"):
        return await _maybe_await(document_service.search_documents(query))

    if hasattr(document_service, "list_documents"):
        return await _maybe_await(document_service.list_documents())

    return []


@router.get("/export")
async def export_documents(
    request: Request,
    format: str = Query("markdown", enum=["markdown", "json", "graph"]),
    ids: str | None = None,
    query: str | None = None,
):
    document_service = getattr(request.app.state, "document_service", None)
    if document_service is None:
        raise HTTPException(status_code=503, detail="Document service unavailable")
    documents = await _load_documents(document_service, ids, query)
    if not documents:
        raise HTTPException(status_code=404, detail="No documents found")
    content = export_service.export(documents, format=format)
    media_types = {
        "markdown": "text/markdown",
        "json": "application/json",
        "graph": "application/json",
    }
    return Response(
        content=content,
        media_type=media_types.get(format, "text/plain"),
        headers={"Content-Disposition": f"attachment; filename=export.{format}"},
    )
