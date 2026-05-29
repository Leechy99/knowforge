"""
Search API Routes
"""
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel


router = APIRouter()


class SearchRequest(BaseModel):
    query: str
    limit: int = 20
    filters: dict[str, Any] | None = None


class SearchResponse(BaseModel):
    results: list[dict[str, Any]]
    total: int


@router.post("/search", response_model=SearchResponse)
async def search_documents(request: Request, payload: SearchRequest):
    search_service = getattr(request.app.state, "search_service", None)
    if search_service is None:
        raise HTTPException(status_code=503, detail="Search service unavailable")

    results = search_service.search(
        query=payload.query,
        limit=payload.limit,
        filters=payload.filters,
    )
    return SearchResponse(results=results, total=len(results))
