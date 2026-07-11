"""
QA API Routes
"""
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field


router = APIRouter()


class QARequest(BaseModel):
    question: str = Field(min_length=1, max_length=8_000)
    filters: dict[str, Any] | None = None
    mode: Literal["vector", "graph", "hybrid"] = "hybrid"
    limit: int = Field(default=10, ge=1, le=100)


class QAResponse(BaseModel):
    answer: str
    sources: list[dict[str, Any]]
    mode: str


@router.post("/qa", response_model=QAResponse)
async def ask_question(request: Request, payload: QARequest):
    qa_service = getattr(request.app.state, "qa_service", None)
    if qa_service is None:
        raise HTTPException(status_code=503, detail="QA service unavailable")

    result = await qa_service.ask(
        question=payload.question,
        filters=payload.filters,
        mode=payload.mode,
        limit=payload.limit,
    )
    return QAResponse(**result)
