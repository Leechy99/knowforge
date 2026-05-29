"""
Failures API Routes
"""
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.learning.failure_tracker import FeedbackAction, ProcessingFailure


router = APIRouter()


class FailureResponse(BaseModel):
    id: str
    source_type: str
    source_identifier: str
    failure_type: str
    error_message: str
    status: str
    created_at: str


def _to_response(failure: ProcessingFailure) -> FailureResponse:
    return FailureResponse(
        id=failure.id,
        source_type=failure.source_type,
        source_identifier=failure.source_identifier,
        failure_type=failure.failure_type.value,
        error_message=failure.error_message,
        status=failure.status.value,
        created_at=failure.created_at.isoformat(),
    )


@router.get("/failures", response_model=list[FailureResponse])
async def list_failures(request: Request, limit: int = 20):
    tracker = getattr(request.app.state, "failure_tracker", None)
    if tracker is None:
        raise HTTPException(status_code=503, detail="Failure tracker unavailable")
    return [_to_response(failure) for failure in tracker.get_pending_failures(limit=limit)]


@router.get("/failures/{failure_id}", response_model=FailureResponse)
async def get_failure(request: Request, failure_id: str):
    tracker = getattr(request.app.state, "failure_tracker", None)
    if tracker is None:
        raise HTTPException(status_code=503, detail="Failure tracker unavailable")
    failure = tracker.failures.get(failure_id)
    if failure is None:
        raise HTTPException(status_code=404, detail="Failure not found")
    return _to_response(failure)


class FeedbackRequest(BaseModel):
    action: FeedbackAction
    instructions: str | None = None


@router.post("/failures/{failure_id}/feedback")
async def submit_feedback(request: Request, failure_id: str, payload: FeedbackRequest):
    tracker = getattr(request.app.state, "failure_tracker", None)
    if tracker is None:
        raise HTTPException(status_code=503, detail="Failure tracker unavailable")
    try:
        tracker.apply_feedback(failure_id, payload.action, payload.instructions)
    except ValueError:
        raise HTTPException(status_code=404, detail="Failure not found") from None
    return {"status": "ok"}
