"""
Failure Tracker - Track and manage processing failures
"""
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from src.utils.time import utc_now


class FailureType(StrEnum):
    NETWORK_ERROR = "network_error"
    ENCODING_ERROR = "encoding_error"
    PARSE_ERROR = "parse_error"
    UNKNOWN_FORMAT = "unknown_format"
    SIZE_LIMIT = "size_limit"
    QUALITY_TOO_LOW = "quality_too_low"


class FailureStatus(StrEnum):
    PENDING = "pending"
    RETRY_SCHEDULED = "retry_scheduled"
    SKIPPED = "skipped"
    RESOLVED = "resolved"
    IGNORED = "ignored"


class FeedbackAction(StrEnum):
    RETRY = "retry"
    SKIP = "skip"
    USER_PROCESSED = "user_processed"
    RESOLVED = "user_processed"
    NEW_STRATEGY = "new_strategy"


class ProcessingFailure(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_type: str
    source_identifier: str
    source_hash: str | None = None
    failure_type: FailureType
    error_code: str | None = None
    error_message: str
    stack_trace: str | None = None
    attempts: int = 0
    last_attempt_at: datetime | None = None
    tried_strategies: list[str] = Field(default_factory=list)
    content_preview: str | None = None
    content_size: int | None = None
    status: FailureStatus = FailureStatus.PENDING
    feedback: FeedbackAction | None = None
    user_instructions: str | None = None
    feedback_by: str | None = None
    feedback_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class FailureTracker:
    """Failure tracker with bounded storage and automatic cleanup."""

    DEFAULT_MAX_SIZE = 10_000
    DEFAULT_TTL_HOURS = 24

    def __init__(
        self,
        postgres_dsn: str | None = None,
        max_size: int = DEFAULT_MAX_SIZE,
        ttl_hours: int = DEFAULT_TTL_HOURS,
    ) -> None:
        self.failures: OrderedDict[str, ProcessingFailure] = OrderedDict()
        self.postgres_dsn = postgres_dsn
        self.max_size = max_size
        self.ttl = timedelta(hours=ttl_hours)

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if over capacity."""
        while len(self.failures) >= self.max_size:
            self.failures.popitem(last=False)

    def _cleanup_expired(self) -> int:
        """Remove failures older than TTL. Returns count of removed items."""
        cutoff = utc_now() - self.ttl
        expired = [
            fid for fid, f in self.failures.items()
            if f.updated_at < cutoff and f.status in (
                FailureStatus.RESOLVED, FailureStatus.SKIPPED, FailureStatus.IGNORED
            )
        ]
        for fid in expired:
            self.failures.pop(fid, None)
        return len(expired)

    def record_failure(
        self,
        source_type: str,
        source_identifier: str,
        failure_type: FailureType,
        error_message: str,
        content_preview: str | None = None,
        content_size: int | None = None,
    ) -> ProcessingFailure:
        import hashlib
        source_hash = hashlib.md5(f"{source_identifier}{content_preview or ''}".encode()).hexdigest()[:16]
        failure = ProcessingFailure(
            source_type=source_type,
            source_identifier=source_identifier,
            source_hash=source_hash,
            failure_type=failure_type,
            error_message=error_message,
            content_preview=content_preview[:1024] if content_preview else None,
            content_size=content_size,
        )
        self._evict_if_needed()
        self.failures[failure.id] = failure
        self.failures.move_to_end(failure.id)
        return failure

    def needs_human_intervention(self, failure: ProcessingFailure) -> bool:
        if failure.attempts >= 3:
            return True
        if failure.failure_type == FailureType.UNKNOWN_FORMAT:
            return True
        if failure.failure_type == FailureType.SIZE_LIMIT:
            return True
        return False

    def apply_feedback(
        self,
        failure_id: str,
        action: FeedbackAction,
        user_instructions: str | None = None,
        feedback_by: str | None = None,
    ) -> None:
        if failure_id not in self.failures:
            raise ValueError(f"Failure {failure_id} not found")
        failure = self.failures[failure_id]
        failure.feedback = action
        failure.user_instructions = user_instructions
        failure.feedback_by = feedback_by
        failure.feedback_at = utc_now()
        if action == FeedbackAction.SKIP:
            failure.status = FailureStatus.SKIPPED
        elif action == FeedbackAction.RETRY:
            failure.status = FailureStatus.RETRY_SCHEDULED
            failure.attempts = 0
        elif action == FeedbackAction.USER_PROCESSED:
            failure.status = FailureStatus.RESOLVED
        elif action == FeedbackAction.NEW_STRATEGY:
            failure.status = FailureStatus.RETRY_SCHEDULED
        failure.updated_at = utc_now()
        self.failures.move_to_end(failure_id)

    def get_pending_failures(self, limit: int = 20) -> list[ProcessingFailure]:
        self._cleanup_expired()
        pending = [
            f for f in reversed(self.failures.values())
            if f.status == FailureStatus.PENDING
        ]
        return pending[:limit]

    def get_failure_stats(self) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        by_status: dict[str, int] = {}
        total = len(self.failures)
        for failure in self.failures.values():
            by_type[failure.failure_type.value] = by_type.get(failure.failure_type.value, 0) + 1
            by_status[failure.status.value] = by_status.get(failure.status.value, 0) + 1
        return {
            "total": total,
            "by_type": by_type,
            "by_status": by_status,
            "pending_count": by_status.get("pending", 0),
            "resolved_count": by_status.get("resolved", 0),
        }
