"""
Unit tests for Learning System
"""
from datetime import UTC

import pytest

from src.learning.failure_tracker import (
    FailureStatus,
    FailureTracker,
    FailureType,
    FeedbackAction,
)
from src.learning.metrics import MetricsCollector
from src.learning.strategy_store import StrategyStore


class TestFailureTracker:
    """Tests for FailureTracker"""

    def test_record_failure(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="github",
            source_identifier="repo/issue/123",
            failure_type=FailureType.NETWORK_ERROR,
            error_message="Connection timeout",
            content_preview="Some content...",
            content_size=1024,
        )

        assert failure.source_type == "github"
        assert failure.source_identifier == "repo/issue/123"
        assert failure.failure_type == FailureType.NETWORK_ERROR
        assert failure.error_message == "Connection timeout"
        assert failure.content_preview == "Some content..."
        assert failure.content_size == 1024
        assert failure.status == FailureStatus.PENDING
        assert failure.id in tracker.failures
        assert failure.created_at.tzinfo is UTC
        assert failure.updated_at.tzinfo is UTC

    def test_record_failure_truncates_preview(self):
        tracker = FailureTracker()
        long_preview = "x" * 2000
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.pdf",
            failure_type=FailureType.PARSE_ERROR,
            error_message="Parse failed",
            content_preview=long_preview,
        )
        assert failure.content_preview is not None
        assert len(failure.content_preview) == 1024

    def test_needs_human_intervention_high_attempts(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.txt",
            failure_type=FailureType.NETWORK_ERROR,
            error_message="Error",
        )
        failure.attempts = 3
        assert tracker.needs_human_intervention(failure) is True

    def test_needs_human_intervention_unknown_format(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.xyz",
            failure_type=FailureType.UNKNOWN_FORMAT,
            error_message="Unknown format",
        )
        assert tracker.needs_human_intervention(failure) is True

    def test_needs_human_intervention_size_limit(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="large.pdf",
            failure_type=FailureType.SIZE_LIMIT,
            error_message="File too large",
        )
        assert tracker.needs_human_intervention(failure) is True

    def test_needs_human_intervention_false(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.txt",
            failure_type=FailureType.NETWORK_ERROR,
            error_message="Error",
        )
        failure.attempts = 1
        assert tracker.needs_human_intervention(failure) is False

    def test_apply_feedback_skip(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.txt",
            failure_type=FailureType.PARSE_ERROR,
            error_message="Parse failed",
        )
        tracker.apply_feedback(failure.id, FeedbackAction.SKIP, feedback_by="admin")

        assert failure.status == FailureStatus.SKIPPED
        assert failure.feedback == FeedbackAction.SKIP
        assert failure.feedback_by == "admin"
        assert failure.feedback_at is not None

    def test_apply_feedback_retry(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.txt",
            failure_type=FailureType.NETWORK_ERROR,
            error_message="Connection failed",
        )
        failure.attempts = 2
        tracker.apply_feedback(failure.id, FeedbackAction.RETRY, user_instructions="Retry later")

        assert failure.status == FailureStatus.RETRY_SCHEDULED
        assert failure.attempts == 0
        assert failure.user_instructions == "Retry later"

    def test_apply_feedback_user_processed(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.txt",
            failure_type=FailureType.PARSE_ERROR,
            error_message="Parse failed",
        )
        tracker.apply_feedback(failure.id, FeedbackAction.USER_PROCESSED)

        assert failure.status == FailureStatus.RESOLVED

    def test_apply_feedback_new_strategy(self):
        tracker = FailureTracker()
        failure = tracker.record_failure(
            source_type="file",
            source_identifier="doc.txt",
            failure_type=FailureType.UNKNOWN_FORMAT,
            error_message="Unknown",
        )
        tracker.apply_feedback(failure.id, FeedbackAction.NEW_STRATEGY, user_instructions="Use OCR")

        assert failure.status == FailureStatus.RETRY_SCHEDULED

    def test_apply_feedback_not_found(self):
        tracker = FailureTracker()
        with pytest.raises(ValueError, match="Failure .* not found"):
            tracker.apply_feedback("non-existent-id", FeedbackAction.SKIP)

    def test_get_pending_failures(self):
        tracker = FailureTracker()
        f1 = tracker.record_failure("a", "1", FailureType.NETWORK_ERROR, "err1")
        tracker.record_failure("a", "2", FailureType.PARSE_ERROR, "err2")
        tracker.record_failure("a", "3", FailureType.SIZE_LIMIT, "err3")
        tracker.apply_feedback(f1.id, FeedbackAction.SKIP)

        pending = tracker.get_pending_failures()
        assert len(pending) == 2
        assert all(f.status == FailureStatus.PENDING for f in pending)

    def test_get_pending_failures_respects_limit(self):
        tracker = FailureTracker()
        for i in range(10):
            tracker.record_failure("a", str(i), FailureType.NETWORK_ERROR, f"err{i}")

        pending = tracker.get_pending_failures(limit=5)
        assert len(pending) == 5

    def test_get_failure_stats(self):
        tracker = FailureTracker()
        f1 = tracker.record_failure("file", "1", FailureType.NETWORK_ERROR, "err1")
        tracker.record_failure("file", "2", FailureType.PARSE_ERROR, "err2")
        tracker.record_failure("github", "3", FailureType.NETWORK_ERROR, "err3")
        tracker.apply_feedback(f1.id, FeedbackAction.RESOLVED)

        stats = tracker.get_failure_stats()
        assert stats["total"] == 3
        assert stats["by_type"]["network_error"] == 2
        assert stats["by_type"]["parse_error"] == 1
        assert stats["pending_count"] == 2
        assert stats["resolved_count"] == 1


class TestStrategyStore:
    """Tests for StrategyStore"""

    def test_record_success_new_strategy(self):
        store = StrategyStore()
        store.record_success("markdown_parser", quality_score=0.9, processing_time_ms=100)

        strategy = store.get_strategy("markdown_parser")
        assert strategy is not None
        assert strategy.success_count == 1
        assert strategy.avg_quality_score == 0.9
        assert strategy.avg_processing_time_ms == 100

    def test_record_success_updates_existing(self):
        store = StrategyStore()
        store.record_success("markdown_parser", quality_score=0.8, processing_time_ms=100)
        store.record_success("markdown_parser", quality_score=1.0, processing_time_ms=80)

        strategy = store.get_strategy("markdown_parser")
        assert strategy.success_count == 2
        assert strategy.avg_quality_score == 0.9
        assert strategy.avg_processing_time_ms == 90

    def test_record_failure(self):
        store = StrategyStore()
        store.record_success("markdown_parser", quality_score=0.9, processing_time_ms=100)
        store.record_failure("markdown_parser")

        strategy = store.get_strategy("markdown_parser")
        assert strategy.failure_count == 1

    def test_record_failure_non_existent_noop(self):
        store = StrategyStore()
        store.record_failure("nonexistent")
        assert store.get_strategy("nonexistent") is None

    def test_get_all_strategies_sorted_by_success_rate(self):
        store = StrategyStore()
        store.record_success("parser_a", quality_score=0.9, processing_time_ms=100)
        store.record_success("parser_a", quality_score=0.9, processing_time_ms=100)
        store.record_success("parser_b", quality_score=0.8, processing_time_ms=50)

        strategies = store.get_all_strategies()
        assert len(strategies) == 2
        assert strategies[0].strategy_name == "parser_a"
        assert strategies[1].strategy_name == "parser_b"

    def test_learn_from_feedback(self):
        store = StrategyStore()
        patterns = {"extension": ".md", "has_headers": True}
        config = {"chunk_size": 1000}

        strategy = store.learn_from_feedback("custom_md", patterns, config)

        assert strategy.strategy_name == "custom_md"
        assert strategy.applicable_patterns == patterns
        assert strategy.parser_config == config
        assert strategy.is_verified is False

    def test_get_strategy_not_found(self):
        store = StrategyStore()
        assert store.get_strategy("nonexistent") is None


class TestMetricsCollector:
    """Tests for MetricsCollector"""

    def test_increment_counter(self):
        collector = MetricsCollector()
        collector.increment("requests")
        collector.increment("requests", 5)

        assert collector.counters["requests"] == 6

    def test_increment_with_labels(self):
        collector = MetricsCollector()
        collector.increment("requests", labels={"method": "GET"})
        collector.increment("requests", labels={"method": "POST"})

        assert collector.counters["requests{method=GET}"] == 1
        assert collector.counters["requests{method=POST}"] == 1

    def test_set_gauge(self):
        collector = MetricsCollector()
        collector.set_gauge("cpu_usage", 0.75)
        collector.set_gauge("memory_usage", 0.5, labels={"host": "server1"})

        assert collector.gauges["cpu_usage"] == 0.75
        assert collector.gauges["memory_usage{host=server1}"] == 0.5

    def test_record_histogram(self):
        collector = MetricsCollector()
        collector.record_histogram("request_duration_ms", 100.0)
        collector.record_histogram("request_duration_ms", 200.0)

        assert "request_duration_ms" in collector.histograms
        assert len(collector.histograms["request_duration_ms"]) == 2

    def test_get_metrics(self):
        collector = MetricsCollector()
        collector.increment("requests")
        collector.set_gauge("cpu", 0.5)
        collector.record_histogram("latency", 50.0)

        metrics = collector.get_metrics()

        assert "counters" in metrics
        assert "gauges" in metrics
        assert "histograms" in metrics
        assert metrics["counters"]["requests"] == 1
        assert metrics["gauges"]["cpu"] == 0.5

    def test_get_metrics_histogram_stats(self):
        collector = MetricsCollector()
        collector.record_histogram("values", 10.0)
        collector.record_histogram("values", 20.0)
        collector.record_histogram("values", 30.0)

        metrics = collector.get_metrics()
        hist = metrics["histograms"]["values"]

        assert hist["count"] == 3
        assert hist["min"] == 10.0
        assert hist["max"] == 30.0
        assert hist["avg"] == 20.0

    def test_record_document_processed_success(self):
        collector = MetricsCollector()
        collector.record_document_processed(success=True, quality_score=0.9, processing_time_ms=150)

        assert collector.counters["documents_processed"] == 1
        assert collector.counters["documents_processed_success"] == 1
        assert collector.gauges["success_rate"] == 1.0

    def test_record_document_processed_failure(self):
        collector = MetricsCollector()
        collector.record_document_processed(success=False, quality_score=0.0, processing_time_ms=50)

        assert collector.counters["documents_processed"] == 1
        assert collector.counters["documents_processed_failure"] == 1

    def test_record_document_processed_updates_success_rate(self):
        collector = MetricsCollector()
        collector.record_document_processed(success=True, quality_score=0.9, processing_time_ms=100)
        collector.record_document_processed(success=True, quality_score=0.8, processing_time_ms=100)
        collector.record_document_processed(success=False, quality_score=0.0, processing_time_ms=50)

        assert collector.counters["documents_processed"] == 3
        assert collector.counters["documents_processed_success"] == 2
        assert collector.counters["documents_processed_failure"] == 1
        assert collector.gauges["success_rate"] == 2 / 3

    def test_record_failure(self):
        collector = MetricsCollector()
        collector.record_failure("network_error")
        collector.record_failure("parse_error")
        collector.record_failure("network_error")

        assert collector.counters["failures_total"] == 3
        assert collector.counters["failures_by_type{type=network_error}"] == 2
        assert collector.counters["failures_by_type{type=parse_error}"] == 1

    def test_format_key_with_labels(self):
        collector = MetricsCollector()
        key = collector._format_key("requests", {"method": "GET", "path": "/api"})
        assert key == "requests{method=GET,path=/api}"

    def test_format_key_without_labels(self):
        collector = MetricsCollector()
        key = collector._format_key("requests", None)
        assert key == "requests"
