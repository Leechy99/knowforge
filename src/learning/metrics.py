"""
Metrics Collector - Collect and report system metrics
"""
from collections import defaultdict
from typing import Any


class MetricsCollector:
    def __init__(self):
        self.counters: dict[str, int] = defaultdict(int)
        self.gauges: dict[str, float] = {}
        self.histograms: dict[str, list[float]] = defaultdict(list)

    def increment(self, metric: str, value: int = 1, labels: dict[str, str] | None = None):
        key = self._format_key(metric, labels)
        self.counters[key] += value

    def set_gauge(self, metric: str, value: float, labels: dict[str, str] | None = None):
        key = self._format_key(metric, labels)
        self.gauges[key] = value

    def record_histogram(self, metric: str, value: float, labels: dict[str, str] | None = None):
        key = self._format_key(metric, labels)
        self.histograms[key].append(value)

    def _format_key(self, metric: str, labels: dict[str, str] | None = None) -> str:
        if not labels:
            return metric
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{metric}{{{label_str}}}"

    def get_metrics(self) -> dict[str, Any]:
        return {
            "counters": dict(self.counters),
            "gauges": self.gauges,
            "histograms": {
                k: {
                    "count": len(v),
                    "min": min(v) if v else 0,
                    "max": max(v) if v else 0,
                    "avg": sum(v) / len(v) if v else 0,
                }
                for k, v in self.histograms.items()
            },
        }

    def record_document_processed(self, success: bool, quality_score: float, processing_time_ms: int):
        self.increment("documents_processed", 1)
        self.increment("documents_processed_success" if success else "documents_processed_failure")
        self.record_histogram("processing_time_ms", processing_time_ms)
        self.record_histogram("quality_score", quality_score)
        total = self.counters.get("documents_processed_success", 0) + self.counters.get("documents_processed_failure", 0)
        if total > 0:
            self.set_gauge("success_rate", self.counters["documents_processed_success"] / total)

    def record_failure(self, failure_type: str):
        self.increment("failures_total")
        self.increment(f"failures_by_type{{type={failure_type}}}")


metrics = MetricsCollector()
