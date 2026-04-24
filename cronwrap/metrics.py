"""Lightweight in-process metrics collection for cron job runs."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RunMetric:
    """Snapshot of a single cron job execution."""
    job_name: str
    started_at: float
    duration_seconds: float
    exit_code: int
    attempt: int = 1
    timed_out: bool = False
    labels: Dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0


class MetricsCollector:
    """Accumulates RunMetrics and exposes simple aggregation helpers."""

    def __init__(self) -> None:
        self._runs: List[RunMetric] = []

    def record(self, metric: RunMetric) -> None:
        """Append a metric record."""
        self._runs.append(metric)

    def all_runs(self) -> List[RunMetric]:
        return list(self._runs)

    def runs_for(self, job_name: str) -> List[RunMetric]:
        return [r for r in self._runs if r.job_name == job_name]

    def success_rate(self, job_name: Optional[str] = None) -> float:
        """Return fraction of successful runs (0.0–1.0). Returns 1.0 when empty."""
        runs = self.runs_for(job_name) if job_name else self._runs
        if not runs:
            return 1.0
        return sum(1 for r in runs if r.succeeded) / len(runs)

    def average_duration(self, job_name: Optional[str] = None) -> float:
        """Return mean duration in seconds across recorded runs."""
        runs = self.runs_for(job_name) if job_name else self._runs
        if not runs:
            return 0.0
        return sum(r.duration_seconds for r in runs) / len(runs)

    def total_runs(self, job_name: Optional[str] = None) -> int:
        runs = self.runs_for(job_name) if job_name else self._runs
        return len(runs)

    def summary(self, job_name: Optional[str] = None) -> Dict:
        return {
            "total_runs": self.total_runs(job_name),
            "success_rate": self.success_rate(job_name),
            "average_duration_seconds": self.average_duration(job_name),
        }

    def clear(self) -> None:
        self._runs.clear()


# Module-level default collector
_default_collector = MetricsCollector()


def get_default_collector() -> MetricsCollector:
    return _default_collector
