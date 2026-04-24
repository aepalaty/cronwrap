"""Tests for cronwrap.metrics — MetricsCollector and RunMetric."""
import pytest

from cronwrap.metrics import MetricsCollector, RunMetric


@pytest.fixture()
def collector() -> MetricsCollector:
    return MetricsCollector()


def _make_metric(job_name="backup", exit_code=0, duration=1.5, attempt=1):
    return RunMetric(
        job_name=job_name,
        started_at=1_000_000.0,
        duration_seconds=duration,
        exit_code=exit_code,
        attempt=attempt,
    )


def test_empty_collector_defaults(collector):
    assert collector.total_runs() == 0
    assert collector.success_rate() == 1.0
    assert collector.average_duration() == 0.0


def test_record_single_success(collector):
    collector.record(_make_metric(exit_code=0))
    assert collector.total_runs() == 1
    assert collector.success_rate() == 1.0


def test_record_single_failure(collector):
    collector.record(_make_metric(exit_code=1))
    assert collector.success_rate() == 0.0


def test_mixed_success_rate(collector):
    collector.record(_make_metric(exit_code=0))
    collector.record(_make_metric(exit_code=0))
    collector.record(_make_metric(exit_code=1))
    assert collector.success_rate() == pytest.approx(2 / 3)


def test_average_duration(collector):
    collector.record(_make_metric(duration=2.0))
    collector.record(_make_metric(duration=4.0))
    assert collector.average_duration() == pytest.approx(3.0)


def test_filter_by_job_name(collector):
    collector.record(_make_metric(job_name="backup", exit_code=0))
    collector.record(_make_metric(job_name="report", exit_code=1))
    assert collector.total_runs("backup") == 1
    assert collector.success_rate("backup") == 1.0
    assert collector.success_rate("report") == 0.0


def test_summary_keys(collector):
    collector.record(_make_metric())
    summary = collector.summary()
    assert set(summary.keys()) == {"total_runs", "success_rate", "average_duration_seconds"}


def test_clear(collector):
    collector.record(_make_metric())
    collector.clear()
    assert collector.total_runs() == 0


def test_run_metric_succeeded_property():
    assert RunMetric("j", 0.0, 1.0, exit_code=0).succeeded is True
    assert RunMetric("j", 0.0, 1.0, exit_code=2).succeeded is False
