"""Tests for cronwrap.metrics_hook — hook-based metrics recording."""
import time

import pytest

from cronwrap.context import ExecutionContext
from cronwrap.hooks import HookRegistry
from cronwrap.metrics import MetricsCollector
from cronwrap.metrics_hook import attach_metrics_hooks


@pytest.fixture()
def registry() -> HookRegistry:
    return HookRegistry()


@pytest.fixture()
def collector() -> MetricsCollector:
    return MetricsCollector()


def _finished_ctx(job_name="sync", exit_code=0) -> ExecutionContext:
    ctx = ExecutionContext(job_name=job_name)
    time.sleep(0.01)
    ctx.finish(exit_code=exit_code)
    return ctx


def test_post_hook_records_success(registry, collector):
    attach_metrics_hooks(registry, collector=collector)
    ctx = _finished_ctx(exit_code=0)
    registry.run_post(ctx)
    assert collector.total_runs() == 1
    assert collector.success_rate() == 1.0


def test_failure_hook_records_failure(registry, collector):
    attach_metrics_hooks(registry, collector=collector)
    ctx = _finished_ctx(exit_code=1)
    registry.run_on_failure(ctx, RuntimeError("boom"))
    assert collector.total_runs() == 1
    assert collector.success_rate() == 0.0


def test_duration_is_positive(registry, collector):
    attach_metrics_hooks(registry, collector=collector)
    ctx = _finished_ctx()
    registry.run_post(ctx)
    metric = collector.all_runs()[0]
    assert metric.duration_seconds > 0


def test_extra_labels_attached(registry, collector):
    attach_metrics_hooks(registry, collector=collector, extra_labels={"env": "prod"})
    ctx = _finished_ctx()
    registry.run_post(ctx)
    assert collector.all_runs()[0].labels == {"env": "prod"}


def test_timeout_error_sets_timed_out(registry, collector):
    attach_metrics_hooks(registry, collector=collector)
    ctx = _finished_ctx(exit_code=1)
    registry.run_on_failure(ctx, TimeoutError("took too long"))
    assert collector.all_runs()[0].timed_out is True


def test_uses_default_collector_when_none(registry):
    from cronwrap.metrics import get_default_collector
    dc = get_default_collector()
    before = dc.total_runs()
    attach_metrics_hooks(registry)
    ctx = _finished_ctx()
    registry.run_post(ctx)
    assert dc.total_runs() == before + 1
    dc.clear()
