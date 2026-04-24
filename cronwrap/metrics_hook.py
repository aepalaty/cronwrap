"""Hook integration that feeds ExecutionContext results into a MetricsCollector."""
from __future__ import annotations

from typing import Optional

from .context import ExecutionContext
from .hooks import HookRegistry
from .metrics import MetricsCollector, RunMetric, get_default_collector


def attach_metrics_hooks(
    registry: HookRegistry,
    collector: Optional[MetricsCollector] = None,
    extra_labels: Optional[dict] = None,
) -> None:
    """Register post/on_failure hooks on *registry* that write to *collector*.

    If *collector* is None the module-level default collector is used.
    *extra_labels* are merged into every RunMetric produced by these hooks.
    """
    if collector is None:
        collector = get_default_collector()
    labels = dict(extra_labels or {})

    def _record(ctx: ExecutionContext, attempt: int = 1, timed_out: bool = False) -> None:
        metric = RunMetric(
            job_name=ctx.job_name,
            started_at=ctx.started_at,
            duration_seconds=ctx.duration_seconds,
            exit_code=ctx.exit_code if ctx.exit_code is not None else -1,
            attempt=attempt,
            timed_out=timed_out,
            labels=labels,
        )
        collector.record(metric)

    @registry.post
    def _post_hook(ctx: ExecutionContext) -> None:  # noqa: F811
        _record(ctx)

    @registry.on_failure
    def _failure_hook(ctx: ExecutionContext, exc: BaseException) -> None:  # noqa: F811
        _record(ctx, timed_out=isinstance(exc, TimeoutError))
