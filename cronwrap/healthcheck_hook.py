"""Attach HealthcheckReporter to a HookRegistry."""
from __future__ import annotations

from cronwrap.context import ExecutionContext
from cronwrap.healthcheck import HealthcheckConfig, HealthcheckReporter
from cronwrap.hooks import HookRegistry


def attach_healthcheck_hooks(
    registry: HookRegistry,
    config: HealthcheckConfig,
) -> HealthcheckReporter:
    """Register pre/post/failure hooks that drive *reporter* and return it."""
    reporter = HealthcheckReporter(config)

    @registry.pre
    def _pre_hook() -> None:  # type: ignore[return]
        reporter.ping_start()

    @registry.post
    def _post_hook(ctx: ExecutionContext) -> None:  # type: ignore[return]
        if ctx.succeeded:
            reporter.ping_success()

    @registry.on_failure
    def _failure_hook(ctx: ExecutionContext) -> None:  # type: ignore[return]
        reporter.ping_failure(exit_code=ctx.exit_code or 1)

    return reporter
