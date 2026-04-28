"""Pipeline builder for composing cronwrap features into a single execution flow.

Provides a fluent interface to attach throttling, rate-limiting, concurrency
guards, retries, timeouts, metrics, audit logging, notifications, healthchecks,
and dead-letter queuing — then run a callable under all of those controls.
"""

from __future__ import annotations

import traceback
from typing import Any, Callable, Optional

from .context import ExecutionContext
from .hooks import HookRegistry
from .logging import CronLogger

# Optional feature imports — each is only wired in when the caller requests it.
from .metrics import MetricsCollector
from .metrics_hook import attach_metrics_hooks
from .audit import AuditConfig
from .audit_hook import attach_audit_hooks
from .notification import NotificationRouter
from .notification_hook import attach_notification_hooks
from .healthcheck import HealthcheckConfig
from .healthcheck_hook import attach_healthcheck_hooks
from .deadletter import DeadLetterConfig
from .throttle import ThrottleConfig, ThrottleGuard
from .ratelimit import RateLimitConfig, RateLimitGuard
from .concurrency import ConcurrencyConfig, ConcurrencyGuard
from .runlock import RunLockConfig, RunLockGuard
from .retry import RetryPolicy, RetryState
from .timeout import TimeoutConfig, timeout_context


class Pipeline:
    """Composable execution pipeline for a cron job callable.

    Usage::

        result = (
            Pipeline("my-job", my_callable)
            .with_retry(RetryPolicy(max_attempts=3))
            .with_timeout(TimeoutConfig(seconds=60))
            .with_metrics(collector)
            .run()
        )
    """

    def __init__(self, job_name: str, fn: Callable[[], Any]) -> None:
        self.job_name = job_name
        self.fn = fn
        self.logger = CronLogger(job_name)
        self.registry = HookRegistry()

        # Optional feature state
        self._retry_policy: Optional[RetryPolicy] = None
        self._timeout_config: Optional[TimeoutConfig] = None
        self._throttle_guard: Optional[ThrottleGuard] = None
        self._ratelimit_guard: Optional[RateLimitGuard] = None
        self._concurrency_guard: Optional[ConcurrencyGuard] = None
        self._runlock_guard: Optional[RunLockGuard] = None

    # ------------------------------------------------------------------
    # Fluent configuration methods
    # ------------------------------------------------------------------

    def with_retry(self, policy: RetryPolicy) -> "Pipeline":
        """Attach a retry policy."""
        self._retry_policy = policy
        return self

    def with_timeout(self, config: TimeoutConfig) -> "Pipeline":
        """Attach a timeout configuration."""
        self._timeout_config = config
        return self

    def with_metrics(self, collector: MetricsCollector) -> "Pipeline":
        """Attach a metrics collector via hooks."""
        attach_metrics_hooks(self.registry, collector)
        return self

    def with_audit(self, config: AuditConfig) -> "Pipeline":
        """Attach audit logging via hooks."""
        attach_audit_hooks(self.registry, config)
        return self

    def with_notifications(self, router: NotificationRouter) -> "Pipeline":
        """Attach notification routing via hooks."""
        attach_notification_hooks(self.registry, router)
        return self

    def with_healthcheck(self, config: HealthcheckConfig) -> "Pipeline":
        """Attach healthcheck pinging via hooks."""
        attach_healthcheck_hooks(self.registry, config)
        return self

    def with_throttle(self, config: ThrottleConfig) -> "Pipeline":
        """Attach a throttle guard (checked before execution)."""
        self._throttle_guard = ThrottleGuard(config)
        return self

    def with_ratelimit(self, config: RateLimitConfig) -> "Pipeline":
        """Attach a rate-limit guard (checked before execution)."""
        self._ratelimit_guard = RateLimitGuard(config)
        return self

    def with_concurrency(self, config: ConcurrencyConfig) -> "Pipeline":
        """Attach a concurrency guard (slot acquired around execution)."""
        self._concurrency_guard = ConcurrencyGuard(config)
        return self

    def with_runlock(self, config: RunLockConfig) -> "Pipeline":
        """Attach a run-lock guard (prevents overlapping runs)."""
        self._runlock_guard = RunLockGuard(config)
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self) -> ExecutionContext:
        """Execute the pipeline and return the finished ExecutionContext.

        Pre-run guards (throttle, rate-limit, concurrency, run-lock) are
        checked first.  If all pass, the callable is invoked — with optional
        retry and timeout wrapping — and post/failure hooks are fired.
        """
        ctx = ExecutionContext(job_name=self.job_name)

        # --- pre-run guards (raise immediately on violation) ---
        for guard_attr in (
            "_throttle_guard",
            "_ratelimit_guard",
            "_concurrency_guard",
            "_runlock_guard",
        ):
            guard = getattr(self, guard_attr)
            if guard is not None:
                guard.acquire()  # raises on violation

        # --- fire pre hooks ---
        self.registry.fire_pre(ctx)

        retry_state = RetryState(self._retry_policy) if self._retry_policy else None
        exit_code = 0
        last_exc: Optional[BaseException] = None

        while True:
            try:
                if self._timeout_config is not None:
                    with timeout_context(self._timeout_config):
                        result = self.fn()
                else:
                    result = self.fn()

                ctx.finish(exit_code=0)
                self.logger.info("job completed", result=repr(result))
                self.registry.fire_post(ctx)
                break

            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                self.logger.error(
                    "job failed",
                    error=str(exc),
                    traceback=traceback.format_exc(),
                )

                if retry_state is not None and retry_state.remaining > 0:
                    delay = self._retry_policy.delay_for_attempt(  # type: ignore[union-attr]
                        retry_state.attempt
                    )
                    self.logger.info(
                        "retrying job",
                        attempt=retry_state.attempt,
                        delay=delay,
                    )
                    retry_state.sleep_and_advance()
                    continue

                exit_code = 1
                ctx.finish(exit_code=exit_code, exception=exc)
                self.registry.fire_failure(ctx)
                break

        # --- release concurrency / run-lock slots ---
        for guard_attr in ("_concurrency_guard", "_runlock_guard"):
            guard = getattr(self, guard_attr)
            if guard is not None and hasattr(guard, "release"):
                try:
                    guard.release()
                except Exception:  # noqa: BLE001
                    pass

        return ctx
