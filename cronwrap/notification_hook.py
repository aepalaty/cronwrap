"""Attach a NotificationRouter to a HookRegistry so notifications fire automatically."""
from __future__ import annotations

from cronwrap.context import ExecutionContext
from cronwrap.hooks import HookRegistry
from cronwrap.notification import NotificationEvent, NotificationRouter


def attach_notification_hooks(
    registry: HookRegistry,
    router: NotificationRouter,
    *,
    notify_on_success: bool = True,
    notify_on_failure: bool = True,
) -> None:
    """Wire *router* into *registry* post/failure hooks.

    Parameters
    ----------
    registry:
        The :class:`HookRegistry` used by the cron job.
    router:
        A configured :class:`NotificationRouter` with at least one channel.
    notify_on_success:
        Whether to fire notifications when the job succeeds.
    notify_on_failure:
        Whether to fire notifications when the job fails.
    """

    def _post_hook(ctx: ExecutionContext) -> None:
        if not notify_on_success:
            return
        event = NotificationEvent.from_context(ctx, message="job completed successfully")
        router.notify(event)

    def _failure_hook(ctx: ExecutionContext, exc: BaseException) -> None:
        if not notify_on_failure:
            return
        event = NotificationEvent.from_context(
            ctx, message=f"job failed: {type(exc).__name__}: {exc}"
        )
        router.notify(event)

    registry.post(_post_hook)
    registry.on_failure(_failure_hook)
