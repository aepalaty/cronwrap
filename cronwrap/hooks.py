"""Lifecycle hooks that fire before/after cron job execution."""

from typing import Callable, List

from cronwrap.context import ExecutionContext

HookFn = Callable[[ExecutionContext], None]


class HookRegistry:
    """Registry of pre- and post-execution hooks."""

    def __init__(self) -> None:
        self._pre: List[HookFn] = []
        self._post: List[HookFn] = []
        self._failure: List[HookFn] = []

    def pre(self, fn: HookFn) -> HookFn:
        """Register a hook to run before the job starts."""
        self._pre.append(fn)
        return fn

    def post(self, fn: HookFn) -> HookFn:
        """Register a hook to run after the job finishes (success or failure)."""
        self._post.append(fn)
        return fn

    def on_failure(self, fn: HookFn) -> HookFn:
        """Register a hook to run only when the job fails."""
        self._failure.append(fn)
        return fn

    def run_pre(self, ctx: ExecutionContext) -> None:
        for hook in self._pre:
            hook(ctx)

    def run_post(self, ctx: ExecutionContext) -> None:
        for hook in self._post:
            hook(ctx)

    def run_failure(self, ctx: ExecutionContext) -> None:
        if not ctx.succeeded:
            for hook in self._failure:
                hook(ctx)

    def clear(self) -> None:
        """Remove all registered hooks (useful in tests)."""
        self._pre.clear()
        self._post.clear()
        self._failure.clear()

    def __repr__(self) -> str:
        return (
            f"HookRegistry(pre={len(self._pre)}, "
            f"post={len(self._post)}, "
            f"failure={len(self._failure)})"
        )
