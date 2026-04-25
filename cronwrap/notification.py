"""Notification channels for cron job outcomes (stdout, email stub, webhook)."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Optional

from cronwrap.context import ExecutionContext


@dataclass
class NotificationEvent:
    """Payload sent to every notification channel."""
    job_name: str
    success: bool
    exit_code: int
    duration_seconds: float
    message: str = ""
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_context(cls, ctx: ExecutionContext, message: str = "") -> "NotificationEvent":
        return cls(
            job_name=ctx.job_name,
            success=ctx.succeeded,
            exit_code=ctx.exit_code,
            duration_seconds=ctx.duration_seconds,
            message=message,
            extra=ctx.to_dict(),
        )

    def to_dict(self) -> dict:
        return {
            "job_name": self.job_name,
            "success": self.success,
            "exit_code": self.exit_code,
            "duration_seconds": self.duration_seconds,
            "message": self.message,
            "extra": self.extra,
        }


ChannelFn = Callable[[NotificationEvent], None]


class NotificationRouter:
    """Routes a NotificationEvent to registered channels."""

    def __init__(self) -> None:
        self._channels: list[tuple[str, ChannelFn]] = []

    def register(self, name: str, fn: ChannelFn) -> None:
        """Register a named notification channel."""
        self._channels.append((name, fn))

    def notify(self, event: NotificationEvent) -> list[tuple[str, Optional[Exception]]]:
        """Send event to all channels; returns list of (name, error_or_None)."""
        results: list[tuple[str, Optional[Exception]]] = []
        for name, fn in self._channels:
            try:
                fn(event)
                results.append((name, None))
            except Exception as exc:  # noqa: BLE001
                results.append((name, exc))
        return results


def webhook_channel(url: str, timeout: int = 5) -> ChannelFn:
    """Return a channel function that POSTs JSON to *url*."""

    def _send(event: NotificationEvent) -> None:
        payload = json.dumps(event.to_dict()).encode()
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=timeout)  # noqa: S310

    return _send


def stdout_channel(event: NotificationEvent) -> None:
    """Simple channel that prints a one-line summary."""
    status = "OK" if event.success else "FAIL"
    print(f"[cronwrap] {event.job_name} {status} exit={event.exit_code} "
          f"duration={event.duration_seconds:.3f}s {event.message}".rstrip())
