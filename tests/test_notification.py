"""Tests for cronwrap.notification and cronwrap.notification_hook."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cronwrap.context import ExecutionContext
from cronwrap.hooks import HookRegistry
from cronwrap.notification import (
    NotificationEvent,
    NotificationRouter,
    stdout_channel,
    webhook_channel,
)
from cronwrap.notification_hook import attach_notification_hooks


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def finished_ctx() -> ExecutionContext:
    ctx = ExecutionContext(job_name="test_job")
    ctx.finish(exit_code=0)
    return ctx


@pytest.fixture()
def failed_ctx() -> ExecutionContext:
    ctx = ExecutionContext(job_name="test_job")
    ctx.finish(exit_code=1)
    return ctx


@pytest.fixture()
def router() -> NotificationRouter:
    return NotificationRouter()


# ---------------------------------------------------------------------------
# NotificationEvent
# ---------------------------------------------------------------------------

def test_event_from_context_success(finished_ctx):
    event = NotificationEvent.from_context(finished_ctx, message="done")
    assert event.job_name == "test_job"
    assert event.success is True
    assert event.exit_code == 0
    assert event.message == "done"


def test_event_from_context_failure(failed_ctx):
    event = NotificationEvent.from_context(failed_ctx)
    assert event.success is False
    assert event.exit_code == 1


def test_event_to_dict_keys(finished_ctx):
    event = NotificationEvent.from_context(finished_ctx)
    d = event.to_dict()
    for key in ("job_name", "success", "exit_code", "duration_seconds", "message", "extra"):
        assert key in d


# ---------------------------------------------------------------------------
# NotificationRouter
# ---------------------------------------------------------------------------

def test_router_calls_registered_channel(finished_ctx, router):
    handler = MagicMock()
    router.register("mock", handler)
    event = NotificationEvent.from_context(finished_ctx)
    results = router.notify(event)
    handler.assert_called_once_with(event)
    assert results == [("mock", None)]


def test_router_captures_channel_exception(finished_ctx, router):
    def bad_channel(event):
        raise RuntimeError("boom")

    router.register("bad", bad_channel)
    event = NotificationEvent.from_context(finished_ctx)
    results = router.notify(event)
    name, exc = results[0]
    assert name == "bad"
    assert isinstance(exc, RuntimeError)


def test_stdout_channel_prints(finished_ctx, capsys):
    event = NotificationEvent.from_context(finished_ctx, message="all good")
    stdout_channel(event)
    captured = capsys.readouterr()
    assert "test_job" in captured.out
    assert "OK" in captured.out


def test_webhook_channel_posts_json(finished_ctx):
    event = NotificationEvent.from_context(finished_ctx)
    channel = webhook_channel("http://example.com/hook")
    with patch("urllib.request.urlopen") as mock_open:
        channel(event)
        mock_open.assert_called_once()


# ---------------------------------------------------------------------------
# notification_hook integration
# ---------------------------------------------------------------------------

def test_attach_hooks_fires_on_success(finished_ctx):
    reg = HookRegistry()
    rtr = NotificationRouter()
    handler = MagicMock()
    rtr.register("test", handler)
    attach_notification_hooks(reg, rtr)
    for fn in reg._post_hooks:
        fn(finished_ctx)
    handler.assert_called_once()


def test_attach_hooks_fires_on_failure(failed_ctx):
    reg = HookRegistry()
    rtr = NotificationRouter()
    handler = MagicMock()
    rtr.register("test", handler)
    attach_notification_hooks(reg, rtr)
    exc = ValueError("oops")
    for fn in reg._failure_hooks:
        fn(failed_ctx, exc)
    handler.assert_called_once()
    event: NotificationEvent = handler.call_args[0][0]
    assert "oops" in event.message


def test_attach_hooks_respects_notify_on_success_false(finished_ctx):
    reg = HookRegistry()
    rtr = NotificationRouter()
    handler = MagicMock()
    rtr.register("test", handler)
    attach_notification_hooks(reg, rtr, notify_on_success=False)
    for fn in reg._post_hooks:
        fn(finished_ctx)
    handler.assert_not_called()
