"""Tests for ExecutionContext and HookRegistry."""

from datetime import timezone

import pytest

from cronwrap.context import ExecutionContext
from cronwrap.hooks import HookRegistry


# ---------------------------------------------------------------------------
# ExecutionContext tests
# ---------------------------------------------------------------------------

def test_context_defaults():
    ctx = ExecutionContext(job_name="backup")
    assert ctx.attempt == 1
    assert ctx.exit_code is None
    assert ctx.succeeded is False
    assert ctx.duration_seconds is None


def test_context_finish_marks_exit_code():
    ctx = ExecutionContext(job_name="backup")
    ctx.finish(exit_code=0, stdout="done")
    assert ctx.exit_code == 0
    assert ctx.succeeded is True
    assert ctx.stdout == "done"
    assert ctx.duration_seconds is not None
    assert ctx.duration_seconds >= 0


def test_context_to_dict_keys():
    ctx = ExecutionContext(job_name="sync")
    ctx.finish(exit_code=2, stderr="oops")
    d = ctx.to_dict()
    for key in ("job_name", "attempt", "started_at", "ended_at",
                "duration_seconds", "exit_code", "succeeded"):
        assert key in d


def test_context_repr():
    ctx = ExecutionContext(job_name="ping", attempt=2)
    assert "ping" in repr(ctx)
    assert "attempt=2" in repr(ctx)


# ---------------------------------------------------------------------------
# HookRegistry tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry():
    r = HookRegistry()
    yield r
    r.clear()


def test_pre_hook_called(registry):
    calls = []
    registry.pre(lambda ctx: calls.append("pre"))
    ctx = ExecutionContext(job_name="job")
    registry.run_pre(ctx)
    assert calls == ["pre"]


def test_post_hook_called(registry):
    calls = []
    registry.post(lambda ctx: calls.append("post"))
    ctx = ExecutionContext(job_name="job")
    ctx.finish(0)
    registry.run_post(ctx)
    assert calls == ["post"]


def test_failure_hook_not_called_on_success(registry):
    calls = []
    registry.on_failure(lambda ctx: calls.append("fail"))
    ctx = ExecutionContext(job_name="job")
    ctx.finish(exit_code=0)
    registry.run_failure(ctx)
    assert calls == []


def test_failure_hook_called_on_failure(registry):
    calls = []
    registry.on_failure(lambda ctx: calls.append("fail"))
    ctx = ExecutionContext(job_name="job")
    ctx.finish(exit_code=1)
    registry.run_failure(ctx)
    assert calls == ["fail"]


def test_multiple_hooks_all_called(registry):
    results = []
    registry.pre(lambda ctx: results.append(1))
    registry.pre(lambda ctx: results.append(2))
    ctx = ExecutionContext(job_name="job")
    registry.run_pre(ctx)
    assert results == [1, 2]


def test_clear_removes_all_hooks(registry):
    registry.pre(lambda ctx: None)
    registry.post(lambda ctx: None)
    registry.clear()
    assert repr(registry) == "HookRegistry(pre=0, post=0, failure=0)"
