"""Unit tests for cronwrap.healthcheck_hook."""
import pytest

from cronwrap.context import ExecutionContext
from cronwrap.healthcheck import HealthcheckConfig
from cronwrap.healthcheck_hook import attach_healthcheck_hooks
from cronwrap.hooks import HookRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(pings: list[str], *, ping_on_start: bool = True) -> HealthcheckConfig:
    def _sender(url: str, _t: float) -> None:
        pings.append(url)

    return HealthcheckConfig(
        url="https://hc.example.com/xyz",
        ping_on_start=ping_on_start,
        sender=_sender,
    )


def _finished_ctx(*, exit_code: int = 0) -> ExecutionContext:
    ctx = ExecutionContext(job_name="healthcheck-job")
    ctx.finish(exit_code=exit_code)
    return ctx


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry() -> HookRegistry:
    return HookRegistry()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_attach_returns_reporter(registry: HookRegistry) -> None:
    from cronwrap.healthcheck import HealthcheckReporter
    reporter = attach_healthcheck_hooks(registry, _make_config([]))
    assert isinstance(reporter, HealthcheckReporter)


def test_pre_hook_pings_start(registry: HookRegistry) -> None:
    pings: list[str] = []
    attach_healthcheck_hooks(registry, _make_config(pings, ping_on_start=True))
    registry.run_pre()
    assert any("/start" in p for p in pings)


def test_post_hook_pings_success_on_exit_0(registry: HookRegistry) -> None:
    pings: list[str] = []
    attach_healthcheck_hooks(registry, _make_config(pings, ping_on_start=False))
    registry.run_post(_finished_ctx(exit_code=0))
    assert pings == ["https://hc.example.com/xyz"]


def test_post_hook_skips_ping_on_failure(registry: HookRegistry) -> None:
    pings: list[str] = []
    attach_healthcheck_hooks(registry, _make_config(pings, ping_on_start=False))
    registry.run_post(_finished_ctx(exit_code=1))
    assert pings == []


def test_failure_hook_pings_fail_path(registry: HookRegistry) -> None:
    pings: list[str] = []
    attach_healthcheck_hooks(registry, _make_config(pings, ping_on_start=False))
    registry.run_failure(_finished_ctx(exit_code=2))
    assert any("/fail" in p for p in pings)
