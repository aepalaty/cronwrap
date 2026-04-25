"""Tests for cronwrap.ratelimit."""

from __future__ import annotations

import time
import pytest

from cronwrap.ratelimit import RateLimitConfig, RateLimitExceeded, RateLimitGuard


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path / "ratelimit")


@pytest.fixture()
def config(state_dir):
    return RateLimitConfig(limit=3, window_seconds=60, state_dir=state_dir)


@pytest.fixture()
def guard(config):
    return RateLimitGuard("test_job", config)


# ---------------------------------------------------------------------------
# RateLimitConfig
# ---------------------------------------------------------------------------


def test_config_defaults():
    cfg = RateLimitConfig()
    assert cfg.limit == 1
    assert cfg.window_seconds == 3600


def test_config_rejects_zero_limit():
    with pytest.raises(ValueError, match="limit"):
        RateLimitConfig(limit=0)


def test_config_rejects_zero_window():
    with pytest.raises(ValueError, match="window_seconds"):
        RateLimitConfig(window_seconds=0)


# ---------------------------------------------------------------------------
# RateLimitGuard — happy path
# ---------------------------------------------------------------------------


def test_first_run_allowed(guard):
    guard.check()  # should not raise


def test_record_then_check_within_limit(guard):
    guard.record()
    guard.record()
    guard.check()  # 2 recorded, limit is 3 — should pass


def test_exceeds_limit_raises(guard):
    for _ in range(3):
        guard.record()
    with pytest.raises(RateLimitExceeded) as exc_info:
        guard.check()
    assert exc_info.value.limit == 3
    assert exc_info.value.job_name == "test_job"


def test_reset_clears_state(guard):
    for _ in range(3):
        guard.record()
    guard.reset()
    guard.check()  # should not raise after reset


# ---------------------------------------------------------------------------
# Rolling window
# ---------------------------------------------------------------------------


def test_old_timestamps_outside_window_are_ignored(state_dir):
    cfg = RateLimitConfig(limit=1, window_seconds=5, state_dir=state_dir)
    g = RateLimitGuard("rolling_job", cfg)

    # Manually inject a timestamp well outside the window
    import json
    from pathlib import Path

    state_path = Path(state_dir) / "rolling_job.ratelimit.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    old_ts = time.time() - 100  # 100 seconds ago, window is 5s
    state_path.write_text(json.dumps([old_ts]))

    g.check()  # old entry should be ignored — should not raise


# ---------------------------------------------------------------------------
# Exception message
# ---------------------------------------------------------------------------


def test_exception_message_contains_details(guard):
    for _ in range(3):
        guard.record()
    with pytest.raises(RateLimitExceeded, match="test_job"):
        guard.check()
