"""Tests for cronwrap.throttle — ThrottleConfig and ThrottleGuard."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from cronwrap.throttle import ThrottleConfig, ThrottleGuard, ThrottleViolation


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "throttle_state"


@pytest.fixture()
def config(state_dir: Path) -> ThrottleConfig:
    return ThrottleConfig(min_interval_seconds=60, state_dir=state_dir)


@pytest.fixture()
def guard(config: ThrottleConfig) -> ThrottleGuard:
    return ThrottleGuard("test_job", config)


# ---------------------------------------------------------------------------
# ThrottleConfig
# ---------------------------------------------------------------------------


def test_config_defaults(state_dir: Path) -> None:
    cfg = ThrottleConfig(min_interval_seconds=30, state_dir=state_dir)
    assert cfg.min_interval_seconds == 30
    assert cfg.raise_on_throttle is True


def test_config_rejects_non_positive(state_dir: Path) -> None:
    with pytest.raises(ValueError):
        ThrottleConfig(min_interval_seconds=0, state_dir=state_dir)

    with pytest.raises(ValueError):
        ThrottleConfig(min_interval_seconds=-5, state_dir=state_dir)


# ---------------------------------------------------------------------------
# ThrottleGuard — first run (no state)
# ---------------------------------------------------------------------------


def test_first_run_always_allowed(guard: ThrottleGuard) -> None:
    assert guard.check() is True


# ---------------------------------------------------------------------------
# ThrottleGuard — after recording
# ---------------------------------------------------------------------------


def test_raises_when_too_soon(guard: ThrottleGuard) -> None:
    guard.record()
    with pytest.raises(ThrottleViolation) as exc_info:
        guard.check()
    assert exc_info.value.job_name == "test_job"
    assert exc_info.value.remaining > 0


def test_silent_skip_when_raise_disabled(state_dir: Path) -> None:
    cfg = ThrottleConfig(
        min_interval_seconds=60,
        state_dir=state_dir,
        raise_on_throttle=False,
    )
    g = ThrottleGuard("silent_job", cfg)
    g.record()
    assert g.check() is False


def test_allowed_after_interval_elapsed(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ThrottleConfig(min_interval_seconds=10, state_dir=state_dir)
    g = ThrottleGuard("fast_job", cfg)
    g.record()

    # Simulate time passing beyond the interval
    monkeypatch.setattr(time, "time", lambda: time.time.__wrapped__() + 11)  # type: ignore[attr-defined]
    # Use a simpler approach: write a past timestamp directly
    import json
    state_file = state_dir / "fast_job.throttle.json"
    state_file.write_text(json.dumps({"job": "fast_job", "last_run": time.time() - 20}))

    assert g.check() is True


# ---------------------------------------------------------------------------
# ThrottleGuard — reset
# ---------------------------------------------------------------------------


def test_reset_clears_state(guard: ThrottleGuard) -> None:
    guard.record()
    guard.reset()
    assert guard.check() is True


def test_reset_is_idempotent(guard: ThrottleGuard) -> None:
    """Calling reset when no state exists should not raise."""
    guard.reset()
    guard.reset()
