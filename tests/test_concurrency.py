"""Tests for cronwrap.concurrency."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from cronwrap.concurrency import ConcurrencyConfig, ConcurrencyGuard, ConcurrencyViolation


@pytest.fixture()
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "locks"


@pytest.fixture()
def config(state_dir: Path) -> ConcurrencyConfig:
    return ConcurrencyConfig(job_name="test_job", state_dir=state_dir)


@pytest.fixture()
def guard(config: ConcurrencyConfig) -> ConcurrencyGuard:
    return ConcurrencyGuard(config)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def test_config_rejects_empty_job_name(state_dir: Path) -> None:
    with pytest.raises(ValueError, match="job_name"):
        ConcurrencyConfig(job_name="", state_dir=state_dir)


def test_config_rejects_non_positive_stale(state_dir: Path) -> None:
    with pytest.raises(ValueError, match="stale_after_seconds"):
        ConcurrencyConfig(job_name="j", state_dir=state_dir, stale_after_seconds=0)


def test_config_defaults() -> None:
    cfg = ConcurrencyConfig(job_name="myjob")
    assert cfg.stale_after_seconds == 3600.0
    assert "cronwrap" in str(cfg.state_dir)


# ---------------------------------------------------------------------------
# Basic lock / unlock
# ---------------------------------------------------------------------------

def test_acquire_creates_lock_file(guard: ConcurrencyGuard, state_dir: Path) -> None:
    guard.acquire()
    lock_file = state_dir / "test_job.lock"
    assert lock_file.exists()
    guard.release()


def test_release_removes_lock_file(guard: ConcurrencyGuard, state_dir: Path) -> None:
    guard.acquire()
    guard.release()
    assert not (state_dir / "test_job.lock").exists()


def test_release_is_idempotent(guard: ConcurrencyGuard) -> None:
    guard.release()  # should not raise even if no lock exists


def test_is_locked_false_initially(guard: ConcurrencyGuard) -> None:
    assert guard.is_locked() is False


def test_is_locked_true_after_acquire(guard: ConcurrencyGuard) -> None:
    guard.acquire()
    assert guard.is_locked() is True
    guard.release()


# ---------------------------------------------------------------------------
# Violation
# ---------------------------------------------------------------------------

def test_second_acquire_raises_violation(config: ConcurrencyConfig) -> None:
    g1 = ConcurrencyGuard(config)
    g2 = ConcurrencyGuard(config)
    g1.acquire()
    try:
        with pytest.raises(ConcurrencyViolation) as exc_info:
            g2.acquire()
        assert "test_job" in str(exc_info.value)
    finally:
        g1.release()


# ---------------------------------------------------------------------------
# Stale lock
# ---------------------------------------------------------------------------

def test_stale_lock_is_overwritten(state_dir: Path) -> None:
    cfg = ConcurrencyConfig(job_name="stale_job", state_dir=state_dir, stale_after_seconds=0.01)
    g = ConcurrencyGuard(cfg)
    g.acquire()
    time.sleep(0.05)  # let the lock go stale
    # Should NOT raise
    g.acquire()
    g.release()


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

def test_context_manager_releases_on_exit(guard: ConcurrencyGuard) -> None:
    with guard:
        assert guard.is_locked()
    assert not guard.is_locked()


def test_context_manager_releases_on_exception(guard: ConcurrencyGuard) -> None:
    with pytest.raises(RuntimeError):
        with guard:
            raise RuntimeError("boom")
    assert not guard.is_locked()
