"""Tests for cronwrap.runlock."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from cronwrap.runlock import RunLockConfig, RunLockGuard, RunLockViolation


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "locks"


@pytest.fixture()
def config(state_dir: Path) -> RunLockConfig:
    return RunLockConfig(job_name="test_job", lock_dir=state_dir)


@pytest.fixture()
def guard(config: RunLockConfig) -> RunLockGuard:
    return RunLockGuard(config)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_config_rejects_empty_job_name(state_dir: Path) -> None:
    with pytest.raises(ValueError, match="job_name"):
        RunLockConfig(job_name="", lock_dir=state_dir)


def test_config_rejects_non_positive_stale(state_dir: Path) -> None:
    with pytest.raises(ValueError, match="stale_after_seconds"):
        RunLockConfig(job_name="j", lock_dir=state_dir, stale_after_seconds=0)


def test_config_defaults() -> None:
    cfg = RunLockConfig(job_name="myjob")
    assert cfg.stale_after_seconds == 3600.0
    assert "cronwrap" in str(cfg.lock_dir)


# ---------------------------------------------------------------------------
# Acquire / release
# ---------------------------------------------------------------------------


def test_acquire_creates_lock_file(guard: RunLockGuard, state_dir: Path) -> None:
    guard.acquire()
    lock_file = state_dir / "test_job.lock"
    assert lock_file.exists()
    guard.release()


def test_release_removes_lock_file(guard: RunLockGuard, state_dir: Path) -> None:
    guard.acquire()
    guard.release()
    assert not (state_dir / "test_job.lock").exists()


def test_is_locked_after_acquire(guard: RunLockGuard) -> None:
    guard.acquire()
    assert guard.is_locked()
    guard.release()


def test_is_not_locked_initially(guard: RunLockGuard) -> None:
    assert not guard.is_locked()


def test_second_acquire_raises(config: RunLockConfig) -> None:
    g1 = RunLockGuard(config)
    g2 = RunLockGuard(config)
    g1.acquire()
    try:
        with pytest.raises(RunLockViolation) as exc_info:
            g2.acquire()
        assert exc_info.value.pid == os.getpid()
        assert exc_info.value.job_name == "test_job"
    finally:
        g1.release()


def test_stale_lock_is_overwritten(state_dir: Path) -> None:
    cfg = RunLockConfig(job_name="stale_job", lock_dir=state_dir, stale_after_seconds=1)
    guard = RunLockGuard(cfg)
    # Write a lock with a very old timestamp and a non-existent PID.
    lock_file = state_dir / "stale_job.lock"
    lock_file.write_text(f"99999999\n{time.time() - 10}\n")
    # Should NOT raise — lock is stale.
    guard.acquire()
    assert lock_file.read_text().startswith(str(os.getpid()))
    guard.release()


def test_violation_message_contains_age(config: RunLockConfig) -> None:
    g1 = RunLockGuard(config)
    g1.acquire()
    try:
        with pytest.raises(RunLockViolation) as exc_info:
            RunLockGuard(config).acquire()
        assert "age=" in str(exc_info.value)
    finally:
        g1.release()
