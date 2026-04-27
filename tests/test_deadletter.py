"""Tests for cronwrap.deadletter."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from cronwrap.context import ExecutionContext
from cronwrap.deadletter import DeadLetterConfig, DeadLetterEntry, DeadLetterQueue


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store_dir(tmp_path: Path) -> Path:
    return tmp_path / "deadletter"


@pytest.fixture()
def config(store_dir: Path) -> DeadLetterConfig:
    return DeadLetterConfig(store_dir=str(store_dir), job_name="nightly-sync")


@pytest.fixture()
def queue(config: DeadLetterConfig) -> DeadLetterQueue:
    return DeadLetterQueue(config)


def _finished_ctx(exit_code: int = 1) -> ExecutionContext:
    ctx = ExecutionContext(job_name="nightly-sync")
    ctx.finish(exit_code=exit_code)
    return ctx


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def test_config_defaults():
    cfg = DeadLetterConfig(store_dir="/tmp", job_name="job")
    assert cfg.max_entries == 100


def test_config_rejects_empty_store_dir():
    with pytest.raises(ValueError, match="store_dir"):
        DeadLetterConfig(store_dir="", job_name="job")


def test_config_rejects_empty_job_name():
    with pytest.raises(ValueError, match="job_name"):
        DeadLetterConfig(store_dir="/tmp", job_name="")


def test_config_rejects_zero_max_entries():
    with pytest.raises(ValueError, match="max_entries"):
        DeadLetterConfig(store_dir="/tmp", job_name="job", max_entries=0)


# ---------------------------------------------------------------------------
# DeadLetterEntry
# ---------------------------------------------------------------------------

def test_entry_from_context_captures_exit_code():
    ctx = _finished_ctx(exit_code=2)
    entry = DeadLetterEntry.from_context(ctx, error_hint="timeout")
    assert entry.exit_code == 2
    assert entry.error_hint == "timeout"
    assert entry.job_name == "nightly-sync"


def test_entry_to_dict_roundtrip():
    ctx = _finished_ctx()
    entry = DeadLetterEntry.from_context(ctx)
    d = entry.to_dict()
    restored = DeadLetterEntry(**d)
    assert restored.exit_code == entry.exit_code
    assert restored.job_name == entry.job_name


# ---------------------------------------------------------------------------
# DeadLetterQueue
# ---------------------------------------------------------------------------

def test_push_creates_file(queue: DeadLetterQueue):
    entry = DeadLetterEntry.from_context(_finished_ctx())
    path = queue.push(entry)
    assert path.exists()


def test_list_entries_returns_pushed(queue: DeadLetterQueue):
    for _ in range(3):
        queue.push(DeadLetterEntry.from_context(_finished_ctx()))
        time.sleep(0.01)
    entries = queue.list_entries()
    assert len(entries) == 3
    assert all(e.exit_code == 1 for e in entries)


def test_eviction_respects_max_entries(store_dir: Path):
    cfg = DeadLetterConfig(store_dir=str(store_dir), job_name="job", max_entries=3)
    q = DeadLetterQueue(cfg)
    for _ in range(5):
        q.push(DeadLetterEntry.from_context(_finished_ctx()))
        time.sleep(0.01)
    assert len(q.list_entries()) == 3


def test_clear_removes_all(queue: DeadLetterQueue):
    for _ in range(4):
        queue.push(DeadLetterEntry.from_context(_finished_ctx()))
    removed = queue.clear()
    assert removed == 4
    assert queue.list_entries() == []
