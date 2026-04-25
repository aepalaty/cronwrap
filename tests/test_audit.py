"""Tests for cronwrap.audit (AuditConfig, AuditEntry, AuditWriter)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from cronwrap.audit import AuditConfig, AuditEntry, AuditWriter
from cronwrap.context import ExecutionContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def audit_dir(tmp_path: Path) -> Path:
    return tmp_path / "audit"


@pytest.fixture()
def config(audit_dir: Path) -> AuditConfig:
    return AuditConfig(audit_dir=str(audit_dir))


@pytest.fixture()
def writer(config: AuditConfig) -> AuditWriter:
    return AuditWriter(config)


def _finished_ctx(exit_code: int = 0) -> ExecutionContext:
    ctx = ExecutionContext(job_name="test-job")
    ctx.finish(exit_code=exit_code)
    return ctx


# ---------------------------------------------------------------------------
# AuditConfig
# ---------------------------------------------------------------------------

def test_config_defaults() -> None:
    cfg = AuditConfig()
    assert cfg.max_entries_per_file == 1000
    assert "{job_name}" in cfg.filename_pattern


def test_config_rejects_zero_max() -> None:
    with pytest.raises(ValueError):
        AuditConfig(max_entries_per_file=0)


# ---------------------------------------------------------------------------
# AuditEntry
# ---------------------------------------------------------------------------

def test_entry_from_success_context() -> None:
    ctx = _finished_ctx(exit_code=0)
    entry = AuditEntry.from_context(ctx)
    assert entry.job_name == "test-job"
    assert entry.succeeded is True
    assert entry.exit_code == 0
    assert entry.finished_at is not None
    assert entry.duration_seconds is not None


def test_entry_from_failure_context() -> None:
    ctx = _finished_ctx(exit_code=1)
    entry = AuditEntry.from_context(ctx)
    assert entry.succeeded is False
    assert entry.exit_code == 1


def test_entry_to_dict_has_recorded_at() -> None:
    ctx = _finished_ctx()
    d = AuditEntry.from_context(ctx).to_dict()
    assert "recorded_at" in d


# ---------------------------------------------------------------------------
# AuditWriter
# ---------------------------------------------------------------------------

def test_writer_creates_dir(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b" / "audit"
    cfg = AuditConfig(audit_dir=str(nested))
    AuditWriter(cfg)  # should not raise
    assert nested.is_dir()


def test_write_and_read_back(writer: AuditWriter, audit_dir: Path) -> None:
    ctx = _finished_ctx()
    entry = AuditEntry.from_context(ctx)
    writer.write(entry)
    records = writer.read_all("test-job")
    assert len(records) == 1
    assert records[0]["job_name"] == "test-job"


def test_read_all_empty_when_no_file(writer: AuditWriter) -> None:
    assert writer.read_all("no-such-job") == []


def test_multiple_writes_appended(writer: AuditWriter) -> None:
    for _ in range(3):
        writer.write(AuditEntry.from_context(_finished_ctx()))
    assert len(writer.read_all("test-job")) == 3


def test_max_entries_per_file_limits_read(audit_dir: Path) -> None:
    cfg = AuditConfig(audit_dir=str(audit_dir), max_entries_per_file=2)
    w = AuditWriter(cfg)
    for _ in range(5):
        w.write(AuditEntry.from_context(_finished_ctx()))
    assert len(w.read_all("test-job")) == 2
