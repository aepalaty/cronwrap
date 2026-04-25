"""Tests for cronwrap.audit_hook (attach_audit_hooks)."""
from __future__ import annotations

from pathlib import Path

import pytest

from cronwrap.audit import AuditConfig, AuditWriter
from cronwrap.audit_hook import attach_audit_hooks
from cronwrap.context import ExecutionContext
from cronwrap.hooks import HookRegistry


@pytest.fixture()
def registry() -> HookRegistry:
    return HookRegistry()


@pytest.fixture()
def config(tmp_path: Path) -> AuditConfig:
    return AuditConfig(audit_dir=str(tmp_path / "audit"))


def _finished_ctx(exit_code: int = 0) -> ExecutionContext:
    ctx = ExecutionContext(job_name="hook-job")
    ctx.finish(exit_code=exit_code)
    return ctx


def test_attach_returns_writer(registry: HookRegistry, config: AuditConfig) -> None:
    writer = attach_audit_hooks(registry, config=config)
    assert isinstance(writer, AuditWriter)


def test_post_hook_writes_success(registry: HookRegistry, config: AuditConfig) -> None:
    writer = attach_audit_hooks(registry, config=config)
    ctx = _finished_ctx(exit_code=0)
    for hook in registry._post_hooks:  # type: ignore[attr-defined]
        hook(ctx)
    records = writer.read_all("hook-job")
    assert len(records) == 1
    assert records[0]["succeeded"] is True


def test_failure_hook_writes_failure(
    registry: HookRegistry, config: AuditConfig
) -> None:
    writer = attach_audit_hooks(registry, config=config)
    ctx = _finished_ctx(exit_code=2)
    for hook in registry._failure_hooks:  # type: ignore[attr-defined]
        hook(ctx)
    records = writer.read_all("hook-job")
    assert len(records) == 1
    assert records[0]["succeeded"] is False


def test_custom_writer_accepted(registry: HookRegistry, config: AuditConfig) -> None:
    custom_writer = AuditWriter(config)
    returned = attach_audit_hooks(registry, writer=custom_writer)
    assert returned is custom_writer


def test_no_config_uses_defaults(registry: HookRegistry) -> None:
    """attach_audit_hooks should not raise when neither config nor writer given."""
    # We don't actually write to avoid polluting /var/log; just check no error.
    try:
        attach_audit_hooks(registry)
    except PermissionError:
        pytest.skip("no write access to default audit dir")
