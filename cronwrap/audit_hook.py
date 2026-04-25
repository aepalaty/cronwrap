"""Hook integration: automatically write an audit entry after every job run."""
from __future__ import annotations

from cronwrap.audit import AuditConfig, AuditEntry, AuditWriter
from cronwrap.context import ExecutionContext
from cronwrap.hooks import HookRegistry


def attach_audit_hooks(
    registry: HookRegistry,
    config: AuditConfig | None = None,
    writer: AuditWriter | None = None,
) -> AuditWriter:
    """Register post and failure hooks that persist audit entries.

    Returns the AuditWriter so callers can inspect written records.
    """
    if writer is None:
        writer = AuditWriter(config or AuditConfig())

    def _write(ctx: ExecutionContext) -> None:
        entry = AuditEntry.from_context(ctx)
        writer.write(entry)

    registry.post(_write)
    registry.on_failure(_write)
    return writer
