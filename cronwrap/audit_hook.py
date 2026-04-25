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

    Both the ``post`` and ``on_failure`` hooks write an :class:`AuditEntry`
    derived from the current :class:`~cronwrap.context.ExecutionContext`.
    Registering on both hooks ensures an entry is recorded regardless of
    whether the job succeeded or raised an exception.

    Args:
        registry: The hook registry to attach the audit hooks to.
        config: Optional audit configuration.  A default
            :class:`~cronwrap.audit.AuditConfig` is used when *None*.
        writer: An existing :class:`~cronwrap.audit.AuditWriter` to reuse.
            When *None* a new writer is created from *config*.

    Returns:
        The :class:`~cronwrap.audit.AuditWriter` used to persist entries so
        callers can inspect or assert on written records.
    """
    if writer is None:
        writer = AuditWriter(config or AuditConfig())

    def _write(ctx: ExecutionContext) -> None:
        entry = AuditEntry.from_context(ctx)
        writer.write(entry)

    registry.post(_write)
    registry.on_failure(_write)
    return writer
