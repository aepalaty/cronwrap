"""Audit trail: persist a structured record of every job run to a file."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from cronwrap.context import ExecutionContext


@dataclass
class AuditConfig:
    """Configuration for the audit log writer."""

    audit_dir: str = "/var/log/cronwrap/audit"
    max_entries_per_file: int = 1000
    filename_pattern: str = "{job_name}.audit.jsonl"

    def __post_init__(self) -> None:
        if self.max_entries_per_file < 1:
            raise ValueError("max_entries_per_file must be >= 1")


@dataclass
class AuditEntry:
    """A single immutable audit record."""

    job_name: str
    run_id: str
    started_at: str
    finished_at: Optional[str]
    exit_code: Optional[int]
    duration_seconds: Optional[float]
    succeeded: bool
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_context(cls, ctx: ExecutionContext) -> "AuditEntry":
        d = ctx.to_dict()
        return cls(
            job_name=d["job_name"],
            run_id=d["run_id"],
            started_at=d["started_at"],
            finished_at=d.get("finished_at"),
            exit_code=d.get("exit_code"),
            duration_seconds=d.get("duration_seconds"),
            succeeded=ctx.succeeded,
        )

    def to_dict(self) -> dict:
        return {
            "job_name": self.job_name,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "exit_code": self.exit_code,
            "duration_seconds": self.duration_seconds,
            "succeeded": self.succeeded,
            "extra": self.extra,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }


class AuditWriter:
    """Appends AuditEntry records to a JSONL file."""

    def __init__(self, config: AuditConfig) -> None:
        self._config = config
        Path(config.audit_dir).mkdir(parents=True, exist_ok=True)

    def _path_for(self, job_name: str) -> Path:
        filename = self._config.filename_pattern.format(job_name=job_name)
        return Path(self._config.audit_dir) / filename

    def write(self, entry: AuditEntry) -> None:
        path = self._path_for(entry.job_name)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_dict()) + "\n")

    def read_all(self, job_name: str) -> list[dict]:
        path = self._path_for(job_name)
        if not path.exists():
            return []
        records: list[dict] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records[-self._config.max_entries_per_file :]
