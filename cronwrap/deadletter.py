"""Dead-letter queue: persist failed job runs for later inspection or replay."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from cronwrap.context import ExecutionContext


@dataclass
class DeadLetterConfig:
    store_dir: str
    job_name: str
    max_entries: int = 100

    def __post_init__(self) -> None:
        if not self.store_dir:
            raise ValueError("store_dir must not be empty")
        if not self.job_name:
            raise ValueError("job_name must not be empty")
        if self.max_entries < 1:
            raise ValueError("max_entries must be >= 1")


@dataclass
class DeadLetterEntry:
    job_name: str
    exit_code: int
    started_at: float
    finished_at: float
    duration_seconds: float
    error_hint: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    @classmethod
    def from_context(cls, ctx: ExecutionContext, error_hint: Optional[str] = None) -> "DeadLetterEntry":
        d = ctx.to_dict()
        return cls(
            job_name=d["job_name"],
            exit_code=d["exit_code"],
            started_at=d["started_at"],
            finished_at=d["finished_at"],
            duration_seconds=d["duration_seconds"],
            error_hint=error_hint,
            metadata=d.get("metadata", {}),
        )

    def to_dict(self) -> dict:
        return {
            "job_name": self.job_name,
            "exit_code": self.exit_code,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "error_hint": self.error_hint,
            "metadata": self.metadata,
        }


class DeadLetterQueue:
    def __init__(self, config: DeadLetterConfig) -> None:
        self._config = config
        self._dir = Path(config.store_dir) / config.job_name
        self._dir.mkdir(parents=True, exist_ok=True)

    def push(self, entry: DeadLetterEntry) -> Path:
        """Persist a failed entry and evict oldest if over capacity."""
        filename = f"{int(time.time() * 1000)}_{os.getpid()}.json"
        dest = self._dir / filename
        dest.write_text(json.dumps(entry.to_dict(), indent=2))
        self._evict()
        return dest

    def list_entries(self) -> List[DeadLetterEntry]:
        """Return all stored entries sorted oldest-first."""
        paths = sorted(self._dir.glob("*.json"))
        entries = []
        for p in paths:
            try:
                data = json.loads(p.read_text())
                entries.append(DeadLetterEntry(**data))
            except Exception:
                pass
        return entries

    def clear(self) -> int:
        """Remove all stored entries. Returns count removed."""
        paths = list(self._dir.glob("*.json"))
        for p in paths:
            p.unlink(missing_ok=True)
        return len(paths)

    def _evict(self) -> None:
        paths = sorted(self._dir.glob("*.json"))
        excess = len(paths) - self._config.max_entries
        for p in paths[:excess]:
            p.unlink(missing_ok=True)
