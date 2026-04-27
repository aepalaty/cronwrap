"""Concurrency guard: prevent multiple instances of the same cron job from running simultaneously."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path


class ConcurrencyViolation(Exception):
    """Raised when a job is already running and overlap is not allowed."""

    def __init__(self, job_name: str, lock_file: Path, locked_at: float) -> None:
        self.job_name = job_name
        self.lock_file = lock_file
        self.locked_at = locked_at
        age = time.time() - locked_at
        super().__init__(
            f"Job '{job_name}' is already running (lock held for {age:.1f}s): {lock_file}"
        )


@dataclass
class ConcurrencyConfig:
    job_name: str
    state_dir: Path = field(default_factory=lambda: Path("/tmp/cronwrap/locks"))
    stale_after_seconds: float = 3600.0  # treat lock as stale after 1 hour

    def __post_init__(self) -> None:
        if not self.job_name:
            raise ValueError("job_name must not be empty")
        if self.stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        self.state_dir = Path(self.state_dir)


class ConcurrencyGuard:
    """File-based lock guard to enforce single-instance execution."""

    def __init__(self, config: ConcurrencyConfig) -> None:
        self._config = config
        config.state_dir.mkdir(parents=True, exist_ok=True)
        self._lock_file = config.state_dir / f"{config.job_name}.lock"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self) -> None:
        """Acquire the lock or raise ConcurrencyViolation."""
        if self._lock_file.exists():
            locked_at = self._read_timestamp()
            if locked_at is not None:
                age = time.time() - locked_at
                if age < self._config.stale_after_seconds:
                    raise ConcurrencyViolation(
                        self._config.job_name, self._lock_file, locked_at
                    )
                # Lock is stale — remove it and proceed
                self._lock_file.unlink(missing_ok=True)
        self._lock_file.write_text(str(time.time()))

    def release(self) -> None:
        """Release the lock (idempotent)."""
        self._lock_file.unlink(missing_ok=True)

    def is_locked(self) -> bool:
        """Return True if a non-stale lock file exists."""
        if not self._lock_file.exists():
            return False
        locked_at = self._read_timestamp()
        if locked_at is None:
            return False
        age = time.time() - locked_at
        return age < self._config.stale_after_seconds

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "ConcurrencyGuard":
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_timestamp(self) -> float | None:
        try:
            return float(self._lock_file.read_text().strip())
        except (ValueError, OSError):
            return None
