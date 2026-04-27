"""Run-lock guard: prevents a job from running if a previous instance is still active."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


class RunLockViolation(Exception):
    """Raised when a job is already running and a new instance is blocked."""

    def __init__(self, job_name: str, pid: int, started_at: float) -> None:
        self.job_name = job_name
        self.pid = pid
        self.started_at = started_at
        age = time.time() - started_at
        super().__init__(
            f"Job '{job_name}' is already running (pid={pid}, age={age:.1f}s)"
        )


@dataclass
class RunLockConfig:
    job_name: str
    lock_dir: Path = field(default_factory=lambda: Path("/tmp/cronwrap/locks"))
    stale_after_seconds: float = 3600.0

    def __post_init__(self) -> None:
        if not self.job_name:
            raise ValueError("job_name must not be empty")
        if self.stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        self.lock_dir = Path(self.lock_dir)


class RunLockGuard:
    """Manages a PID-based lock file for a cron job."""

    def __init__(self, config: RunLockConfig) -> None:
        self._config = config
        config.lock_dir.mkdir(parents=True, exist_ok=True)
        self._lock_path = config.lock_dir / f"{config.job_name}.lock"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self) -> None:
        """Acquire the lock or raise RunLockViolation if already held."""
        existing = self._read_lock()
        if existing is not None:
            pid, started_at = existing
            age = time.time() - started_at
            if age < self._config.stale_after_seconds and self._pid_alive(pid):
                raise RunLockViolation(self._config.job_name, pid, started_at)
            # Lock is stale — overwrite it.
        self._write_lock()

    def release(self) -> None:
        """Release the lock file if it belongs to the current process."""
        existing = self._read_lock()
        if existing is not None and existing[0] == os.getpid():
            try:
                self._lock_path.unlink()
            except FileNotFoundError:
                pass

    def is_locked(self) -> bool:
        """Return True if a live (non-stale) lock exists."""
        existing = self._read_lock()
        if existing is None:
            return False
        pid, started_at = existing
        age = time.time() - started_at
        return age < self._config.stale_after_seconds and self._pid_alive(pid)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _write_lock(self) -> None:
        content = f"{os.getpid()}\n{time.time()}\n"
        self._lock_path.write_text(content)

    def _read_lock(self) -> Optional[tuple[int, float]]:
        try:
            parts = self._lock_path.read_text().strip().splitlines()
            return int(parts[0]), float(parts[1])
        except (FileNotFoundError, ValueError, IndexError):
            return None

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False
