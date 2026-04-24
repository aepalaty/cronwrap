"""Throttle / rate-limit support for cron jobs.

Prevents a job from running more frequently than a configured minimum
interval by persisting the last-run timestamp to a small state file.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


_DEFAULT_STATE_DIR = Path(os.environ.get("CRONWRAP_STATE_DIR", "/tmp/cronwrap"))


class ThrottleViolation(Exception):
    """Raised when a job is invoked before its minimum interval has elapsed."""

    def __init__(self, job_name: str, remaining: float) -> None:
        self.job_name = job_name
        self.remaining = remaining
        super().__init__(
            f"Job '{job_name}' throttled — {remaining:.1f}s remaining before next allowed run."
        )


@dataclass
class ThrottleConfig:
    """Configuration for job throttling."""

    min_interval_seconds: float
    """Minimum number of seconds that must pass between successful runs."""

    state_dir: Path = field(default_factory=lambda: _DEFAULT_STATE_DIR)
    """Directory where per-job last-run state files are stored."""

    raise_on_throttle: bool = True
    """If True, raise ThrottleViolation; if False, silently skip (return False)."""

    def __post_init__(self) -> None:
        if self.min_interval_seconds <= 0:
            raise ValueError("min_interval_seconds must be positive.")
        self.state_dir = Path(self.state_dir)


class ThrottleGuard:
    """Checks and records job run timestamps to enforce throttling."""

    def __init__(self, job_name: str, config: ThrottleConfig) -> None:
        self._job_name = job_name
        self._config = config
        self._state_file = config.state_dir / f"{job_name}.throttle.json"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self) -> bool:
        """Return True if the job is allowed to run now.

        Raises ThrottleViolation (or returns False) when the minimum
        interval has not yet elapsed.
        """
        last_run = self._read_last_run()
        if last_run is None:
            return True

        elapsed = time.time() - last_run
        remaining = self._config.min_interval_seconds - elapsed
        if remaining > 0:
            if self._config.raise_on_throttle:
                raise ThrottleViolation(self._job_name, remaining)
            return False
        return True

    def record(self) -> None:
        """Persist the current timestamp as the last successful run time."""
        self._config.state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(
            json.dumps({"job": self._job_name, "last_run": time.time()})
        )

    def reset(self) -> None:
        """Remove stored state, allowing the job to run immediately."""
        if self._state_file.exists():
            self._state_file.unlink()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_last_run(self) -> Optional[float]:
        if not self._state_file.exists():
            return None
        try:
            data = json.loads(self._state_file.read_text())
            return float(data["last_run"])
        except (KeyError, ValueError, json.JSONDecodeError):
            return None
