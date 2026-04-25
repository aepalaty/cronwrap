"""Rate limiting for cron jobs — prevents a job from running more than N times
within a rolling time window."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


class RateLimitExceeded(Exception):
    """Raised when a job has exceeded its allowed run rate."""

    def __init__(self, job_name: str, limit: int, window_seconds: int) -> None:
        self.job_name = job_name
        self.limit = limit
        self.window_seconds = window_seconds
        super().__init__(
            f"Job '{job_name}' exceeded rate limit: "
            f"{limit} runs per {window_seconds}s window."
        )


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    limit: int = 1
    window_seconds: int = 3600
    state_dir: str = "/tmp/cronwrap/ratelimit"

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be >= 1")
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")


class RateLimitGuard:
    """Tracks and enforces a rolling-window rate limit for a named job."""

    def __init__(self, job_name: str, config: RateLimitConfig) -> None:
        self.job_name = job_name
        self.config = config
        self._state_path = (
            Path(config.state_dir) / f"{job_name}.ratelimit.json"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self) -> None:
        """Raise RateLimitExceeded if the job has hit its limit."""
        timestamps = self._load_timestamps()
        cutoff = time.time() - self.config.window_seconds
        recent = [t for t in timestamps if t >= cutoff]
        if len(recent) >= self.config.limit:
            raise RateLimitExceeded(
                self.job_name, self.config.limit, self.config.window_seconds
            )

    def record(self) -> None:
        """Record a run timestamp (call after a successful check)."""
        timestamps = self._load_timestamps()
        cutoff = time.time() - self.config.window_seconds
        recent = [t for t in timestamps if t >= cutoff]
        recent.append(time.time())
        self._save_timestamps(recent)

    def reset(self) -> None:
        """Clear all recorded timestamps."""
        if self._state_path.exists():
            self._state_path.unlink()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_timestamps(self) -> List[float]:
        if not self._state_path.exists():
            return []
        try:
            return json.loads(self._state_path.read_text())
        except (json.JSONDecodeError, OSError):
            return []

    def _save_timestamps(self, timestamps: List[float]) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(json.dumps(timestamps))
