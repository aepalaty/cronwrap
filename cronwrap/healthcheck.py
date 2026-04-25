"""Healthcheck reporting for cron jobs — push a heartbeat to a URL on success."""
from __future__ import annotations

import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class HealthcheckConfig:
    """Configuration for a healthcheck endpoint."""

    url: str
    timeout_seconds: float = 10.0
    ping_on_start: bool = False
    ping_on_failure: bool = True
    # Optional override: receives the full URL and returns None
    sender: Optional[Callable[[str, float], None]] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("HealthcheckConfig.url must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("HealthcheckConfig.timeout_seconds must be positive")


def _default_sender(url: str, timeout: float) -> None:
    """Send a GET request to *url*, ignoring the response body."""
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout):
        pass


class HealthcheckReporter:
    """Sends heartbeat pings based on job lifecycle events."""

    def __init__(self, config: HealthcheckConfig) -> None:
        self._config = config
        self._send = config.sender or _default_sender

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def ping_start(self) -> None:
        """Ping the /start sub-path (if ping_on_start is enabled)."""
        if self._config.ping_on_start:
            self._ping(self._config.url.rstrip("/") + "/start")

    def ping_success(self) -> None:
        """Ping the base URL to signal a successful run."""
        self._ping(self._config.url)

    def ping_failure(self, exit_code: int = 1) -> None:
        """Ping the /fail sub-path (if ping_on_failure is enabled)."""
        if self._config.ping_on_failure:
            self._ping(self._config.url.rstrip("/") + "/fail")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ping(self, url: str) -> None:
        try:
            self._send(url, self._config.timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            # Healthcheck failures must never crash the job itself.
            import warnings
            warnings.warn(f"Healthcheck ping failed ({url}): {exc}", RuntimeWarning, stacklevel=2)
