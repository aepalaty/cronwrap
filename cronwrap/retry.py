"""Retry logic for cronwrap — configurable backoff and attempt tracking."""

import logging
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """Defines how a cron job should be retried on failure."""

    max_attempts: int = 3
    delay_seconds: float = 5.0
    backoff_factor: float = 2.0
    max_delay_seconds: float = 300.0
    retry_on_timeout: bool = False

    def delay_for_attempt(self, attempt: int) -> float:
        """Return the delay in seconds for the given attempt number (1-indexed)."""
        if attempt <= 1:
            return self.delay_seconds
        delay = self.delay_seconds * (self.backoff_factor ** (attempt - 1))
        return min(delay, self.max_delay_seconds)


class RetryState:
    """Tracks retry state for a single job execution."""

    def __init__(self, policy: RetryPolicy):
        self.policy = policy
        self.attempt: int = 0
        self.exhausted: bool = False

    @property
    def remaining(self) -> int:
        return max(0, self.policy.max_attempts - self.attempt)

    def should_retry(self, timed_out: bool = False) -> bool:
        if timed_out and not self.policy.retry_on_timeout:
            return False
        return self.attempt < self.policy.max_attempts

    def record_attempt(self) -> None:
        self.attempt += 1
        if self.attempt >= self.policy.max_attempts:
            self.exhausted = True

    def wait(self) -> None:
        """Sleep for the appropriate backoff delay before the next attempt."""
        delay = self.policy.delay_for_attempt(self.attempt)
        logger.debug(
            "Retry attempt %d/%d — waiting %.1fs before next run.",
            self.attempt,
            self.policy.max_attempts,
            delay,
        )
        time.sleep(delay)

    def __repr__(self) -> str:
        return (
            f"RetryState(attempt={self.attempt}, "
            f"max={self.policy.max_attempts}, exhausted={self.exhausted})"
        )
