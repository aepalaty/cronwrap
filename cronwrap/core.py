"""Core module for cronwrap — wraps cron job execution with logging, alerting, and retry logic."""

import subprocess
import time
import logging
import sys
from datetime import datetime
from typing import Optional, List

logger = logging.getLogger(__name__)


class CronJobResult:
    """Holds the result of a cron job execution."""

    def __init__(
        self,
        command: str,
        exit_code: int,
        stdout: str,
        stderr: str,
        duration_seconds: float,
        attempt: int,
        started_at: datetime,
        finished_at: datetime,
    ):
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.duration_seconds = duration_seconds
        self.attempt = attempt
        self.started_at = started_at
        self.finished_at = finished_at
        self.success = exit_code == 0

    def __repr__(self) -> str:
        status = "SUCCESS" if self.success else f"FAILED(exit={self.exit_code})"
        return (
            f"<CronJobResult command={self.command!r} status={status} "
            f"duration={self.duration_seconds:.2f}s attempt={self.attempt}>"
        )


class CronWrapper:
    """
    Wraps a shell command for use as a cron job.

    Features:
    - Structured logging of execution details
    - Configurable retry logic with backoff
    - Timeout enforcement
    - Hook support for alerting on failure or success
    """

    def __init__(
        self,
        command: List[str],
        retries: int = 0,
        retry_delay: float = 5.0,
        timeout: Optional[float] = None,
        job_name: Optional[str] = None,
    ):
        """
        Initialize the CronWrapper.

        Args:
            command: The command to run as a list of strings (e.g. ['python', 'myscript.py']).
            retries: Number of times to retry on failure (0 means no retries).
            retry_delay: Seconds to wait between retries.
            timeout: Maximum seconds to allow the command to run before killing it.
            job_name: Optional human-readable name for this job used in logs.
        """
        if not command:
            raise ValueError("command must be a non-empty list of strings")
        if retries < 0:
            raise ValueError("retries must be a non-negative integer")
        if retry_delay < 0:
            raise ValueError("retry_delay must be a non-negative number")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be a positive number")
        self.command = command
        self.retries = retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.job_name = job_name or " ".join(command)

    def run(self) -> CronJobResult:
        """
        Execute the wrapped command, applying retry logic if configured.

        Returns:
            CronJobResult for the final attempt (successful or last failure).
        """
        last_result: Optional[CronJobResult] = None
        max_attempts = self.retries + 1

        for attempt in range(1, max_attempts + 1):
            logger.info(
                "Starting job",
                extra={
                    "job_name": self.job_name,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "command": self.command,
                },
            )

          