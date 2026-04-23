"""Execution context passed through the cron job lifecycle."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class ExecutionContext:
    """Holds metadata about a single cron job execution attempt."""

    job_name: str
    attempt: int = 1
    started_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    ended_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Return elapsed seconds if the job has finished."""
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0

    def finish(self, exit_code: int, stdout: str = "", stderr: str = "") -> None:
        """Mark the execution as complete."""
        self.ended_at = datetime.now(timezone.utc)
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_name": self.job_name,
            "attempt": self.attempt,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "exit_code": self.exit_code,
            "succeeded": self.succeeded,
            "stdout_length": len(self.stdout),
            "stderr_length": len(self.stderr),
            "metadata": self.metadata,
        }

    def __repr__(self) -> str:
        return (
            f"ExecutionContext(job={self.job_name!r}, attempt={self.attempt}, "
            f"exit_code={self.exit_code})"
        )
