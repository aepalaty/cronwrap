"""Structured logging for cron job execution."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """Formats log records as JSON for structured output."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        if hasattr(record, "job_name"):
            log_data["job_name"] = record.job_name
        if hasattr(record, "attempt"):
            log_data["attempt"] = record.attempt
        if hasattr(record, "duration_seconds"):
            log_data["duration_seconds"] = record.duration_seconds
        if hasattr(record, "exit_code"):
            log_data["exit_code"] = record.exit_code
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


class CronLogger:
    """Logger wrapper that injects job context into every log record."""

    def __init__(self, job_name: str, structured: bool = True) -> None:
        self.job_name = job_name
        self._logger = logging.getLogger(f"cronwrap.{job_name}")

        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            if structured:
                handler.setFormatter(StructuredFormatter())
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.DEBUG)
            self._logger.propagate = False

    def _extra(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        base = {"job_name": self.job_name}
        if extra:
            base["extra_fields"] = extra
        return base

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(message, extra=self._extra(kwargs or None))

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(message, extra=self._extra(kwargs or None))

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(message, extra=self._extra(kwargs or None))

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug(message, extra=self._extra(kwargs or None))

    def log_start(self, attempt: int = 1) -> None:
        self._logger.info(
            "Job started",
            extra={"job_name": self.job_name, "attempt": attempt},
        )

    def log_end(
        self,
        exit_code: int,
        duration_seconds: float,
        attempt: int = 1,
    ) -> None:
        level = logging.INFO if exit_code == 0 else logging.ERROR
        self._logger.log(
            level,
            "Job finished",
            extra={
                "job_name": self.job_name,
                "attempt": attempt,
                "exit_code": exit_code,
                "duration_seconds": round(duration_seconds, 4),
            },
        )
