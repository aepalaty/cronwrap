"""Tests for cronwrap.logging structured logger."""

import json
import logging
import io

import pytest

from cronwrap.logging import CronLogger, StructuredFormatter


@pytest.fixture()
def stream_logger(tmp_path):
    """Return a CronLogger whose output is captured in a StringIO buffer."""
    logger = CronLogger(job_name="test_job", structured=True)
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(StructuredFormatter())
    # Replace existing handlers
    logger._logger.handlers = [handler]
    return logger, buf


def _last_record(buf) -> dict:
    buf.seek(0)
    lines = [l for l in buf.read().splitlines() if l.strip()]
    return json.loads(lines[-1])


def test_info_message_is_json(stream_logger):
    logger, buf = stream_logger
    logger.info("hello world")
    record = _last_record(buf)
    assert record["message"] == "hello world"
    assert record["level"] == "INFO"
    assert "timestamp" in record


def test_job_name_injected(stream_logger):
    logger, buf = stream_logger
    logger.info("check name")
    record = _last_record(buf)
    assert record["job_name"] == "test_job"


def test_extra_kwargs_nested(stream_logger):
    logger, buf = stream_logger
    logger.warning("disk full", disk_usage=0.99)
    record = _last_record(buf)
    assert record["extra_fields"]["disk_usage"] == 0.99


def test_log_start_includes_attempt(stream_logger):
    logger, buf = stream_logger
    logger.log_start(attempt=3)
    record = _last_record(buf)
    assert record["attempt"] == 3
    assert record["message"] == "Job started"


def test_log_end_success(stream_logger):
    logger, buf = stream_logger
    logger.log_end(exit_code=0, duration_seconds=1.234, attempt=1)
    record = _last_record(buf)
    assert record["exit_code"] == 0
    assert record["duration_seconds"] == 1.234
    assert record["level"] == "INFO"


def test_log_end_failure_uses_error_level(stream_logger):
    logger, buf = stream_logger
    logger.log_end(exit_code=1, duration_seconds=0.5)
    record = _last_record(buf)
    assert record["level"] == "ERROR"
    assert record["exit_code"] == 1
