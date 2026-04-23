"""Tests for cronwrap.alerting module."""

import pytest
from unittest.mock import MagicMock, patch

from cronwrap.alerting import AlertConfig, AlertManager


@pytest.fixture()
def config():
    return AlertConfig(
        recipients=["ops@example.com"],
        smtp_host="smtp.example.com",
        smtp_port=587,
        from_address="cronwrap@example.com",
    )


@pytest.fixture()
def manager(config):
    return AlertManager(config)


def test_alert_config_defaults():
    cfg = AlertConfig()
    assert cfg.alert_on_failure is True
    assert cfg.alert_on_timeout is True
    assert cfg.subject_prefix == "[cronwrap]"
    assert cfg.recipients == []


def test_custom_handler_called(manager):
    handler = MagicMock()
    manager.add_handler(handler)
    manager.send("Job failed", "Exit code 1")
    handler.assert_called_once()
    subject, body = handler.call_args[0]
    assert "[cronwrap]" in subject
    assert "Job failed" in subject
    assert body == "Exit code 1"


def test_multiple_custom_handlers(manager):
    h1, h2 = MagicMock(), MagicMock()
    manager.add_handler(h1)
    manager.add_handler(h2)
    manager.send("Test", "body")
    h1.assert_called_once()
    h2.assert_called_once()


def test_custom_handler_exception_does_not_propagate(manager):
    def bad_handler(subject, body):
        raise RuntimeError("handler error")

    manager.add_handler(bad_handler)
    # Should not raise
    manager.send("Test", "body")


@patch("cronwrap.alerting.smtplib.SMTP")
def test_send_email_called(mock_smtp, manager):
    ctx = MagicMock()
    mock_smtp.return_value.__enter__ = MagicMock(return_value=ctx)
    mock_smtp.return_value.__exit__ = MagicMock(return_value=False)
    manager.send("Failure", "Something went wrong")
    mock_smtp.assert_called_once_with("smtp.example.com", 587)


def test_no_email_sent_when_no_recipients():
    cfg = AlertConfig(recipients=[])
    mgr = AlertManager(cfg)
    handler = MagicMock()
    mgr.add_handler(handler)
    with patch("cronwrap.alerting.smtplib.SMTP") as mock_smtp:
        mgr.send("Test", "body")
        mock_smtp.assert_not_called()
    handler.assert_called_once()
