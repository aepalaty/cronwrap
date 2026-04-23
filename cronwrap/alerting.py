"""Alerting module for cronwrap — sends notifications on job failure or threshold breach."""

import logging
import smtplib
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Callable

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    """Configuration for alerting on cron job events."""

    recipients: List[str] = field(default_factory=list)
    smtp_host: str = "localhost"
    smtp_port: int = 25
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    from_address: str = "cronwrap@localhost"
    alert_on_failure: bool = True
    alert_on_timeout: bool = True
    alert_on_retry_exhausted: bool = True
    subject_prefix: str = "[cronwrap]"


class AlertManager:
    """Manages alert dispatch for cron job lifecycle events."""

    def __init__(self, config: AlertConfig):
        self.config = config
        self._custom_handlers: List[Callable[[str, str], None]] = []

    def add_handler(self, handler: Callable[[str, str], None]) -> None:
        """Register a custom alert handler callable(subject, body)."""
        self._custom_handlers.append(handler)

    def send(self, subject: str, body: str) -> None:
        """Dispatch alert via all configured channels."""
        full_subject = f"{self.config.subject_prefix} {subject}"
        if self.config.recipients:
            self._send_email(full_subject, body)
        for handler in self._custom_handlers:
            try:
                handler(full_subject, body)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Custom alert handler raised an exception: %s", exc)

    def _send_email(self, subject: str, body: str) -> None:
        msg = MIMEMultipart()
        msg["From"] = self.config.from_address
        msg["To"] = ", ".join(self.config.recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        try:
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                if self.config.smtp_user and self.config.smtp_password:
                    server.login(self.config.smtp_user, self.config.smtp_password)
                server.sendmail(
                    self.config.from_address,
                    self.config.recipients,
                    msg.as_string(),
                )
            logger.info("Alert email sent to %s", self.config.recipients)
        except smtplib.SMTPException as exc:
            logger.error("Failed to send alert email: %s", exc)
