"""Unit tests for cronwrap.healthcheck."""
import warnings
import pytest

from cronwrap.healthcheck import HealthcheckConfig, HealthcheckReporter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def pings() -> list[str]:
    return []


@pytest.fixture()
def config(pings: list[str]) -> HealthcheckConfig:
    def _fake_sender(url: str, timeout: float) -> None:
        pings.append(url)

    return HealthcheckConfig(
        url="https://hc.example.com/abc123",
        sender=_fake_sender,
    )


@pytest.fixture()
def reporter(config: HealthcheckConfig) -> HealthcheckReporter:
    return HealthcheckReporter(config)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def test_config_rejects_empty_url() -> None:
    with pytest.raises(ValueError, match="url"):
        HealthcheckConfig(url="")


def test_config_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError, match="timeout"):
        HealthcheckConfig(url="https://x.com", timeout_seconds=0)


def test_config_defaults() -> None:
    cfg = HealthcheckConfig(url="https://x.com")
    assert cfg.timeout_seconds == 10.0
    assert cfg.ping_on_start is False
    assert cfg.ping_on_failure is True


# ---------------------------------------------------------------------------
# Reporter behaviour
# ---------------------------------------------------------------------------

def test_ping_success_calls_base_url(reporter: HealthcheckReporter, pings: list[str]) -> None:
    reporter.ping_success()
    assert pings == ["https://hc.example.com/abc123"]


def test_ping_start_skipped_by_default(reporter: HealthcheckReporter, pings: list[str]) -> None:
    reporter.ping_start()
    assert pings == []


def test_ping_start_when_enabled(pings: list[str]) -> None:
    def _sender(url: str, _t: float) -> None:
        pings.append(url)

    r = HealthcheckReporter(HealthcheckConfig(url="https://hc.example.com/j", ping_on_start=True, sender=_sender))
    r.ping_start()
    assert pings == ["https://hc.example.com/j/start"]


def test_ping_failure_appends_fail_path(reporter: HealthcheckReporter, pings: list[str]) -> None:
    reporter.ping_failure()
    assert pings == ["https://hc.example.com/abc123/fail"]


def test_ping_failure_skipped_when_disabled(pings: list[str]) -> None:
    def _sender(url: str, _t: float) -> None:
        pings.append(url)

    r = HealthcheckReporter(
        HealthcheckConfig(url="https://hc.example.com/j", ping_on_failure=False, sender=_sender)
    )
    r.ping_failure()
    assert pings == []


def test_sender_exception_raises_warning(pings: list[str]) -> None:
    def _bad_sender(url: str, _t: float) -> None:
        raise OSError("network down")

    r = HealthcheckReporter(HealthcheckConfig(url="https://hc.example.com/j", sender=_bad_sender))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        r.ping_success()
    assert any("network down" in str(w.message) for w in caught)
