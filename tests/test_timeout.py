"""Tests for cronwrap.timeout."""

import time
import pytest

from cronwrap.timeout import (
    TimeoutConfig,
    TimeoutExpired,
    enforce_timeout,
)


# ---------------------------------------------------------------------------
# TimeoutConfig
# ---------------------------------------------------------------------------

def test_timeout_config_defaults():
    cfg = TimeoutConfig(seconds=30)
    assert cfg.seconds == 30
    assert cfg.kill_on_timeout is True
    assert cfg.on_timeout is None


def test_timeout_config_rejects_non_positive():
    with pytest.raises(ValueError, match="positive"):
        TimeoutConfig(seconds=0)

    with pytest.raises(ValueError, match="positive"):
        TimeoutConfig(seconds=-5)


def test_timeout_config_custom_callback():
    called_with = []
    cfg = TimeoutConfig(seconds=1, on_timeout=lambda s: called_with.append(s))
    assert cfg.on_timeout is not None
    cfg.on_timeout(1)
    assert called_with == [1]


# ---------------------------------------------------------------------------
# enforce_timeout — happy path (no timeout)
# ---------------------------------------------------------------------------

def test_no_timeout_when_fast():
    cfg = TimeoutConfig(seconds=5)
    with enforce_timeout(cfg):
        result = 1 + 1
    assert result == 2


def test_return_value_unaffected():
    cfg = TimeoutConfig(seconds=5)
    output = []
    with enforce_timeout(cfg):
        output.append("done")
    assert output == ["done"]


# ---------------------------------------------------------------------------
# enforce_timeout — timeout fires
# ---------------------------------------------------------------------------

def test_timeout_raises_timeout_expired():
    cfg = TimeoutConfig(seconds=0.1)
    with pytest.raises(TimeoutExpired) as exc_info:
        with enforce_timeout(cfg):
            time.sleep(5)  # should be interrupted
    assert exc_info.value.seconds == pytest.approx(0.1)


def test_timeout_expired_message():
    err = TimeoutExpired(seconds=42)
    assert "42" in str(err)


def test_on_timeout_callback_invoked():
    fired = []
    cfg = TimeoutConfig(seconds=0.1, on_timeout=lambda s: fired.append(s))
    with pytest.raises(TimeoutExpired):
        with enforce_timeout(cfg):
            time.sleep(5)
    assert fired == [pytest.approx(0.1)]


# ---------------------------------------------------------------------------
# TimeoutExpired attributes
# ---------------------------------------------------------------------------

def test_timeout_expired_stores_seconds():
    exc = TimeoutExpired(7.5)
    assert exc.seconds == 7.5
    assert isinstance(exc, Exception)
