"""Tests for cronwrap.retry module."""

import pytest
from unittest.mock import patch

from cronwrap.retry import RetryPolicy, RetryState


def test_policy_defaults():
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.delay_seconds == 5.0
    assert policy.backoff_factor == 2.0
    assert policy.retry_on_timeout is False


def test_delay_for_first_attempt():
    policy = RetryPolicy(delay_seconds=10.0)
    assert policy.delay_for_attempt(1) == 10.0


def test_delay_increases_with_backoff():
    policy = RetryPolicy(delay_seconds=5.0, backoff_factor=2.0)
    assert policy.delay_for_attempt(2) == 10.0
    assert policy.delay_for_attempt(3) == 20.0


def test_delay_capped_at_max():
    policy = RetryPolicy(delay_seconds=5.0, backoff_factor=10.0, max_delay_seconds=50.0)
    assert policy.delay_for_attempt(5) == 50.0


def test_retry_state_initial():
    state = RetryState(RetryPolicy(max_attempts=3))
    assert state.attempt == 0
    assert state.exhausted is False
    assert state.remaining == 3


def test_should_retry_before_exhaustion():
    state = RetryState(RetryPolicy(max_attempts=3))
    assert state.should_retry() is True


def test_record_attempt_increments():
    state = RetryState(RetryPolicy(max_attempts=3))
    state.record_attempt()
    assert state.attempt == 1
    assert state.remaining == 2
    assert state.exhausted is False


def test_exhausted_after_max_attempts():
    policy = RetryPolicy(max_attempts=2)
    state = RetryState(policy)
    state.record_attempt()
    state.record_attempt()
    assert state.exhausted is True
    assert state.should_retry() is False


def test_no_retry_on_timeout_by_default():
    state = RetryState(RetryPolicy(max_attempts=3, retry_on_timeout=False))
    assert state.should_retry(timed_out=True) is False


def test_retry_on_timeout_when_enabled():
    state = RetryState(RetryPolicy(max_attempts=3, retry_on_timeout=True))
    assert state.should_retry(timed_out=True) is True


def test_wait_calls_sleep():
    state = RetryState(RetryPolicy(delay_seconds=5.0))
    state.record_attempt()
    with patch("cronwrap.retry.time.sleep") as mock_sleep:
        state.wait()
        mock_sleep.assert_called_once()
        args = mock_sleep.call_args[0]
        assert args[0] > 0


def test_repr():
    state = RetryState(RetryPolicy(max_attempts=5))
    r = repr(state)
    assert "RetryState" in r
    assert "max=5" in r
