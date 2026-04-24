"""Timeout enforcement for cron job execution."""

import signal
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional


class TimeoutExpired(Exception):
    """Raised when a cron job exceeds its allowed execution time."""

    def __init__(self, seconds: float):
        self.seconds = seconds
        super().__init__(f"Job timed out after {seconds}s")


@dataclass
class TimeoutConfig:
    """Configuration for job timeout behaviour."""

    seconds: float
    kill_on_timeout: bool = True
    # Optional callback invoked just before the TimeoutExpired is raised.
    on_timeout: Optional[callable] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError("timeout seconds must be positive")


class _TimeoutContext:
    """Internal helper that works on both Unix (SIGALRM) and other platforms."""

    def __init__(self, config: TimeoutConfig) -> None:
        self._config = config
        self._timed_out = False
        self._use_signal = hasattr(signal, "SIGALRM")

    # ------------------------------------------------------------------
    # Signal-based implementation (Unix only, main thread only)
    # ------------------------------------------------------------------
    def _alarm_handler(self, signum, frame):  # noqa: ARG002
        self._timed_out = True
        if self._config.on_timeout:
            try:
                self._config.on_timeout(self._config.seconds)
            except Exception:  # noqa: BLE001
                pass
        raise TimeoutExpired(self._config.seconds)

    @contextmanager
    def _signal_timeout(self):
        old_handler = signal.signal(signal.SIGALRM, self._alarm_handler)
        signal.setitimer(signal.ITIMER_REAL, self._config.seconds)
        try:
            yield self
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)

    # ------------------------------------------------------------------
    # Thread-based fallback (Windows / background threads)
    # ------------------------------------------------------------------
    @contextmanager
    def _thread_timeout(self):
        timer: threading.Timer

        def _expire():
            self._timed_out = True
            if self._config.on_timeout:
                try:
                    self._config.on_timeout(self._config.seconds)
                except Exception:  # noqa: BLE001
                    pass

        timer = threading.Timer(self._config.seconds, _expire)
        timer.daemon = True
        timer.start()
        try:
            yield self
            if self._timed_out:
                raise TimeoutExpired(self._config.seconds)
        finally:
            timer.cancel()
            if self._timed_out:
                raise TimeoutExpired(self._config.seconds)

    @contextmanager
    def enforce(self):
        if self._use_signal and threading.current_thread() is threading.main_thread():
            with self._signal_timeout():
                yield self
        else:
            with self._thread_timeout():
                yield self


@contextmanager
def enforce_timeout(config: TimeoutConfig):
    """Context manager that raises TimeoutExpired if the block exceeds config.seconds."""
    ctx = _TimeoutContext(config)
    with ctx.enforce():
        yield ctx
