"""Shared rate limiter for bulk runs. Stdlib only, thread-safe."""
import threading
import time
from typing import Optional


class RateLimiter:
    """Enforces a max sustained request rate across worker threads.

    - `rps`: target requests/second (e.g. 8.0). <=0 disables pacing.
    - `cooldown_until`: timestamp until which all callers sleep
      (set when a provider reports HTTP 429 / IP throttling).
    Call `wait()` before every HTTP request.
    """

    def __init__(self, rps: float = 0.0):
        self.min_interval = (1.0 / rps) if rps and rps > 0 else 0.0
        self._lock = threading.Lock()
        self._next_allowed = 0.0
        self._cooldown_until = 0.0

    def cool_down(self, seconds: float) -> None:
        """Force all workers to pause (e.g. after a 429)."""
        with self._lock:
            self._cooldown_until = max(
                self._cooldown_until, time.monotonic() + max(0.0, seconds))

    def wait(self, stop: Optional[threading.Event] = None) -> bool:
        """Block until a request may proceed. Returns False if `stop` was
        set while waiting (caller should abandon the request so --abort-after
        takes effect promptly instead of sleeping out cooldowns)."""
        while True:
            if stop is not None and stop.is_set():
                return False
            with self._lock:
                now = time.monotonic()
                target = max(self._next_allowed, self._cooldown_until)
                delay = target - now
                if delay <= 0:
                    self._next_allowed = now + self.min_interval
                    return True
            # sleep outside the lock so cool_down() can extend the wait
            if stop is not None:
                if stop.wait(timeout=min(delay, 0.2)):
                    return False
            else:
                time.sleep(min(delay, 5.0))
