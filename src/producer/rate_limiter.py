import time
from typing import Optional


class StreamingRateLimiter:
    """
    Controls transaction streaming rate to simulate real-time event feeds.
    Supports fixed delay per record or target transactions-per-second (TPS).
    """

    def __init__(
        self,
        delay_seconds: float = 0.0,
        events_per_second: float = 0.0,
    ):
        self.delay_seconds = max(0.0, delay_seconds)
        self.events_per_second = max(0.0, events_per_second)
        
        # If events_per_second is specified and > 0, calculate delay interval
        if self.events_per_second > 0:
            self._target_interval = 1.0 / self.events_per_second
        elif self.delay_seconds > 0:
            self._target_interval = self.delay_seconds
        else:
            self._target_interval = 0.0

        self._last_event_time = 0.0

    def pace(self) -> None:
        """
        Sleeps if necessary to maintain the configured streaming rate.
        """
        if self._target_interval <= 0:
            return

        now = time.perf_counter()
        elapsed = now - self._last_event_time
        remaining = self._target_interval - elapsed

        if remaining > 0:
            time.sleep(remaining)

        self._last_event_time = time.perf_counter()
