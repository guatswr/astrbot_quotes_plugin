from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from math import ceil
from time import monotonic
from typing import Hashable


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    notify: bool = False
    retry_after: int = 0


class SlidingWindowRateLimiter:
    """Per-key sliding-window limiter that warns once per blocked period."""

    def __init__(self, *, limit: int, window_seconds: float):
        self.limit = max(1, int(limit))
        self.window_seconds = max(1.0, float(window_seconds))
        self._events: dict[Hashable, deque[float]] = {}
        self._blocked_until: dict[Hashable, float] = {}
        self._last_cleanup = 0.0

    def check(self, key: Hashable, *, now: float | None = None) -> RateLimitResult:
        current = monotonic() if now is None else float(now)
        self._cleanup(current)

        blocked_until = self._blocked_until.get(key, 0.0)
        if blocked_until > current:
            return RateLimitResult(
                allowed=False,
                notify=False,
                retry_after=max(1, ceil(blocked_until - current)),
            )
        self._blocked_until.pop(key, None)

        events = self._events.setdefault(key, deque())
        cutoff = current - self.window_seconds
        while events and events[0] <= cutoff:
            events.popleft()

        if len(events) < self.limit:
            events.append(current)
            return RateLimitResult(allowed=True)

        blocked_until = events[0] + self.window_seconds
        self._blocked_until[key] = blocked_until
        return RateLimitResult(
            allowed=False,
            notify=True,
            retry_after=max(1, ceil(blocked_until - current)),
        )

    def _cleanup(self, now: float) -> None:
        if now - self._last_cleanup < self.window_seconds:
            return
        cutoff = now - self.window_seconds
        for key, events in list(self._events.items()):
            while events and events[0] <= cutoff:
                events.popleft()
            if not events:
                self._events.pop(key, None)
        for key, blocked_until in list(self._blocked_until.items()):
            if blocked_until <= now:
                self._blocked_until.pop(key, None)
        self._last_cleanup = now
