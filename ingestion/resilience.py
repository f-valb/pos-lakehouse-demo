"""Client-side protections for a fragile upstream.

* CircuitBreaker   per region: stop calling a region that keeps failing, probe after a cooldown.
* AimdLimiter      global: additive-increase / multiplicative-decrease on concurrency, so the
                   poller backs off as soon as the API signals overload (503/500/truncation).
* TokenBucket      per region: caps request rate so one region cannot starve the others.
* AdaptiveWindow   per region: shrink the pull window when records/latency/errors go up
                   (peak hours), grow it again when the API is healthy.
* backoff_delay    exponential backoff with full jitter.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field


def backoff_delay(attempt: int, base: float = 0.5, cap: float = 8.0) -> float:
    return random.uniform(0, min(cap, base * (2**attempt)))


@dataclass
class CircuitBreaker:
    failure_threshold: int = 3
    base_cooldown_s: float = 30.0
    max_cooldown_s: float = 300.0
    state: str = "closed"  # closed | open | half_open
    failures: int = 0
    opened_at: float = 0.0
    cooldown_s: float = 30.0
    probe_in_flight: bool = False
    clock: callable = field(default=time.time, repr=False)

    def allow(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open" and self.clock() - self.opened_at >= self.cooldown_s:
            self.state = "half_open"
        if self.state == "half_open" and not self.probe_in_flight:
            self.probe_in_flight = True
            return True
        return False

    def remaining_open_s(self) -> float:
        return max(0.0, self.cooldown_s - (self.clock() - self.opened_at)) if self.state == "open" else 0.0

    def record_success(self) -> None:
        self.state, self.failures, self.probe_in_flight = "closed", 0, False
        self.cooldown_s = self.base_cooldown_s

    def record_failure(self) -> None:
        self.probe_in_flight = False
        if self.state == "half_open":
            self._open(min(self.max_cooldown_s, self.cooldown_s * 2))
            return
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self._open(self.base_cooldown_s)

    def _open(self, cooldown: float) -> None:
        self.state, self.opened_at, self.cooldown_s = "open", self.clock(), cooldown

    def snapshot(self) -> dict:
        return {"state": self.state, "failures": self.failures, "cooldown_s": self.cooldown_s,
                "remaining_open_s": round(self.remaining_open_s(), 1)}


class AimdLimiter:
    """Concurrency limit that halves on overload signals and creeps back up on success."""

    def __init__(self, max_limit: int, min_limit: int = 1, increase_every: int = 5):
        self.max_limit, self.min_limit, self.increase_every = max_limit, min_limit, increase_every
        self.limit = max_limit
        self.in_use = 0
        self._ok_streak = 0
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._cond:
            await self._cond.wait_for(lambda: self.in_use < self.limit)
            self.in_use += 1

    async def release(self, overloaded: bool) -> None:
        async with self._cond:
            self.in_use -= 1
            if overloaded:
                self.limit = max(self.min_limit, self.limit // 2)
                self._ok_streak = 0
            else:
                self._ok_streak += 1
                if self._ok_streak >= self.increase_every and self.limit < self.max_limit:
                    self.limit += 1
                    self._ok_streak = 0
            self._cond.notify_all()


class TokenBucket:
    def __init__(self, rate_per_s: float, burst: float = 1.0):
        self.rate, self.capacity = rate_per_s, burst
        self.tokens, self.updated = burst, time.monotonic()
        self._lock = asyncio.Lock()

    async def take(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.rate)


@dataclass
class AdaptiveWindow:
    """Window sizing from feedback. Starts at 5 min; at peak it shrinks toward 1 min."""

    size_s: int = 300
    min_s: int = 60
    max_s: int = 600
    target_records: int = 15000
    slow_latency_s: float = 3.0

    def on_success(self, records: int, window_s: int, latency_s: float) -> None:
        rate = max(records, 1) / max(window_s, 1)
        ideal = self.target_records / rate
        if latency_s > self.slow_latency_s:
            ideal = min(ideal, self.size_s * 0.5)
        # Smooth growth (max +50% per pull, at least one minute so we can leave the floor),
        # but allow immediate shrink.
        new = min(ideal, max(self.size_s * 1.5, self.size_s + 60))
        self.size_s = self._clamp(new)

    def on_overload(self) -> None:
        self.size_s = self._clamp(self.size_s * 0.5)

    def _clamp(self, v: float) -> int:
        v = max(self.min_s, min(self.max_s, v))
        return int(v // 60 * 60) or self.min_s
