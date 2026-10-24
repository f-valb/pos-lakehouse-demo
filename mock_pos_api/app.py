"""Mock of the fragile, pull-only legacy POS REST API.

GET /transactions?region=PH&from=2026-10-24T10:00:00Z&to=2026-10-24T10:05:00Z
    -> text/plain, pipe-delimited, header line + records + "TRL|<count>" trailer.

There is deliberately no CDC, no "updated since" filter and no paging: the only way to see a
late void is to re-pull the window. The service degrades the way the real one does:

* in-flight requests above `soft_limit` -> slow responses, random 503s and truncated bodies
* in-flight requests above `hard_limit` -> the process "crashes": every request returns 500
  for `crash_seconds` (simulates the JVM falling over and being restarted)
* peak mode: store traffic eats capacity, so both limits drop and latency doubles
* per-region outage switch for the failure-injection demo

Admin endpoints (demo only): POST /admin/peak, POST /admin/fail, POST /admin/reset, GET /admin/stats
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse

from common.regions import REGIONS
from mock_pos_api.generator import header, window_lines

MAX_WINDOW_MIN = 60


@dataclass
class ApiState:
    total_rpm: int = int(os.environ.get("POS_API_RECORDS_PER_MINUTE", "50000"))
    soft_limit: int = 6
    hard_limit: int = 10
    peak_soft_limit: int = 3
    peak_hard_limit: int = 6
    crash_seconds: float = 20.0
    peak: bool = False
    failing_regions: dict[str, str] = field(default_factory=dict)  # region -> "error" | "timeout"
    in_flight: int = 0
    max_in_flight: int = 0
    crashed_until: float = 0.0
    crashes: int = 0
    counts: Counter = field(default_factory=Counter)
    recent: deque = field(default_factory=lambda: deque(maxlen=2000))  # (ts, status)
    clock_offset_s: float = 0.0  # tests can shift "now"

    @property
    def limits(self) -> tuple[int, int]:
        return (self.peak_soft_limit, self.peak_hard_limit) if self.peak else (self.soft_limit, self.hard_limit)

    def now(self) -> datetime:
        return datetime.fromtimestamp(time.time() + self.clock_offset_s, UTC)


state = ApiState()
app = FastAPI(title="Legacy POS API (mock)")


def _parse_ts(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError as e:
        raise HTTPException(400, f"bad timestamp {s!r}") from e
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _record(status: int) -> None:
    state.counts[status] += 1
    state.recent.append((time.time(), status))


@app.get("/transactions", response_class=PlainTextResponse)
async def transactions(
    region: str = Query(...),
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
):
    region = region.upper()
    if region not in REGIONS:
        raise HTTPException(400, f"unknown region {region}")
    start, end = _parse_ts(from_), _parse_ts(to)
    if not start < end or (end - start).total_seconds() > MAX_WINDOW_MIN * 60:
        raise HTTPException(400, f"window must be 0 < to-from <= {MAX_WINDOW_MIN} min")

    if time.time() < state.crashed_until:
        _record(500)
        raise HTTPException(500, "upstream connection reset (service restarting)")

    state.in_flight += 1
    state.max_in_flight = max(state.max_in_flight, state.in_flight)
    try:
        soft, hard = state.limits
        if state.in_flight > hard:
            state.crashed_until = time.time() + state.crash_seconds
            state.crashes += 1
            _record(500)
            raise HTTPException(500, "OutOfMemoryError: Java heap space")

        mode = state.failing_regions.get(region)
        if mode == "error":
            await asyncio.sleep(0.2)
            _record(503)
            raise HTTPException(503, f"region {region} backend unavailable")
        if mode == "timeout":
            await asyncio.sleep(30)
            _record(504)
            raise HTTPException(504, "gateway timeout")

        lines = await asyncio.to_thread(window_lines, region, start, end, state.now(), state.total_rpm)

        overloaded = state.in_flight > soft
        latency = 0.05 + len(lines) * 0.00002
        if state.peak:
            latency *= 2
        if overloaded:
            latency *= 3
        await asyncio.sleep(latency)

        if overloaded:
            r = random.random()
            if r < 0.25:
                _record(503)
                raise HTTPException(503, "Service Unavailable: too many requests")
            if r < 0.35:
                # Truncated body: no trailer. Clients must detect this and retry.
                _record(200)
                cut = lines[: len(lines) // 2]
                return header() + "\n" + "\n".join(cut) + "\n"
        if random.random() < 0.15:
            # Legacy retry bug: sometimes repeats a handful of lines in the same response.
            lines = lines + random.sample(lines, min(len(lines), 5))
        _record(200)
        return header() + "\n" + "\n".join(lines) + ("\n" if lines else "") + f"TRL|{len(lines)}\n"
    finally:
        state.in_flight -= 1


@app.post("/admin/peak")
async def set_peak(on: bool = True):
    state.peak = on
    return {"peak": state.peak, "limits": state.limits}


@app.post("/admin/fail")
async def set_fail(region: str, mode: str = "error"):
    region = region.upper()
    if mode == "off":
        state.failing_regions.pop(region, None)
    elif mode in ("error", "timeout"):
        state.failing_regions[region] = mode
    else:
        raise HTTPException(400, "mode must be error|timeout|off")
    return {"failing_regions": state.failing_regions}


@app.post("/admin/reset")
async def reset():
    state.peak = False
    state.failing_regions.clear()
    state.crashed_until = 0.0
    state.counts.clear()
    state.recent.clear()
    state.max_in_flight = 0
    state.crashes = 0
    return {"ok": True}


@app.get("/admin/stats")
async def stats():
    now = time.time()
    last60 = [s for t, s in state.recent if now - t <= 60]
    errors = sum(1 for s in last60 if s >= 500)
    return {
        "peak": state.peak,
        "limits": {"soft": state.limits[0], "hard": state.limits[1]},
        "failing_regions": state.failing_regions,
        "in_flight": state.in_flight,
        "max_in_flight": state.max_in_flight,
        "crashed": now < state.crashed_until,
        "crashes": state.crashes,
        "status_counts": dict(state.counts),
        "last_60s": {"requests": len(last60), "errors": errors,
                     "error_rate": round(errors / len(last60), 3) if last60 else 0.0},
    }


@app.get("/health")
async def health():
    return {"ok": time.time() >= state.crashed_until}
