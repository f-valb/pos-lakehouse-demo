"""Planner: runs every minute (EventBridge Scheduler -> Lambda in prod; a loop locally).

Per region it:
1. Plans *live* windows from the region's watermark up to `now - live_lag`, using the window
   size the poller last fed back (smaller at peak). Live tasks go to the high-priority queue.
2. Plans one *re-pull* of every window ~21 min after it closed. The API has no "updated since"
   filter, so re-pulling is the only way to see late voids. Re-pulls go to the backfill queue.
3. If the region's circuit is open, it does *not* send live tasks (they would only fail and
   add load); those windows are recorded as gaps instead.
4. When the circuit is closed again, it backfills gaps newest-first, at most
   `backfill_per_tick` tasks per tick, into the low-priority queue.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from ingestion.config import Settings
from ingestion.models import PullTask, compact, iso, parse_iso

STATE_KEY = "planner/state.json"


def feedback_key(region: str) -> str:
    return f"feedback/{region}.json"


def gap_key(region: str, start: datetime, end: datetime) -> str:
    return f"gaps/{region}/{compact(start)}_{compact(end)}.json"


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def merge_ranges(ranges: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    out: list[tuple[datetime, datetime]] = []
    for s, e in sorted(ranges):
        if out and s <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], e))
        else:
            out.append((s, e))
    return out


class Planner:
    def __init__(self, settings: Settings, store, live_q, backfill_q, clock=time.time):
        self.s, self.store, self.live_q, self.backfill_q, self.clock = settings, store, live_q, backfill_q, clock

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.clock(), UTC).replace(microsecond=0)

    def tick(self) -> dict:
        now = self.now()
        state = self.store.get(STATE_KEY) or {}
        summary = {}
        for region in self.s.regions:
            summary[region] = self._plan_region(region, now, state.setdefault(region, {}))
        self.store.put(STATE_KEY, state)
        return summary

    def _task(self, region, start, end, kind, now) -> PullTask:
        return PullTask(region, iso(start), iso(end), kind, iso(now), iso(now))

    def _plan_region(self, region: str, now: datetime, rs: dict) -> dict:
        fb = self.store.get(feedback_key(region)) or {}
        size = int(fb.get("window_s", 300))
        circuit = fb.get("circuit", {}).get("state", "closed")
        horizon = floor_minute(now - timedelta(seconds=self.s.live_lag_seconds))
        wm = parse_iso(rs["live_watermark"]) if rs.get("live_watermark") else horizon - timedelta(seconds=size)

        windows = []
        while wm + timedelta(seconds=size) <= horizon:
            windows.append((wm, wm + timedelta(seconds=size)))
            wm += timedelta(seconds=size)

        sent_live, new_gaps = 0, 0
        if circuit == "open":
            for s, e in windows:
                self.store.put(gap_key(region, s, e), {"start": iso(s), "end": iso(e), "reason": "circuit_open"})
                new_gaps += 1
        else:
            # If we fell far behind (e.g. planner was down), go live on the newest window now and
            # treat the rest as backfill, so freshness recovers immediately.
            for s, e in windows[:-2]:
                self.store.put(gap_key(region, s, e), {"start": iso(s), "end": iso(e), "reason": "behind"})
                new_gaps += 1
            for s, e in windows[-2:]:
                self.live_q.send(self._task(region, s, e, "live", now))
                sent_live += 1
        rs["live_watermark"] = iso(wm)
        rs["window_s"] = size

        sent_repull = 0
        if circuit == "closed":
            sent_repull = self._plan_repulls(region, now, rs)
        sent_backfill = self._plan_backfill(region, now, size) if circuit == "closed" else 0
        return {"window_s": size, "circuit": circuit, "live": sent_live, "repull": sent_repull,
                "backfill": sent_backfill, "new_gaps": new_gaps, "watermark": rs["live_watermark"]}

    def _plan_repulls(self, region: str, now: datetime, rs: dict) -> int:
        """Re-pull, in 10-min chunks, everything that closed ~repull_after_seconds ago."""
        horizon = floor_minute(now - timedelta(seconds=self.s.repull_after_seconds))
        wm = parse_iso(rs["repull_watermark"]) if rs.get("repull_watermark") else horizon
        n = 0
        while wm < horizon:
            end = min(horizon, wm + timedelta(minutes=10))
            self.backfill_q.send(self._task(region, wm, end, "repull", now))
            wm, n = end, n + 1
        rs["repull_watermark"] = iso(wm)
        return n

    def _plan_backfill(self, region: str, now: datetime, size: int) -> int:
        keys = self.store.list(f"gaps/{region}/")
        if not keys:
            return 0
        gaps = []
        for k in keys:
            g = self.store.get(k)
            if g:
                gaps.append((parse_iso(g["start"]), parse_iso(g["end"])))
        # Split merged gaps into window-sized chunks, newest first.
        chunks = []
        for s, e in merge_ranges(gaps):
            t = e
            while t > s:
                cs = max(s, t - timedelta(seconds=size))
                chunks.append((cs, t))
                t = cs
        chunks.sort(key=lambda c: c[0], reverse=True)
        take, rest = chunks[: self.s.backfill_per_tick], chunks[self.s.backfill_per_tick:]
        for s, e in take:
            self.backfill_q.send(self._task(region, s, e, "backfill", now))
        # Rewrite the ledger as the remaining (merged) ranges.
        for k in keys:
            self.store.delete(k)
        for s, e in merge_ranges(rest):
            self.store.put(gap_key(region, s, e), {"start": iso(s), "end": iso(e), "reason": "pending_backfill"})
        return len(take)
