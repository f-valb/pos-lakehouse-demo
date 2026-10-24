"""Poller: drains the task queues and pulls from the legacy API without hurting it.

Protection layers, outermost first:
  AIMD global concurrency (max 4, below the API's soft limit of 6; halves on any overload signal)
  -> per-region circuit breaker (3 consecutive failures -> open; half-open probe after cooldown)
  -> per-region token bucket (requests/s)
  -> per-request retries with exponential backoff + full jitter
  -> SQS redelivery with growing visibility timeout -> DLQ after 5 receives

Idempotency: the output key is a pure function of the task. Before calling the API the poller
checks the primary sink; if the object exists (a previous attempt finished but crashed before
acking) it only replicates it to the other sinks and acks. Writes are conditional (If-None-Match).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from ingestion.config import Settings
from ingestion.legacy_parser import BadResponse, TruncatedResponse, parse_body, to_parquet_bytes, to_table
from ingestion.metrics import Metrics
from ingestion.models import PullTask, iso, parse_iso
from ingestion.planner import feedback_key, gap_key
from ingestion.queues import Message
from ingestion.resilience import AdaptiveWindow, AimdLimiter, CircuitBreaker, TokenBucket, backoff_delay


class Overload(Exception):
    """503 / 500 / timeout / truncated body: the API is struggling. Back off."""


class Poison(Exception):
    """4xx: the task itself is bad. Retrying will not help; let it reach the DLQ."""


@dataclass
class RegionCtl:
    breaker: CircuitBreaker
    window: AdaptiveWindow
    bucket: TokenBucket
    last_live_end: str | None = None
    last_ok_at: float | None = None
    last_error: str | None = None
    stats: dict = field(default_factory=lambda: {"ok": 0, "errors": 0, "records": 0, "parked": 0})


class Poller:
    def __init__(self, s: Settings, live_q, backfill_q, store, sinks: list, metrics: Metrics | None = None,
                 http: httpx.AsyncClient | None = None, log=print):
        self.s, self.live_q, self.backfill_q, self.store, self.sinks = s, live_q, backfill_q, store, sinks
        self.metrics = metrics or Metrics()
        self.http = http
        self.log = log
        self.limiter = AimdLimiter(s.max_global_concurrency)
        self.backfill_slots = asyncio.Semaphore(s.max_backfill_concurrency)
        self.regions = {r: self._restore(r) for r in s.regions}
        self.processed = 0

    # ---- state ---------------------------------------------------------------------------
    def _restore(self, region: str) -> RegionCtl:
        fb = self.store.get(feedback_key(region)) or {}
        ctl = RegionCtl(
            CircuitBreaker(self.s.breaker_failures, self.s.breaker_cooldown_s, cooldown_s=self.s.breaker_cooldown_s),
            AdaptiveWindow(int(fb.get("window_s", 300)), self.s.min_window_s, self.s.max_window_s,
                           self.s.target_records_per_pull),
            TokenBucket(self.s.region_rps),
            last_live_end=fb.get("last_live_end"),
            last_ok_at=fb.get("last_ok_at"),
        )
        c = fb.get("circuit") or {}
        if c.get("state") == "open":
            ctl.breaker.state, ctl.breaker.cooldown_s = "open", c.get("cooldown_s", self.s.breaker_cooldown_s)
            ctl.breaker.opened_at = time.time() - (ctl.breaker.cooldown_s - c.get("remaining_open_s", 0))
        return ctl

    def _persist(self, region: str) -> None:
        c = self.regions[region]
        self.store.put(feedback_key(region), {
            "window_s": c.window.size_s, "circuit": c.breaker.snapshot(), "last_live_end": c.last_live_end,
            "last_ok_at": c.last_ok_at, "last_error": c.last_error, "stats": c.stats, "updated_at": time.time(),
        })
        self.metrics.put("WindowSizeSeconds", c.window.size_s, "Seconds", Region=region)
        self.metrics.put("CircuitOpen", 1 if c.breaker.state != "closed" else 0, Region=region)
        if c.last_live_end:
            lag = time.time() - parse_iso(c.last_live_end).timestamp()
            self.metrics.put("IngestFreshnessSeconds", lag, "Seconds", Region=region)

    # ---- main loop -----------------------------------------------------------------------
    async def run(self, stop: asyncio.Event | None = None, max_tasks: int | None = None, idle_exit_s: float | None = None):
        stop = stop or asyncio.Event()
        own_http = self.http is None
        if own_http:
            self.http = httpx.AsyncClient(timeout=self.s.http_timeout_s)
        last_work = time.time()
        workers: set[asyncio.Task] = set()
        try:
            while not stop.is_set():
                if max_tasks is not None and self.processed >= max_tasks:
                    break
                await self.limiter.acquire()
                msg, q, is_backfill = await self._next()
                if msg is None:
                    await self.limiter.release(overloaded=False)
                    if idle_exit_s is not None and time.time() - last_work > idle_exit_s and not workers:
                        break
                    await asyncio.sleep(0.5)
                    continue
                last_work = time.time()
                t = asyncio.create_task(self._handle(msg, q, is_backfill))
                workers.add(t)
                t.add_done_callback(workers.discard)
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
        finally:
            self.metrics.flush()
            if own_http:
                await self.http.aclose()

    async def _next(self):
        for m in await asyncio.to_thread(self.live_q.receive, 1, 0, self.s.visibility_s):
            return m, self.live_q, False
        if not self.backfill_slots.locked():
            await self.backfill_slots.acquire()
            got = await asyncio.to_thread(self.backfill_q.receive, 1, 0, self.s.visibility_s * 2)
            if got:
                return got[0], self.backfill_q, True
            self.backfill_slots.release()
        return None, None, False

    async def _handle(self, msg: Message, q, is_backfill: bool) -> None:
        overloaded = False
        try:
            overloaded = await self.process(msg, q)
        except Exception as e:  # never let one task kill the worker
            self.log(f"[poller] unexpected error on {msg.task.task_id}: {e!r}")
            await asyncio.to_thread(q.release, msg, 30)
        finally:
            self.processed += 1
            await self.limiter.release(overloaded)
            if is_backfill:
                self.backfill_slots.release()

    # ---- one task ------------------------------------------------------------------------
    async def process(self, msg: Message, q) -> bool:
        """Returns True if the API showed overload (feeds the AIMD limiter)."""
        task, ctl = msg.task, self.regions[msg.task.region]
        key = task.object_key()
        self.metrics.put("TaskAgeAtReceiveSeconds", time.time() - msg.sent_ts, "Seconds", Queue=q.name)

        if await self._already_written(task, key):
            await asyncio.to_thread(q.delete, msg)
            self.log(f"[poller] {task.task_id} already written -> ack (idempotent skip)")
            return False

        if not ctl.breaker.allow():
            await self._park(task, "circuit_open")
            await asyncio.to_thread(q.delete, msg)
            return False

        await ctl.bucket.take()
        overloaded, last_err = False, None
        for attempt in range(self.s.attempts_per_receive):
            try:
                t0 = time.time()
                body = await self._fetch(task)
                latency = time.time() - t0
                table = to_table(body, task)
                data = to_parquet_bytes(table)
                await self._write_all(task.region, key, data)
                ctl.breaker.record_success()
                ctl.window.on_success(len(body), task.window_seconds, latency)
                ctl.last_ok_at, ctl.last_error = time.time(), None
                if task.kind == "live":
                    if not ctl.last_live_end or task.window_end > ctl.last_live_end:
                        ctl.last_live_end = task.window_end
                ctl.stats["ok"] += 1
                ctl.stats["records"] += len(body)
                self.metrics.put("ApiRequests", 1, Region=task.region)
                self.metrics.put("ApiLatencyMs", latency * 1000, "Milliseconds", Region=task.region)
                self.metrics.put("RecordsPulled", len(body), Region=task.region)
                self.metrics.put("DuplicateLinesCollapsed", len(body) - table.num_rows, Region=task.region)
                self._persist(task.region)
                await asyncio.to_thread(q.delete, msg)
                self.log(f"[poller] OK {task.task_id} {len(body)} lines {latency:.2f}s -> window now {ctl.window.size_s}s")
                return overloaded
            except Overload as e:
                overloaded, last_err = True, str(e)
                ctl.stats["errors"] += 1
                self.metrics.put("ApiRequests", 1, Region=task.region)
                self.metrics.put("ApiErrors", 1, Region=task.region)
                ctl.window.on_overload()
                await asyncio.sleep(backoff_delay(attempt))
            except Poison as e:
                last_err = str(e)
                self.metrics.put("ApiErrors", 1, Region=task.region)
                break

        ctl.breaker.record_failure()
        ctl.last_error = last_err
        self._persist(task.region)
        if ctl.breaker.state == "open":
            # Region is down: stop hammering it. Hand the window to the backfill ledger.
            await self._park(task, f"circuit_open: {last_err}")
            await asyncio.to_thread(q.delete, msg)
            self.log(f"[poller] circuit OPEN for {task.region} ({last_err}); parked {task.task_id}")
        else:
            delay = int(min(300, 10 * 2 ** (msg.receive_count - 1)))
            await asyncio.to_thread(q.release, msg, delay)
            self.log(f"[poller] FAIL {task.task_id} ({last_err}); retry in {delay}s (receive #{msg.receive_count})")
        return overloaded

    async def _fetch(self, task: PullTask) -> list[str]:
        try:
            r = await self.http.get(f"{self.s.api_url}/transactions", params={
                "region": task.region, "from": task.window_start, "to": task.window_end})
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise Overload(type(e).__name__) from e
        if r.status_code >= 500 or r.status_code == 429:
            raise Overload(f"HTTP {r.status_code}")
        if r.status_code >= 400:
            raise Poison(f"HTTP {r.status_code}: {r.text[:100]}")
        try:
            return parse_body(r.text)
        except TruncatedResponse as e:
            raise Overload(f"truncated: {e}") from e
        except BadResponse as e:
            raise Overload(f"bad body: {e}") from e

    async def _already_written(self, task: PullTask, key: str) -> bool:
        primary = self.sinks[0]
        if not await asyncio.to_thread(primary.exists, task.region, key):
            return False
        data = None
        for sink in self.sinks[1:]:
            if not await asyncio.to_thread(sink.exists, task.region, key):
                data = data or await asyncio.to_thread(primary.read, task.region, key)
                await asyncio.to_thread(sink.put, task.region, key, data)
        return True

    async def _write_all(self, region: str, key: str, data: bytes) -> None:
        # Primary first: its existence is the "task done" marker checked on retry.
        for sink in self.sinks:
            await asyncio.to_thread(sink.put, region, key, data)

    async def _park(self, task: PullTask, reason: str) -> None:
        s, e = parse_iso(task.window_start), parse_iso(task.window_end)
        self.regions[task.region].stats["parked"] += 1
        await asyncio.to_thread(self.store.put, gap_key(task.region, s, e),
                                {"start": task.window_start, "end": task.window_end, "reason": reason,
                                 "parked_at": iso(datetime.now(UTC))})
