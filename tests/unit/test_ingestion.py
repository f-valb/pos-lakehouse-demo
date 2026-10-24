import asyncio
import hashlib
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from ingestion.config import Settings
from ingestion.legacy_parser import TruncatedResponse, parse_body, to_parquet_bytes, to_table
from ingestion.models import PullTask, iso
from ingestion.planner import Planner, feedback_key
from ingestion.poller import Poller
from ingestion.queues import MemoryQueue
from ingestion.resilience import AdaptiveWindow, CircuitBreaker
from ingestion.sinks import MemorySink
from ingestion.state import MemoryStore
from mock_pos_api import app as api

HIST = datetime(2026, 10, 20, 4, 0, tzinfo=UTC)  # well in the past: void states are final


def settings(**kw) -> Settings:
    base = dict(regions=("PH", "SG", "DE", "US"), sinks=("mem",))
    base.update(kw)
    return Settings(**base)


# ---------------------------------------------------------------- resilience primitives
def test_circuit_breaker_opens_and_half_opens():
    t = [0.0]
    b = CircuitBreaker(failure_threshold=3, base_cooldown_s=30, clock=lambda: t[0])
    for _ in range(3):
        assert b.allow()
        b.record_failure()
    assert b.state == "open" and not b.allow()
    t[0] = 31
    assert b.allow() and b.state == "half_open"
    assert not b.allow()  # only one probe at a time
    b.record_failure()
    assert b.state == "open" and b.cooldown_s == 60  # doubled
    t[0] = 100
    assert b.allow()
    b.record_success()
    assert b.state == "closed" and b.cooldown_s == 30


def test_adaptive_window_shrinks_at_peak_and_grows_back():
    w = AdaptiveWindow(size_s=300, target_records=15000)
    w.on_success(records=60000, window_s=300, latency_s=1.0)  # 200 rec/s -> 75 s ideal
    assert w.size_s == 60
    w.on_overload()
    assert w.size_s == 60  # floor
    for _ in range(8):
        w.on_success(records=1500, window_s=w.size_s, latency_s=0.2)  # quiet hour
    assert w.size_s == 600


def test_parser_rejects_truncated_body():
    from mock_pos_api.generator import header

    with pytest.raises(TruncatedResponse):
        parse_body(header() + "\nPH|a\nPH|b\n")
    with pytest.raises(TruncatedResponse):
        parse_body(header() + "\nPH|a\nTRL|2\n")
    assert parse_body(header() + "\nPH|a\nTRL|1\n") == ["PH|a"]


def test_parquet_is_deterministic_and_collapses_in_response_dupes():
    task = PullTask("PH", iso(HIST), iso(HIST + timedelta(minutes=1)), "live", iso(HIST), iso(HIST))
    body = ["b|line", "a|line", "a|line"]
    t1, t2 = to_table(body, task), to_table(list(reversed(body)), task)
    assert t1.num_rows == 2
    assert to_parquet_bytes(t1) == to_parquet_bytes(t2)


# ---------------------------------------------------------------- planner
def make_planner(s, clock):
    store, dlq = MemoryStore(), MemoryQueue("dlq")
    live, back = MemoryQueue("live", dlq=dlq, clock=clock), MemoryQueue("backfill", dlq=dlq, clock=clock)
    return Planner(s, store, live, back, clock=clock), store, live, back


def drain(q):
    out = []
    while True:
        got = q.receive(10)
        if not got:
            return out
        for m in got:
            q.delete(m)
            out.append(m.task)


def test_planner_records_gaps_while_circuit_open_then_backfills_newest_first():
    t = [HIST.timestamp()]
    s = settings(regions=("SG",), backfill_per_tick=2, live_lag_seconds=0)
    p, store, live, back = make_planner(s, lambda: t[0])
    p.tick()
    assert len(drain(live)) == 1

    store.put(feedback_key("SG"), {"window_s": 300, "updated_at": t[0],
                                   "circuit": {"state": "open", "remaining_open_s": 10**6}})  # long cooldown
    for _ in range(6):  # 30 min outage
        t[0] += 300
        p.tick()
    assert drain(live) == []
    assert len(store.list("gaps/SG/")) == 6

    store.put(feedback_key("SG"), {"window_s": 300, "circuit": {"state": "closed"}})
    t[0] += 300
    p.tick()
    live_tasks = drain(live)
    assert len(live_tasks) == 1  # live resumes at "now" immediately
    bf = [x for x in drain(back) if x.kind == "backfill"]
    assert len(bf) == 2  # rate limited
    assert bf[0].window_start > bf[1].window_start  # newest first
    assert bf[0].window_end <= live_tasks[0].window_start
    assert len(store.list("gaps/SG/")) == 1  # remaining gap merged into one range


def test_planner_schedules_repulls_for_late_voids():
    t = [HIST.timestamp()]
    s = settings(regions=("PH",), repull_after_seconds=1260, live_lag_seconds=0)
    p, store, live, back = make_planner(s, lambda: t[0])
    p.tick()
    t[0] += 600
    p.tick()
    rp = [x for x in drain(back) if x.kind == "repull"]
    assert rp and all(x.window_end <= iso(HIST + timedelta(seconds=600 - 1260)) for x in rp)


# ---------------------------------------------------------------- poller against the mock API
@pytest.fixture
def api_state(monkeypatch):
    st = api.ApiState(total_rpm=8000, soft_limit=6, hard_limit=10, crash_seconds=5)
    monkeypatch.setattr(api, "state", st)
    return st


def historical_tasks(n_per_region=4):
    tasks = []
    for r in ("PH", "SG", "DE", "US"):
        for i in range(n_per_region):
            s = HIST + timedelta(minutes=5 * i)
            tasks.append(PullTask(r, iso(s), iso(s + timedelta(minutes=5)), "backfill", iso(HIST + timedelta(hours=3)),
                                  iso(HIST + timedelta(hours=3))))
    return tasks


def run_poller(s, live, back, store, sink, max_tasks=None, cancel_after=None):
    async def go():
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app), base_url="http://t")
        p = Poller(s, live, back, store, [sink], http=http, log=lambda m: None)
        coro = p.run(max_tasks=max_tasks, idle_exit_s=0.5)
        if cancel_after:
            try:
                await asyncio.wait_for(coro, timeout=cancel_after)
            except TimeoutError:
                pass  # simulated kill -9: in-flight messages are not acked
        else:
            await coro
        await http.aclose()
        return p

    return asyncio.run(go())


def test_poller_never_exceeds_api_limits(api_state):
    s = settings(api_url="http://t", max_global_concurrency=4, max_backfill_concurrency=4, region_rps=50)
    live, back = MemoryQueue("live"), MemoryQueue("backfill")
    for t in historical_tasks(5):
        live.send(t)
    sink = MemorySink()
    run_poller(s, live, back, MemoryStore(), sink)
    assert api_state.crashes == 0
    assert api_state.max_in_flight <= 4 < api_state.soft_limit
    assert sink.writes == 20


def test_poller_backs_off_at_peak_without_crashing_api(api_state):
    api_state.peak = True  # soft 3 / hard 6
    s = settings(api_url="http://t", max_global_concurrency=4, max_backfill_concurrency=4, region_rps=50)
    live, back = MemoryQueue("live"), MemoryQueue("backfill")
    for t in historical_tasks(4):
        live.send(t)
    sink = MemorySink()
    p = run_poller(s, live, back, MemoryStore(), sink)
    assert api_state.crashes == 0
    assert api_state.max_in_flight < api_state.peak_hard_limit
    assert sink.writes + sum(c.stats["parked"] for c in p.regions.values()) == 16 or live.depth()["delayed"] > 0


def test_kill_mid_run_and_retry_produces_identical_objects(api_state):
    s = settings(api_url="http://t", max_global_concurrency=2, region_rps=50)
    tasks = historical_tasks(3)

    def digest(sink):
        return {k: hashlib.sha256(v).hexdigest() for k, v in sink.objects.items()}

    # Clean run
    live, back = MemoryQueue("live"), MemoryQueue("backfill")
    for t in tasks:
        live.send(t)
    clean = MemorySink()
    run_poller(s, live, back, MemoryStore(), clean)

    # Killed run: cancel mid-flight, let visibility timeouts expire, resume with a new poller
    live, back = MemoryQueue("live"), MemoryQueue("backfill")
    for t in tasks:
        live.send(t)
    crashed = MemorySink()
    run_poller(s, live, back, MemoryStore(), crashed, cancel_after=0.8)
    partial = len(crashed.objects)
    live.requeue_inflight()
    run_poller(s, live, back, MemoryStore(), crashed)

    assert 0 < partial < len(tasks) or partial == len(tasks)
    assert digest(clean) == digest(crashed)
    assert len(clean.objects) == len(tasks)


def test_planner_sends_one_probe_after_cooldown():
    t = [HIST.timestamp()]
    s = settings(regions=("SG",), live_lag_seconds=0)
    p, store, live, back = make_planner(s, lambda: t[0])
    p.tick()
    drain(live)
    store.put(feedback_key("SG"), {"window_s": 60, "updated_at": t[0],
                                   "circuit": {"state": "open", "remaining_open_s": 120}})
    t[0] += 60
    p.tick()
    assert drain(live) == []  # still cooling down: nothing reaches the dead region
    t[0] += 180
    p.tick()
    probes = drain(live)
    assert len(probes) == 1  # exactly one half-open probe, the newest window
    gaps = [store.get(k) for k in store.list("gaps/SG/")]
    assert all(g["end"] <= probes[0].window_start for g in gaps)
