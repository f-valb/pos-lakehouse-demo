"""Scripted failure-injection demo (docs/demo_script.md, part 3). Needs the mock API, LocalStack and
`ingestion.run_local run` running. Prints evidence after every step.

    uv run python scripts/failure_demo.py                # full run (~14 min)
    uv run python scripts/failure_demo.py --step peak    # one step: peak | outage | restore | status
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import UTC, datetime

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from ingestion.aws import client  # noqa: E402
from ingestion.config import Settings  # noqa: E402
from ingestion.planner import feedback_key  # noqa: E402
from ingestion.state import S3Store  # noqa: E402

s = Settings()
API = s.api_url
store = S3Store(client("s3", s), s.control_bucket)
cw = client("cloudwatch", s)


def banner(msg):
    print(f"\n{'=' * 100}\n{datetime.now(UTC):%H:%M:%S}Z  {msg}\n{'=' * 100}", flush=True)


def snapshot(note=""):
    rows = []
    for r in s.regions:
        fb = store.get(feedback_key(r)) or {}
        c = fb.get("circuit", {})
        lag = (time.time() - datetime.fromisoformat(fb["last_live_end"].replace("Z", "+00:00")).timestamp()) / 60 \
            if fb.get("last_live_end") else float("nan")
        rows.append(f"{r}: window={fb.get('window_s', '-'):>4}s circuit={c.get('state', '-'):9} gaps={len(store.list(f'gaps/{r}/')):>2} "
                    f"ingest_lag={lag:5.1f}m err={fb.get('stats', {}).get('errors', 0):>3} last_error={(fb.get('last_error') or '')[:40]}")
    api = httpx.get(f"{API}/admin/stats", timeout=5).json()
    # Filter client-side: LocalStack ignores the StateValue filter of DescribeAlarms.
    alarms = [a["AlarmName"] for a in cw.describe_alarms(AlarmNamePrefix="pos-")["MetricAlarms"] if a["StateValue"] == "ALARM"]
    print(f"--- {datetime.now(UTC):%H:%M:%S}Z {note}")
    for line in rows:
        print("   ", line)
    print(f"    api: peak={api['peak']} limits={api['limits']} max_in_flight={api['max_in_flight']} crashes={api['crashes']} "
          f"last60s={api['last_60s']} failing={api['failing_regions']}")
    print(f"    CloudWatch ALARM: {alarms or 'none'}", flush=True)


def watch(minutes, every=60, note=""):
    end = time.time() + minutes * 60
    while time.time() < end:
        time.sleep(every)
        snapshot(note)


def peak(minutes):
    banner("STEP 1  Peak mode: the API degrades faster (soft limit 6 -> 3, hard 10 -> 6, latency x2)")
    httpx.post(f"{API}/admin/reset")
    httpx.post(f"{API}/admin/peak", params={"on": True})
    snapshot("peak ON")
    watch(minutes, note="peak: 503s -> AIMD halves concurrency, windows shrink, API must not crash")
    httpx.post(f"{API}/admin/peak", params={"on": False})


def outage(minutes, region="SG"):
    banner(f"STEP 2  {region} backend fails: breaker opens, live windows become gaps, freshness alarm fires")
    httpx.post(f"{API}/admin/fail", params={"region": region, "mode": "error"})
    snapshot(f"{region} failing")
    watch(minutes, note=f"{region} down (other regions unaffected)")


def restore(minutes, region="SG"):
    banner(f"STEP 3  {region} restored: half-open probe, then newest-first backfill at low priority (3 windows/tick)")
    httpx.post(f"{API}/admin/fail", params={"region": region, "mode": "off"})
    snapshot(f"{region} restored")
    watch(minutes, note="backfill draining newest-first while live windows keep priority")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", choices=["all", "peak", "outage", "restore", "status"], default="all")
    ap.add_argument("--minutes", type=float, default=None)
    a = ap.parse_args()
    if a.step == "status":
        snapshot("status")
    if a.step in ("all", "peak"):
        peak(a.minutes or 3)
    if a.step in ("all", "outage"):
        outage(a.minutes or 6)
    if a.step in ("all", "restore"):
        restore(a.minutes or 5)
