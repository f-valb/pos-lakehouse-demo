"""Local runner for the ingestion layer against LocalStack.

    uv run python -m ingestion.run_local setup      # buckets, queues (+DLQ redrive), SNS, alarms
    uv run python -m ingestion.run_local planner    # EventBridge Scheduler stand-in (tick loop)
    uv run python -m ingestion.run_local poller     # SQS consumer (Lambda/ECS stand-in)
    uv run python -m ingestion.run_local run        # planner + poller in one process
    uv run python -m ingestion.run_local status     # live view: windows, circuits, queues, alarms
    uv run python -m ingestion.run_local compact    # hourly small-file compaction
    uv run python -m ingestion.run_local reset      # purge queues + control state (demo restart)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from datetime import UTC, datetime

import httpx

from ingestion.aws import client
from ingestion.compact import compact_region
from ingestion.config import Settings
from ingestion.metrics import Metrics
from ingestion.models import parse_iso
from ingestion.observability import alarm_specs
from ingestion.planner import Planner, feedback_key
from ingestion.poller import Poller
from ingestion.queues import SqsQueue
from ingestion.sinks import S3Sink, VolumeSink
from ingestion.state import S3Store


def setup(s: Settings) -> None:
    s3, sqs, sns, cw = client("s3", s), client("sqs", s), client("sns", s), client("cloudwatch", s)
    for b in [s.control_bucket] + [s.raw_bucket(r) for r in s.regions]:
        try:
            s3.create_bucket(Bucket=b, CreateBucketConfiguration={"LocationConstraint": s.aws_region})
        except s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        if b != s.control_bucket:
            s3.put_bucket_lifecycle_configuration(Bucket=b, LifecycleConfiguration={"Rules": [
                {"ID": "expire-raw-small-files", "Status": "Enabled", "Filter": {"Prefix": "raw/"},
                 "Expiration": {"Days": 7}}]})
            s3.put_bucket_tagging(Bucket=b, Tagging={"TagSet": [
                {"Key": "cost_center", "Value": "retail-data"}, {"Key": "data_country", "Value": b[-2:].upper()},
                {"Key": "env", "Value": s.env}, {"Key": "component", "Value": "ingestion"}]})
    dlq_url = sqs.create_queue(QueueName=s.dlq, Attributes={"MessageRetentionPeriod": "1209600"})["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    for q in (s.live_queue, s.backfill_queue):
        sqs.create_queue(QueueName=q, Attributes={
            "VisibilityTimeout": "60",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(s.max_receives)})})
    topic = sns.create_topic(Name="pos-ingestion-alarms")["TopicArn"]
    for spec in alarm_specs(s.regions, s.dlq, s.live_queue):
        cw.put_metric_alarm(**spec, AlarmActions=[topic], OKActions=[topic], ActionsEnabled=True)
    print(f"buckets: {[s.control_bucket] + [s.raw_bucket(r) for r in s.regions]}")
    print(f"queues: {s.live_queue}, {s.backfill_queue} -> DLQ {s.dlq} after {s.max_receives} receives")
    print(f"alarms: {len(alarm_specs(s.regions, s.dlq, s.live_queue))} -> SNS {topic}")


def _deps(s: Settings):
    s3, sqs = client("s3", s), client("sqs", s)
    store = S3Store(s3, s.control_bucket)
    live, backfill = SqsQueue(sqs, s.live_queue), SqsQueue(sqs, s.backfill_queue)
    sinks = []
    for name in s.sinks:
        if name == "s3":
            sinks.append(S3Sink(s3, s.raw_bucket))
        elif name == "volume":
            from databricks.sdk import WorkspaceClient

            sinks.append(VolumeSink(WorkspaceClient(), s.volume_root))
    return store, live, backfill, sinks, Metrics(client("cloudwatch", s))


async def planner_loop(s: Settings, stop: asyncio.Event) -> None:
    store, live, backfill, _, _ = _deps(s)
    p = Planner(s, store, live, backfill)
    while not stop.is_set():
        summary = await asyncio.to_thread(p.tick)
        line = "  ".join(f"{r}:w={v['window_s']}s c={v['circuit']} L{v['live']} R{v['repull']} B{v['backfill']}"
                         + (f" gaps+{v['new_gaps']}" if v["new_gaps"] else "") for r, v in summary.items())
        print(f"[planner {datetime.now(UTC):%H:%M:%S}] {line}", flush=True)
        try:
            await asyncio.wait_for(stop.wait(), timeout=s.tick_seconds)
        except TimeoutError:
            pass


async def poller_main(s: Settings, stop: asyncio.Event, idle_exit_s=None, max_tasks=None) -> Poller:
    store, live, backfill, sinks, metrics = _deps(s)
    p = Poller(s, live, backfill, store, sinks, metrics, log=lambda m: print(m, flush=True))
    await p.run(stop, max_tasks=max_tasks, idle_exit_s=idle_exit_s)
    return p


def _stop_on_signals() -> asyncio.Event:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    return stop


def status(s: Settings) -> None:
    s3, sqs, cw = client("s3", s), client("sqs", s), client("cloudwatch", s)
    store = S3Store(s3, s.control_bucket)
    now = time.time()
    print(f"=== ingestion status @ {datetime.now(UTC):%Y-%m-%d %H:%M:%S}Z ===")
    print(f"{'region':6} {'window':>7} {'circuit':>10} {'ingest lag':>11} {'ok':>5} {'err':>5} {'parked':>6} {'gaps':>5}  last_error")
    for r in s.regions:
        fb = store.get(feedback_key(r)) or {}
        lag = f"{(now - parse_iso(fb['last_live_end']).timestamp()) / 60:.1f} min" if fb.get("last_live_end") else "-"
        st = fb.get("stats", {})
        gaps = len(store.list(f"gaps/{r}/"))
        c = fb.get("circuit", {})
        circ = c.get("state", "-") + (f" {c.get('remaining_open_s', 0):.0f}s" if c.get("state") == "open" else "")
        print(f"{r:6} {fb.get('window_s', '-'):>6}s {circ:>10} {lag:>11} {st.get('ok', 0):>5} {st.get('errors', 0):>5} "
              f"{st.get('parked', 0):>6} {gaps:>5}  {fb.get('last_error') or ''}")
    for q in (s.live_queue, s.backfill_queue, s.dlq):
        print(f"queue {q:18} {SqsQueue(sqs, q).depth()}")
    try:
        api = httpx.get(f"{s.api_url}/admin/stats", timeout=3).json()
        print(f"api: peak={api['peak']} limits={api['limits']} in_flight={api['in_flight']} crashed={api['crashed']} "
              f"crashes={api['crashes']} failing={api['failing_regions']} last60s={api['last_60s']}")
    except Exception as e:
        print(f"api: unreachable ({e})")
    alarms = cw.describe_alarms(AlarmNamePrefix="pos-")["MetricAlarms"]
    firing = [a["AlarmName"] for a in alarms if a["StateValue"] == "ALARM"]
    print(f"alarms: {len(alarms)} defined, ALARM: {firing or 'none'}")


def reset(s: Settings) -> None:
    s3, sqs = client("s3", s), client("sqs", s)
    for q in (s.live_queue, s.backfill_queue, s.dlq):
        sqs.purge_queue(QueueUrl=sqs.get_queue_url(QueueName=q)["QueueUrl"])
    store = S3Store(s3, s.control_bucket)
    for k in store.list(""):
        store.delete(k)
    print("queues purged, control state cleared (raw data kept)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["setup", "planner", "poller", "run", "status", "compact", "reset"])
    ap.add_argument("--idle-exit", type=float, default=None, help="poller: exit after N idle seconds")
    ap.add_argument("--max-tasks", type=int, default=None)
    a = ap.parse_args()
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    s = Settings()

    if a.cmd == "setup":
        setup(s)
    elif a.cmd == "status":
        status(s)
    elif a.cmd == "reset":
        reset(s)
    elif a.cmd == "compact":
        s3 = client("s3", s)
        for r in s.regions:
            for line in compact_region(s3, s.raw_bucket(r), r):
                print(line)
    else:
        async def go():
            stop = _stop_on_signals()
            jobs = []
            if a.cmd in ("planner", "run"):
                jobs.append(planner_loop(s, stop))
            if a.cmd in ("poller", "run"):
                jobs.append(poller_main(s, stop, a.idle_exit, a.max_tasks))
            await asyncio.gather(*jobs)

        asyncio.run(go())


if __name__ == "__main__":
    main()
