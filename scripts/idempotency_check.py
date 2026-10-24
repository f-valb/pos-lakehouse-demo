"""Acceptance check: kill -9 the poller mid-run, let SQS redeliver, resume, and compare the
resulting S3 objects with a clean run of the same tasks. Needs LocalStack + mock API running.

    uv run python scripts/idempotency_check.py
"""

from __future__ import annotations

import hashlib
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ["POLLER_SINKS"] = "s3"
os.environ["POLLER_VISIBILITY_S"] = "10"

from ingestion.aws import client  # noqa: E402
from ingestion.config import Settings  # noqa: E402
from ingestion.models import PullTask, iso  # noqa: E402
from ingestion.queues import SqsQueue  # noqa: E402

s = Settings()
s3, sqs = client("s3", s), client("sqs", s)
live = SqsQueue(sqs, s.live_queue)

base = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
pulled_at = iso(base + timedelta(hours=2))
tasks = [PullTask(r, iso(base + timedelta(minutes=5 * i)), iso(base + timedelta(minutes=5 * (i + 1))),
                  "backfill", pulled_at, pulled_at) for r in s.regions for i in range(6)]
keys = {(t.region, t.object_key()) for t in tasks}


def wipe():
    for r, k in keys:
        s3.delete_object(Bucket=s.raw_bucket(r), Key=k)
    for q in (s.live_queue, s.backfill_queue):
        sqs.purge_queue(QueueUrl=sqs.get_queue_url(QueueName=q)["QueueUrl"])
    time.sleep(1)


def enqueue():
    for t in tasks:
        live.send(t)


def digests() -> dict:
    out = {}
    for r, k in sorted(keys):
        try:
            out[k] = hashlib.sha256(s3.get_object(Bucket=s.raw_bucket(r), Key=k)["Body"].read()).hexdigest()
        except s3.exceptions.NoSuchKey:
            out[k] = None
    return out


poller = [sys.executable, "-m", "ingestion.run_local", "poller", "--idle-exit", "12"]

print(f"{len(tasks)} tasks: 4 regions x 6 windows starting {iso(base)} (pulled_at={pulled_at})")
print("\n--- run A: start poller, SIGKILL once ~5 objects exist (mid-run), then resume ---")
wipe()
enqueue()
p = subprocess.Popen(poller, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
deadline = time.time() + 120
while time.time() < deadline and sum(v is not None for v in digests().values()) < 5:
    time.sleep(0.3)
p.send_signal(signal.SIGKILL)
p.wait()
mid = digests()
done_mid = sum(v is not None for v in mid.values())
print(f"killed: {done_mid}/{len(tasks)} objects written, rest in-flight or queued")
time.sleep(11)  # let visibility timeouts expire so SQS redelivers in-flight tasks
subprocess.run(poller, check=True, stdout=subprocess.DEVNULL)
run_a = digests()
print(f"resumed: {sum(v is not None for v in run_a.values())}/{len(tasks)} objects")
unchanged = all(run_a[k] == v for k, v in mid.items() if v is not None)
print(f"objects written before the kill unchanged after resume: {unchanged}")

print("\n--- run B: clean run of the same tasks ---")
wipe()
enqueue()
subprocess.run(poller, check=True, stdout=subprocess.DEVNULL)
run_b = digests()

same = run_a == run_b and None not in run_a.values()
print(f"run A == run B (same keys, same sha256): {same}")
for k in list(run_a)[:3]:
    print(f"  {k[:90]}  {run_a[k][:16]}  {run_b[k][:16]}")
dlq = SqsQueue(sqs, s.dlq).depth()
print(f"DLQ: {dlq}")
mid_run = 0 < done_mid < len(tasks)
ok = same and unchanged and mid_run
print("PASS" if ok else f"FAIL (same={same} unchanged={unchanged} killed_mid_run={mid_run})")
sys.exit(0 if ok else 1)
