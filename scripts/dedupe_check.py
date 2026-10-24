"""Acceptance check: re-pulling the same window (twice) adds rows to bronze but none to silver.

Needs: mock API + ingestion (`run_local run` with POLLER_SINKS=s3,volume) running, bundle deployed.

    uv run python scripts/dedupe_check.py --country PH
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from databricks.sdk import WorkspaceClient  # noqa: E402

from ingestion.aws import client  # noqa: E402
from ingestion.config import Settings  # noqa: E402
from ingestion.models import PullTask, iso  # noqa: E402
from ingestion.queues import SqsQueue  # noqa: E402
from ingestion.sinks import VolumeSink  # noqa: E402
from scripts.dbsql import run, warehouse_id  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--country", default="PH")
ap.add_argument("--env", default=os.environ.get("DEMO_ENV", "dev"))
a = ap.parse_args()
cc, env = a.country.upper(), a.env
cat = f"pos_{cc.lower()}"
w = WorkspaceClient()
wh = warehouse_id(w)


def q(sql):
    return run(w, wh, sql, show=False)


win = q(f"""SELECT min(_window_start), min(_window_end) FROM {cat}.{env}_bronze.pos_lines_raw
            WHERE _pull_kind = 'live' AND _window_start = (SELECT min(_window_start) FROM {cat}.{env}_bronze.pos_lines_raw
                                                           WHERE _pull_kind = 'live')""")[0]
ws, we = (datetime.fromisoformat(x.replace("Z", "+00:00")) for x in win)
pred = f"txn_ts_utc >= '{iso(ws)}' AND txn_ts_utc < '{iso(we)}'"
bpred = f"_window_start <= '{iso(ws)}' AND _window_end >= '{iso(we)}'"


def counts():
    b = q(f"SELECT count(*) FROM {cat}.{env}_bronze.pos_lines_raw WHERE {bpred}")[0][0]
    s, k, v = q(f"""SELECT count(*), count(DISTINCT store_id, register_id, txn_id, line_no), count_if(is_void)
                   FROM {cat}.{env}_silver.pos_lines WHERE {pred}""")[0]
    return int(b), int(s), int(k), int(v)


print(f"window {iso(ws)} .. {iso(we)} ({cc}, {env})")
b0, s0, k0, v0 = counts()
print(f"before: bronze={b0} silver={s0} distinct_keys={k0} voids={v0}")

s = Settings()
live = SqsQueue(client("sqs", s), s.live_queue)
now = datetime.now(UTC).replace(microsecond=0)
tasks = [PullTask(cc, iso(ws), iso(we), "repull", iso(now + timedelta(seconds=i)), iso(now)) for i in range(2)]
for t in tasks:
    live.send(t)
print("enqueued 2 re-pulls of the same window; waiting for them to land in the volume")
sink = VolumeSink(w, s.volume_root)
deadline = time.time() + 300
while time.time() < deadline and not all(sink.exists(cc, t.object_key()) for t in tasks):
    time.sleep(5)
assert all(sink.exists(cc, t.object_key()) for t in tasks), "re-pulls did not land (is ingestion running?)"

print(f"refreshing pipeline pos_{cc.lower()} (waits for a scheduled update if one is active) ...")
for _ in range(3):
    r = subprocess.run(["databricks", "bundle", "run", f"pos_{cc.lower()}", "-t", env],
                       capture_output=True, text=True)
    if r.returncode == 0:
        break
    if "active update" not in (r.stderr + r.stdout):
        sys.exit(r.stderr[-500:])
    time.sleep(60)  # the 5-min refresh job is mid-update; it will also pick up the new files
b1, s1, k1, v1 = counts()
print(f"after:  bronze={b1} silver={s1} distinct_keys={k1} voids={v1}")
ok = b1 > b0 and s1 == k1 and k1 == k0
print(f"bronze grew by {b1 - b0} rows (every pulled version kept); silver rows == distinct keys: {s1 == k1}; "
      f"no new keys: {k1 == k0}; voids picked up by re-pull: {v1 - v0}")
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
