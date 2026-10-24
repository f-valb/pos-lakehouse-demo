"""Small-file compaction for the raw landing zone.

Live pulls produce one small Parquet per (region, window, pull). Auto Loader is fine with that
(bronze is Delta and is compacted by predictive optimization / OPTIMIZE), but the raw zone is
also our replay archive, and millions of small objects are slow and expensive to list.

Hourly job: for each closed hour older than `min_age_hours`, merge every raw file whose window
starts in that hour into `compacted/region=../date=../hour=HH/part-00000.parquet` (sorted,
deterministic, so re-running produces the same object). Raw objects are then expired by an S3
lifecycle rule (7 days) rather than deleted here, so Auto Loader never misses a file.
"""

from __future__ import annotations

import io
import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

WINDOW_RE = re.compile(r"raw/region=(\w+)/date=([\d-]+)/window=(\d{8}T\d{2})")


def compact_region(s3, bucket: str, region: str, now: datetime | None = None, min_age_hours: int = 2) -> list[str]:
    now = now or datetime.now(UTC)
    cutoff = (now - timedelta(hours=min_age_hours)).strftime("%Y%m%dT%H")
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"raw/region={region}/"):
        for o in page.get("Contents", []):
            m = WINDOW_RE.match(o["Key"])
            if m and m.group(3) < cutoff:
                groups[(m.group(2), m.group(3)[-2:])].append(o["Key"])
    written = []
    for (date, hour), keys in sorted(groups.items()):
        out_key = f"compacted/region={region}/date={date}/hour={hour}/part-00000.parquet"
        tables = [pq.read_table(io.BytesIO(s3.get_object(Bucket=bucket, Key=k)["Body"].read())) for k in sorted(keys)]
        t = pa.concat_tables(tables).sort_by([("_window_start", "ascending"), ("_ingest_ts", "ascending"),
                                              ("_raw_line", "ascending")])
        buf = io.BytesIO()
        pq.write_table(t, buf, compression="zstd", row_group_size=1_000_000)
        s3.put_object(Bucket=bucket, Key=out_key, Body=buf.getvalue(),
                      Metadata={"source-files": str(len(keys)), "rows": str(t.num_rows)})
        written.append(f"{out_key} <- {len(keys)} files, {t.num_rows} rows")
    return written
