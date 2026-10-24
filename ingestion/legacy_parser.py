"""Turn one legacy API response into a deterministic Parquet file for bronze.

Bronze keeps the data *raw* (every field still a string, original codes and date formats),
plus lineage columns stamped by the poller. Translation happens in silver.

Determinism: lines are sorted and exact duplicates *within one response* are collapsed (the
API's retry bug repeats random lines, so keeping a count would make retries differ). The count
of collapsed lines is reported as a metric instead. Same task + same API content -> byte-identical
Parquet.
"""

from __future__ import annotations

import hashlib
import io

import pyarrow as pa
import pyarrow.parquet as pq

from common.regions import LEGACY_COLUMNS
from ingestion.models import PullTask, parse_iso

HEADER = "|".join(LEGACY_COLUMNS)


class TruncatedResponse(Exception):
    """Body cut off (no trailer or count mismatch): retry, never write a partial file."""


class BadResponse(Exception):
    pass


SCHEMA = pa.schema(
    [pa.field(c, pa.string()) for c in LEGACY_COLUMNS]
    + [
        pa.field("_raw_line", pa.string()),
        pa.field("_record_hash", pa.string()),
        pa.field("_field_count", pa.int32()),
        pa.field("_ingest_ts", pa.timestamp("us", tz="UTC")),
        pa.field("_pull_id", pa.string()),
        pa.field("_pull_kind", pa.string()),
        pa.field("_window_start", pa.timestamp("us", tz="UTC")),
        pa.field("_window_end", pa.timestamp("us", tz="UTC")),
        pa.field("_region", pa.string()),
    ]
)


def parse_body(text: str) -> list[str]:
    lines = text.rstrip("\n").split("\n") if text else []
    if not lines or lines[0] != HEADER:
        raise BadResponse("missing/unknown header")
    if not lines[-1].startswith("TRL|"):
        raise TruncatedResponse("no trailer")
    expected = int(lines[-1].split("|", 1)[1])
    body = lines[1:-1]
    if len(body) != expected:
        raise TruncatedResponse(f"trailer says {expected}, got {len(body)}")
    return body


def to_table(body: list[str], task: PullTask) -> pa.Table:
    unique = sorted(set(body))
    n = len(unique)
    cols: dict[str, list] = {c: [] for c in LEGACY_COLUMNS}
    raw, hashes, nfields = [], [], []
    for line in unique:
        parts = line.split("|")
        for i, c in enumerate(LEGACY_COLUMNS):
            cols[c].append(parts[i] if i < len(parts) and len(parts) == len(LEGACY_COLUMNS) else None)
        raw.append(line)
        hashes.append(hashlib.sha256(line.encode()).hexdigest())
        nfields.append(len(parts))
    ingest = parse_iso(task.pulled_at)
    data = {
        **cols,
        "_raw_line": raw,
        "_record_hash": hashes,
        "_field_count": nfields,
        "_ingest_ts": [ingest] * n,
        "_pull_id": [task.task_id] * n,
        "_pull_kind": [task.kind] * n,
        "_window_start": [parse_iso(task.window_start)] * n,
        "_window_end": [parse_iso(task.window_end)] * n,
        "_region": [task.region] * n,
    }
    return pa.table(data, schema=SCHEMA)


def to_parquet_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", write_statistics=True)
    return buf.getvalue()
