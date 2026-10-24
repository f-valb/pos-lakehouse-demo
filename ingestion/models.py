from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def compact(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class PullTask:
    """One (region, transaction-time window) pull.

    `pulled_at` is the *logical* ingestion timestamp, fixed when the planner creates the task.
    It is the Auto CDC sequence column and part of the output key, so every retry of the task
    produces the same object key and the same `_ingest_ts` values (idempotent by construction).
    A later re-pull of the same window is a *new* task with a later `pulled_at`, which is how
    late voids win over the original sale in silver.
    """

    region: str
    window_start: str
    window_end: str
    kind: str  # live | repull | backfill
    pulled_at: str
    enqueued_at: str

    @property
    def task_id(self) -> str:
        return f"{self.region}-{compact(parse_iso(self.window_start))}-{compact(parse_iso(self.window_end))}-{self.kind}-{compact(parse_iso(self.pulled_at))}"

    @property
    def window_seconds(self) -> int:
        return int((parse_iso(self.window_end) - parse_iso(self.window_start)).total_seconds())

    def object_key(self) -> str:
        """Deterministic path: same task -> same key, so retries overwrite nothing new."""
        ws, we = parse_iso(self.window_start), parse_iso(self.window_end)
        return (
            f"raw/region={self.region}/date={ws:%Y-%m-%d}/"
            f"window={compact(ws)}_{compact(we)}/{self.kind}-{compact(parse_iso(self.pulled_at))}.parquet"
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @staticmethod
    def from_json(s: str) -> PullTask:
        return PullTask(**json.loads(s))
