"""Runtime settings. Everything comes from env vars; there are no secrets in code.

Databricks auth is resolved by the SDK from ~/.databrickscfg / DATABRICKS_* env vars.
AWS auth is resolved by boto3 (LocalStack in local mode, IAM role in Lambda/ECS).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    env: str = field(default_factory=lambda: _env("DEMO_ENV", "dev"))
    api_url: str = field(default_factory=lambda: _env("POS_API_URL", "http://127.0.0.1:8080"))
    aws_endpoint: str | None = field(default_factory=lambda: os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:4566"))
    aws_region: str = field(default_factory=lambda: _env("AWS_DEFAULT_REGION", "ap-southeast-1"))
    control_bucket: str = field(default_factory=lambda: _env("CONTROL_BUCKET", "pos-control"))
    live_queue: str = field(default_factory=lambda: _env("LIVE_QUEUE", "pos-pull-live"))
    backfill_queue: str = field(default_factory=lambda: _env("BACKFILL_QUEUE", "pos-pull-backfill"))
    dlq: str = field(default_factory=lambda: _env("DLQ", "pos-pull-dlq"))
    sinks: tuple[str, ...] = field(default_factory=lambda: tuple(s for s in _env("POLLER_SINKS", "s3").split(",") if s))
    regions: tuple[str, ...] = field(default_factory=lambda: tuple(_env("REGIONS", "PH,SG,DE,US").split(",")))

    # Planner
    tick_seconds: int = int(_env("PLANNER_TICK_SECONDS", "60"))
    live_lag_seconds: int = int(_env("LIVE_LAG_SECONDS", "45"))  # legacy exposes sales ~20s late
    repull_after_seconds: int = int(_env("REPULL_AFTER_SECONDS", "1260"))  # catch late voids (<=20 min)
    backfill_per_tick: int = int(_env("BACKFILL_PER_TICK", "3"))

    # Poller
    max_global_concurrency: int = int(_env("POLLER_MAX_CONCURRENCY", "4"))  # below API soft limit
    max_backfill_concurrency: int = int(_env("POLLER_MAX_BACKFILL_CONCURRENCY", "1"))
    region_rps: float = float(_env("POLLER_REGION_RPS", "2"))
    http_timeout_s: float = float(_env("POLLER_HTTP_TIMEOUT", "10"))
    visibility_s: int = int(_env("POLLER_VISIBILITY_S", "60"))  # SQS redelivery after a crash
    attempts_per_receive: int = 3
    max_receives: int = 5  # SQS maxReceiveCount -> DLQ
    target_records_per_pull: int = int(_env("TARGET_RECORDS_PER_PULL", "15000"))
    min_window_s: int = 60
    max_window_s: int = 600
    breaker_failures: int = 3
    breaker_cooldown_s: float = float(_env("BREAKER_COOLDOWN_S", "30"))

    def raw_bucket(self, region: str) -> str:
        # One bucket per country: in prod each lives in that country's AWS account/region.
        return f"pos-raw-{region.lower()}"

    def volume_root(self, region: str) -> str:
        return f"/Volumes/pos_{region.lower()}/{self.env}_bronze/landing"
