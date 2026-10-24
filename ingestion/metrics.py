"""Buffered CloudWatch metrics (namespace POS/Ingestion). LocalStack locally, CloudWatch in prod."""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime

NAMESPACE = "POS/Ingestion"


class Metrics:
    def __init__(self, cloudwatch=None, flush_every_s: float = 10):
        self.cw, self.flush_every_s = cloudwatch, flush_every_s
        self._buf: list[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()

    def put(self, name: str, value: float, unit: str = "Count", **dims: str) -> None:
        d = {"MetricName": name, "Value": float(value), "Unit": unit, "Timestamp": datetime.now(UTC),
             "Dimensions": [{"Name": k, "Value": v} for k, v in dims.items()]}
        with self._lock:
            self._buf.append(d)
        if time.time() - self._last_flush >= self.flush_every_s:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            buf, self._buf = self._buf, []
            self._last_flush = time.time()
        if not self.cw:
            return
        for i in range(0, len(buf), 20):
            try:
                self.cw.put_metric_data(Namespace=NAMESPACE, MetricData=buf[i:i + 20])
            except Exception as e:  # metrics must never break ingestion
                print(f"[metrics] flush failed: {e}")
