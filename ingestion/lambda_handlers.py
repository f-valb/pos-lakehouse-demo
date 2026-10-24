"""AWS Lambda entry points (prod). Same Planner/Poller code as local mode.

planner_handler   EventBridge Scheduler, rate(1 minute), one schedule per country account.
poller_handler    SQS event source on the live + backfill queues. Global API protection comes
                  from Lambda *reserved concurrency* (= max_global_concurrency) on top of the
                  in-process AIMD limiter; per-region breaker state is shared via the control
                  store so concurrent invocations agree that a region is down.
                  Uses ReportBatchItemFailures so only failed messages are redelivered.

Not deployed in the demo (no AWS account); see infra/terraform/aws/ingestion.tf.
"""

from __future__ import annotations

import asyncio
import json
import os

import httpx

from ingestion.aws import client
from ingestion.config import Settings
from ingestion.metrics import Metrics
from ingestion.models import PullTask
from ingestion.planner import Planner
from ingestion.poller import Poller
from ingestion.queues import Message, SqsQueue
from ingestion.sinks import S3Sink
from ingestion.state import S3Store


def _settings() -> Settings:
    os.environ.pop("AWS_ENDPOINT_URL", None)  # real AWS endpoints inside Lambda
    return Settings(aws_endpoint=None)


def planner_handler(event, context):
    s = _settings()
    sqs = client("sqs", s)
    p = Planner(s, S3Store(client("s3", s), s.control_bucket), SqsQueue(sqs, s.live_queue), SqsQueue(sqs, s.backfill_queue))
    return p.tick()


class _NoopQueue:
    """Lambda's SQS integration owns delete/redelivery; we just report failures."""

    def __init__(self, name):
        self.name, self.failed = name, set()

    def delete(self, msg):
        pass

    def release(self, msg, delay_s):
        self.failed.add(msg.handle)


def poller_handler(event, context):
    s = _settings()
    q = _NoopQueue(s.live_queue)
    poller = Poller(s, q, q, S3Store(client("s3", s), s.control_bucket),
                    [S3Sink(client("s3", s), s.raw_bucket)], Metrics(client("cloudwatch", s)))

    async def go():
        async with httpx.AsyncClient(timeout=s.http_timeout_s) as http:
            poller.http = http
            for rec in event["Records"]:
                msg = Message(PullTask.from_json(rec["body"]), rec["messageId"],
                              int(rec["attributes"]["ApproximateReceiveCount"]),
                              int(rec["attributes"]["SentTimestamp"]) / 1000)
                await poller.process(msg, q)
        poller.metrics.flush()

    asyncio.run(go())
    return {"batchItemFailures": [{"itemIdentifier": h} for h in q.failed]}


if __name__ == "__main__":  # local smoke: python -m ingestion.lambda_handlers
    print(json.dumps({"handlers": ["planner_handler", "poller_handler"]}))
