"""Task queues. Two priorities: `live` (current windows) and `backfill` (re-pulls for late voids
and outage backfill). The poller always drains live first. Both redrive to one DLQ after
`max_receives` deliveries (SQS RedrivePolicy; emulated in MemoryQueue)."""

from __future__ import annotations

import heapq
import itertools
import time
from dataclasses import dataclass

from ingestion.models import PullTask


@dataclass
class Message:
    task: PullTask
    handle: str
    receive_count: int
    sent_ts: float


class SqsQueue:
    def __init__(self, sqs, name: str):
        self.sqs, self.name = sqs, name
        self.url = sqs.get_queue_url(QueueName=name)["QueueUrl"]

    def send(self, task: PullTask, delay_s: int = 0) -> None:
        self.sqs.send_message(QueueUrl=self.url, MessageBody=task.to_json(), DelaySeconds=min(900, delay_s))

    def receive(self, max_n: int = 1, wait_s: int = 1, visibility_s: int = 60) -> list[Message]:
        resp = self.sqs.receive_message(
            QueueUrl=self.url, MaxNumberOfMessages=max_n, WaitTimeSeconds=wait_s,
            VisibilityTimeout=visibility_s, AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        )
        return [
            Message(PullTask.from_json(m["Body"]), m["ReceiptHandle"],
                    int(m["Attributes"]["ApproximateReceiveCount"]), int(m["Attributes"]["SentTimestamp"]) / 1000)
            for m in resp.get("Messages", [])
        ]

    def delete(self, msg: Message) -> None:
        self.sqs.delete_message(QueueUrl=self.url, ReceiptHandle=msg.handle)

    def release(self, msg: Message, delay_s: int) -> None:
        self.sqs.change_message_visibility(QueueUrl=self.url, ReceiptHandle=msg.handle,
                                           VisibilityTimeout=min(43200, max(0, delay_s)))

    def depth(self) -> dict:
        a = self.sqs.get_queue_attributes(QueueUrl=self.url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])
        a = a["Attributes"]
        return {"visible": int(a["ApproximateNumberOfMessages"]),
                "in_flight": int(a["ApproximateNumberOfMessagesNotVisible"]),
                "delayed": int(a.get("ApproximateNumberOfMessagesDelayed", 0))}


class MemoryQueue:
    def __init__(self, name: str, max_receives: int = 5, dlq: MemoryQueue | None = None, clock=time.time):
        self.name, self.max_receives, self.dlq, self.clock = name, max_receives, dlq, clock
        self._heap: list = []
        self._ids = itertools.count()
        self._inflight: dict[str, tuple] = {}

    def send(self, task: PullTask, delay_s: int = 0, _count: int = 0, _sent: float | None = None) -> None:
        heapq.heappush(self._heap, (self.clock() + delay_s, next(self._ids), task, _count, _sent or self.clock()))

    def receive(self, max_n: int = 1, wait_s: int = 0, visibility_s: int = 60) -> list[Message]:
        out = []
        while self._heap and self._heap[0][0] <= self.clock() and len(out) < max_n:
            _, i, task, count, sent = heapq.heappop(self._heap)
            count += 1
            if count > self.max_receives and self.dlq is not None:
                self.dlq.send(task, _count=0, _sent=sent)
                continue
            h = f"{self.name}-{i}"
            self._inflight[h] = (task, count, sent)
            out.append(Message(task, h, count, sent))
        return out

    def delete(self, msg: Message) -> None:
        self._inflight.pop(msg.handle, None)

    def release(self, msg: Message, delay_s: int) -> None:
        task, count, sent = self._inflight.pop(msg.handle)
        heapq.heappush(self._heap, (self.clock() + delay_s, next(self._ids), task, count, sent))

    def requeue_inflight(self) -> None:
        """Simulate visibility timeout expiry after a crash."""
        for h, (task, count, sent) in list(self._inflight.items()):
            heapq.heappush(self._heap, (self.clock(), next(self._ids), task, count, sent))
            del self._inflight[h]

    def depth(self) -> dict:
        now = self.clock()
        return {"visible": sum(1 for e in self._heap if e[0] <= now), "in_flight": len(self._inflight),
                "delayed": sum(1 for e in self._heap if e[0] > now)}
