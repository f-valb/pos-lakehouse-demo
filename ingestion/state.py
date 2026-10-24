"""Small JSON control-plane store (planner state, poller feedback, gap ledger).

Local mode: objects in the LocalStack `pos-control` bucket. Prod: a DynamoDB table with
the same keys (see infra/terraform/aws). Each key has a single writer, so plain puts are safe.
"""

from __future__ import annotations

import json

from botocore.exceptions import ClientError


class MemoryStore:
    def __init__(self):
        self.data: dict[str, dict] = {}

    def get(self, key: str) -> dict | None:
        v = self.data.get(key)
        return json.loads(json.dumps(v)) if v is not None else None

    def put(self, key: str, value: dict) -> None:
        self.data[key] = json.loads(json.dumps(value))

    def delete(self, key: str) -> None:
        self.data.pop(key, None)

    def list(self, prefix: str) -> list[str]:
        return sorted(k for k in self.data if k.startswith(prefix))


class S3Store:
    def __init__(self, s3, bucket: str):
        self.s3, self.bucket = s3, bucket

    def get(self, key: str) -> dict | None:
        try:
            return json.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def put(self, key: str, value: dict) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(value, sort_keys=True).encode())

    def delete(self, key: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def list(self, prefix: str) -> list[str]:
        keys, token = [], None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": prefix, **({"ContinuationToken": token} if token else {})}
            resp = self.s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents", [])]
            if not resp.get("IsTruncated"):
                return sorted(keys)
            token = resp["NextContinuationToken"]


class DynamoStore:
    """Prod control store (infra/terraform/aws: <prefix>-ingestion-control, hash key `key`)."""

    def __init__(self, dynamodb, table: str):
        self.ddb, self.table = dynamodb, table

    def get(self, key: str) -> dict | None:
        item = self.ddb.get_item(TableName=self.table, Key={"key": {"S": key}}).get("Item")
        return json.loads(item["value"]["S"]) if item else None

    def put(self, key: str, value: dict) -> None:
        self.ddb.put_item(TableName=self.table, Item={"key": {"S": key}, "value": {"S": json.dumps(value, sort_keys=True)}})

    def delete(self, key: str) -> None:
        self.ddb.delete_item(TableName=self.table, Key={"key": {"S": key}})

    def list(self, prefix: str) -> list[str]:
        keys, start = [], None
        while True:
            kw = {"TableName": self.table, "ProjectionExpression": "#k", "ExpressionAttributeNames": {"#k": "key"},
                  "FilterExpression": "begins_with(#k, :p)", "ExpressionAttributeValues": {":p": {"S": prefix}}}
            if start:
                kw["ExclusiveStartKey"] = start
            resp = self.ddb.scan(**kw)
            keys += [i["key"]["S"] for i in resp.get("Items", [])]
            start = resp.get("LastEvaluatedKey")
            if not start:
                return sorted(keys)
