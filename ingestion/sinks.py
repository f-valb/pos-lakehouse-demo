"""Write-once sinks for raw Parquet. Keys are deterministic per task, so a retry either finds
the object already there (skip) or writes the same key. Nothing is ever appended or renamed.

* S3Sink      per-country bucket (LocalStack locally; real S3 in prod, read by Auto Loader
              through a UC external location).
* VolumeSink  UC Volume via the Databricks Files API. Needed because Databricks Free Edition
              cannot reach LocalStack on a laptop; in prod this sink does not exist.
"""

from __future__ import annotations

import io

from botocore.exceptions import ClientError


class S3Sink:
    name = "s3"

    def __init__(self, s3, bucket_for_region):
        self.s3, self.bucket_for_region = s3, bucket_for_region

    def exists(self, region: str, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket_for_region(region), Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def read(self, region: str, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket_for_region(region), Key=key)["Body"].read()

    def put(self, region: str, key: str, data: bytes) -> bool:
        """Returns False if the object already existed (conditional write lost the race)."""
        try:
            self.s3.put_object(Bucket=self.bucket_for_region(region), Key=key, Body=data, IfNoneMatch="*")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
                return False
            raise


class VolumeSink:
    name = "volume"

    def __init__(self, workspace_client, root_for_region):
        self.w, self.root_for_region = workspace_client, root_for_region

    def _path(self, region: str, key: str) -> str:
        return f"{self.root_for_region(region)}/{key}"

    def exists(self, region: str, key: str) -> bool:
        from databricks.sdk.errors import NotFound

        try:
            self.w.files.get_metadata(self._path(region, key))
            return True
        except NotFound:
            return False

    def read(self, region: str, key: str) -> bytes:
        return self.w.files.download(self._path(region, key)).contents.read()

    def put(self, region: str, key: str, data: bytes) -> bool:
        from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists

        try:
            self.w.files.upload(self._path(region, key), io.BytesIO(data), overwrite=False)
            return True
        except (AlreadyExists, ResourceAlreadyExists):
            return False


class MemorySink:
    def __init__(self, name: str = "mem"):
        self.name = name
        self.objects: dict[tuple[str, str], bytes] = {}
        self.writes = 0

    def exists(self, region, key):
        return (region, key) in self.objects

    def read(self, region, key):
        return self.objects[(region, key)]

    def put(self, region, key, data):
        if (region, key) in self.objects:
            return False
        self.objects[(region, key)] = data
        self.writes += 1
        return True
