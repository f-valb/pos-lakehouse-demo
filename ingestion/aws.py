from __future__ import annotations

import boto3

from ingestion.config import Settings


def client(service: str, s: Settings):
    # In local mode AWS_ENDPOINT_URL points at LocalStack; in Lambda/ECS it is unset.
    return boto3.client(service, endpoint_url=s.aws_endpoint or None, region_name=s.aws_region)
