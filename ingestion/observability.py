"""CloudWatch alarm definitions, created identically in LocalStack (local_setup) and
mirrored in infra/terraform/aws/observability.tf for prod."""

from __future__ import annotations

from ingestion.metrics import NAMESPACE


def alarm_specs(regions, dlq_name: str, live_queue: str) -> list[dict]:
    specs = []
    for r in regions:
        specs += [
            {
                "AlarmName": f"pos-{r}-api-error-rate",
                "AlarmDescription": f"Legacy API errors for {r} > 20% of requests over 2 min",
                "Metrics": [
                    {"Id": "err", "MetricStat": {"Metric": {"Namespace": NAMESPACE, "MetricName": "ApiErrors",
                                                            "Dimensions": [{"Name": "Region", "Value": r}]},
                                                 "Period": 60, "Stat": "Sum"}, "ReturnData": False},
                    {"Id": "req", "MetricStat": {"Metric": {"Namespace": NAMESPACE, "MetricName": "ApiRequests",
                                                            "Dimensions": [{"Name": "Region", "Value": r}]},
                                                 "Period": 60, "Stat": "Sum"}, "ReturnData": False},
                    {"Id": "rate", "Expression": "100 * err / MAX([req, 1])", "ReturnData": True},
                ],
                "Threshold": 20, "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 2,
                "TreatMissingData": "notBreaching",
            },
            {
                "AlarmName": f"pos-{r}-circuit-open",
                "AlarmDescription": f"Poller circuit breaker open for {r}",
                "Namespace": NAMESPACE, "MetricName": "CircuitOpen",
                "Dimensions": [{"Name": "Region", "Value": r}],
                "Statistic": "Maximum", "Period": 60, "EvaluationPeriods": 1, "Threshold": 0,
                "ComparisonOperator": "GreaterThanThreshold", "TreatMissingData": "notBreaching",
            },
            {
                "AlarmName": f"pos-{r}-ingest-freshness",
                "AlarmDescription": f"Newest ingested live window for {r} older than 10 min (15 min SLA budget)",
                "Namespace": NAMESPACE, "MetricName": "IngestFreshnessSeconds",
                "Dimensions": [{"Name": "Region", "Value": r}],
                "Statistic": "Maximum", "Period": 60, "EvaluationPeriods": 2, "Threshold": 600,
                "ComparisonOperator": "GreaterThanThreshold", "TreatMissingData": "breaching",
            },
        ]
    specs += [
        {
            "AlarmName": "pos-dlq-not-empty",
            "AlarmDescription": "Tasks exhausted retries and landed in the DLQ",
            "Namespace": "AWS/SQS", "MetricName": "ApproximateNumberOfMessagesVisible",
            "Dimensions": [{"Name": "QueueName", "Value": dlq_name}],
            "Statistic": "Maximum", "Period": 60, "EvaluationPeriods": 1, "Threshold": 0,
            "ComparisonOperator": "GreaterThanThreshold", "TreatMissingData": "notBreaching",
        },
        {
            "AlarmName": "pos-live-queue-age",
            "AlarmDescription": "Oldest live task waiting > 5 min (poller not keeping up)",
            "Namespace": "AWS/SQS", "MetricName": "ApproximateAgeOfOldestMessage",
            "Dimensions": [{"Name": "QueueName", "Value": live_queue}],
            "Statistic": "Maximum", "Period": 60, "EvaluationPeriods": 2, "Threshold": 300,
            "ComparisonOperator": "GreaterThanThreshold", "TreatMissingData": "notBreaching",
        },
    ]
    return specs
