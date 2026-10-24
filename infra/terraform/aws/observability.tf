# Same alarms as ingestion/observability.py (LocalStack), per country.
resource "aws_sns_topic" "alarms" {
  name              = "${local.prefix}-ingestion-alarms"
  kms_master_key_id = aws_kms_key.pos.id
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

locals {
  dims = { Region = upper(var.country) }
}

resource "aws_cloudwatch_metric_alarm" "api_error_rate" {
  alarm_name          = "${local.prefix}-api-error-rate"
  alarm_description   = "Legacy API errors > 20% of requests over 2 min"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  threshold           = 20
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
  metric_query {
    id = "err"
    metric {
      namespace   = "POS/Ingestion"
      metric_name = "ApiErrors"
      dimensions  = local.dims
      period      = 60
      stat        = "Sum"
    }
  }
  metric_query {
    id = "req"
    metric {
      namespace   = "POS/Ingestion"
      metric_name = "ApiRequests"
      dimensions  = local.dims
      period      = 60
      stat        = "Sum"
    }
  }
  metric_query {
    id          = "rate"
    expression  = "100 * err / MAX([req, 1])"
    return_data = true
  }
}

resource "aws_cloudwatch_metric_alarm" "circuit_open" {
  alarm_name          = "${local.prefix}-circuit-open"
  namespace           = "POS/Ingestion"
  metric_name         = "CircuitOpen"
  dimensions          = local.dims
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "freshness" {
  alarm_name          = "${local.prefix}-ingest-freshness"
  alarm_description   = "Newest ingested live window older than 10 min (15-min end-to-end SLA)"
  namespace           = "POS/Ingestion"
  metric_name         = "IngestFreshnessSeconds"
  dimensions          = local.dims
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 600
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "dlq" {
  alarm_name          = "${local.prefix}-dlq-not-empty"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "live_queue_age" {
  alarm_name          = "${local.prefix}-live-queue-age"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.pull["live"].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}
