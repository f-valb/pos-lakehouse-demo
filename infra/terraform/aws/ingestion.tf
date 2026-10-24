# Planner (EventBridge Scheduler, every minute) -> SQS live/backfill -> poller Lambda -> S3.
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-pull-dlq"
  message_retention_seconds = 1209600
  kms_master_key_id         = aws_kms_key.pos.arn
}

resource "aws_sqs_queue" "pull" {
  for_each                   = toset(["live", "backfill"])
  name                       = "${local.prefix}-pull-${each.key}"
  visibility_timeout_seconds = 120 # > poller Lambda timeout
  message_retention_seconds  = 86400
  kms_master_key_id          = aws_kms_key.pos.arn
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_ecr_repository" "ingestion" {
  name                 = "${local.prefix}-ingestion"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingestion" {
  name               = "${local.prefix}-ingestion"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "ingestion" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
  }
  statement {
    actions   = ["sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
    resources = concat([for q in aws_sqs_queue.pull : q.arn], [aws_sqs_queue.dlq.arn])
  }
  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan"]
    resources = [aws_dynamodb_table.control.arn]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.pos.arn]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["POS/Ingestion"]
    }
  }
}

resource "aws_iam_role_policy" "ingestion" {
  role   = aws_iam_role.ingestion.id
  policy = data.aws_iam_policy_document.ingestion.json
}

resource "aws_iam_role_policy_attachment" "vpc" {
  role       = aws_iam_role.ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

locals {
  lambda_env = {
    DEMO_ENV       = var.env
    REGIONS        = upper(var.country)
    POS_API_URL    = var.pos_api_url
    LIVE_QUEUE     = aws_sqs_queue.pull["live"].name
    BACKFILL_QUEUE = aws_sqs_queue.pull["backfill"].name
    DLQ            = aws_sqs_queue.dlq.name
    CONTROL_TABLE  = aws_dynamodb_table.control.name
    RAW_BUCKET     = aws_s3_bucket.raw.bucket
  }
}

resource "aws_lambda_function" "planner" {
  function_name = "${local.prefix}-planner"
  role          = aws_iam_role.ingestion.arn
  package_type  = "Image"
  image_uri     = var.poller_image_uri
  image_config { command = ["ingestion.lambda_handlers.planner_handler"] }
  timeout     = 50
  memory_size = 256
  environment { variables = local.lambda_env }
}

resource "aws_lambda_function" "poller" {
  function_name = "${local.prefix}-poller"
  role          = aws_iam_role.ingestion.arn
  package_type  = "Image"
  image_uri     = var.poller_image_uri
  image_config { command = ["ingestion.lambda_handlers.poller_handler"] }
  timeout     = 90
  memory_size = 1024
  # The global API-protection cap: never more than N concurrent pulls from this country.
  reserved_concurrent_executions = var.max_api_concurrency
  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }
  environment { variables = local.lambda_env }
}

resource "aws_lambda_event_source_mapping" "live" {
  event_source_arn        = aws_sqs_queue.pull["live"].arn
  function_name           = aws_lambda_function.poller.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  scaling_config { maximum_concurrency = var.max_api_concurrency }
}

resource "aws_lambda_event_source_mapping" "backfill" {
  event_source_arn        = aws_sqs_queue.pull["backfill"].arn
  function_name           = aws_lambda_function.poller.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  scaling_config { maximum_concurrency = 2 } # backfill never takes more than 2 of the slots (live priority)
}

# EventBridge Scheduler -> planner, every minute.
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.prefix}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "lambda:InvokeFunction", Resource = aws_lambda_function.planner.arn }]
  })
}

resource "aws_scheduler_schedule" "planner" {
  name                = "${local.prefix}-planner"
  schedule_expression = "rate(1 minute)"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_lambda_function.planner.arn
    role_arn = aws_iam_role.scheduler.arn
    retry_policy { maximum_retry_attempts = 0 } # the next tick re-plans anyway
  }
}
