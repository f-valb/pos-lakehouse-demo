# IAM role assumed by Databricks Unity Catalog (storage credential) to read the raw bucket and
# to manage file-event notifications for Auto Loader (managed file events).
data "aws_iam_policy_document" "uc_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.databricks_uc_master_role_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_storage_credential_external_id]
    }
  }
  statement { # self-assume, required by UC
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.me.account_id}:root"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:PrincipalArn"
      values   = ["arn:aws:iam::${data.aws_caller_identity.me.account_id}:role/${local.prefix}-uc-access"]
    }
  }
}

resource "aws_iam_role" "uc_access" {
  name               = "${local.prefix}-uc-access"
  assume_role_policy = data.aws_iam_policy_document.uc_assume.json
}

data "aws_iam_policy_document" "uc_access" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*"]
    resources = [aws_kms_key.pos.arn]
  }
  statement { # managed file events (S3 -> SNS/SQS set up by Databricks)
    actions = ["s3:GetBucketNotification", "s3:PutBucketNotification", "sns:ListSubscriptionsByTopic", "sns:GetTopicAttributes",
      "sns:SetTopicAttributes", "sns:CreateTopic", "sns:TagResource", "sns:Publish", "sns:Subscribe", "sqs:CreateQueue",
      "sqs:DeleteMessage", "sqs:ReceiveMessage", "sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes",
    "sqs:SetQueueAttributes", "sqs:TagQueue", "sqs:ChangeMessageVisibility", "sqs:PurgeQueue"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "uc_access" {
  role   = aws_iam_role.uc_access.id
  policy = data.aws_iam_policy_document.uc_access.json
}

output "raw_bucket" { value = aws_s3_bucket.raw.bucket }
output "uc_access_role_arn" { value = aws_iam_role.uc_access.arn }
output "live_queue_url" { value = aws_sqs_queue.pull["live"].url }
output "alarm_topic_arn" { value = aws_sns_topic.alarms.arn }
