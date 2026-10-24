# Raw landing bucket (country-local), KMS-encrypted, versioned, TLS-only.
resource "aws_kms_key" "pos" {
  description         = "${local.prefix} raw POS data"
  enable_key_rotation = true
}

resource "aws_s3_bucket" "raw" {
  bucket = "${local.prefix}-raw-${data.aws_caller_identity.me.account_id}"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.pos.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-small-raw-files" # compaction has copied them to compacted/
    status = "Enabled"
    filter { prefix = "raw/" }
    expiration { days = 7 }
    noncurrent_version_expiration { noncurrent_days = 7 }
  }
  rule {
    id     = "archive-compacted"
    status = "Enabled"
    filter { prefix = "compacted/" }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

data "aws_iam_policy_document" "tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "raw" {
  bucket = aws_s3_bucket.raw.id
  policy = data.aws_iam_policy_document.tls_only.json
}

# S3 -> SQS event notifications are created by Databricks (managed file events on the external
# location), so Auto Loader does not have to list the bucket.

# Control-plane state (planner watermarks, poller feedback, gap ledger). Local mode: pos-control bucket.
resource "aws_dynamodb_table" "control" {
  name         = "${local.prefix}-ingestion-control"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "key"
  attribute {
    name = "key"
    type = "S"
  }
  point_in_time_recovery { enabled = true }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.pos.arn
  }
}
