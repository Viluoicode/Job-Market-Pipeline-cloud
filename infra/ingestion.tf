# Python Shell keeps ingestion inside the existing Glue/Step Functions stack.
# Unlike Spark ETL, this job uses only 0.0625 DPU and has a 15-minute ceiling.
resource "aws_glue_job" "ingestion" {
  name         = "${var.project}-ingestion"
  role_arn     = aws_iam_role.ingestion.arn
  max_capacity = 0.0625
  timeout      = 15
  max_retries  = 0

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/${aws_s3_object.ingestion.key}"
  }
  execution_property {
    max_concurrent_runs = 1
  }
  default_arguments = {
    "--library-set"               = "analytics"
    "--additional-python-modules" = "httpx==0.28.1"
    "--bucket"                    = aws_s3_bucket.lake.bucket
    "--sources-s3-uri"            = "s3://${aws_s3_bucket.scripts.bucket}/${aws_s3_object.sources.key}"
    "--min-success-ratio"         = tostring(var.ingestion_min_success_ratio)
  }
  depends_on = [aws_iam_role_policy.ingestion]
}

resource "aws_s3_object" "ingestion" {
  bucket       = aws_s3_bucket.scripts.id
  key          = "ingestion/land_to_bronze.py"
  source       = "${path.module}/../ingestion/land_to_bronze.py"
  etag         = filemd5("${path.module}/../ingestion/land_to_bronze.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "sources" {
  bucket       = aws_s3_bucket.scripts.id
  key          = "ingestion/sources.json"
  source       = "${path.module}/../ingestion/sources.json"
  etag         = filemd5("${path.module}/../ingestion/sources.json")
  content_type = "application/json"
}

resource "aws_iam_role" "ingestion" {
  name               = "${var.project}-ingestion-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "ingestion" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.scripts.arn}/ingestion/*"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/control/ingestion/*"]
  }
  statement {
    # Exact-prefix discovery allows idempotent manifest lookup without broad bucket listing.
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["control/ingestion/*"]
    }
  }
  statement {
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn, aws_s3_bucket.scripts.arn]
  }
  statement {
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/bronze/*",
      "${aws_s3_bucket.lake.arn}/control/ingestion/*",
    ]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "ingestion" {
  name   = "${var.project}-ingestion"
  role   = aws_iam_role.ingestion.id
  policy = data.aws_iam_policy_document.ingestion.json
}

resource "aws_dynamodb_table" "pipeline_lock" {
  name         = "${var.project}-pipeline-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockId"
  attribute {
    name = "LockId"
    type = "S"
  }
  # No TTL: an automatic expiry could unlock while Glue is still writing after SFN times out.
  # An aborted/timed-out execution requires the recovery procedure in docs/operations.md.
}
