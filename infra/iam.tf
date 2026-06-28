# ============================================================================================
# IAM — one role for Glue (jobs + crawler), one for the Step Functions state machine.
# ============================================================================================

# ---- Glue role -----------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

# Managed policy: Glue catalog access, CloudWatch Logs, networking, etc.
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Our own buckets aren't named "aws-glue-*", so grant S3 access explicitly.
data "aws_iam_policy_document" "glue_s3" {
  statement {
    sid     = "ObjectAccess"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/*",
      "${aws_s3_bucket.scripts.arn}/*",
      "${aws_s3_bucket.athena_results.arn}/*",
    ]
  }
  statement {
    sid     = "ListBuckets"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.lake.arn,
      aws_s3_bucket.scripts.arn,
      aws_s3_bucket.athena_results.arn,
    ]
  }
  statement {
    sid       = "Metrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${var.project}-glue-s3"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

# ---- Step Functions role -------------------------------------------------------------------
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.project}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn_policy" {
  statement {
    sid     = "RunGlueJobs"
    actions = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [
      aws_glue_job.bronze_to_silver.arn,
      aws_glue_job.silver_to_gold.arn,
    ]
  }
  statement {
    sid       = "RunGlueCrawler"
    actions   = ["glue:StartCrawler", "glue:GetCrawler"]
    resources = [aws_glue_crawler.gold.arn]
  }
}

resource "aws_iam_role_policy" "sfn_policy" {
  name   = "${var.project}-sfn-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn_policy.json
}
