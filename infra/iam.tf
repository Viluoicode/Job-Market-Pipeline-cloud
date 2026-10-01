# ============================================================================================
# IAM â€” one role for Glue (jobs + crawler), one for the Step Functions state machine.
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

# ---- Step Functions role -------------------------------------------------------------------
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = ["arn:aws:states:${var.region}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.project}-pipeline"]
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
      aws_glue_job.ingestion.arn,
      aws_glue_job.bronze_to_silver.arn,
      aws_glue_job.silver_to_gold.arn,
    ]
  }
  statement {
    sid       = "RunGlueCrawler"
    actions   = ["glue:StartCrawler", "glue:GetCrawler"]
    resources = [aws_glue_crawler.gold.arn]
  }
  statement {
    sid       = "SerializePipeline"
    actions   = ["dynamodb:PutItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.pipeline_lock.arn]
    condition {
      test     = "ForAllValues:StringEquals"
      variable = "dynamodb:LeadingKeys"
      values   = ["pipeline"]
    }
  }
  statement {
    sid       = "PublishCompletion"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/state/pipeline/latest.json"]
  }
}

resource "aws_iam_role_policy" "sfn_policy" {
  name   = "${var.project}-sfn-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn_policy.json
}
