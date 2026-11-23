# Stage-specific runtime roles: one least-privilege role per workload (Silver, Gold, dashboard
# reader). The Gold crawler and its role were removed when Gold moved to Iceberg (2026-11).

# --------------------------------------------------------------------------------------------
# Silver job (bronze -> silver): reads Bronze + its manifest + previous Silver, writes a new
# immutable Silver run, DQ results and the Silver state pointer.
# --------------------------------------------------------------------------------------------
resource "aws_iam_role" "silver" {
  name               = "${var.project}-silver-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "silver" {
  statement {
    sid     = "ReadInputs"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/bronze/*",
      "${aws_s3_bucket.lake.arn}/control/ingestion/*",
      "${aws_s3_bucket.lake.arn}/silver/*",
      "${aws_s3_bucket.lake.arn}/state/silver/latest.json",
    ]
  }
  statement {
    sid     = "ReadCode"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.scripts.arn}/jobs/bronze_to_silver.py",
      "${aws_s3_bucket.scripts.arn}/jobs/pipeline_contract.py",
    ]
  }
  statement {
    sid = "WriteOutputs"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${aws_s3_bucket.lake.arn}/silver/runs/*",
      "${aws_s3_bucket.lake.arn}/quality/silver_jobs/*",
      "${aws_s3_bucket.scripts.arn}/tmp/silver/*",
    ]
  }
  statement {
    sid = "BucketMetadata"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      aws_s3_bucket.lake.arn,
      aws_s3_bucket.scripts.arn,
    ]
  }
  statement {
    sid = "GlueLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"]
  }
  statement {
    sid       = "GlueMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values = [
        "Glue",
        "Glue Data Quality",
      ]
    }
  }
  statement {
    sid = "OwnJobStatus"
    actions = [
      "glue:GetJob",
      "glue:GetJobRun",
      "glue:GetJobRuns",
    ]
    resources = ["arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:job/${var.project}-bronze-to-silver"]
  }
  statement {
    sid       = "PublishSilverState"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/state/silver/latest.json"]
  }
  statement {
    sid = "PublishQuality"
    actions = [
      "glue:PublishDataQuality",
      "glue:GetDataQualityResult",
    ]
    resources = ["arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:dataQualityRuleset/*"]
  }
}

resource "aws_iam_role_policy" "silver" {
  name   = "${var.project}-silver"
  role   = aws_iam_role.silver.id
  policy = data.aws_iam_policy_document.silver.json
}

# --------------------------------------------------------------------------------------------
# Gold job (silver -> gold): reads the committed Silver run, writes the three Gold marts.
# --------------------------------------------------------------------------------------------
resource "aws_iam_role" "gold" {
  name               = "${var.project}-gold-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "gold" {
  statement {
    sid     = "ReadInputs"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/silver/runs/*",
      "${aws_s3_bucket.lake.arn}/state/silver/latest.json",
      "${aws_s3_bucket.lake.arn}/control/ingestion/*",
    ]
  }
  statement {
    sid     = "ReadCode"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.scripts.arn}/jobs/silver_to_gold.py",
      "${aws_s3_bucket.scripts.arn}/jobs/pipeline_contract.py",
    ]
  }
  statement {
    sid = "WriteOutputs"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.scripts.arn}/tmp/gold/*"]
  }
  statement {
    sid = "BucketMetadata"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      aws_s3_bucket.lake.arn,
      aws_s3_bucket.scripts.arn,
    ]
  }
  statement {
    sid = "GlueLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"]
  }
  statement {
    sid       = "GlueMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values = [
        "Glue",
        "Glue Data Quality",
      ]
    }
  }
  statement {
    sid = "OwnJobStatus"
    actions = [
      "glue:GetJob",
      "glue:GetJobRun",
      "glue:GetJobRuns",
    ]
    resources = ["arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:job/${var.project}-silver-to-gold"]
  }
  statement {
    sid = "WriteIcebergGold"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lake.arn}/gold_iceberg/*"]
  }
  statement {
    sid       = "RecordGoldVersions"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/state/gold_iceberg/*"]
  }
  statement {
    sid = "ManageIcebergTables"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${var.gold_database_name}_iceberg",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${var.gold_database_name}_iceberg/*",
    ]
  }
}

resource "aws_iam_role_policy" "gold" {
  name   = "${var.project}-gold"
  role   = aws_iam_role.gold.id
  policy = data.aws_iam_policy_document.gold.json
}

# --------------------------------------------------------------------------------------------
# Dashboard reader: assumed by the local dashboard; reads published Gold and metadata only.
# --------------------------------------------------------------------------------------------
resource "aws_iam_role" "dashboard_reader" {
  count = length(var.dashboard_reader_principal_arns) > 0 ? 1 : 0
  name  = "${var.project}-dashboard-reader-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.dashboard_reader_principal_arns }
      Action    = "sts:AssumeRole"
    }]
  })
  max_session_duration = 3600
}

data "aws_iam_policy_document" "dashboard_reader" {
  statement {
    sid     = "ReadPublishedData"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/gold_iceberg/*",
      "${aws_s3_bucket.lake.arn}/state/pipeline/latest.json",
      "${aws_s3_bucket.lake.arn}/state/silver/latest.json",
      "${aws_s3_bucket.lake.arn}/control/ingestion/*/manifest.json",
    ]
  }
  statement {
    sid       = "ListPublishedData"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "gold_iceberg",
        "gold_iceberg/*",
        "state/*",
        "control/ingestion/*",
      ]
    }
  }
  statement {
    sid     = "BucketLocation"
    actions = ["s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.lake.arn,
      aws_s3_bucket.athena_results.arn,
    ]
  }
  statement {
    sid = "QueryResults"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.athena_results.arn}/results/*"]
  }
  statement {
    sid = "ListResults"
    actions = [
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.athena_results.arn]
  }
  statement {
    sid = "AthenaQueries"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.main.arn]
  }
  statement {
    sid = "CatalogRead"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartitions",
      "glue:GetPartition",
      "glue:BatchGetPartition",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.gold_iceberg.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.gold_iceberg.name}/*",
    ]
  }
  statement {
    sid       = "ReadPipelineLock"
    actions   = ["dynamodb:GetItem"]
    resources = [aws_dynamodb_table.pipeline_lock.arn]
    condition {
      test     = "ForAllValues:StringEquals"
      variable = "dynamodb:LeadingKeys"
      values   = ["pipeline"]
    }
  }
}

resource "aws_iam_role_policy" "dashboard_reader" {
  count  = length(var.dashboard_reader_principal_arns) > 0 ? 1 : 0
  name   = "${var.project}-dashboard-reader"
  role   = aws_iam_role.dashboard_reader[0].id
  policy = data.aws_iam_policy_document.dashboard_reader.json
}
