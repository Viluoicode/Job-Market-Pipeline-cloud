# ============================================================================================
# Glue — Gold catalog database, two PySpark ETL jobs, and the Gold crawler.
# Jobs are on-demand only (no triggers/schedule) — you start them via Step Functions.
# ============================================================================================

resource "aws_glue_catalog_database" "gold" {
  name = var.gold_database_name
}

locals {
  glue_common_args = {
    "--job-language"                     = "python"
    "--LAKE_BUCKET"                      = aws_s3_bucket.lake.bucket
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }
}

resource "aws_glue_job" "bronze_to_silver" {
  name              = "${var.project}-bronze-to-silver"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/jobs/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  depends_on        = [aws_s3_object.bronze_to_silver]
}

resource "aws_glue_job" "silver_to_gold" {
  name              = "${var.project}-silver-to-gold"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/jobs/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  depends_on        = [aws_s3_object.silver_to_gold]
}

# Crawls s3://<lake>/gold/ and registers fact_job_posting + demand_by_role (one table per
# top-level folder via TableLevelConfiguration = 3: <bucket>/gold/<table>/snapshot_date=.../).
resource "aws_glue_crawler" "gold" {
  name          = "${var.project}-gold-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.gold.name

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/gold/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableLevelConfiguration = 3
    }
  })
}
