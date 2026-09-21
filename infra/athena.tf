# ============================================================================================
# Athena workgroup — enforces the result location and caps bytes scanned per query (cost guard).
# ============================================================================================

resource "aws_athena_workgroup" "main" {
  name          = var.athena_workgroup_name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}


resource "aws_athena_named_query" "inspect_today_jobs" {
  name        = "Jobs observed today (Vietnam)"
  description = "Inspect today's actual observations in the latest Gold snapshot; timestamps shown in UTC and Vietnam time."
  database    = aws_glue_catalog_database.gold.name
  workgroup   = aws_athena_workgroup.main.name
  query       = file("${path.module}/../sql/inspect_today_jobs.sql")
}
