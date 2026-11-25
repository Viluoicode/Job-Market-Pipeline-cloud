# ============================================================================================
# Glue — Gold catalog databases (Iceberg + legacy Parquet) and two PySpark ETL jobs.
# Jobs are on-demand only (no triggers/schedule) — you start them via Step Functions.
# ============================================================================================

# Legacy Parquet Gold (until 2026-11): kept so its historical tables stay queryable. Nothing
# writes to it any more; the crawler that registered it was removed with the Iceberg switch.
resource "aws_glue_catalog_database" "gold" {
  name = var.gold_database_name
}

# Gold marts as Apache Iceberg tables. The Gold job creates and commits them, so their schema comes
# from code; data and metadata live under gold_iceberg/.
resource "aws_glue_catalog_database" "gold_iceberg" {
  name         = "${var.gold_database_name}_iceberg"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/gold_iceberg"
}

locals {
  iceberg_warehouse = "s3://${aws_s3_bucket.lake.bucket}/gold_iceberg/"
  # Glue accepts a single --conf key; the remaining settings are chained with " --conf ".
  iceberg_spark_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.glue_catalog.warehouse=${local.iceberg_warehouse}",
  ])
}

locals {
  glue_common_args = {
    "--job-language"                     = "python"
    "--LAKE_BUCKET"                      = aws_s3_bucket.lake.bucket
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.scripts.bucket}/${aws_s3_object.pipeline_contract.key}"
    "--stale_after_days"                 = tostring(var.stale_after_days)
    "--freshness_hours"                  = tostring(var.freshness_hours)
  }
}

resource "aws_glue_job" "bronze_to_silver" {
  #checkov:skip=CKV_AWS_195:buckets enforce SSE-S3 and TLS; a security configuration without KMS adds nothing
  name              = "${var.project}-bronze-to-silver"
  role_arn          = aws_iam_role.silver.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 0
  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/jobs/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, { "--TempDir" = "s3://${aws_s3_bucket.scripts.bucket}/tmp/silver/" })
  depends_on        = [aws_s3_object.bronze_to_silver, aws_iam_role_policy.silver]
}

resource "aws_glue_job" "silver_to_gold" {
  #checkov:skip=CKV_AWS_195:buckets enforce SSE-S3 and TLS; a security configuration without KMS adds nothing
  name              = "${var.project}-silver-to-gold"
  role_arn          = aws_iam_role.gold.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 0
  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/jobs/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--TempDir"          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/gold/"
    "--datalake-formats" = "iceberg"
    "--conf"             = local.iceberg_spark_conf
    "--iceberg_database" = aws_glue_catalog_database.gold_iceberg.name
    # Snapshot expiry after each publish (see silver_to_gold.maintain_iceberg).
    "--snapshot_retention_days" = tostring(var.iceberg_snapshot_retention_days)
  })
  depends_on = [aws_s3_object.silver_to_gold, aws_iam_role_policy.gold]
}
