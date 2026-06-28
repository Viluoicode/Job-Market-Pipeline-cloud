output "lake_bucket" {
  description = "Data lake bucket (point ingestion at this; holds bronze/silver/gold)."
  value       = aws_s3_bucket.lake.bucket
}

output "scripts_bucket" {
  description = "Bucket holding the Glue PySpark scripts + TempDir."
  value       = aws_s3_bucket.scripts.bucket
}

output "athena_results_bucket" {
  description = "Bucket for Athena query results."
  value       = aws_s3_bucket.athena_results.bucket
}

output "state_machine_arn" {
  description = "Step Functions state machine ARN (start this to run the pipeline)."
  value       = aws_sfn_state_machine.pipeline.arn
}

output "athena_workgroup" {
  description = "Athena workgroup to select in the console."
  value       = aws_athena_workgroup.main.name
}

output "gold_database" {
  description = "Glue Data Catalog database for the Gold marts."
  value       = aws_glue_catalog_database.gold.name
}

output "bronze_to_silver_job" {
  description = "Glue job name (Bronze -> Silver)."
  value       = aws_glue_job.bronze_to_silver.name
}

output "silver_to_gold_job" {
  description = "Glue job name (Silver -> Gold)."
  value       = aws_glue_job.silver_to_gold.name
}

output "gold_crawler" {
  description = "Glue crawler name (Gold)."
  value       = aws_glue_crawler.gold.name
}
