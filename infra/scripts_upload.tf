# ============================================================================================
# Upload the PySpark job scripts to the scripts bucket. etag = filemd5(...) so editing a script
# locally and re-applying re-uploads it (and the Glue job picks up the new version on next run).
# ============================================================================================

resource "aws_s3_object" "bronze_to_silver" {
  bucket       = aws_s3_bucket.scripts.id
  key          = "jobs/bronze_to_silver.py"
  source       = "${path.module}/../glue/jobs/bronze_to_silver.py"
  etag         = filemd5("${path.module}/../glue/jobs/bronze_to_silver.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "silver_to_gold" {
  bucket       = aws_s3_bucket.scripts.id
  key          = "jobs/silver_to_gold.py"
  source       = "${path.module}/../glue/jobs/silver_to_gold.py"
  etag         = filemd5("${path.module}/../glue/jobs/silver_to_gold.py")
  content_type = "text/x-python"
}
