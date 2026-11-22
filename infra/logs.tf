# Glue creates these log groups itself, shared by every Glue job and crawler in the account and
# region, with no expiry. Terraform manages only their retention: the groups were imported
# (see docs/operations.md) and skip_destroy keeps them when this stack is destroyed.
locals {
  glue_log_groups = toset([
    "/aws-glue/crawlers",
    "/aws-glue/jobs/error",
    "/aws-glue/jobs/logs-v2",
    "/aws-glue/jobs/output",
    "/aws-glue/python-jobs/error",
    "/aws-glue/python-jobs/output",
  ])
}

resource "aws_cloudwatch_log_group" "glue" {
  #checkov:skip=CKV_AWS_158:logs hold no secrets; CloudWatch encrypts log data at rest by default
  #checkov:skip=CKV_AWS_338:30-day retention bounds cost; durable evidence lives in S3 manifests and state
  for_each          = local.glue_log_groups
  name              = each.key
  retention_in_days = var.glue_log_retention_days
  skip_destroy      = true
}
