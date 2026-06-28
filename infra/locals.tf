locals {
  # Globally-unique bucket names without manual bookkeeping: <project>-<account>-<region>-<zone>.
  # Override any of them via the *_bucket_name variables.
  name_base = "${var.project}-${data.aws_caller_identity.current.account_id}-${var.region}"

  lake_bucket    = var.lake_bucket_name != "" ? var.lake_bucket_name : "${local.name_base}-lake"
  athena_bucket  = var.athena_results_bucket_name != "" ? var.athena_results_bucket_name : "${local.name_base}-athena"
  scripts_bucket = var.scripts_bucket_name != "" ? var.scripts_bucket_name : "${local.name_base}-scripts"
}
