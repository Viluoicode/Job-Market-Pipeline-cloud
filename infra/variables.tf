variable "project" {
  description = "Project name — used as a tag and as the resource name prefix."
  type        = string
  default     = "jobmarket-aws"
}

variable "region" {
  description = "AWS region (Singapore is closest to HCMC)."
  type        = string
  default     = "ap-southeast-1"
}

# ---- Bucket name overrides (empty = auto-name <project>-<account>-<region>-<zone>) ----
variable "lake_bucket_name" {
  description = "Override the data lake bucket name. Empty = auto."
  type        = string
  default     = ""
}

variable "athena_results_bucket_name" {
  description = "Override the Athena results bucket name. Empty = auto."
  type        = string
  default     = ""
}

variable "scripts_bucket_name" {
  description = "Override the Glue scripts bucket name. Empty = auto."
  type        = string
  default     = ""
}

# ---- Glue sizing (the main cost lever) ----
variable "glue_version" {
  description = "Glue version (4.0 = Spark 3.3 / Python 3.10)."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers per job."
  type        = number
  default     = 2
}

variable "glue_timeout_minutes" {
  description = "Glue job timeout in minutes."
  type        = number
  default     = 30
}

# ---- Catalog / Athena ----
variable "gold_database_name" {
  description = "Glue Data Catalog database for the Gold marts."
  type        = string
  default     = "jobmarket_aws_gold"
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name."
  type        = string
  default     = "jobmarket-aws"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query bytes-scanned cap (bytes). Default 10 GiB."
  type        = number
  default     = 10737418240
}

# ---- S3 lifecycle (keep storage near zero) ----
variable "bronze_expiration_days" {
  description = "Expire raw bronze/ objects after N days."
  type        = number
  default     = 30
}

variable "athena_results_expiration_days" {
  description = "Expire Athena query results after N days."
  type        = number
  default     = 7
}

# ---- Cost guardrail ----
variable "monthly_budget_usd" {
  description = "Monthly AWS Budgets limit (USD)."
  type        = number
  default     = 10
}

variable "alert_email" {
  description = "Email for the budget alert (empty = no email subscription)."
  type        = string
  default     = ""
}

# ---- EventBridge schedule (optional daily run) ----
variable "enable_schedule" {
  description = "Enable the EventBridge daily trigger for the pipeline. Keep false unless you want automatic daily runs (each run costs a few cents of Glue)."
  type        = bool
  default     = false
}

variable "schedule_expression" {
  description = "EventBridge schedule for the pipeline (cron/rate). Default: daily at 18:00 UTC (01:00 GMT+7)."
  type        = string
  default     = "cron(0 18 * * ? *)"
}

variable "ingestion_min_success_ratio" {
  description = "Minimum successful board fraction before publishing a run manifest."
  type        = number
  default     = 0.8
  validation {
    condition     = var.ingestion_min_success_ratio > 0 && var.ingestion_min_success_ratio <= 1
    error_message = "Success ratio must be in (0, 1]."
  }
}

variable "stale_after_days" {
  description = "Expire unverified postings after this many days without an observation."
  type        = number
  default     = 7
  validation {
    condition     = var.stale_after_days >= 1 && floor(var.stale_after_days) == var.stale_after_days
    error_message = "stale_after_days must be a positive integer."
  }
}

variable "freshness_hours" {
  description = "Gold excludes postings last observed more than this many hours before ingestion completion."
  type        = number
  default     = 26
  validation {
    condition     = var.freshness_hours >= 1 && floor(var.freshness_hours) == var.freshness_hours
    error_message = "freshness_hours must be a positive integer."
  }
}
