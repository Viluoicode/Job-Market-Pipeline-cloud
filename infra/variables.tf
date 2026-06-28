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
