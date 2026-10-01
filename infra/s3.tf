# ============================================================================================
# S3 — three buckets: the data lake (bronze/silver/gold), Athena results, and Glue scripts.
# Nonempty buckets are protected unless cleanup explicitly opts into force_destroy.
# ============================================================================================

# ---- Data lake -----------------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  bucket        = local.lake_bucket
  force_destroy = var.allow_bucket_force_destroy
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-raw-bronze"
    status = "Enabled"
    filter {
      prefix = "bronze/"
    }
    expiration {
      days = var.bronze_expiration_days
    }
  }
  rule {
    id     = "expire-ingestion-manifests"
    status = "Enabled"
    filter {
      prefix = "control/ingestion/"
    }
    expiration {
      days = var.bronze_expiration_days
    }
  }
  rule {
    id     = "expire-quality-audits"
    status = "Enabled"
    filter {
      prefix = "quality/"
    }
    expiration {
      days = 90
    }
  }
}

# ---- Athena results ------------------------------------------------------------------------
resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_bucket
  force_destroy = var.allow_bucket_force_destroy
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "results/"
    }
    expiration {
      days = var.athena_results_expiration_days
    }
  }
}

# ---- Glue scripts + TempDir ----------------------------------------------------------------
resource "aws_s3_bucket" "scripts" {
  bucket        = local.scripts_bucket
  force_destroy = var.allow_bucket_force_destroy
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    id     = "expire-glue-temp"
    status = "Enabled"
    filter {
      prefix = "tmp/"
    }
    expiration {
      days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# Preserve existing lake history; do not expire noncurrent versions implicitly.
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration { status = "Enabled" }
}
locals {
  protected_buckets = {
    lake    = { id = aws_s3_bucket.lake.id, arn = aws_s3_bucket.lake.arn }
    scripts = { id = aws_s3_bucket.scripts.id, arn = aws_s3_bucket.scripts.arn }
    results = { id = aws_s3_bucket.athena_results.id, arn = aws_s3_bucket.athena_results.arn }
  }
}
resource "aws_s3_bucket_ownership_controls" "private" {
  for_each = local.protected_buckets
  bucket   = each.value.id
  rule { object_ownership = "BucketOwnerEnforced" }
}
resource "aws_s3_bucket_policy" "https_only" {
  for_each = local.protected_buckets
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false", "aws:PrincipalIsAWSService" = "false" } }
    }]
  })
}
