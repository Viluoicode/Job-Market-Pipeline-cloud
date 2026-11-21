terraform {
  required_version = ">= 1.10" # S3 backend use_lockfile

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}
