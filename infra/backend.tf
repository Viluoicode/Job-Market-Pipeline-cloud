# Remote state in S3 so the stack can be managed from any machine and state is never lost with a
# laptop. Partial configuration: the bucket name (it contains the AWS account ID) comes from
# backend.hcl, which is not committed. Copy backend.hcl.example and run once:
#   terraform -chdir=infra init -backend-config=backend.hcl
# The state bucket is created by hand, outside this stack (see docs/operations.md), so a destroy of
# the stack can never delete its own state. CI validates with `init -backend=false`.
terraform {
  backend "s3" {
    key          = "job-market/terraform.tfstate"
    region       = "ap-southeast-1"
    encrypt      = true
    use_lockfile = true # S3-native state lock (Terraform >= 1.10); no DynamoDB lock table needed
  }
}
