# Lock janitor: release the writer lock left behind by an ABORTED/TIMED_OUT (or failed-to-release)
# execution, but only after every pipeline Glue job run is inactive. The table keeps no TTL
# on purpose (see ingestion.tf); this function is the safe, condition-checked replacement.
locals {
  pipeline_job_names = [
    aws_glue_job.ingestion.name,
    aws_glue_job.bronze_to_silver.name,
    aws_glue_job.silver_to_gold.name,
  ]
}

data "archive_file" "lock_janitor" {
  type        = "zip"
  output_path = "${path.module}/.terraform/lock-janitor.zip"
  source {
    content  = file("${path.module}/../monitoring/lock_janitor.py")
    filename = "lock_janitor.py"
  }
}

resource "aws_cloudwatch_log_group" "lock_janitor" {
  #checkov:skip=CKV_AWS_158:logs hold no secrets; CloudWatch encrypts log data at rest by default
  #checkov:skip=CKV_AWS_338:14-day retention bounds cost; durable evidence lives in S3 manifests and state
  name              = "/aws/lambda/${var.project}-lock-janitor"
  retention_in_days = 14
}

resource "aws_iam_role" "lock_janitor" {
  name               = "${var.project}-lock-janitor-role"
  assume_role_policy = data.aws_iam_policy_document.monitor_assume.json
}

data "aws_iam_policy_document" "lock_janitor" {
  statement {
    sid       = "InspectAndReleasePipelineLock"
    actions   = ["dynamodb:GetItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.pipeline_lock.arn]
    condition {
      test     = "ForAllValues:StringEquals"
      variable = "dynamodb:LeadingKeys"
      values   = ["pipeline"]
    }
  }
  statement {
    sid       = "ReadOwnerExecutionStatus"
    actions   = ["states:DescribeExecution"]
    resources = ["arn:aws:states:${var.region}:${data.aws_caller_identity.current.account_id}:execution:${aws_sfn_state_machine.pipeline.name}:*"]
  }
  statement {
    sid       = "ReadPipelineJobRuns"
    actions   = ["glue:GetJobRuns"]
    resources = [aws_glue_job.ingestion.arn, aws_glue_job.bronze_to_silver.arn, aws_glue_job.silver_to_gold.arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lock_janitor.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lock_janitor" {
  name   = "${var.project}-lock-janitor"
  role   = aws_iam_role.lock_janitor.id
  policy = data.aws_iam_policy_document.lock_janitor.json
}

resource "aws_lambda_function" "lock_janitor" {
  #checkov:skip=CKV_AWS_115:no reserved concurrency: small accounts cannot reserve any; overlapping runs are safe
  #checkov:skip=CKV_AWS_116:scheduled async invoke; failure is alarmed and the next 15-minute run retries
  #checkov:skip=CKV_AWS_117:calls only AWS public APIs; a VPC would need NAT/endpoints for no isolation gain
  #checkov:skip=CKV_AWS_173:env vars hold resource names only, no secrets; Lambda encrypts them at rest by default
  #checkov:skip=CKV_AWS_272:single-operator Terraform deployment from reviewed source; no code-signing profile
  #checkov:skip=CKV_AWS_50:short single-call function; CloudWatch logs and alarms are sufficient, no X-Ray
  function_name    = "${var.project}-lock-janitor"
  role             = aws_iam_role.lock_janitor.arn
  runtime          = "python3.12"
  handler          = "lock_janitor.handler"
  filename         = data.archive_file.lock_janitor.output_path
  source_code_hash = data.archive_file.lock_janitor.output_base64sha256
  timeout          = 30
  memory_size      = 128
  # No reserved concurrency (small accounts cannot reserve any); overlapping sweeps are safe
  # because the delete is conditional on the inspected owner.
  environment {
    variables = {
      LOCK_TABLE        = aws_dynamodb_table.pipeline_lock.name
      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn
      GLUE_JOB_NAMES    = join(",", local.pipeline_job_names)
    }
  }
  depends_on = [aws_iam_role_policy.lock_janitor, aws_cloudwatch_log_group.lock_janitor]
}

# Fast path: react as soon as an execution ends without releasing its lock.
resource "aws_cloudwatch_event_rule" "lock_janitor_on_stop" {
  name        = "${var.project}-lock-janitor-on-stop"
  description = "Sweep the writer lock when a pipeline execution fails, times out or is aborted."
  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
    }
  })
}

# Fallback: a Glue run that outlives its execution blocks the fast path; retry until it ends.
resource "aws_cloudwatch_event_rule" "lock_janitor_schedule" {
  name                = "${var.project}-lock-janitor-schedule"
  description         = "Retry the writer-lock sweep while an orphaned lock waits for Glue to stop."
  schedule_expression = "rate(15 minutes)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "lock_janitor" {
  for_each = {
    on_stop  = aws_cloudwatch_event_rule.lock_janitor_on_stop.name
    schedule = aws_cloudwatch_event_rule.lock_janitor_schedule.name
  }
  rule = each.value
  arn  = aws_lambda_function.lock_janitor.arn
}

resource "aws_lambda_permission" "lock_janitor" {
  for_each = {
    on_stop  = aws_cloudwatch_event_rule.lock_janitor_on_stop.arn
    schedule = aws_cloudwatch_event_rule.lock_janitor_schedule.arn
  }
  statement_id  = "AllowLockJanitor-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lock_janitor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = each.value
}

# A janitor that keeps erroring would silently leave the next daily run blocked.
resource "aws_cloudwatch_metric_alarm" "lock_janitor_errors" {
  alarm_name          = "${var.project}-lock-janitor-errors"
  alarm_description   = "Lock janitor invocation failed. The writer lock may stay orphaned; follow the lock recovery steps in docs/operations.md."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.lock_janitor.function_name }
  statistic           = "Sum"
  period              = 900
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  depends_on          = [aws_sns_topic_policy.pipeline_alerts]
}
