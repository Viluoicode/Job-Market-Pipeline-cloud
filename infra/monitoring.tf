# Independent of the daily pipeline: detect stale data even when no execution starts.
locals {
  monitor_namespace = "JobMarket/Pipeline"
  failure_metrics = {
    failed    = "ExecutionsFailed"
    timed-out = "ExecutionsTimedOut"
    aborted   = "ExecutionsAborted"
  }
}

data "archive_file" "health_monitor" {
  type        = "zip"
  output_path = "${path.module}/.terraform/health-monitor.zip"
  source {
    content  = file("${path.module}/../monitoring/health_monitor.py")
    filename = "health_monitor.py"
  }
  source {
    content  = file("${path.module}/../scripts/check_freshness.py")
    filename = "check_freshness.py"
  }
  source {
    content  = file("${path.module}/../glue/jobs/pipeline_contract.py")
    filename = "pipeline_contract.py"
  }
}

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project}-pipeline-alerts"
}

data "aws_iam_policy_document" "pipeline_alerts" {
  statement {
    sid       = "CloudWatchAlarms"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline_alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:cloudwatch:${var.region}:${data.aws_caller_identity.current.account_id}:alarm:${var.project}-*"]
    }
  }
}

resource "aws_sns_topic_policy" "pipeline_alerts" {
  arn    = aws_sns_topic.pipeline_alerts.arn
  policy = data.aws_iam_policy_document.pipeline_alerts.json
}

resource "aws_sns_topic_subscription" "monitor_email" {
  count     = var.enable_monitor_email ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = sensitive(var.alert_email)
  lifecycle {
    precondition {
      condition     = var.alert_email != ""
      error_message = "Set alert_email before enabling monitoring email notifications."
    }
  }
}

resource "aws_cloudwatch_log_group" "health_monitor" {
  name              = "/aws/lambda/${var.project}-health-monitor"
  retention_in_days = 14
}

data "aws_iam_policy_document" "monitor_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "health_monitor" {
  name               = "${var.project}-health-monitor-role"
  assume_role_policy = data.aws_iam_policy_document.monitor_assume.json
}

data "aws_iam_policy_document" "health_monitor" {
  statement {
    sid     = "ReadPublicationMetadata"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/state/pipeline/latest.json",
      "${aws_s3_bucket.lake.arn}/state/silver/latest.json",
      "${aws_s3_bucket.lake.arn}/control/ingestion/*/manifest.json",
    ]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.health_monitor.arn}:*"]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = [local.monitor_namespace]
    }
  }
}

resource "aws_iam_role_policy" "health_monitor" {
  name   = "${var.project}-health-monitor"
  role   = aws_iam_role.health_monitor.id
  policy = data.aws_iam_policy_document.health_monitor.json
}

resource "aws_lambda_function" "health_monitor" {
  function_name    = "${var.project}-health-monitor"
  role             = aws_iam_role.health_monitor.arn
  runtime          = "python3.12"
  handler          = "health_monitor.handler"
  filename         = data.archive_file.health_monitor.output_path
  source_code_hash = data.archive_file.health_monitor.output_base64sha256
  timeout          = 60
  memory_size      = 128
  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.bucket
      PROJECT          = var.project
      METRIC_NAMESPACE = local.monitor_namespace
      MAX_AGE_HOURS    = tostring(var.freshness_hours)
    }
  }
  depends_on = [aws_iam_role_policy.health_monitor]
}

resource "aws_cloudwatch_event_rule" "health_monitor" {
  name                = "${var.project}-health-monitor-schedule"
  description         = "Check publication freshness independently of daily pipeline execution."
  schedule_expression = "rate(15 minutes)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "health_monitor" {
  rule = aws_cloudwatch_event_rule.health_monitor.name
  arn  = aws_lambda_function.health_monitor.arn
}

resource "aws_lambda_permission" "health_monitor" {
  statement_id  = "AllowScheduledHealthCheck"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.health_monitor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.health_monitor.arn
}

resource "aws_cloudwatch_metric_alarm" "pipeline_failure" {
  for_each            = local.failure_metrics
  alarm_name          = "${var.project}-pipeline-${each.key}"
  alarm_description   = "Pipeline ${each.key}. Inspect Step Functions and follow docs/operations.md before retrying or recovering the lock."
  namespace           = "AWS/States"
  metric_name         = each.value
  dimensions          = { StateMachineArn = aws_sfn_state_machine.pipeline.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  depends_on          = [aws_sns_topic_policy.pipeline_alerts]
}

resource "aws_cloudwatch_metric_alarm" "pipeline_health" {
  alarm_name          = "${var.project}-pipeline-unhealthy"
  alarm_description   = "Two unhealthy/missing 15-minute health periods: stale ingestion, missing metadata, incomplete publication, or a stopped health monitor. Inspect the health-monitor Lambda logs."
  namespace           = local.monitor_namespace
  metric_name         = "Healthy"
  dimensions          = { Project = var.project }
  statistic           = "Minimum"
  period              = 900
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions          = [aws_sns_topic.pipeline_alerts.arn]
  depends_on          = [aws_sns_topic_policy.pipeline_alerts]
}

resource "aws_cloudwatch_metric_alarm" "source_coverage" {
  alarm_name          = "${var.project}-source-coverage-low"
  alarm_description   = "Successful-board ratio below the monitoring threshold. Inspect the committed manifest. Incomplete pagination alone does not count as a failed board."
  namespace           = local.monitor_namespace
  metric_name         = "SourceSuccessRatio"
  dimensions          = { Project = var.project }
  statistic           = "Minimum"
  period              = 900
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = var.monitor_min_success_ratio
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions          = [aws_sns_topic.pipeline_alerts.arn]
  depends_on          = [aws_sns_topic_policy.pipeline_alerts]
}
