# ============================================================================================
# EventBridge — optional scheduled trigger for the pipeline state machine.
# DISABLED by default (cost guardrail): the pipeline stays on-demand unless you set
# enable_schedule = true. When enabled, EventBridge starts the same Step Functions state machine
# on `schedule_expression` (default: daily 18:00 UTC).
# ============================================================================================

resource "aws_cloudwatch_event_rule" "pipeline" {
  name                = "${var.project}-pipeline-schedule"
  description         = "Scheduled trigger for the job-market pipeline (disabled by default)."
  schedule_expression = var.schedule_expression
  state               = var.enable_schedule ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "pipeline" {
  rule     = aws_cloudwatch_event_rule.pipeline.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.events.arn
}

# ---- Role EventBridge assumes to start the state machine --------------------------------------
data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events" {
  name               = "${var.project}-events-role"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

data "aws_iam_policy_document" "events_start_sfn" {
  statement {
    sid       = "StartPipeline"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "events_start_sfn" {
  name   = "${var.project}-events-start-sfn"
  role   = aws_iam_role.events.id
  policy = data.aws_iam_policy_document.events_start_sfn.json
}
