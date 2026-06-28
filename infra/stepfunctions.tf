# ============================================================================================
# Step Functions (Standard) — orchestrates the pipeline:
#   bronze_to_silver (.sync) -> silver_to_gold (.sync) -> start crawler -> poll until READY.
# The Glue .sync integration blocks until each job finishes. The crawler has no .sync form, so
# we start it and poll GetCrawler in a Wait/Choice loop.
# ============================================================================================

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "Bronze->Silver->Gold, then crawl the Gold catalog"
    StartAt = "BronzeToSilver"
    States = {
      BronzeToSilver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_to_silver.name
        }
        Retry = [{
          ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
          IntervalSeconds = 30
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "Failed"
        }]
        Next = "SilverToGold"
      }

      SilverToGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_to_gold.name
        }
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "Failed"
        }]
        Next = "StartCrawler"
      }

      StartCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.gold.name
        }
        Catch = [
          {
            ErrorEquals = ["Glue.CrawlerRunningException"]
            Next        = "WaitForCrawler"
          },
          {
            ErrorEquals = ["States.ALL"]
            Next        = "Failed"
          },
        ]
        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 30
        Next    = "GetCrawler"
      }

      GetCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.gold.name
        }
        Next = "CrawlerFinished"
      }

      CrawlerFinished = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.Crawler.State"
          StringEquals = "READY"
          Next         = "Succeeded"
        }]
        Default = "WaitForCrawler"
      }

      Succeeded = {
        Type = "Succeed"
      }

      Failed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "A Glue job or the crawler failed; see the execution history."
      }
    }
  })
}
