# One execution owns the pipeline lock through ingestion, transforms and the crawler.
locals {
  pipeline_stages = {
    Ingest = {
      Type     = "Task"
      Resource = "arn:aws:states:::glue:startJobRun.sync"
      Parameters = {
        JobName = aws_glue_job.ingestion.name
        Arguments = {
          "--run-id.$"               = "$.run.run_id"
          "--snapshot-date.$"        = "$.run.snapshot_date"
          "--execution-started-at.$" = "$.run.started_at"
        }
      }
      ResultPath = null
      Next       = "BronzeToSilver"
    }
    BronzeToSilver = {
      Type     = "Task"
      Resource = "arn:aws:states:::glue:startJobRun.sync"
      Parameters = {
        JobName = aws_glue_job.bronze_to_silver.name
        Arguments = {
          "--run_id.$"        = "$.run.run_id"
          "--snapshot_date.$" = "$.run.snapshot_date"
        }
      }
      ResultPath = null
      Next       = "SilverToGold"
    }
    SilverToGold = {
      Type     = "Task"
      Resource = "arn:aws:states:::glue:startJobRun.sync"
      Parameters = {
        JobName = aws_glue_job.silver_to_gold.name
        Arguments = {
          "--run_id.$"        = "$.run.run_id"
          "--snapshot_date.$" = "$.run.snapshot_date"
        }
      }
      ResultPath = null
      Next       = "MarkCrawlerStart"
    }
    MarkCrawlerStart = {
      Type = "Pass"
      Parameters = {
        # Glue crawl timestamps have second precision; avoid false failures on millisecond skew.
        "started_at.$" = "States.Format('{}Z', States.ArrayGetItem(States.StringSplit($$.State.EnteredTime, '.Z'), 0))"
      }
      ResultPath = "$.crawl"
      Next       = "StartCrawler"
    }
    StartCrawler = {
      Type       = "Task"
      Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
      Parameters = { Name = aws_glue_crawler.gold.name }
      # An unrelated crawl must finish before OUR crawl is started.
      Retry = [{
        ErrorEquals     = ["Glue.CrawlerRunningException"]
        IntervalSeconds = 30
        BackoffRate     = 1.0
        MaxAttempts     = 6
      }]
      ResultPath = null
      Next       = "WaitForCrawler"
    }
    WaitForCrawler = {
      Type    = "Wait"
      Seconds = 30
      Next    = "GetCrawler"
    }
    GetCrawler = {
      Type       = "Task"
      Resource   = "arn:aws:states:::aws-sdk:glue:getCrawler"
      Parameters = { Name = aws_glue_crawler.gold.name }
      ResultPath = "$.crawler"
      Next       = "CrawlerFinished"
    }
    CrawlerFinished = {
      Type = "Choice"
      Choices = [{
        Variable     = "$.crawler.Crawler.State"
        StringEquals = "READY"
        Next         = "CrawlerSucceeded"
      }]
      Default = "WaitForCrawler"
    }
    CrawlerSucceeded = {
      Type = "Choice"
      Choices = [{
        And = [
          { Variable = "$.crawler.Crawler.LastCrawl.Status", IsPresent = true },
          { Variable = "$.crawler.Crawler.LastCrawl.Status", StringEquals = "SUCCEEDED" },
          { Variable = "$.crawler.Crawler.LastCrawl.StartTime", IsPresent = true },
          { Variable = "$.crawler.Crawler.LastCrawl.StartTime", TimestampGreaterThanEqualsPath = "$.crawl.started_at" },
        ]
        Next = "BuildCompletion"
      }]
      Default = "CrawlerFailed"
    }
    CrawlerFailed = {
      Type  = "Fail"
      Error = "CrawlerFailed"
      Cause = "Crawler READY is insufficient: this execution's crawl must have succeeded."
    }
    BuildCompletion = {
      Type = "Pass"
      Parameters = {
        "run_id.$"        = "$.run.run_id"
        "snapshot_date.$" = "$.run.snapshot_date"
        "started_at.$"    = "$.run.started_at"
        "completed_at.$"  = "$$.State.EnteredTime"
      }
      ResultPath = "$.completion"
      Next       = "PublishCompletion"
    }
    PublishCompletion = {
      Type     = "Task"
      Resource = "arn:aws:states:::aws-sdk:s3:putObject"
      Parameters = {
        Bucket = aws_s3_bucket.lake.bucket
        Key    = "state/pipeline/latest.json"
        # The SDK integration serializes Body; keep an object to avoid double encoding.
        "Body.$"    = "$.completion"
        ContentType = "application/json"
      }
      ResultPath = null
      End        = true
    }
  }
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"
  definition = jsonencode({
    Comment        = "Ingest current boards, reconcile lifecycle, publish fresh Gold and verify crawler"
    StartAt        = "InitializeRun"
    TimeoutSeconds = 5400
    States = {
      InitializeRun = {
        Type = "Pass"
        Parameters = {
          run = {
            "run_id.$"        = "$$.Execution.Name"
            "snapshot_date.$" = "States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)"
            "started_at.$"    = "$$.Execution.StartTime"
            "execution_arn.$" = "$$.Execution.Id"
          }
        }
        Next = "AcquireLock"
      }
      AcquireLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"
        Parameters = {
          TableName = aws_dynamodb_table.pipeline_lock.name
          Item = {
            LockId       = { S = "pipeline" }
            ExecutionArn = { "S.$" = "$.run.execution_arn" }
            StartedAt    = { "S.$" = "$.run.started_at" }
          }
          ConditionExpression = "attribute_not_exists(LockId)"
        }
        ResultPath = null
        Catch = [{
          ErrorEquals = ["DynamoDB.ConditionalCheckFailedException"]
          ResultPath  = "$.error"
          Next        = "PipelineBusy"
        }]
        Next = "RunPipeline"
      }
      PipelineBusy = {
        Type  = "Fail"
        Error = "PipelineBusy"
        Cause = "Another execution owns the lock. See docs/operations.md for interrupted-run recovery."
      }
      # A one-branch Parallel scope gives all pipeline tasks a common failure handler.
      RunPipeline = {
        Type       = "Parallel"
        Branches   = [{ StartAt = "Ingest", States = local.pipeline_stages }]
        ResultPath = null
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "ReleaseFailedLock"
        }]
        Next = "ReleaseLock"
      }
      ReleaseLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:deleteItem"
        Parameters = {
          TableName                 = aws_dynamodb_table.pipeline_lock.name
          Key                       = { LockId = { S = "pipeline" } }
          ConditionExpression       = "ExecutionArn = :owner"
          ExpressionAttributeValues = { ":owner" = { "S.$" = "$.run.execution_arn" } }
        }
        ResultPath = null
        Next       = "Succeeded"
      }
      ReleaseFailedLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:deleteItem"
        Parameters = {
          TableName                 = aws_dynamodb_table.pipeline_lock.name
          Key                       = { LockId = { S = "pipeline" } }
          ConditionExpression       = "ExecutionArn = :owner"
          ExpressionAttributeValues = { ":owner" = { "S.$" = "$.run.execution_arn" } }
        }
        ResultPath = null
        Next       = "Failed"
      }
      Succeeded = { Type = "Succeed" }
      Failed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "Pipeline failed; inspect $.error and the execution history."
      }
    }
  })
}
