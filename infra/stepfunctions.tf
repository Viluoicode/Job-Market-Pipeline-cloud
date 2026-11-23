# One execution owns the pipeline lock through ingestion, transforms and Gold publication.
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
      Next       = "ReadGoldVersions"
    }
    # The Gold job writes this record only after all three Iceberg commits succeeded. Its snapshot
    # ids go into the completion marker, so readers query exactly the versions this run committed.
    ReadGoldVersions = {
      Type     = "Task"
      Resource = "arn:aws:states:::aws-sdk:s3:getObject"
      Parameters = {
        Bucket  = aws_s3_bucket.lake.bucket
        "Key.$" = "States.Format('state/gold_iceberg/run_id={}.json', $.run.run_id)"
      }
      ResultSelector = { "record.$" = "States.StringToJson($.Body)" }
      ResultPath     = "$.gold"
      Next           = "GoldVersionsValid"
    }
    GoldVersionsValid = {
      Type = "Choice"
      Choices = [{
        And = [
          { Variable = "$.gold.record.run_id", StringEqualsPath = "$.run.run_id" },
          { Variable = "$.gold.record.snapshot_date", StringEqualsPath = "$.run.snapshot_date" },
          { Variable = "$.gold.record.status", StringEquals = "SUCCEEDED" },
          { Variable = "$.gold.record.tables.fact_job_posting.snapshot_id", IsPresent = true },
          { Variable = "$.gold.record.tables.demand_by_role.snapshot_id", IsPresent = true },
          { Variable = "$.gold.record.tables.role_opportunity.snapshot_id", IsPresent = true },
        ]
        Next = "BuildCompletion"
      }]
      Default = "GoldVersionsInvalid"
    }
    GoldVersionsInvalid = {
      Type  = "Fail"
      Error = "GoldVersionsInvalid"
      Cause = "The Gold version record is missing, belongs to another run, or lacks a table."
    }
    BuildCompletion = {
      Type = "Pass"
      Parameters = {
        "run_id.$"        = "$.run.run_id"
        "snapshot_date.$" = "$.run.snapshot_date"
        "started_at.$"    = "$.run.started_at"
        "completed_at.$"  = "$$.State.EnteredTime"
        # Iceberg version (snapshot id, as text) and row count of each Gold table for this run.
        "iceberg_snapshots.$" = "$.gold.record.tables"
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
  #checkov:skip=CKV_AWS_284:Glue/DynamoDB service integrations; X-Ray adds little over execution history
  #checkov:skip=CKV_AWS_285:Standard workflow keeps full execution history 90 days; failures alarm via SNS
  name     = "${var.project}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"
  definition = jsonencode({
    Comment        = "Ingest current boards, reconcile lifecycle, commit Gold to Iceberg and publish its versions"
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
