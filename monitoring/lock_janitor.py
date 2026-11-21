"""Lambda: release the pipeline writer lock once it is provably orphaned.

The state machine releases its own lock on success and on caught failures. It cannot when the
execution is ABORTED or TIMED_OUT (or the release step itself fails), and the next daily run then
stops with PipelineBusy. This janitor automates the manual recovery in docs/operations.md:

  1. read LockId=pipeline and its owner ExecutionArn;
  2. keep the lock while the owner execution is still RUNNING;
  3. keep it while any pipeline Glue job run or the crawler is active, because stopping Step
     Functions does not guarantee an external Glue job stopped writing;
  4. otherwise delete it, conditional on the owner still matching (a new execution may have
     taken the lock in between).

Triggered by Step Functions status-change events and by a 15-minute fallback schedule. The event
only wakes the function: which lock and owner are checked comes from configuration and DynamoDB,
never from the event payload. It never stops jobs or starts executions.
"""
import json
import os

from botocore.exceptions import ClientError

LOCK_KEY = {"LockId": {"S": "pipeline"}}
ACTIVE_JOB_RUN_STATES = {"STARTING", "RUNNING", "STOPPING", "WAITING"}


def _active_job_runs(glue, job_names):
    active = []
    for name in job_names:
        # Newest runs come first; an active run of a daily pipeline is always among the latest.
        for run in glue.get_job_runs(JobName=name, MaxResults=10).get("JobRuns", []):
            if run.get("JobRunState") in ACTIVE_JOB_RUN_STATES:
                active.append({"job": name, "run_id": run["Id"], "state": run["JobRunState"]})
    return active


def _owner_status(sfn, owner):
    try:
        return sfn.describe_execution(executionArn=owner)["status"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ExecutionDoesNotExist":
            return "NOT_FOUND"  # closed executions expire after 90 days; it cannot be running
        raise


def sweep(dynamodb, sfn, glue, *, table, state_machine_arn, job_names, crawler_name):
    item = dynamodb.get_item(TableName=table, Key=LOCK_KEY, ConsistentRead=True).get("Item")
    if not item:
        return {"action": "no_lock"}
    owner = item["ExecutionArn"]["S"]
    # Only manage locks taken by executions of THIS state machine.
    execution_prefix = state_machine_arn.replace(":stateMachine:", ":execution:", 1) + ":"
    if not owner.startswith(execution_prefix):
        return {"action": "foreign_owner", "owner": owner}

    status = _owner_status(sfn, owner)
    if status == "RUNNING":
        return {"action": "owner_running", "owner": owner}

    active = _active_job_runs(glue, job_names)
    if active:
        return {"action": "waiting_for_glue", "owner": owner, "active_job_runs": active}
    crawler_state = glue.get_crawler(Name=crawler_name)["Crawler"]["State"]
    if crawler_state != "READY":
        return {"action": "waiting_for_crawler", "owner": owner, "crawler_state": crawler_state}

    try:
        dynamodb.delete_item(
            TableName=table, Key=LOCK_KEY,
            ConditionExpression="ExecutionArn = :owner",
            ExpressionAttributeValues={":owner": {"S": owner}},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return {"action": "lock_changed", "owner": owner}
        raise
    return {"action": "released", "owner": owner, "owner_status": status}


def _client(name):
    import boto3
    return boto3.client(name)


def handler(event, context):
    result = sweep(
        _client("dynamodb"), _client("stepfunctions"), _client("glue"),
        table=os.environ["LOCK_TABLE"],
        state_machine_arn=os.environ["STATE_MACHINE_ARN"],
        job_names=[n for n in os.environ["GLUE_JOB_NAMES"].split(",") if n],
        crawler_name=os.environ["CRAWLER_NAME"],
    )
    print(json.dumps({"trigger": (event or {}).get("source", "manual"), **result}, sort_keys=True))
    return result
