"""Unit tests for the lock janitor Lambda (monitoring/lock_janitor.py).

The janitor releases the DynamoDB writer lock only when it is provably orphaned: its owner
execution has ended AND no Glue job run / crawl that could still be writing is active. Every
"not safe yet" case must leave the lock untouched so the next sweep can retry.
"""
import importlib.util
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

path = Path(__file__).resolve().parents[1] / "monitoring" / "lock_janitor.py"
spec = importlib.util.spec_from_file_location("lock_janitor", path)
janitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(janitor)

SM_ARN = "arn:aws:states:ap-southeast-1:123456789012:stateMachine:jobmarket-aws-pipeline"
OWNER = "arn:aws:states:ap-southeast-1:123456789012:execution:jobmarket-aws-pipeline:run-1"
JOBS = ["jobmarket-aws-ingestion", "jobmarket-aws-bronze-to-silver", "jobmarket-aws-silver-to-gold"]
CRAWLER = "jobmarket-aws-gold-crawler"


class DynamoDB:
    def __init__(self, owner=OWNER):
        self.item = None if owner is None else {
            "LockId": {"S": "pipeline"}, "ExecutionArn": {"S": owner},
            "StartedAt": {"S": "2026-11-20T18:00:00Z"}}
        self.deletes = []
        self.changed_before_delete = False

    def get_item(self, TableName, Key, ConsistentRead):
        assert Key == {"LockId": {"S": "pipeline"}} and ConsistentRead
        return {} if self.item is None else {"Item": self.item}

    def delete_item(self, **kwargs):
        self.deletes.append(kwargs)
        if self.changed_before_delete:  # another execution took the lock in between
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "DeleteItem")
        self.item = None


class StepFunctions:
    def __init__(self, status="TIMED_OUT", missing=False):
        self.status, self.missing, self.described = status, missing, []

    def describe_execution(self, executionArn):
        self.described.append(executionArn)
        if self.missing:
            raise ClientError({"Error": {"Code": "ExecutionDoesNotExist"}}, "DescribeExecution")
        return {"executionArn": executionArn, "status": self.status}


class Glue:
    def __init__(self, active=None, crawler_state="READY"):
        self.active = active or {}          # job name -> JobRunState of its newest run
        self.crawler_state = crawler_state

    def get_job_runs(self, JobName, MaxResults):
        runs = [{"Id": "jr_old", "JobRunState": "SUCCEEDED"}]
        if JobName in self.active:
            runs.insert(0, {"Id": "jr_new", "JobRunState": self.active[JobName]})
        return {"JobRuns": runs}

    def get_crawler(self, Name):
        assert Name == CRAWLER
        return {"Crawler": {"Name": Name, "State": self.crawler_state}}


def sweep(ddb, sfn=None, glue=None):
    return janitor.sweep(ddb, sfn or StepFunctions(), glue or Glue(), table="lock",
                         state_machine_arn=SM_ARN, job_names=JOBS, crawler_name=CRAWLER)


def test_no_lock_is_a_noop():
    ddb = DynamoDB(owner=None)
    assert sweep(ddb)["action"] == "no_lock"
    assert ddb.deletes == []


def test_lock_of_running_execution_is_kept():
    ddb = DynamoDB()
    result = sweep(ddb, StepFunctions(status="RUNNING"))
    assert result["action"] == "owner_running"
    assert ddb.deletes == []


@pytest.mark.parametrize("status", ["TIMED_OUT", "ABORTED", "FAILED"])
def test_orphaned_lock_is_released_with_owner_condition(status):
    ddb = DynamoDB()
    result = sweep(ddb, StepFunctions(status=status))
    assert result == {"action": "released", "owner": OWNER, "owner_status": status}
    [delete] = ddb.deletes
    # Delete only if the lock still belongs to the inspected owner (race with a new execution).
    assert delete["Key"] == {"LockId": {"S": "pipeline"}}
    assert delete["ConditionExpression"] == "ExecutionArn = :owner"
    assert delete["ExpressionAttributeValues"] == {":owner": {"S": OWNER}}


def test_expired_execution_history_counts_as_ended():
    # Step Functions keeps closed executions for 90 days; an unknown owner cannot be running.
    ddb = DynamoDB()
    result = sweep(ddb, StepFunctions(missing=True))
    assert result["action"] == "released" and result["owner_status"] == "NOT_FOUND"


@pytest.mark.parametrize("state", ["STARTING", "RUNNING", "STOPPING", "WAITING"])
def test_active_glue_run_blocks_release(state):
    # Stopping Step Functions does not guarantee its Glue job stopped: it may still be writing.
    ddb = DynamoDB()
    result = sweep(ddb, glue=Glue(active={"jobmarket-aws-silver-to-gold": state}))
    assert result["action"] == "waiting_for_glue"
    assert result["active_job_runs"] == [{"job": "jobmarket-aws-silver-to-gold", "run_id": "jr_new", "state": state}]
    assert ddb.deletes == []


@pytest.mark.parametrize("state", ["RUNNING", "STOPPING"])
def test_running_crawler_blocks_release(state):
    ddb = DynamoDB()
    result = sweep(ddb, glue=Glue(crawler_state=state))
    assert result == {"action": "waiting_for_crawler", "owner": OWNER, "crawler_state": state}
    assert ddb.deletes == []


def test_lock_owned_by_another_state_machine_is_never_touched():
    foreign = "arn:aws:states:ap-southeast-1:123456789012:execution:other-machine:run-9"
    ddb, sfn = DynamoDB(owner=foreign), StepFunctions()
    result = sweep(ddb, sfn)
    assert result["action"] == "foreign_owner"
    assert sfn.described == [] and ddb.deletes == []


def test_lock_taken_over_between_check_and_delete_is_not_an_error():
    ddb = DynamoDB()
    ddb.changed_before_delete = True
    assert sweep(ddb)["action"] == "lock_changed"


def test_handler_uses_configuration_not_event(monkeypatch):
    ddb, sfn, glue = DynamoDB(), StepFunctions(status="ABORTED"), Glue()
    clients = {"dynamodb": ddb, "stepfunctions": sfn, "glue": glue}
    monkeypatch.setattr(janitor, "_client", clients.__getitem__)
    monkeypatch.setenv("LOCK_TABLE", "lock")
    monkeypatch.setenv("STATE_MACHINE_ARN", SM_ARN)
    monkeypatch.setenv("GLUE_JOB_NAMES", ",".join(JOBS))
    monkeypatch.setenv("CRAWLER_NAME", CRAWLER)
    # A forged event naming some other execution must not change which lock owner is checked.
    event = {"source": "aws.states", "detail": {"executionArn": "arn:forged", "status": "ABORTED"}}
    result = janitor.handler(event, None)
    assert result["action"] == "released" and sfn.described == [OWNER]
