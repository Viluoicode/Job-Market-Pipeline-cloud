"""Publication gating and Gold/display semantics. No live AWS calls."""
import ast
import copy
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dashboard"))
import data_access as data
from model import DEFAULT_ROLES, classify, safe_link


def publication():
    completion = {"run_id": "run-1", "snapshot_date": "2026-09-27", "completed_at": "2026-09-27T18:08:00Z"}
    silver = dict(completion)
    manifest = dict(completion, status="SUCCEEDED", total_records=2, successful_boards=2, failed_boards=0,
                    boards=[{"status": "SUCCEEDED", "complete": True}, {"status": "SUCCEEDED", "complete": False}])
    return completion, silver, manifest


def test_current_publication_and_incomplete_coverage():
    result = data.validate_publication(*publication(), now=datetime(2026, 9, 28, tzinfo=timezone.utc))
    assert result["incomplete_boards"] == 1
    assert result["snapshot_date"] == "2026-09-27"


@pytest.mark.parametrize("kind", ["run", "date", "failed", "stale", "future"])
def test_reject_unpublished_or_stale_data(kind):
    c, s, m = publication()
    now = datetime(2026, 9, 28, tzinfo=timezone.utc)
    if kind == "run": s["run_id"] = "other"
    if kind == "date": m["snapshot_date"] = "2026-09-26"
    if kind == "failed": m["status"] = "FAILED"
    if kind == "stale": now = datetime(2026, 9, 30, tzinfo=timezone.utc)
    if kind == "future": now = datetime(2026, 9, 26, tzinfo=timezone.utc)
    with pytest.raises(data.PublicationUnavailable):
        data.validate_publication(c, s, m, now=now)


def test_writer_lock_blocks_reads():
    session = Mock()
    session.client.return_value.get_item.return_value = {"Item": {"ExecutionArn": {"S": "running"}}}
    with pytest.raises(data.PublicationUnavailable, match="in progress"):
        data.read_publication(session, "lake", "lock")


def test_republication_during_query_is_rejected(monkeypatch):
    reads = iter([({"snapshot_date": "2026-09-27"}, ("old",)), ({}, ("new",))])
    monkeypatch.setattr(data, "read_publication", lambda *a: next(reads))
    monkeypatch.setattr(data, "query_fact", lambda *a: ([{}], {}))
    with pytest.raises(data.PublicationUnavailable, match="changed"):
        data.load_verified(None, "lake", "lock", "db", "wg")


def test_pagination_does_not_drop_second_page_first_record():
    session = Mock()
    athena = session.client.return_value
    athena.start_query_execution.return_value = {"QueryExecutionId": "q"}
    athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    def row(*values): return {"Data": [{"VarCharValue": v} for v in values]}
    athena.get_paginator.return_value.paginate.return_value = [
        {"ResultSet": {"Rows": [row("job_id", "is_remote"), row("1", "true")]}},
        {"ResultSet": {"Rows": [row("2", "false")]}}]
    records, _ = data.query_fact(session, "db", "wg", "2026-09-27")
    assert records == [{"job_id": "1", "is_remote": True}, {"job_id": "2", "is_remote": False}]
    sql = athena.start_query_execution.call_args.kwargs["QueryString"]
    assert "snapshot_date = '2026-09-27'" in sql
    with pytest.raises(data.PublicationUnavailable, match="limit"):
        data.query_fact(session, "db", "wg", "2026-09-27", max_rows=1)


def test_title_rules_match_gold_and_keep_unclassified():
    tree = ast.parse((ROOT / "glue/jobs/silver_to_gold.py").read_text(encoding="utf-8"))
    gold = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "DEFAULT_ROLES" for t in n.targets))
    assert DEFAULT_ROLES == gold
    assert classify("Data Engineer / Backend Developer") == ["Backend Engineer", "Data Engineer"]
    assert classify("Accountant") == ["Unclassified"]


def test_only_web_links_are_clickable():
    assert safe_link("https://example.com/job/1") == "https://example.com/job/1"
    assert safe_link("javascript:alert(1)") is None
    assert safe_link("file:///secret") is None


def test_dashboard_requires_reader_role():
    with pytest.raises(ValueError, match="AWS_DASHBOARD_ROLE_ARN"):
        data.create_reader_session("ap-southeast-1", "")


def test_dashboard_uses_only_assumed_credentials(monkeypatch):
    import boto3
    base = Mock()
    base.client.return_value.assume_role.return_value = {"Credentials": {
        "AccessKeyId": "test-key", "SecretAccessKey": "test-secret", "SessionToken": "test-token"}}
    factory = Mock()
    monkeypatch.setattr(boto3, "Session", factory)
    arn = "arn:aws:iam::123456789012:role/dashboard-reader"
    assert data.create_reader_session("ap-southeast-1", arn, base_session=base) == factory.return_value
    base.client.return_value.assume_role.assert_called_once_with(
        RoleArn=arn, RoleSessionName="jobmarket-dashboard", DurationSeconds=3600)
    factory.assert_called_once_with(region_name="ap-southeast-1", aws_access_key_id="test-key",
                                   aws_secret_access_key="test-secret", aws_session_token="test-token")


def test_dashboard_assume_failure_has_no_fallback():
    base = Mock()
    base.client.return_value.assume_role.side_effect = RuntimeError("AccessDenied")
    with pytest.raises(RuntimeError, match="AccessDenied"):
        data.create_reader_session("ap-southeast-1", "arn:aws:iam::123456789012:role/reader", base_session=base)
