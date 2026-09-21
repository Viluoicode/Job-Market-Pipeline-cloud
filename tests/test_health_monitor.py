import copy
import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

path = Path(__file__).resolve().parents[1] / "monitoring" / "health_monitor.py"
spec = importlib.util.spec_from_file_location("health_monitor", path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
NOW = datetime(2026, 9, 17, 2, tzinfo=timezone.utc)
COMPLETE = {"run_id": "run-1", "snapshot_date": "2026-09-16", "completed_at": "2026-09-16T18:08:00Z"}
MANIFEST = {"run_id": "run-1", "status": "SUCCEEDED", "completed_at": "2026-09-16T18:01:00Z",
            "total_records": 100, "successful_boards": 2, "failed_boards": 0,
            "boards": [{"status": "SUCCEEDED", "complete": True},
                       {"status": "SUCCEEDED", "complete": False}]}


class S3:
    def __init__(self):
        self.data = {"state/pipeline/latest.json": copy.deepcopy(COMPLETE),
                     "state/silver/latest.json": {"run_id": "run-1"},
                     "control/ingestion/run_id=run-1/manifest.json": copy.deepcopy(MANIFEST)}

    def get_object(self, Bucket, Key):
        if Key not in self.data:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(json.dumps(self.data[Key]).encode())}


class CloudWatch:
    def put_metric_data(self, **kwargs):
        self.request = kwargs


def check(s3):
    cw = CloudWatch()
    result = monitor.run_check(s3, cw, bucket="lake", project="test", namespace="Test", now=NOW)
    return result, {m["MetricName"]: m["Value"] for m in cw.request["MetricData"]}


def test_successful_incomplete_feed_does_not_trigger_coverage_alarm():
    result, metrics = check(S3())
    assert result["healthy"] and result["incomplete_boards"] == 1
    assert metrics == {"Healthy": 1, "SourceSuccessRatio": 1.0}


@pytest.mark.parametrize("completed_at", ["2026-09-15T18:01:00Z", "2026-09-18T18:01:00Z"])
def test_stale_or_future_observation_emits_unhealthy(completed_at):
    s3 = S3()
    s3.data["control/ingestion/run_id=run-1/manifest.json"]["completed_at"] = completed_at
    result, metrics = check(s3)
    assert not result["healthy"] and metrics["Healthy"] == 0


def test_unpublished_silver_emits_unhealthy():
    s3 = S3()
    s3.data["state/silver/latest.json"]["run_id"] = "new-run"
    result, metrics = check(s3)
    assert result["reason"] == "silver_not_fully_published" and metrics["Healthy"] == 0


@pytest.mark.parametrize("key", ["state/pipeline/latest.json", "state/silver/latest.json",
                                "control/ingestion/run_id=run-1/manifest.json"])
def test_missing_metadata_emits_unhealthy(key):
    s3 = S3()
    del s3.data[key]
    assert check(s3)[1]["Healthy"] == 0


def test_failed_board_reduces_coverage_even_if_published_run_is_fresh():
    s3 = S3()
    manifest = s3.data["control/ingestion/run_id=run-1/manifest.json"]
    manifest["boards"][1] = {"status": "FAILED", "complete": False}
    manifest["successful_boards"], manifest["failed_boards"] = 1, 1
    result, metrics = check(s3)
    assert result["healthy"] and metrics["SourceSuccessRatio"] == 0.5


def test_access_denied_emits_unhealthy():
    class Denied:
        def get_object(self, **kwargs):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    result, metrics = check(Denied())
    assert result["error_type"] == "ClientError" and metrics == {"Healthy": 0}


@pytest.mark.parametrize("value", ['{"run_id":"run-1"}', {"run_id": "../invalid"}])
def test_malformed_completion_emits_unhealthy(value):
    s3 = S3()
    s3.data["state/pipeline/latest.json"] = value
    assert check(s3)[1]["Healthy"] == 0


def test_metric_write_failure_is_not_reported_as_success():
    class Denied:
        def put_metric_data(self, **kwargs):
            raise RuntimeError("CloudWatch unavailable")
    with pytest.raises(RuntimeError, match="CloudWatch unavailable"):
        monitor.run_check(S3(), Denied(), bucket="lake", project="test", namespace="Test", now=NOW)
