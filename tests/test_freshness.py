import importlib.util
from pathlib import Path
from datetime import datetime, timezone

path = Path(__file__).resolve().parents[1] / "scripts" / "check_freshness.py"
spec = importlib.util.spec_from_file_location("check_freshness", path)
health = importlib.util.module_from_spec(spec)
spec.loader.exec_module(health)


def test_transform_completion_does_not_make_old_ingestion_fresh():
    completion = {"run_id": "r", "snapshot_date": "2026-09-14", "completed_at": "2026-09-14T10:00:00Z"}
    manifest = {"run_id": "r", "status": "SUCCEEDED", "completed_at": "2026-09-01T10:00:00Z",
                "total_records": 1, "successful_boards": 1, "failed_boards": 0, "boards": []}
    result = health.assess(completion, {"run_id": "r"}, manifest,
                           now=datetime(2026, 9, 14, 11, tzinfo=timezone.utc))
    assert not result["healthy"]
    assert result["reason"] == "stale_or_future_ingestion"


def test_new_silver_without_matching_gold_is_unhealthy():
    assert health.assess({"run_id": "old"}, {"run_id": "new"}, {"run_id": "old"}) == {
        "healthy": False, "reason": "silver_not_fully_published"}


def test_legacy_deployment_without_markers_is_not_reported_as_fresh():
    assert not health.assess(None, None, None)["healthy"]


def test_double_encoded_completion_is_unhealthy():
    result = health.assess('{"run_id": "r"}', {"run_id": "r"}, {"run_id": "r"})
    assert result == {"healthy": False, "reason": "invalid_committed_run_metadata"}
