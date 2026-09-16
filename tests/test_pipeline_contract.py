import copy
from datetime import datetime, timezone

import pytest
from pipeline_contract import validate_manifest

NOW = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)
MANIFEST = {
    "version": 1, "status": "SUCCEEDED", "run_id": "run-1", "snapshot_date": "2026-09-14",
    "started_at": "2026-09-14T10:00:00+00:00", "completed_at": "2026-09-14T10:05:00+00:00",
    "total_records": 1,
    "boards": [{"source": "greenhouse", "board_token": "acme", "status": "SUCCEEDED",
                "complete": True, "row_count": 1, "fetched_at": "2026-09-14T10:01:00+00:00",
                "key": "bronze/run_id=run-1/source=greenhouse/board=acme/jobs.json"}],
}


@pytest.mark.parametrize("field,value", [
    ("status", "FAILED"), ("run_id", "old-run"), ("snapshot_date", "2026-09-13"),
    ("completed_at", "2026-09-12T10:05:00+00:00"),
    ("completed_at", "2026-09-15T10:05:00+00:00"), ("total_records", 2),
])
def test_reject_wrong_failed_or_stale_manifest(field, value):
    manifest = copy.deepcopy(MANIFEST)
    manifest[field] = value
    with pytest.raises(ValueError):
        validate_manifest(manifest, "run-1", "2026-09-14", now=NOW)


def test_manifest_cannot_reference_an_older_run():
    manifest = copy.deepcopy(MANIFEST)
    manifest["boards"][0]["key"] = "bronze/run_id=old/source=greenhouse/board=acme/jobs.json"
    with pytest.raises(ValueError):
        validate_manifest(manifest, "run-1", "2026-09-14", now=NOW)


def test_reject_duplicate_manifest_board():
    manifest = copy.deepcopy(MANIFEST)
    manifest["boards"].append(manifest["boards"][0])
    with pytest.raises(ValueError):
        validate_manifest(manifest, "run-1", "2026-09-14", now=NOW)
