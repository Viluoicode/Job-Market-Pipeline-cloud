"""Unit tests for the single-purpose steps that run_ingestion orchestrates.

tests/test_ingestion.py exercises the whole run and was left unchanged by the refactor; these
tests pin the rules of each extracted step directly.
"""
import httpx
import land_to_bronze as ingest
import pytest


def manifest(statuses, total_records):
    return {"boards": [{"status": s} for s in statuses], "total_records": total_records}


def test_success_ratio_at_threshold_is_published():
    m = ingest.finalize_manifest(manifest(["SUCCEEDED"] * 4 + ["FAILED"], 10), 5, 0.8)
    assert (m["successful_boards"], m["failed_boards"], m["success_ratio"]) == (4, 1, 0.8)
    assert m["status"] == "SUCCEEDED"


def test_success_ratio_below_threshold_fails():
    assert ingest.finalize_manifest(manifest(["SUCCEEDED", "FAILED"], 10), 2, 0.8)["status"] == "FAILED"


def test_zero_rows_fail_even_when_every_board_succeeded():
    # Every API answered with an empty list: that needs investigation, not publication.
    assert ingest.finalize_manifest(manifest(["SUCCEEDED"] * 3, 0), 3, 0.8)["status"] == "FAILED"


def gh_client(status=200, jobs=None):
    payload = {"jobs": jobs if jobs is not None else [{"id": 1}]}
    return httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(status, json=payload)))


BOARD = {"source": "Greenhouse", "token": "acme", "company": "Acme"}


def test_api_failure_marks_only_this_board_failed(tmp_path):
    with gh_client(status=503) as client:
        entry = ingest.ingest_board(client, BOARD, run_id="r", arbeitnow_pages=1, out_dir=tmp_path)
    assert entry == {"source": "greenhouse", "board_token": "acme", "status": "FAILED", "complete": False,
                     "row_count": 0, "error_type": "HTTPStatusError", "http_status": 503}
    assert not list(tmp_path.rglob("*.json"))      # nothing landed in Bronze


def test_storage_failure_propagates_instead_of_failing_the_board(monkeypatch):
    def broken_put(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr(ingest, "put_bytes", broken_put)
    with gh_client() as client, pytest.raises(OSError):
        ingest.ingest_board(client, BOARD, run_id="r", arbeitnow_pages=1, out_dir="unused")


def test_committed_manifest_is_reused_only_for_the_same_run():
    ok = {"status": "SUCCEEDED", "run_id": "r", "snapshot_date": "2026-11-21"}
    assert ingest.committed_manifest(ok, "r", "2026-11-21") is ok
    assert ingest.committed_manifest({**ok, "status": "FAILED"}, "r", "2026-11-21") is None
    assert ingest.committed_manifest(None, "r", "2026-11-21") is None
    with pytest.raises(ValueError, match="different identity"):
        ingest.committed_manifest(ok, "r", "2026-11-20")
