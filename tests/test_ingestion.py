"""Exercise API failures, completeness and the manifest commit boundary without AWS."""
import json
from datetime import datetime, timezone

import httpx
import pytest

import land_to_bronze as ingest
from pipeline_contract import validate_manifest


def run(tmp_path, boards, handler, **kwargs):
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        return ingest.run_ingestion(boards, run_id="test-run",
            snapshot_date=datetime.now(timezone.utc).date().isoformat(),
            out_dir=tmp_path, client=client, **kwargs)


def board(token):
    return {"source": "greenhouse", "token": token, "company": token}


def test_manifest_is_committed_after_board_objects_and_tracks_empty_board(tmp_path):
    manifest = run(tmp_path, [board("hiring"), board("empty")],
        lambda req: httpx.Response(200, json={"jobs": [{"id": 1}] if "hiring" in str(req.url) else []}))
    assert manifest["status"] == "SUCCEEDED"
    keys, complete = validate_manifest(manifest, "test-run", manifest["snapshot_date"])
    assert len(keys) == 1
    assert set(complete) == {("greenhouse", "hiring"), ("greenhouse", "empty")}
    record = json.loads((tmp_path / keys[0]).read_text())
    assert record["run_id"] == "test-run"
    assert record["fetched_at"] == manifest["boards"][0]["fetched_at"]
    assert json.loads((tmp_path / "control/ingestion/run_id=test-run/manifest.json").read_text()) == manifest


def test_failed_board_is_not_an_empty_complete_board(tmp_path):
    manifest = run(tmp_path, [board("ok"), board("broken")],
        lambda req: httpx.Response(404) if "broken" in str(req.url) else httpx.Response(200, json={"jobs": [{"id": 1}]}),
        min_success_ratio=0.5)
    assert manifest["status"] == "SUCCEEDED"
    assert manifest["boards"][1]["status"] == "FAILED"
    assert manifest["boards"][1]["complete"] is False
    assert "key" not in manifest["boards"][1]
    assert validate_manifest(manifest, "test-run", manifest["snapshot_date"])[1] == [("greenhouse", "ok")]


def test_failure_threshold_stops_publishing(tmp_path):
    manifest = run(tmp_path, [board("ok"), board("bad")],
        lambda req: httpx.Response(404) if "bad" in str(req.url) else httpx.Response(200, json={"jobs": [{"id": 1}]}))
    assert manifest["status"] == "FAILED"
    with pytest.raises(ValueError, match="committed"):
        validate_manifest(manifest, "test-run", manifest["snapshot_date"])


@pytest.mark.parametrize("payload", [{}, {"jobs": None}, {"jobs": [None]}, {"jobs": [{}]}])
def test_malformed_api_is_failure_not_empty_success(tmp_path, payload):
    manifest = run(tmp_path, [board("bad")], lambda req: httpx.Response(200, json=payload))
    assert manifest["boards"][0]["status"] == "FAILED"
    assert manifest["status"] == "FAILED"


def test_all_empty_run_fails_closed(tmp_path):
    manifest = run(tmp_path, [board("empty")], lambda req: httpx.Response(200, json={"jobs": []}))
    assert manifest["boards"][0]["complete"] is True
    assert manifest["status"] == "FAILED"


def test_arbeitnow_page_cap_does_not_imply_closed_jobs(tmp_path):
    manifest = run(tmp_path, [{"source": "arbeitnow", "token": "arbeitnow"}],
        lambda req: httpx.Response(200, json={"data": [{"slug": "one"}],
                                             "links": {"next": "https://www.arbeitnow.com/api/job-board-api?page=2"}}),
        arbeitnow_pages=1)
    assert manifest["status"] == "SUCCEEDED"
    assert manifest["boards"][0]["complete"] is False
    assert validate_manifest(manifest, "test-run", manifest["snapshot_date"])[1] == []


def test_retry_transient_response_then_success(monkeypatch):
    responses = iter([httpx.Response(429, headers={"Retry-After": "1"}), httpx.Response(503),
                      httpx.Response(200, json={"jobs": []})])
    waits = []
    monkeypatch.setattr(ingest.time, "sleep", waits.append)
    with httpx.Client(transport=httpx.MockTransport(lambda req: next(responses))) as client:
        assert ingest.fetch_greenhouse(client, "test", "Test") == []
    assert waits == [1, 2]


def test_storage_failure_never_commits_a_success_manifest(tmp_path, monkeypatch):
    writes = []
    def fail_storage(key, *args, **kwargs):
        writes.append(key)
        raise OSError("disk full")
    monkeypatch.setattr(ingest, "put_bytes", fail_storage)
    with pytest.raises(OSError):
        run(tmp_path, [board("one")], lambda req: httpx.Response(200, json={"jobs": [{"id": 1}]}))
    assert all("manifest" not in key for key in writes)


def test_limit_cannot_produce_partial_production_manifest():
    with pytest.raises(SystemExit):
        ingest.main(["--bucket", "test-lake", "--limit", "1"])


def test_retry_of_committed_run_does_not_refresh_its_observation_clock(tmp_path):
    first = run(tmp_path, [board("one")], lambda req: httpx.Response(200, json={"jobs": [{"id": 1}]}))
    def unexpected_fetch(req):
        raise AssertionError("A committed run must not be fetched again")
    second = run(tmp_path, [board("one")], unexpected_fetch)
    assert first == second


def test_execution_keeps_its_utc_date_across_midnight(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 15, 0, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(ingest, "datetime", Clock)
    with httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"jobs": [{"id": 1}]}))) as client:
        manifest = ingest.run_ingestion([board("one")], run_id="midnight", snapshot_date="2026-09-14",
            execution_started_at="2026-09-14T23:59:00Z", out_dir=tmp_path, client=client)
    assert manifest["snapshot_date"] == "2026-09-14"
    assert validate_manifest(manifest, "midnight", "2026-09-14", now=Clock.now())[0]


def test_glue_python_shell_arguments_without_job_name(tmp_path, monkeypatch):
    sources = tmp_path / "sources.json"
    sources.write_text(json.dumps({"boards": [board("one")]}), encoding="utf-8")
    calls = []
    def fake_ingestion(boards, **kwargs):
        calls.append((boards, kwargs))
        return {"status": "SUCCEEDED"}
    monkeypatch.setattr(ingest, "run_ingestion", fake_ingestion)
    result = ingest.main([
        "--sources", str(sources), "--out-dir", str(tmp_path), "--run-id", "aws-run",
        "--additional-python-modules", "httpx==0.28.1",
        "--scriptLocation", "s3://scripts/ingestion/land_to_bronze.py",
        "--library-set", "analytics", "--python-version", "3.9",
    ])
    assert result == 0
    assert calls[0][1]["run_id"] == "aws-run"


def test_unknown_argument_still_fails_with_glue_runtime_flags():
    with pytest.raises(SystemExit):
        ingest.main(["--JOB_NAME", "ingestion", "--misspelled-argument", "value"])
