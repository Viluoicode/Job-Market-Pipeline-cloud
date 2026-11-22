"""Exercise API failures, completeness and the manifest commit boundary without AWS."""
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import land_to_bronze as ingest
import pytest
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


def _fetch(fetch, payload, **kwargs):
    with httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json=payload))) as client:
        return fetch(client, "t", "T", **kwargs)


# is_remote is tri-state: True = fully remote, False = onsite or hybrid, None = source gave no signal.
@pytest.mark.parametrize("location, expected", [
    ("Remote - US", True),
    ("Hybrid - New York", False),
    ("San Francisco, CA", None),        # a city alone does not prove onsite
    (None, None),
])
def test_greenhouse_remote_from_location_text(location, expected):
    job = {"id": 1, "title": "X", "location": {"name": location} if location else None}
    assert _fetch(ingest.fetch_greenhouse, {"jobs": [job]})[0]["remote"] is expected


@pytest.mark.parametrize("workplace, location, expected", [
    ("remote", "Berlin", True),
    ("hybrid", "Remote - EU", False),   # explicit workplace type wins over location text
    ("onsite", "Paris", False),
    (None, "Remote", True),
    (None, "Paris", None),
])
def test_lever_remote_from_workplace_type(workplace, location, expected):
    job = {"id": "a", "text": "X", "categories": {"location": location}}
    if workplace:
        job["workplaceType"] = workplace
    assert _fetch(ingest.fetch_lever, [job])[0]["remote"] is expected


@pytest.mark.parametrize("is_remote, workplace, location, expected", [
    (True, "Hybrid", "London", False),  # Ashby flags hybrid jobs isRemote=True: not fully remote
    (True, "Remote", "Anywhere", True),
    (False, "OnSite", "London", False),
    (True, None, "London", True),
    (None, None, "Remote, US", True),
    (None, None, "London", None),
])
def test_ashby_remote_prefers_workplace_type(is_remote, workplace, location, expected):
    job = {"id": "a", "title": "X", "location": location}
    if is_remote is not None:
        job["isRemote"] = is_remote
    if workplace:
        job["workplaceType"] = workplace
    assert _fetch(ingest.fetch_ashby, {"jobs": [job]})[0]["remote"] is expected


@pytest.mark.parametrize("flag, expected", [(True, True), (False, False), ("missing", None)])
def test_arbeitnow_remote_flag_or_unknown(flag, expected):
    job = {"slug": "s", "title": "X"}
    if flag != "missing":
        job["remote"] = flag
    payload = {"data": [job], "links": {"next": None}}
    assert _fetch(ingest.fetch_arbeitnow, payload)[0]["remote"] is expected


CATALOG = Path(ingest.__file__).with_name("sources.json")


def test_source_catalog_is_well_formed():
    boards = json.loads(CATALOG.read_text(encoding="utf-8"))["boards"]
    identities = [(b["source"].lower(), b["token"]) for b in boards]
    assert all(source in ingest.CONNECTORS for source, _ in identities)
    # URL path segment and S3 key component: keep tokens to unreserved characters.
    assert all(re.fullmatch(r"[A-Za-z0-9._-]{1,80}", token) for _, token in identities)
    assert len(set(identities)) == len(identities)
    # A company that moves ATS must be replaced, not added twice (its postings would double count).
    companies = [b["company"] for b in boards]
    assert len(set(companies)) == len(companies)


def test_ashby_board_token_with_dot_round_trips(tmp_path):
    # Mistral AI's Ashby board is "mistral.ai": the dot must survive the URL, the Bronze key
    # and manifest validation unchanged.
    requested = []

    def handler(req):
        requested.append(req.url.path)
        return httpx.Response(200, json={"jobs": [
            {"id": "a1", "title": "ML Engineer", "location": "Paris", "workplaceType": "Hybrid"}]})

    manifest = run(tmp_path, [{"source": "Ashby", "token": "mistral.ai", "company": "Mistral AI"}], handler)
    assert requested == ["/posting-api/job-board/mistral.ai"]
    keys, complete = validate_manifest(manifest, "test-run", manifest["snapshot_date"])
    assert keys == ["bronze/run_id=test-run/source=ashby/board=mistral.ai/jobs.json"]
    assert complete == [("ashby", "mistral.ai")]
    record = json.loads((tmp_path / keys[0]).read_text())
    assert record["board_token"] == "mistral.ai" and record["company"] == "Mistral AI"
    assert record["remote"] is False          # Hybrid is not remote


def _concurrency_probe(delay_for):
    """MockTransport handler that records how many requests are in flight at once."""
    state = {"active": 0, "peak": 0, "started": []}
    lock = threading.Lock()

    def handler(req):
        token = req.url.path.split("/")[3]
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            state["started"].append(token)
        time.sleep(delay_for(token))
        with lock:
            state["active"] -= 1
        return httpx.Response(200, json={"jobs": [{"id": token}]})
    return state, handler


def test_boards_are_fetched_concurrently_with_at_most_five_requests(tmp_path):
    state, handler = _concurrency_probe(lambda token: 0.05)
    manifest = run(tmp_path, [board(f"b{i}") for i in range(12)], handler)
    assert manifest["status"] == "SUCCEEDED"
    assert 1 < state["peak"] <= 5


def test_manifest_keeps_configured_board_order_when_fetches_finish_out_of_order(tmp_path):
    # The first board is the slowest; every other board finishes before it.
    state, handler = _concurrency_probe(lambda token: 0.3 if token == "b0" else 0.01)
    manifest = run(tmp_path, [board(f"b{i}") for i in range(8)], handler)
    assert [b["board_token"] for b in manifest["boards"]] == [f"b{i}" for i in range(8)]
    assert state["started"][0] == "b0"


def test_fetch_window_bounds_boards_held_in_memory(tmp_path):
    # While the first board is still in flight, at most 2 x 5 boards may be fetched ahead of
    # the writer, so a slow board cannot make the whole run's payloads accumulate in memory.
    seen_while_first_pending = []
    state, handler = _concurrency_probe(lambda token: 0.5 if token == "b0" else 0.0)

    def watching(req):
        if req.url.path.split("/")[3] == "b0":
            response = handler(req)
            seen_while_first_pending.append(len(state["started"]))
            return response
        return handler(req)

    manifest = run(tmp_path, [board(f"b{i}") for i in range(30)], watching)
    assert manifest["successful_boards"] == 30
    assert seen_while_first_pending == [10]
