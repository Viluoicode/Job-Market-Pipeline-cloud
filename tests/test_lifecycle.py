from pyspark.sql import functions as F
import pytest

import bronze_to_silver as b2s
import silver_to_gold as s2g


def posting(board, jid, description="old", posted_at="2026-01-01T00:00:00+00:00"):
    return ("greenhouse", board, jid, board, "Data Engineer", "NYC", True, "u", posted_at, description)


def observed(bronze, rows, time):
    return b2s.build_silver(bronze(rows).withColumn("fetched_at", F.lit(time)), time[:10])


def test_identity_is_scoped_to_board(bronze):
    rows = b2s.build_silver(bronze([posting("a", "1"), posting("b", "1")]), "2026-09-14").collect()
    assert len({r.job_id for r in rows}) == 2


def test_observation_time_wins_over_source_posted_at(bronze):
    old = bronze([posting("a", "1", "old", "2026-09-13T00:00:00+00:00")]).withColumn("fetched_at", F.lit("2026-09-13T10:00:00Z"))
    new = bronze([posting("a", "1", "edited", "2026-01-01T00:00:00+00:00")]).withColumn("fetched_at", F.lit("2026-09-14T10:00:00Z"))
    row = b2s.build_silver(old.unionByName(new), "2026-09-14").first()
    assert row.description == "edited"
    assert row.last_seen_at.day == 14


def test_close_only_missing_jobs_from_complete_boards_and_preserve_history(bronze):
    previous = observed(bronze, [posting("ok", "1"), posting("ok", "2"), posting("failed", "3"),
                                 posting("truncated", "4")], "2026-09-13T10:00:00Z")
    current = observed(bronze, [posting("ok", "1", "updated"), posting("new", "5")], "2026-09-14T10:00:00Z")
    output = b2s.reconcile_lifecycle(current, previous, [("greenhouse", "ok")], "2026-09-14T10:05:00Z")
    rows = {r.source_job_id: r for r in output.collect()}
    assert rows["1"].first_seen_at.day == 13
    assert rows["1"].last_seen_at.day == 14
    assert rows["1"].description == "updated"
    assert rows["2"].is_active is False
    assert rows["2"].status_reason == "absent_from_complete_board"
    assert rows["3"].is_active is True and rows["3"].last_seen_at.day == 13
    assert rows["4"].status_reason == "board_unverified"
    assert rows["5"].first_seen_at.day == 14


def test_successful_empty_board_closes_previous_jobs(bronze):
    previous = observed(bronze, [posting("empty", "1")], "2026-09-13T10:00:00Z")
    current = observed(bronze, [posting("other", "2")], "2026-09-14T10:00:00Z")
    row = b2s.reconcile_lifecycle(current, previous, [("greenhouse", "empty")],
                                   "2026-09-14T10:05:00Z").where("source_job_id = '1'").first()
    assert row.is_active is False


def test_unverified_jobs_leave_gold_before_lifecycle_expiry(bronze):
    previous = observed(bronze, [posting("failed", "1")], "2026-09-12T10:00:00Z")
    current = observed(bronze, [posting("ok", "2")], "2026-09-14T10:00:00Z")
    output = b2s.reconcile_lifecycle(current, previous, [], "2026-09-14T10:05:00Z")
    old = output.where("source_job_id = '1'").first()
    assert old.is_active is True and old.is_fresh is False
    assert s2g.build_fact(output).count() == 1


def test_expire_unverified_and_reactivate_without_resetting_first_seen(bronze):
    previous = observed(bronze, [posting("failed", "1")], "2026-09-01T10:00:00Z")
    current = observed(bronze, [posting("ok", "2")], "2026-09-14T10:00:00Z")
    expired = b2s.reconcile_lifecycle(current, previous, [], "2026-09-14T10:05:00Z")
    row = expired.where("source_job_id = '1'").first()
    assert row.is_active is False and row.status_reason == "stale_observation"
    back = observed(bronze, [posting("failed", "1", "reopened")], "2026-09-15T10:00:00Z")
    row = b2s.reconcile_lifecycle(back, expired, [], "2026-09-15T10:05:00Z").where("source_job_id = '1'").first()
    assert row.is_active is True and row.is_fresh is True
    assert row.first_seen_at.day == 1 and row.last_seen_at.day == 15


def test_observation_validation_rejects_rows_from_another_run(bronze):
    raw = (bronze([posting("a", "1")]).withColumn("fetched_at", F.lit("2026-09-14T10:00:00Z"))
           .withColumn("run_id", F.lit("old-run")))
    with pytest.raises(ValueError, match="observation metadata"):
        b2s.validate_observations(raw, {"run_id": "new-run"})


def test_observation_validation_rejects_truncated_bronze_object(bronze):
    raw = (bronze([posting("a", "1")]).withColumn("fetched_at", F.lit("2026-09-14T10:00:00Z"))
           .withColumn("run_id", F.lit("run")))
    manifest = {"run_id": "run", "boards": [{"source": "greenhouse", "board_token": "a",
        "status": "SUCCEEDED", "row_count": 2, "fetched_at": "2026-09-14T10:00:00Z"}]}
    with pytest.raises(ValueError, match="board counts"):
        b2s.validate_observations(raw, manifest)
