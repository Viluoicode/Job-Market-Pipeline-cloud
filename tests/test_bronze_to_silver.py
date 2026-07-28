"""Unit tests for the Bronze -> Silver transform (glue/jobs/bronze_to_silver.py).

These assert the two keys that are the heart of the pipeline behave as designed:
  * job_id      — stable per (source, source_job_id); re-crawls collapse to one row.
  * dedup_hash  — content fingerprint that ignores case/punctuation/whitespace, so the same job
                  posted on two boards hashes identically (cross-source dedup happens in Gold).
"""

import bronze_to_silver as b2s

# Bronze record shape (the 11 columns land_to_bronze.py writes, minus raw_json which Silver drops).
COLUMNS = [
    "source", "board_token", "source_job_id", "company", "title",
    "location", "remote", "apply_url", "posted_at", "description",
]


def _raw(spark, rows):
    return spark.createDataFrame([dict(zip(COLUMNS, r)) for r in rows])


def test_dedup_within_source_keeps_latest(spark):
    # Same posting crawled twice (same source + source_job_id) -> one Silver row, latest posted_at.
    rows = [
        ("greenhouse", "acme", "1", "Acme", "Data Engineer", "NYC", True, "u",
         "2026-01-01T00:00:00+00:00", "old"),
        ("greenhouse", "acme", "1", "Acme", "Data Engineer", "NYC", True, "u",
         "2026-02-01T00:00:00+00:00", "new"),
    ]
    out = b2s.build_silver(_raw(spark, rows), "2026-02-01").collect()
    assert len(out) == 1
    assert out[0]["description"] == "new"          # latest posted_at wins
    assert out[0]["job_id"] == out[0]["job_id"].lower()


def test_dedup_hash_equal_across_sources_for_same_content(spark):
    # Punctuation/case/whitespace differences must normalize to the SAME dedup_hash.
    rows = [
        ("greenhouse", "a", "1", "Acme", "Senior  Engineer!", "New York", False, "u",
         "2026-01-01T00:00:00+00:00", "x"),
        ("lever", "b", "9", "acme", "senior engineer", "new york", False, "u",
         "2026-01-01T00:00:00+00:00", "y"),
    ]
    hashes = {r["dedup_hash"] for r in b2s.build_silver(_raw(spark, rows), "2026-01-01").collect()}
    assert len(hashes) == 1


def test_job_id_differs_across_sources(spark):
    # Different source -> different job_id even for identical content (within-source identity).
    rows = [
        ("greenhouse", "a", "1", "Acme", "Data Engineer", "NYC", False, "u",
         "2026-01-01T00:00:00+00:00", "x"),
        ("lever", "b", "1", "Acme", "Data Engineer", "NYC", False, "u",
         "2026-01-01T00:00:00+00:00", "y"),
    ]
    ids = {r["job_id"] for r in b2s.build_silver(_raw(spark, rows), "2026-01-01").collect()}
    assert len(ids) == 2


def test_drops_rows_without_source_job_id(spark):
    rows = [
        ("greenhouse", "a", "", "Acme", "X", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "x"),
        ("greenhouse", "a", "2", "Acme", "X", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "y"),
    ]
    assert b2s.build_silver(_raw(spark, rows), "2026-01-01").count() == 1


def test_remote_defaults_to_false_when_null(spark):
    rows = [
        ("greenhouse", "a", "1", "Acme", "X", "NYC", None, "u", "2026-01-01T00:00:00+00:00", "x"),
    ]
    assert b2s.build_silver(_raw(spark, rows), "2026-01-01").collect()[0]["is_remote"] is False
