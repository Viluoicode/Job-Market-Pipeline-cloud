"""SQL builders and verdict of scripts/compare_iceberg.py (Parquet Gold vs shadow Iceberg Gold)."""
import importlib.util
from pathlib import Path

import pytest

path = Path(__file__).resolve().parents[1] / "scripts" / "compare_iceberg.py"
spec = importlib.util.spec_from_file_location("compare_iceberg", path)
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)

COLUMNS = [("job_id", "string"), ("is_remote", "boolean"), ("last_seen_at", "timestamp"),
           ("posting_count", "int"), ("snapshot_date", "string")]


def test_except_sql_normalizes_types_and_filters_one_day_on_both_sides():
    sql = compare.except_sql(COLUMNS, "pq_db.fact_job_posting", "ice_db.fact_job_posting", "2026-11-21")
    # Parquet partitions are strings, Iceberg's is a DATE; timestamps differ in time-zone type.
    assert "CAST(job_id AS varchar)" in sql and "to_unixtime(last_seen_at)" in sql
    assert "snapshot_date" not in sql.split("WHERE")[0]          # the partition column is not compared
    assert "FROM pq_db.fact_job_posting WHERE snapshot_date = '2026-11-21'" in sql
    assert "FROM ice_db.fact_job_posting WHERE snapshot_date = DATE '2026-11-21'" in sql
    assert sql.count("EXCEPT") == 1


def test_except_sql_rejects_unsafe_identifiers_and_dates():
    with pytest.raises(ValueError):
        compare.except_sql(COLUMNS, "pq_db.fact; DROP", "ice_db.fact", "2026-11-21")
    with pytest.raises(ValueError):
        compare.except_sql(COLUMNS, "pq_db.fact", "ice_db.fact", "2026-11-21' OR '1'='1")


def test_verdict_requires_equal_counts_and_no_row_differences():
    good = {"fact_job_posting": {"parquet": 8, "iceberg": 8}, "demand_by_role": {"parquet": 3, "iceberg": 3}}
    assert compare.verdict(good, {"parquet_only": 0, "iceberg_only": 0}) == "MATCH"
    bad_count = {**good, "demand_by_role": {"parquet": 3, "iceberg": 2}}
    assert compare.verdict(bad_count, {"parquet_only": 0, "iceberg_only": 0}) == "MISMATCH"
    assert compare.verdict(good, {"parquet_only": 1, "iceberg_only": 1}) == "MISMATCH"
