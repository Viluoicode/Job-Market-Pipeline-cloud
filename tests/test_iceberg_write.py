"""Gold's Iceberg writer (silver_to_gold.write_iceberg) and the publish step around it.

The Iceberg test runs real Apache Iceberg in a child process (the shared test SparkSession has no
Iceberg jar) against an in-memory catalog, so it needs no filesystem, Hadoop binaries or AWS.
It runs when ICEBERG_SPARK_JAR points at iceberg-spark-runtime-3.5_2.12 (CI downloads it);
Glue 4.0 runs Iceberg 1.0 on Spark 3.3 with the same DataFrameWriterV2 calls.
"""
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import silver_to_gold as s2g

JOBS_DIR = Path(__file__).resolve().parents[1] / "glue" / "jobs"
ICEBERG_JAR = os.environ.get("ICEBERG_SPARK_JAR")


class Mart:
    def __init__(self, rows):
        self.rows = rows

    def count(self):
        return self.rows


def test_publish_records_snapshot_ids_as_text_and_row_counts():
    calls = []

    def writer(df, table, snapshot_date):
        calls.append((table, snapshot_date))
        return 811978490607233390 + len(calls)      # real ids exceed 2**53

    record = s2g.publish_iceberg(
        [("glue_catalog.gold_iceberg.fact_job_posting", Mart(8)), ("glue_catalog.gold_iceberg.demand_by_role", Mart(3))],
        run_id="run-1", snapshot_date="2026-11-21", writer=writer)
    assert record == {
        "run_id": "run-1", "snapshot_date": "2026-11-21", "status": "SUCCEEDED",
        "tables": {"fact_job_posting": {"snapshot_id": "811978490607233391", "rows": 8},
                   "demand_by_role": {"snapshot_id": "811978490607233392", "rows": 3}},
    }


def test_publish_failure_fails_the_job():
    # Iceberg is the published Gold now: a failed commit must stop the run (lock released, alarm).
    def writer(df, table, snapshot_date):
        raise RuntimeError("AccessDenied: glue:UpdateTable")

    with pytest.raises(RuntimeError, match="UpdateTable"):
        s2g.publish_iceberg([("c.db.fact_job_posting", Mart(8))], run_id="run-1",
                            snapshot_date="2026-11-21", writer=writer)


def test_publish_requires_a_committed_snapshot():
    with pytest.raises(ValueError, match="snapshot"):
        s2g.publish_iceberg([("c.db.fact_job_posting", Mart(0))], run_id="run-1",
                            snapshot_date="2026-11-21", writer=lambda df, table, day: None)


# ---- snapshot expiry --------------------------------------------------------------------------
NOW = datetime(2026, 11, 26, 18, 10, tzinfo=timezone.utc)


def test_expiry_cutoff_keeps_the_retention_window():
    assert s2g.snapshot_expiry_cutoff(NOW, 30) == NOW - timedelta(days=30)


def test_expiry_cutoff_never_passes_the_snapshot_readers_are_pinned_to():
    pinned_at = NOW - timedelta(days=45)          # publication stalled: readers still use an old version
    assert s2g.snapshot_expiry_cutoff(NOW, 30, pinned_at) == pinned_at
    recent = NOW - timedelta(days=1)              # a recent pin does not shorten the window
    assert s2g.snapshot_expiry_cutoff(NOW, 30, recent) == NOW - timedelta(days=30)


def test_maintenance_expires_each_table_up_to_its_pinned_snapshot():
    pinned_at = {"111": NOW - timedelta(days=40), "222": NOW - timedelta(days=1)}
    calls = []

    def expire(spark, table, older_than):
        calls.append((table, older_than))
        return {"deleted_data_files_count": 4, "deleted_manifest_files_count": 2}

    record = s2g.maintain_iceberg(
        None, ["c.db.fact_job_posting", "c.db.demand_by_role"], run_id="run-9", now=NOW, retention_days=30,
        pinned={"fact_job_posting": "111", "demand_by_role": "222"},
        expire=expire, committed_at=lambda spark, table, snapshot_id: pinned_at[snapshot_id])
    assert calls == [("c.db.fact_job_posting", NOW - timedelta(days=40)),
                     ("c.db.demand_by_role", NOW - timedelta(days=30))]
    assert record["status"] == "SUCCEEDED" and record["run_id"] == "run-9" and record["retention_days"] == 30
    assert record["tables"]["fact_job_posting"] == {
        "older_than": "2026-10-17T18:10:00+00:00", "pinned_snapshot_id": "111",
        "deleted_data_files_count": 4, "deleted_manifest_files_count": 2}


def test_maintenance_does_not_expire_a_table_whose_pinned_snapshot_is_unknown():
    # If the pin cannot be dated, expiring by age alone could delete what the dashboard reads.
    calls = []
    record = s2g.maintain_iceberg(
        None, ["c.db.fact_job_posting"], run_id="run-9", now=NOW, retention_days=30,
        pinned={"fact_job_posting": "111"}, expire=lambda *a: calls.append(a),
        committed_at=lambda spark, table, snapshot_id: None)
    assert calls == []
    assert record["status"] == "FAILED"
    assert "111" in record["tables"]["fact_job_posting"]["error"]


def test_maintenance_failure_is_recorded_and_the_other_tables_still_run():
    def expire(spark, table, older_than):
        if table.endswith("fact_job_posting"):
            raise RuntimeError("AccessDenied: s3:DeleteObject")
        return {"deleted_data_files_count": 0}

    record = s2g.maintain_iceberg(
        None, ["c.db.fact_job_posting", "c.db.demand_by_role"], run_id="run-9", now=NOW, retention_days=30,
        pinned={}, expire=expire, committed_at=lambda *a: None)
    assert record["status"] == "FAILED"
    assert record["tables"]["fact_job_posting"]["error_type"] == "RuntimeError"
    assert record["tables"]["demand_by_role"]["deleted_data_files_count"] == 0


def test_pinned_snapshots_come_from_the_completion_marker():
    marker = {"iceberg_snapshots": {"fact_job_posting": {"snapshot_id": "111", "rows": 8}}}
    assert s2g.pinned_snapshots(marker) == {"fact_job_posting": "111"}
    assert s2g.pinned_snapshots(None) == {}                                   # first ever run
    assert s2g.pinned_snapshots({"run_id": "r", "snapshot_date": "d"}) == {}  # marker from before Iceberg


ICEBERG_SCRIPT = textwrap.dedent("""
    import sys
    sys.path.insert(0, sys.argv[2])
    from pyspark.sql import SparkSession, functions as F
    import silver_to_gold as s2g

    if sys.argv[3] == "spark33":
        # Glue 4.0 runs PySpark 3.3, whose DataFrameWriterV2.overwrite hands the Python Column to
        # py4j unconverted ("Column is not iterable"); 3.4+ converts it. Verbatim from v3.3.0.
        from pyspark.sql.readwriter import DataFrameWriterV2

        def overwrite_33(self, condition):
            self._jwriter.overwrite(condition)
        DataFrameWriterV2.overwrite = overwrite_33

    spark = (SparkSession.builder.master("local[1]")
             .config("spark.driver.extraClassPath", sys.argv[1])   # classpath, not spark.jars: no Hadoop copy
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.test.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
             .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS test.gold")
    t = "test.gold.fact_job_posting"

    def day(d, ids):
        df = spark.createDataFrame([(i, d) for i in ids], "job_id string, snapshot_date string")
        return df.withColumn("snapshot_date", F.col("snapshot_date").cast("date"))

    def rows():
        return sorted((r.job_id, str(r.snapshot_date)) for r in spark.table(t).collect())

    s1 = s2g.write_iceberg(day("2026-11-20", ["a", "b"]), t, "2026-11-20")      # creates the table
    s2 = s2g.write_iceberg(day("2026-11-21", ["c"]), t, "2026-11-21")           # another day: appended
    assert rows() == [("a", "2026-11-20"), ("b", "2026-11-20"), ("c", "2026-11-21")], rows()
    s3 = s2g.write_iceberg(day("2026-11-21", ["d", "e"]), t, "2026-11-21")      # same-day rerun replaces
    assert rows() == [("a", "2026-11-20"), ("b", "2026-11-20"), ("d", "2026-11-21"), ("e", "2026-11-21")], rows()
    s4 = s2g.write_iceberg(day("2026-11-21", []), t, "2026-11-21")              # empty rerun clears the day
    assert rows() == [("a", "2026-11-20"), ("b", "2026-11-20")], rows()

    snapshots = [r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at").collect()]
    assert snapshots == [s1, s2, s3, s4], (snapshots, [s1, s2, s3, s4])          # one atomic commit per write
    travel = sorted(r.job_id for r in spark.read.option("snapshot-id", s3).table(t).collect())
    assert travel == ["a", "b", "d", "e"], travel                                 # time travel to the third run
    spec = spark.sql(f"SELECT * FROM {t}.partitions").columns
    assert "partition" in spec
    assert [str(r.partition.snapshot_date) for r in spark.table(f"{t}.partitions").collect()] == ["2026-11-20"]

    # New tables drop metadata files beyond the newest versions on each commit.
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {t}").collect()}
    assert props.get("write.metadata.delete-after-commit.enabled") == "true", props
    assert props.get("write.metadata.previous-versions-max") == "30", props

    # Snapshot expiry. Readers are pinned to s3, so nothing from s3 on may go, whatever the window.
    from datetime import datetime, timedelta, timezone
    s5 = s2g.write_iceberg(day("2026-11-22", ["f"]), t, "2026-11-22")
    now = datetime.now(timezone.utc) + timedelta(seconds=1)
    rec = s2g.maintain_iceberg(spark, [t], run_id="r", now=now, retention_days=0,
                               pinned={"fact_job_posting": str(s3)})
    assert rec["status"] == "SUCCEEDED", rec
    snaps = [r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at").collect()]
    assert snaps == [s3, s4, s5], (snaps, [s3, s4, s5])
    # Only s2 used the file holding "c" (s3 replaced that day), so exactly one data file is deleted.
    assert rec["tables"]["fact_job_posting"]["deleted_data_files_count"] == 1, rec
    assert sorted(r.job_id for r in spark.read.option("snapshot-id", s3).table(t).collect()) == ["a", "b", "d", "e"]
    try:
        spark.read.option("snapshot-id", s2).table(t).collect()
        raise AssertionError("expired snapshot s2 is still readable")
    except Exception as exc:
        assert "s2 is still readable" not in str(exc), exc

    # Once readers move to the newest snapshot, everything older than the window goes.
    rec = s2g.maintain_iceberg(spark, [t], run_id="r2", now=now, retention_days=0,
                               pinned={"fact_job_posting": str(s5)})
    snaps = [r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {t}.snapshots").collect()]
    assert snaps == [s5], snaps
    # s3 wrote "d" and "e" in one file; s4 (the empty rerun) wrote none. So one file goes.
    assert rec["tables"]["fact_job_posting"]["deleted_data_files_count"] == 1, rec
    assert rows() == [("a", "2026-11-20"), ("b", "2026-11-20"), ("f", "2026-11-22")], rows()

    # A table created before these properties existed gets them on its next maintenance.
    old = "test.gold.legacy"
    day("2026-11-20", ["x"]).writeTo(old).using("iceberg").create()
    s2g.ensure_table_properties(spark, old)
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {old}").collect()}
    assert props.get("write.metadata.delete-after-commit.enabled") == "true", props
    before = spark.sql(f"SELECT count(*) c FROM {old}.metadata_log_entries").first().c
    s2g.ensure_table_properties(spark, old)                                          # idempotent: no new commit
    assert spark.sql(f"SELECT count(*) c FROM {old}.metadata_log_entries").first().c == before
    print("ICEBERG_OK")
""")


@pytest.mark.skipif(not ICEBERG_JAR, reason="set ICEBERG_SPARK_JAR to an iceberg-spark-runtime-3.5_2.12 jar")
@pytest.mark.parametrize("pyspark_api", ["current", "spark33"])
def test_write_iceberg_creates_replaces_one_day_atomically_and_time_travels(tmp_path, pyspark_api):
    script = tmp_path / "iceberg_check.py"
    script.write_text(ICEBERG_SCRIPT, encoding="utf-8")
    result = subprocess.run([sys.executable, str(script), ICEBERG_JAR, str(JOBS_DIR), pyspark_api],
                            capture_output=True, text=True, timeout=600)
    assert "ICEBERG_OK" in result.stdout, result.stdout[-3000:] + result.stderr[-5000:]
