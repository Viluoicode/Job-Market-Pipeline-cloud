"""Gold's Iceberg writer (silver_to_gold.write_iceberg) and its shadow-mode wrapper.

The Iceberg test runs real Apache Iceberg in a child process (the shared test SparkSession has no
Iceberg jar) against an in-memory catalog, so it needs no filesystem, Hadoop binaries or AWS.
It runs when ICEBERG_SPARK_JAR points at iceberg-spark-runtime-3.5_2.12 (CI downloads it);
Glue 4.0 runs Iceberg 1.0 on Spark 3.3 with the same DataFrameWriterV2 calls.
"""
import os
import subprocess
import sys
import textwrap
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


def test_shadow_write_records_snapshot_ids_and_row_counts():
    calls = []

    def writer(df, table, snapshot_date):
        calls.append((table, snapshot_date))
        return 1000 + len(calls)

    record = s2g.shadow_write_iceberg(
        [("glue_catalog.gold_iceberg.fact_job_posting", Mart(8)), ("glue_catalog.gold_iceberg.demand_by_role", Mart(3))],
        run_id="run-1", snapshot_date="2026-11-21", writer=writer)
    assert record == {
        "run_id": "run-1", "snapshot_date": "2026-11-21", "mode": "shadow", "status": "SUCCEEDED",
        "tables": {"fact_job_posting": {"snapshot_id": 1001, "rows": 8},
                   "demand_by_role": {"snapshot_id": 1002, "rows": 3}},
    }


def test_shadow_write_failure_is_recorded_not_raised():
    # Shadow mode must never break the published Parquet path.
    def writer(df, table, snapshot_date):
        if table.endswith("demand_by_role"):
            raise RuntimeError("AccessDenied: glue:UpdateTable")
        return 7

    record = s2g.shadow_write_iceberg(
        [("c.db.fact_job_posting", Mart(8)), ("c.db.demand_by_role", Mart(3)), ("c.db.role_opportunity", Mart(2))],
        run_id="run-1", snapshot_date="2026-11-21", writer=writer)
    assert record["status"] == "FAILED"
    assert record["failed_table"] == "demand_by_role"
    assert record["error_type"] == "RuntimeError" and "glue:UpdateTable" in record["error"]
    assert record["tables"] == {"fact_job_posting": {"snapshot_id": 7, "rows": 8}}   # stops at the failure


ICEBERG_SCRIPT = textwrap.dedent("""
    import sys
    sys.path.insert(0, sys.argv[2])
    from pyspark.sql import SparkSession, functions as F
    import silver_to_gold as s2g

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
    print("ICEBERG_OK")
""")


@pytest.mark.skipif(not ICEBERG_JAR, reason="set ICEBERG_SPARK_JAR to an iceberg-spark-runtime-3.5_2.12 jar")
def test_write_iceberg_creates_replaces_one_day_atomically_and_time_travels(tmp_path):
    script = tmp_path / "iceberg_check.py"
    script.write_text(ICEBERG_SCRIPT, encoding="utf-8")
    result = subprocess.run([sys.executable, str(script), ICEBERG_JAR, str(JOBS_DIR)],
                            capture_output=True, text=True, timeout=600)
    assert "ICEBERG_OK" in result.stdout, result.stdout[-3000:] + result.stderr[-5000:]
