"""Glue (PySpark) — Bronze -> Silver.

Reads raw newline-delimited JSON from ``s3://<LAKE_BUCKET>/bronze/`` (landed by
``ingestion/land_to_bronze.py``), types the columns, computes the stable surrogate key and the
cross-source content dedup hash, then writes typed, deduped Parquet to
``s3://<LAKE_BUCKET>/silver/jobs/snapshot_date=<YYYY-MM-DD>/``.

This is the cloud/Spark equivalent of SkillRadar's Bronze->Silver step:
  * ``job_id``     = SHA-256(source | source_job_id)              (port of dedup.make_job_id)
  * ``dedup_hash`` = SHA-256(norm(company) | norm(title) | norm(location)).upper()
                                                                  (port of dedup.compute_dedup_hash)
  * ``norm`` lower-cases, strips punctuation to spaces, collapses whitespace
                                                                  (port of text.normalize_for_key)

Args (Glue job parameters):
  --JOB_NAME       (provided by Glue)
  --LAKE_BUCKET    data lake bucket name
  --snapshot_date  optional YYYY-MM-DD; defaults to today (UTC)
"""

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


# Data Quality gate on the Silver table — the cloud/Glue parallel of SkillRadar's dbt tests
# (not_null / unique / accepted_values). Rules are written in DQDL (Data Quality Definition
# Language); EvaluateDataQuality runs them inside the job and publishes a score to the Glue
# Data Quality console. Chosen to assert the invariants the transform is supposed to guarantee.
DQ_RULESET = """Rules = [
    RowCount > 0,
    IsComplete "job_id",
    IsUnique "job_id",
    IsComplete "dedup_hash",
    IsComplete "is_remote",
    ColumnValues "source" in [ "greenhouse", "lever", "ashby", "arbeitnow" ],
    Completeness "company" >= 0.9
]"""


def run_data_quality(glue, df, bucket, snapshot_date, enforce=True):
    """Evaluate DQ_RULESET against the Silver DataFrame.

    Publishes results to the Glue Data Quality console + CloudWatch, persists the per-rule
    outcomes to ``s3://<bucket>/quality/silver_jobs/snapshot_date=<date>/`` for audit/Athena,
    and (when ``enforce``) fails the job if any rule fails — so bad data never reaches Silver.
    """
    dyf = DynamicFrame.fromDF(df, glue, "silver_dq_input")
    results = EvaluateDataQuality().process_rows(
        frame=dyf,
        ruleset=DQ_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "silver_jobs",
            "enableDataQualityResultsPublishing": True,
            "enableDataQualityCloudWatchMetrics": True,
        },
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
    )

    outcomes = SelectFromCollection.apply(dfc=results, key="ruleOutcomes").toDF()
    outcomes.cache()
    print("[dq] per-rule outcomes:")
    outcomes.select("Rule", "Outcome", "FailureReason").show(50, truncate=False)

    quality_path = f"s3://{bucket}/quality/silver_jobs/"
    (
        outcomes.withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .write.mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(quality_path)
    )

    total = outcomes.count()
    n_failed = outcomes.where(F.col("Outcome") == "Failed").count()
    print(f"[dq] {total - n_failed}/{total} rules passed (snapshot_date={snapshot_date})")
    if n_failed and enforce:
        raise RuntimeError(f"[dq] {n_failed} data-quality rule(s) failed on Silver — failing the job")


def optional_arg(name: str, default: str) -> str:
    """Read --name value from argv (getResolvedOptions can't express optional args)."""
    flag = f"--{name}"
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def norm(colname: str):
    """Stable comparison key — port of SkillRadar text.normalize_for_key.

    lower -> replace every char outside [a-z0-9 ] with a space -> collapse whitespace -> trim.
    """
    c = F.coalesce(F.col(colname).cast("string"), F.lit(""))
    cleaned = F.regexp_replace(F.lower(c), "[^a-z0-9 ]", " ")
    return F.trim(F.regexp_replace(cleaned, r"\s+", " "))


def parse_ts(colname: str):
    """Best-effort parse of the ISO-8601 strings written by the ingester (offset or not)."""
    c = F.col(colname)
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(c),
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "LAKE_BUCKET"])
    snapshot_date = optional_arg("snapshot_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    bucket = args["LAKE_BUCKET"]

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    bronze_path = f"s3://{bucket}/bronze/"
    print(f"[bronze_to_silver] reading {bronze_path} (snapshot_date={snapshot_date})")

    # recursiveFileLookup: read every jobs.json under bronze/ and take `source` from the record
    # body (not the partition dir), so it never clashes with the `source=` path column.
    raw = (
        spark.read.option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")
        .json(bronze_path)
    )

    now_ts = F.current_timestamp()
    typed = (
        raw.select(
            F.col("source").cast("string").alias("source"),
            F.col("board_token").cast("string").alias("board_token"),
            F.col("source_job_id").cast("string").alias("source_job_id"),
            F.col("company").cast("string").alias("company"),
            F.col("title").cast("string").alias("title"),
            F.col("location").cast("string").alias("location"),
            F.coalesce(F.col("remote").cast("boolean"), F.lit(False)).alias("is_remote"),
            F.col("apply_url").cast("string").alias("apply_url"),
            parse_ts("posted_at").alias("posted_at"),
            F.col("description").cast("string").alias("description"),
        )
        .where(F.col("source_job_id").isNotNull() & (F.col("source_job_id") != ""))
        .withColumn(
            "job_id",
            F.lower(F.sha2(F.concat_ws("|", F.col("source"), F.col("source_job_id")), 256)),
        )
        .withColumn(
            "dedup_hash",
            F.upper(F.sha2(F.concat_ws("|", norm("company"), norm("title"), norm("location")), 256)),
        )
        .withColumn("first_seen_at", now_ts)
        .withColumn("last_seen_at", now_ts)
        .withColumn("is_active", F.lit(True))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
    )

    # One row per posting: keep the most recently posted record for each job_id.
    keep = Window.partitionBy("job_id").orderBy(F.col("posted_at").desc_nulls_last())
    silver = (
        typed.withColumn("_rn", F.row_number().over(keep))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "job_id", "source", "source_job_id", "board_token", "company", "title",
            "location", "is_remote", "apply_url", "posted_at", "dedup_hash",
            "first_seen_at", "last_seen_at", "is_active", "description", "snapshot_date",
        )
    )

    # Data Quality gate: assert Silver invariants before publishing (set --dq_enforce false to
    # log-only instead of failing the job).
    enforce = optional_arg("dq_enforce", "true").strip().lower() not in ("false", "0", "no")
    print(f"[bronze_to_silver] running data quality on Silver (enforce={enforce})")
    run_data_quality(glue, silver, bucket, snapshot_date, enforce=enforce)

    out_path = f"s3://{bucket}/silver/jobs/"
    print(f"[bronze_to_silver] writing Parquet -> {out_path} (partition snapshot_date={snapshot_date})")
    (
        silver.write.mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(out_path)
    )

    job.commit()
    print("[bronze_to_silver] done")


if __name__ == "__main__":
    main()
