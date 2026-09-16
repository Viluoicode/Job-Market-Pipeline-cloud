"""Glue (PySpark) — Bronze -> Silver.

Reads only the current ingestion manifest's Bronze objects, validates their freshness and
coverage, and reconciles posting lifecycle with the previous committed Silver run. Publishes
typed, deduped Parquet to ``s3://<LAKE_BUCKET>/silver/runs/<run_id>/jobs/`` after enforced DQ.

This is the cloud/Spark equivalent of SkillRadar's Bronze->Silver step:
  * ``job_id``     = SHA-256(source | board_token | source_job_id)              (port of dedup.make_job_id)
  * ``dedup_hash`` = SHA-256(norm(company) | norm(title) | norm(location)).upper()
                                                                  (port of dedup.compute_dedup_hash)
  * ``norm`` lower-cases, strips punctuation to spaces, collapses whitespace
                                                                  (port of text.normalize_for_key)

The type/key/dedup logic lives in an importable, Glue-free function (``build_silver``) so
``tests/`` can exercise it on a local SparkSession without AWS. Glue-only imports (GlueContext,
EvaluateDataQuality, ...) are done lazily inside the functions that need them, so importing this
module for testing needs only ``pyspark``.

Args (Glue job parameters):
  --JOB_NAME       (provided by Glue)
  --LAKE_BUCKET    data lake bucket name
  --snapshot_date  required: execution UTC date
  --run_id         required: committed ingestion run ID
"""

import json
import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pipeline_contract import read_json, silver_path, timestamp, validate_manifest


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


def run_data_quality(glue, df, bucket, snapshot_date, enforce=True, run_id=None):
    """Evaluate DQ_RULESET against the Silver DataFrame.

    Publishes results to the Glue Data Quality console + CloudWatch, persists the per-rule
    outcomes to ``s3://<bucket>/quality/silver_jobs/snapshot_date=<date>/`` for audit/Athena,
    and (when ``enforce``) fails the job if any rule fails — so bad data never reaches Silver.
    """
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.transforms import SelectFromCollection
    from awsgluedq.transforms import EvaluateDataQuality

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

    quality_path = f"s3://{bucket}/quality/silver_jobs/{run_id or snapshot_date}/"
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


def build_silver(raw: DataFrame, snapshot_date: str) -> DataFrame:
    """Type the raw Bronze JSON, compute job_id + dedup_hash, keep one row per job_id.

    Pure PySpark (no Glue) so it is unit-testable on a local SparkSession.
    """
    # Production always supplies fetched_at. The fallback keeps offline fixtures deterministic.
    observed_ts = parse_ts("fetched_at") if "fetched_at" in raw.columns else F.lit(snapshot_date).cast("timestamp")
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
            observed_ts.alias("observed_at"),
        )
        .where(F.col("source_job_id").isNotNull() & (F.col("source_job_id") != ""))
        .withColumn(
            "job_id",
            F.lower(F.sha2(F.concat_ws("|", F.col("source"), F.col("board_token"), F.col("source_job_id")), 256)),
        )
        .withColumn(
            "dedup_hash",
            F.upper(F.sha2(F.concat_ws("|", norm("company"), norm("title"), norm("location")), 256)),
        )
        .withColumn("first_seen_at", F.col("observed_at"))
        .withColumn("last_seen_at", F.col("observed_at"))
        .withColumn("is_active", F.lit(True))
        .withColumn("is_fresh", F.lit(True))
        .withColumn("status_reason", F.lit("observed"))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
    )

    # One row per posting: keep the most recently posted record for each job_id.
    keep = Window.partitionBy("job_id").orderBy(F.col("last_seen_at").desc(), F.col("posted_at").desc_nulls_last())
    return (
        typed.withColumn("_rn", F.row_number().over(keep))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "job_id", "source", "source_job_id", "board_token", "company", "title",
            "location", "is_remote", "apply_url", "posted_at", "dedup_hash",
            "first_seen_at", "last_seen_at", "is_active", "is_fresh", "status_reason", "description", "snapshot_date",
        )
    )


def reconcile_lifecycle(current, previous, complete_boards, observed_at,
                        stale_after_days=7, freshness_hours=26):
    """Carry history forward without treating failed/truncated feeds as closed postings."""
    if stale_after_days < 1 or freshness_hours <= 0:
        raise ValueError("Lifecycle and freshness windows must be positive")
    if previous is None:
        return current
    old_seen = previous.select("job_id", F.col("first_seen_at").alias("old_first_seen"))
    observed = (current.join(old_seen, "job_id", "left")
                .withColumn("first_seen_at", F.coalesce("old_first_seen", "first_seen_at"))
                .drop("old_first_seen"))
    missing = previous.join(current.select("job_id"), "job_id", "left_anti")
    boards = current.sparkSession.createDataFrame(
        list(complete_boards), "source string, board_token string"
    ).withColumn("board_complete", F.lit(True))
    missing = missing.join(F.broadcast(boards), ["source", "board_token"], "left")
    clock = F.lit(observed_at).cast("timestamp")
    too_old = F.col("last_seen_at") < clock - F.expr(f"INTERVAL {int(stale_after_days)} DAYS")
    missing = (missing
        .withColumn("status_reason", F.when(~F.col("is_active"), F.col("status_reason"))
                    .when(F.col("board_complete"), "absent_from_complete_board")
                    .when(too_old, "stale_observation").otherwise("board_unverified"))
        .withColumn("is_active", F.col("is_active") & ~F.coalesce("board_complete", F.lit(False)) & ~too_old)
        .withColumn("is_fresh", F.col("last_seen_at") >= clock - F.expr(f"INTERVAL {int(freshness_hours)} HOURS"))
        .withColumn("snapshot_date", F.to_date(clock))
        .drop("board_complete"))
    return observed.unionByName(missing)


def validate_observations(raw, manifest):
    """Check every board count and observation identity before normalization can drop rows."""
    invalid = (F.col("source_job_id").isNull() | (F.trim(F.col("source_job_id")) == "") |
               F.col("run_id").isNull() | (F.col("run_id") != manifest["run_id"]) |
               parse_ts("fetched_at").isNull())
    if raw.where(invalid).limit(1).count():
        raise ValueError("Invalid job identity or observation metadata in Bronze")
    counts = raw.groupBy("source", "board_token", "fetched_at").count().collect()
    actual = {(r.source, r.board_token, r.fetched_at): r['count'] for r in counts}
    expected = {(b["source"], b["board_token"], b["fetched_at"]): b["row_count"]
                for b in manifest["boards"] if b["status"] == "SUCCEEDED" and b["row_count"]}
    if actual != expected:
        raise ValueError("Bronze board counts/timestamps differ from the ingestion manifest")


def main() -> None:
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "LAKE_BUCKET", "run_id", "snapshot_date"])
    bucket, run_id, snapshot_date = args["LAKE_BUCKET"], args["run_id"], args["snapshot_date"]
    s3 = boto3.client("s3")
    manifest = read_json(s3, bucket, f"control/ingestion/run_id={run_id}/manifest.json")
    keys, complete_boards = validate_manifest(manifest, run_id, snapshot_date,
        max_age_hours=float(optional_arg("max_ingestion_age_hours", "24")))
    previous_state = read_json(s3, bucket, "state/silver/latest.json", optional=True)
    if previous_state and previous_state["run_id"] != run_id and (
            timestamp(previous_state["observed_at"]) >= timestamp(manifest["completed_at"])):
        raise ValueError("Cannot overwrite lifecycle state with an older ingestion run")

    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)
    if previous_state and previous_state["run_id"] == run_id:
        # A committed run is immutable. Redrive may safely resume Gold without rewriting it.
        print(f"[silver] run {run_id} already committed")
        job.commit()
        return

    from pyspark.sql.types import BooleanType, StringType, StructField, StructType
    columns = ["source", "board_token", "source_job_id", "company", "title", "location",
               "remote", "description", "apply_url", "posted_at", "run_id", "fetched_at"]
    schema = StructType([StructField(c, BooleanType() if c == "remote" else StringType()) for c in columns])
    raw = spark.read.schema(schema).option("mode", "FAILFAST").json([f"s3://{bucket}/{key}" for key in keys]).cache()
    validate_observations(raw, manifest)
    current = build_silver(raw, snapshot_date)
    # Legacy July snapshots have synthetic last_seen timestamps: they are not lifecycle evidence.
    previous = spark.read.parquet(silver_path(bucket, previous_state["run_id"])) if previous_state else None
    silver = reconcile_lifecycle(current, previous, complete_boards, manifest["completed_at"],
        stale_after_days=int(optional_arg("stale_after_days", "7")),
        freshness_hours=int(optional_arg("freshness_hours", "26")))
    silver = silver.withColumn("snapshot_date", F.lit(snapshot_date).cast("date")).cache()
    run_data_quality(glue, silver, bucket, snapshot_date, enforce=True, run_id=run_id)
    silver.write.mode("overwrite").parquet(silver_path(bucket, run_id))
    # The pipeline-wide DynamoDB lock serializes this pointer; it has no S3 expiration rule.
    state = {"version": 1, "run_id": run_id, "snapshot_date": snapshot_date,
             "observed_at": manifest["completed_at"], "row_count": silver.count()}
    s3.put_object(Bucket=bucket, Key="state/silver/latest.json",
                  Body=json.dumps(state).encode("utf-8"), ContentType="application/json")
    job.commit()
    print(f"[silver] committed {state}")


if __name__ == "__main__":
    main()
