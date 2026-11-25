"""Glue (PySpark) — Silver -> Gold.

Reads typed Silver Parquet for one ``snapshot_date`` and writes three Gold marts as Apache Iceberg
tables (database ``--iceberg_database``, data under ``s3://<lake>/gold_iceberg/``):

  * ``fact_job_posting`` — one row per ACTIVE posting, deduplicated cross-source by content
    hash (keep a single representative per ``dedup_hash``). ``posting_count = 1`` is the additive
    measure. Mirrors SkillRadar's ``fact_job_posting`` dbt model.
  * ``demand_by_role``   — per target role, the count of DISTINCT deduped postings whose
    title matches the role's patterns, for this snapshot. Mirrors ``int_posting_roles`` +
    role-level demand (first matching role family by priority, one role per posting).
  * ``role_opportunity`` — the DECISION mart: per role, demand rank + remote share + the
    single top-hiring company, so the question "which role should I learn / apply for?" has one
    table to answer it. Enriches ``demand_by_role`` with the columns a candidate actually decides on.

Each table is partitioned by ``snapshot_date`` and each day is replaced in one atomic commit, so a
reader sees the previous version or the new one, never half a day. The job records the three new
snapshot ids in ``state/gold_iceberg/run_id=<run>.json``; Step Functions copies them into the
completion marker, and readers query that exact version. The schema comes from this code: there is
no crawler. (Until 2026-11 the marts were plain Parquet under ``gold/``; those files are kept.)

The transform logic lives in importable, Glue-free functions (``build_fact``,
``build_demand_by_role``, ``build_role_opportunity``) so ``tests/`` can exercise them on a local
SparkSession without AWS. ``main()`` wires them to Glue I/O.

Args (Glue job parameters):
  --JOB_NAME       (provided by Glue)
  --LAKE_BUCKET    data lake bucket name
  --snapshot_date  required execution UTC date
  --run_id         required committed Silver run ID
  --iceberg_database  Glue database of the Gold Iceberg tables
  --snapshot_retention_days  expire Iceberg snapshots older than this (never the pinned ones)
"""

import json
import sys
from datetime import datetime, timedelta, timezone

from pipeline_contract import read_json, silver_path, validate_manifest
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Role taxonomy: each posting gets AT MOST ONE role family, the first one (top to bottom) with a
# regex matching its lower-cased title; titles matching ROLE_EXCLUDE get none. Families are ordered
# specific -> general, and a family may appear twice: the first entry matches the role written as
# the job ("Backend Engineer"), the later one the role written as a qualifier
# ("Software Engineer, Data Platform"). Regexes are valid in both Python re and Java (Spark rlike).
# Calibrated on 8,103 live postings from the 36 configured boards (2026-11-20).
# dashboard/model.py keeps an identical copy (enforced by tests/test_dashboard.py).
ROLE_EXCLUDE = [
    r"\b(?:product|program|project|technical program|account|marketing|partner|delivery) manager\b",
    r"\b(?:recruit\w*|sourcer|assistant|counsel|account executive|account director|account manager)\b",
    r"\b(?:market|ux|user|product|qualitative|design|experience) researcher\b",
]
DEFAULT_ROLES = [
    ("Forward Deployed / Solutions", [
        r"\bforward[\- ]deployed\b", r"\bfde\b", r"\bdeployment strategist\b",
        r"\bsolutions? (?:engineer\w*|architect|consultant)\b",
        r"\b(?:sales|presales|pre-sales|customer|field|partner|value) engineer\w*\b",
        r"\bimplementation (?:engineer|consultant)\b", r"\bdeveloper (?:advocate|relations)\b",
    ]),
    ("Engineering Manager", [
        r"\bengineering manager\b", r"\bmanager,? (?:of )?(?:software )?engineering\b",
        r"\b(?:head|director|vp) of engineering\b",
    ]),
    ("Research Scientist / Engineer", [r"\bresearch (?:engineer|scientist)\b", r"\bresearcher\b"]),
    ("ML / AI Engineer", [
        r"\bmachine learning (?:engineer|systems engineer)\b", r"\bml (?:engineer|ops)\b", r"\bmlops\b",
        r"\b(?:applied )?ai (?:engineer|software engineer)\b", r"\bllm engineer\b",
    ]),
    ("Data Scientist", [r"\bdata scien(?:tist|ce)\b", r"\b(?:machine learning|applied) scientist\b"]),
    ("Data Engineer", [r"\bdata engineer(?:ing)?\b", r"\betl (?:engineer|developer)\b", r"\banalytics engineer\b"]),
    ("Data Analyst / BI", [
        r"\bdata analyst\b", r"\b(?:business intelligence|bi) (?:analyst|engineer|developer)\b", r"\bproduct analyst\b",
    ]),
    ("Security Engineer", [
        r"\bsecurity (?:engineer|software engineer|operations engineer|systems engineer)\b",
        r"\b(?:application|product|cloud|infrastructure|offensive) security engineer\b",
        r"\bdetection (?:and response )?engineer\b",
    ]),
    ("DevOps / SRE / Platform", [
        r"\bdevops\b", r"\bsite reliability\b", r"\bsre\b",
        r"\b(?:platform|infrastructure|cloud|kubernetes|production) engineer\b",
    ]),
    ("Mobile Engineer", [r"\b(?:mobile|ios|android)\b"]),
    ("Full Stack Engineer", [r"\bfull[\- ]?stack\b"]),
    ("Frontend Engineer", [r"\bfront[\- ]?end\b", r"\bui engineer\b", r"\bweb (?:engineer|developer)\b"]),
    ("Backend Engineer", [
        r"\bback[\- ]?end\b", r"\bapi engineer\b", r"\bdistributed systems\b",
        r"\b(?:java|golang|go|python|scala|rust|c\+\+) (?:software )?(?:engineer|developer)\b",
        r"\.net (?:software )?(?:engineer|developer)\b",
    ]),
    # Second pass: the specialisation written as a qualifier of a generic engineering title.
    ("Data Engineer", [
        r"(?:engineer|engineering|developer|entwickler)\b.*\b(?:data (?:platform|infrastructure|pipelines?)|big data)\b",  # noqa: E501
    ]),
    ("ML / AI Engineer", [
        r"(?:engineer|engineering|developer|entwickler)\b.*\b(?:machine learning|ml infrastructure)\b",
    ]),
    ("Security Engineer", [r"(?:engineer|engineering|developer|entwickler)\b.*\bsecurity\b"]),
    ("DevOps / SRE / Platform", [
        r"(?:engineer|engineering|developer|entwickler)\b.*\b(?:infrastructure|infra|observability|ci/cd|developer productivity|developer infrastructure|reliability)\b",  # noqa: E501
    ]),
    ("Technical Support Engineer", [r"\bsupport engineer\b", r"\btechnical (?:support|services) engineer\b"]),
    ("Software Engineer (General)", [
        r"\bsoftware (?:engineer|developer|engineering)\b", r"\bsoftwareentwickler\b",
        r"\bmember of technical staff\b", r"\bfounding engineer\b", r"^(?:senior |staff |principal |lead )?engineer\b",
    ]),
]
# (priority, role, regex): lower priority wins; exclusions come first with role = NULL.
ROLE_PATTERNS = [(0, None, regex) for regex in ROLE_EXCLUDE] + [
    (priority, role, regex)
    for priority, (role, regexes) in enumerate(DEFAULT_ROLES, start=1)
    for regex in regexes
]
ROLE_FAMILIES = list(dict.fromkeys(role for role, _ in DEFAULT_ROLES))


def make_roles_df(spark: SparkSession) -> DataFrame:
    """The (priority, role, regex) lookup as a small DataFrame, broadcast-joined to postings."""
    return spark.createDataFrame(ROLE_PATTERNS, schema="priority int, role string, regex string")


def build_fact(silver: DataFrame) -> DataFrame:
    """fact_job_posting: keep one representative per content hash (cross-source dedup) + enrich.

    Representative = the copy observed most recently (a board that failed today keeps an older
    last_seen_at), then the one that reports posted_at, then the lowest job_id so reruns are
    deterministic. Same precedence as the per-job_id dedup in bronze_to_silver.build_silver.
    """
    keep = Window.partitionBy("dedup_hash").orderBy(
        F.col("last_seen_at").desc(), F.col("posted_at").desc_nulls_last(), F.col("job_id"),
    )
    return (
        silver.where(F.col("is_active") & F.col("is_fresh"))
        .withColumn("_rn", F.row_number().over(keep))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("title_lower", F.lower(F.col("title")))
        .withColumn("company_key", F.md5(F.coalesce(F.col("company"), F.lit(""))))
        .withColumn("posted_date_key", F.to_date(F.coalesce(F.col("posted_at"), F.col("first_seen_at"))))
        .withColumn("first_seen_date_key", F.to_date(F.col("first_seen_at")))
        .withColumn("posting_count", F.lit(1))
        .select(
            "job_id", "dedup_hash", "company_key", "posted_date_key", "first_seen_date_key",
            "source", "board_token", "company", "title", "title_lower", "location",
            "is_remote", "apply_url", "posted_at", "first_seen_at", "last_seen_at",
            "posting_count", "snapshot_date",
        )
    )


def _role_bridge(fact: DataFrame, roles_df: DataFrame) -> DataFrame:
    """One row per classified posting: (job_id, role) for its highest-priority matching family.

    A title matching an exclusion (priority 0, role NULL) or no regex at all gets no row. Carries
    is_remote + company so downstream marts can measure them. Each posting counts in one role only,
    so role counts sum to the number of classified postings.
    """
    first_match = Window.partitionBy("job_id").orderBy("priority")
    return (
        fact.select("job_id", "title_lower", "is_remote", "company")
        .join(F.broadcast(roles_df), F.expr("title_lower rlike regex"))
        .withColumn("_rn", F.row_number().over(first_match))
        .where((F.col("_rn") == 1) & F.col("role").isNotNull())
        .select("job_id", "role", "is_remote", "company")
    )


def build_demand_by_role(fact: DataFrame, roles_df: DataFrame, snapshot_date: str) -> DataFrame:
    """Per role, the count of DISTINCT deduped postings whose title matches the role."""
    return (
        _role_bridge(fact, roles_df)
        .groupBy("role")
        .agg(F.countDistinct("job_id").alias("job_count"))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .orderBy(F.col("job_count").desc())
    )


def build_role_opportunity(fact: DataFrame, roles_df: DataFrame, snapshot_date: str) -> DataFrame:
    """Decision mart: per role, demand rank + remote share + the single top-hiring company.

    is_remote is tri-state. remote_pct = remote / ALL postings of the role (a lower bound: unknown
    is not counted as remote), and remote_unknown_pct shows how much of the role has no signal.
    Dividing by known postings only would overstate remote work: Greenhouse exposes the flag
    mainly through "Remote" in the location text, so its known postings skew remote.
    """
    bridge = _role_bridge(fact, roles_df)

    demand = bridge.groupBy("role").agg(
        F.countDistinct("job_id").alias("job_count"),
        F.count(F.when(F.col("is_remote"), 1)).alias("remote_count"),
        F.count(F.when(F.col("is_remote").isNull(), 1)).alias("remote_unknown_count"),
    )

    # Top-hiring company per role: rank companies by distinct postings, keep #1 (name breaks ties).
    per_company = bridge.groupBy("role", "company").agg(F.countDistinct("job_id").alias("company_count"))
    top_win = Window.partitionBy("role").orderBy(F.col("company_count").desc(), F.col("company").asc())
    top_company = (
        per_company.withColumn("_rn", F.row_number().over(top_win))
        .where(F.col("_rn") == 1)
        .select("role", F.col("company").alias("top_company"), F.col("company_count").alias("top_company_count"))
    )

    rank_win = Window.orderBy(F.col("job_count").desc())
    return (
        demand.join(top_company, "role", "left")
        .withColumn("remote_pct", F.round(100.0 * F.col("remote_count") / F.col("job_count"), 1))
        .withColumn("remote_unknown_pct", F.round(100.0 * F.col("remote_unknown_count") / F.col("job_count"), 1))
        .withColumn("demand_rank", F.row_number().over(rank_win))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .select(
            "demand_rank", "role", "job_count", "remote_count", "remote_pct",
            "remote_unknown_count", "remote_unknown_pct",
            "top_company", "top_company_count", "snapshot_date",
        )
        .orderBy("demand_rank")
    )


ICEBERG_CATALOG = "glue_catalog"   # Spark catalog name configured by the Glue job's --conf

# Each commit writes a new metadata.json. Keep only the newest ones; snapshot expiry below cleans
# up manifests and data files. 30 matches the snapshot retention default.
TABLE_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "30",
}


def write_iceberg(df: DataFrame, table: str, snapshot_date: str):
    """Replace one ``snapshot_date`` of an Iceberg table in a single atomic commit.

    The first write creates the table (schema from the DataFrame, identity partition on
    snapshot_date). Later writes use ``overwrite(<this day>)``, not ``overwritePartitions()``:
    dynamic overwrite would keep the previous rows of this day when a rerun yields an empty mart.
    Readers see the previous snapshot or the new one, never a half-written day. Returns the new
    snapshot id (None only if a first write of an empty mart created no snapshot).
    """
    spark = df.sparkSession
    namespace, name = table.rsplit(".", 1)
    existing = {row.tableName for row in spark.sql(f"SHOW TABLES IN {namespace}").collect()}
    if name not in existing:
        writer = df.writeTo(table).using("iceberg").partitionedBy(F.col("snapshot_date"))
        for key, value in TABLE_PROPERTIES.items():
            writer = writer.tableProperty(key, value)
        writer.create()
    else:
        condition = F.col("snapshot_date") == F.lit(snapshot_date).cast("date")
        # Call the JVM writer with the JVM column. PySpark 3.3 (Glue 4.0) passes the Python
        # Column to py4j unconverted and fails with "Column is not iterable"; 3.4+ converts it.
        df.writeTo(table)._jwriter.overwrite(condition._jc)
    latest = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return latest["snapshot_id"] if latest else None


def publish_iceberg(marts, *, run_id: str, snapshot_date: str, writer=write_iceberg) -> dict:
    """Commit each (table, DataFrame) to Iceberg and return the run's version record.

    Any failure raises: Gold is the published contract, so the run must fail, release its lock and
    alarm rather than publish an incomplete set. Snapshot ids are stored as text because they exceed
    2**53 and would lose digits in any JSON reader that parses numbers as floats.
    """
    record = {"run_id": run_id, "snapshot_date": snapshot_date, "status": "SUCCEEDED", "tables": {}}
    for table, df in marts:
        name = table.rsplit(".", 1)[-1]
        rows = df.count()
        snapshot_id = writer(df, table, snapshot_date)
        if snapshot_id is None:
            raise ValueError(f"{name}: the write committed no snapshot")
        record["tables"][name] = {"snapshot_id": str(snapshot_id), "rows": rows}
    return record


# ---- table maintenance ------------------------------------------------------------------------
# Every nightly commit keeps the files it replaced, because older snapshots still reference them.
# Expiring snapshots past a retention window bounds that growth. Readers are pinned to the snapshot
# ids in the completion marker, so expiry never reaches back past those, however old they are.

def snapshot_expiry_cutoff(now: datetime, retention_days: int, pinned_committed_at: datetime = None) -> datetime:
    """Snapshots committed before this instant may be expired."""
    cutoff = now - timedelta(days=retention_days)
    if pinned_committed_at is not None and pinned_committed_at < cutoff:
        return pinned_committed_at
    return cutoff


def pinned_snapshots(marker) -> dict:
    """Table name -> snapshot id that readers query, from the completion marker (if any)."""
    tables = (marker or {}).get("iceberg_snapshots") or {}
    return {name: str(entry["snapshot_id"]) for name, entry in tables.items()}


def snapshot_committed_at(spark: SparkSession, table: str, snapshot_id: str):
    """Commit time (UTC) of one snapshot, or None if the table no longer has it."""
    row = spark.sql(f"SELECT unix_millis(committed_at) AS ms FROM {table}.snapshots "
                    f"WHERE snapshot_id = {int(snapshot_id)}").first()
    return datetime.fromtimestamp(row.ms / 1000, tz=timezone.utc) if row else None


def ensure_table_properties(spark: SparkSession, table: str) -> None:
    """Set TABLE_PROPERTIES on a table created before they existed; no commit when already set."""
    current = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    missing = {k: v for k, v in TABLE_PROPERTIES.items() if current.get(k) != v}
    if missing:
        pairs = ", ".join(f"'{k}' = '{v}'" for k, v in missing.items())
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({pairs})")


def expire_snapshots(spark: SparkSession, table: str, older_than: datetime) -> dict:
    """Expire snapshots committed before ``older_than``, always keeping the newest one.

    Iceberg deletes the data and manifest files that only expired snapshots used. Returns the
    procedure's counts of deleted files.
    """
    ensure_table_properties(spark, table)
    catalog, identifier = table.split(".", 1)
    stamp = older_than.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # The session time zone is UTC (set in main and in the tests), so the literal is UTC.
    row = spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{identifier}', "
                    f"older_than => TIMESTAMP '{stamp}', retain_last => 1)").first()
    return {key: int(value) for key, value in row.asDict().items()}


def maintain_iceberg(spark, tables, *, run_id: str, now: datetime, retention_days: int, pinned: dict,
                     expire=expire_snapshots, committed_at=snapshot_committed_at) -> dict:
    """Expire old snapshots of each table and return a record; never raises.

    Housekeeping must not fail a run whose data is already committed, so each table's error is
    recorded instead. A table whose pinned snapshot cannot be dated is left alone: expiring it by
    age alone could delete the version the dashboard reads.
    """
    record = {"run_id": run_id, "retention_days": retention_days, "status": "SUCCEEDED", "tables": {}}
    for table in tables:
        name = table.rsplit(".", 1)[-1]
        try:
            pin = pinned.get(name)
            pinned_at = committed_at(spark, table, pin) if pin else None
            if pin and pinned_at is None:
                raise ValueError(f"pinned snapshot {pin} not found in {name}; nothing expired")
            cutoff = snapshot_expiry_cutoff(now, retention_days, pinned_at)
            result = expire(spark, table, cutoff)
            record["tables"][name] = {"older_than": cutoff.isoformat(), "pinned_snapshot_id": pin, **result}
        except Exception as exc:  # noqa: BLE001 - recorded, see docstring
            record["status"] = "FAILED"
            record["tables"][name] = {"error_type": type(exc).__name__, "error": str(exc)[:1000]}
    return record


def main() -> None:
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "LAKE_BUCKET", "run_id", "snapshot_date", "iceberg_database",
                                         "snapshot_retention_days"])
    snapshot_date, run_id = args["snapshot_date"], args["run_id"]
    bucket = args["LAKE_BUCKET"]
    s3 = boto3.client("s3")
    manifest = read_json(s3, bucket, f"control/ingestion/run_id={run_id}/manifest.json")
    validate_manifest(manifest, run_id, snapshot_date)
    state = read_json(s3, bucket, "state/silver/latest.json")
    if state["run_id"] != run_id or state["snapshot_date"] != snapshot_date:
        raise ValueError("Gold requires Silver committed for this exact run")

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    input_path = silver_path(bucket, run_id)
    print(f"[silver_to_gold] reading {input_path} (snapshot_date={snapshot_date})")
    silver = spark.read.parquet(input_path)

    # ---- fact_job_posting ----------------------------------------------------------------------
    fact = build_fact(silver).cache()
    print(f"[silver_to_gold] fact_job_posting rows (deduped): {fact.count()}")
    roles_df = make_roles_df(spark)

    # ---- demand_by_role ------------------------------------------------------------------------
    demand = build_demand_by_role(fact, roles_df, snapshot_date)

    # ---- role_opportunity (decision mart) ------------------------------------------------------
    opportunity = build_role_opportunity(fact, roles_df, snapshot_date)
    for row in opportunity.collect():
        print(
            f"    #{row['demand_rank']} {row['role']:<28} demand={row['job_count']:<5} "
            f"remote={row['remote_pct']}% (unknown {row['remote_unknown_pct']}%)  "
            f"top={row['top_company']} ({row['top_company_count']})"
        )

    # ---- publish: one atomic Iceberg commit per table, then the version record --------------
    marts = [(f"{ICEBERG_CATALOG}.{args['iceberg_database']}.{name}", df) for name, df in
             (("fact_job_posting", fact), ("demand_by_role", demand), ("role_opportunity", opportunity))]
    record = publish_iceberg(marts, run_id=run_id, snapshot_date=snapshot_date)
    # Written last: Step Functions publishes completion only from this record.
    s3.put_object(Bucket=bucket, Key=f"state/gold_iceberg/run_id={run_id}.json",
                  Body=json.dumps(record).encode("utf-8"), ContentType="application/json")
    print(f"[silver_to_gold] committed Iceberg versions: {json.dumps(record)}")

    # ---- housekeeping: expire old snapshots, never past what readers are pinned to -------------
    # Runs under the same writer lock as the commits above, so no other write can interleave.
    try:
        pinned = pinned_snapshots(read_json(s3, bucket, "state/pipeline/latest.json", optional=True))
        maintenance = maintain_iceberg(spark, [table for table, _ in marts], run_id=run_id,
                                       now=datetime.now(timezone.utc),
                                       retention_days=int(args["snapshot_retention_days"]), pinned=pinned)
    except Exception as exc:  # noqa: BLE001 - e.g. the marker is unreadable: expire nothing
        maintenance = {"run_id": run_id, "status": "FAILED", "error_type": type(exc).__name__,
                       "error": str(exc)[:1000]}
    s3.put_object(Bucket=bucket, Key=f"state/gold_iceberg/maintenance/run_id={run_id}.json",
                  Body=json.dumps(maintenance).encode("utf-8"), ContentType="application/json")
    print(f"[silver_to_gold] snapshot expiry: {json.dumps(maintenance)}")

    job.commit()
    print("[silver_to_gold] done")


if __name__ == "__main__":
    main()
