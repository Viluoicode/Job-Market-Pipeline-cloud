"""Glue (PySpark) — Silver -> Gold.

Reads typed Silver Parquet for one ``snapshot_date`` and writes three Gold marts:

  * ``gold/fact_job_posting/`` — one row per ACTIVE posting, deduplicated cross-source by content
    hash (keep a single representative per ``dedup_hash``). ``posting_count = 1`` is the additive
    measure. Mirrors SkillRadar's ``fact_job_posting`` dbt model.
  * ``gold/demand_by_role/``   — per target role, the count of DISTINCT deduped postings whose
    title matches the role's patterns, for this snapshot. Mirrors ``int_posting_roles`` +
    role-level demand (substring match on the lower-cased title, a posting may match many roles).
  * ``gold/role_opportunity/`` — the DECISION mart: per role, demand rank + remote share + the
    single top-hiring company, so the question "which role should I learn / apply for?" has one
    table to answer it. Enriches ``demand_by_role`` with the columns a candidate actually decides on.

All three are Parquet partitioned by ``snapshot_date`` so Athena prunes and the crawler discovers
them.

The transform logic lives in importable, Glue-free functions (``build_fact``,
``build_demand_by_role``, ``build_role_opportunity``) so ``tests/`` can exercise them on a local
SparkSession without AWS. ``main()`` wires them to Glue I/O.

Args (Glue job parameters):
  --JOB_NAME       (provided by Glue)
  --LAKE_BUCKET    data lake bucket name
  --snapshot_date  optional YYYY-MM-DD; defaults to today (UTC)
"""

import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Target role families and the lower-cased title substrings that classify a posting into each.
# Port of SkillRadar domain/roles.py DEFAULT_ROLES (== transform/seeds/seed_roles.csv).
DEFAULT_ROLES = [
    ("Backend Engineer", ["backend engineer", "back-end engineer", "backend developer", "backend software engineer"]),
    ("Frontend Engineer", ["frontend engineer", "front-end engineer", "frontend developer", "ui engineer"]),
    ("Full Stack Engineer", ["full stack", "fullstack", "full-stack engineer"]),
    ("Data Engineer", ["data engineer", "etl engineer", "analytics engineer"]),
    ("Data Scientist", ["data scientist", "machine learning scientist"]),
    ("Machine Learning Engineer", ["machine learning engineer", "ml engineer", "ai engineer"]),
    ("DevOps Engineer", ["devops", "site reliability", "sre", "platform engineer", "infrastructure engineer"]),
    ("Mobile Engineer", ["mobile engineer", "ios engineer", "android engineer", "mobile developer"]),
]
ROLE_PATTERNS = [(role, pattern) for role, patterns in DEFAULT_ROLES for pattern in patterns]


def optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def make_roles_df(spark: SparkSession) -> DataFrame:
    """The (role, pattern) lookup as a small DataFrame, broadcast-joined to postings."""
    return spark.createDataFrame(ROLE_PATTERNS, schema=["role", "pattern"])


def build_fact(silver: DataFrame) -> DataFrame:
    """fact_job_posting: keep one representative per content hash (cross-source dedup) + enrich."""
    keep = Window.partitionBy("dedup_hash").orderBy("job_id")
    return (
        silver.where(F.col("is_active"))
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
    """One row per (job_id, role) — a posting whose lower-cased title contains a role pattern.

    Carries is_remote + company so downstream marts can measure them. distinct() collapses the
    duplicate rows a posting gets when its title contains several patterns for the same role.
    """
    return (
        fact.select("job_id", "title_lower", "is_remote", "company")
        .join(F.broadcast(roles_df), F.col("title_lower").contains(F.col("pattern")))
        .select("job_id", "role", "is_remote", "company")
        .distinct()
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
    """Decision mart: per role, demand rank + remote share + the single top-hiring company."""
    bridge = _role_bridge(fact, roles_df)

    demand = bridge.groupBy("role").agg(
        F.countDistinct("job_id").alias("job_count"),
        F.sum(F.col("is_remote").cast("int")).alias("remote_count"),
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
        .withColumn("demand_rank", F.row_number().over(rank_win))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .select(
            "demand_rank", "role", "job_count", "remote_count", "remote_pct",
            "top_company", "top_company_count", "snapshot_date",
        )
        .orderBy("demand_rank")
    )


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "LAKE_BUCKET"])
    snapshot_date = optional_arg("snapshot_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    bucket = args["LAKE_BUCKET"]

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    silver_path = f"s3://{bucket}/silver/jobs/"
    print(f"[silver_to_gold] reading {silver_path} (snapshot_date={snapshot_date})")
    silver = spark.read.parquet(silver_path).where(
        F.col("snapshot_date") == F.lit(snapshot_date).cast("date")
    )

    # ---- fact_job_posting ----------------------------------------------------------------------
    fact = build_fact(silver).cache()
    print(f"[silver_to_gold] fact_job_posting rows (deduped): {fact.count()}")
    fact_path = f"s3://{bucket}/gold/fact_job_posting/"
    (fact.write.mode("overwrite").partitionBy("snapshot_date").parquet(fact_path))
    print(f"[silver_to_gold] wrote {fact_path}")

    roles_df = make_roles_df(spark)

    # ---- demand_by_role ------------------------------------------------------------------------
    demand = build_demand_by_role(fact, roles_df, snapshot_date)
    demand_path = f"s3://{bucket}/gold/demand_by_role/"
    (demand.write.mode("overwrite").partitionBy("snapshot_date").parquet(demand_path))
    print(f"[silver_to_gold] wrote {demand_path}")

    # ---- role_opportunity (decision mart) ------------------------------------------------------
    opportunity = build_role_opportunity(fact, roles_df, snapshot_date)
    opp_path = f"s3://{bucket}/gold/role_opportunity/"
    (opportunity.write.mode("overwrite").partitionBy("snapshot_date").parquet(opp_path))
    print(f"[silver_to_gold] wrote {opp_path}")
    for row in opportunity.collect():
        print(
            f"    #{row['demand_rank']} {row['role']:<28} demand={row['job_count']:<5} "
            f"remote={row['remote_pct']}%  top={row['top_company']} ({row['top_company_count']})"
        )

    job.commit()
    print("[silver_to_gold] done")


if __name__ == "__main__":
    main()
