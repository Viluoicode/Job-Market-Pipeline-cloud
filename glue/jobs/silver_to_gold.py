"""Glue (PySpark) — Silver -> Gold.

Reads typed Silver Parquet for one ``snapshot_date`` and writes two Gold marts:

  * ``gold/fact_job_posting/`` — one row per ACTIVE posting, deduplicated cross-source by content
    hash (keep a single representative per ``dedup_hash``). ``posting_count = 1`` is the additive
    measure. Mirrors SkillRadar's ``fact_job_posting`` dbt model.
  * ``gold/demand_by_role/``   — per target role, the count of DISTINCT deduped postings whose
    title matches the role's patterns, for this snapshot. Mirrors ``int_posting_roles`` +
    role-level demand (substring match on the lower-cased title, a posting may match many roles).

Both are Parquet partitioned by ``snapshot_date`` so Athena prunes and the crawler discovers them.

Args (Glue job parameters):
  --JOB_NAME       (provided by Glue)
  --LAKE_BUCKET    data lake bucket name
  --snapshot_date  optional YYYY-MM-DD; defaults to today (UTC)
"""

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
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

    silver_path = f"s3://{bucket}/silver/jobs/"
    print(f"[silver_to_gold] reading {silver_path} (snapshot_date={snapshot_date})")
    silver = spark.read.parquet(silver_path).where(
        F.col("snapshot_date") == F.lit(snapshot_date).cast("date")
    )

    # ---- fact_job_posting: keep one representative per content hash -----------------------------
    keep = Window.partitionBy("dedup_hash").orderBy("job_id")
    fact = (
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
    ).cache()

    fact_count = fact.count()
    print(f"[silver_to_gold] fact_job_posting rows (deduped): {fact_count}")

    fact_path = f"s3://{bucket}/gold/fact_job_posting/"
    (fact.write.mode("overwrite").partitionBy("snapshot_date").parquet(fact_path))
    print(f"[silver_to_gold] wrote {fact_path}")

    # ---- demand_by_role: posting<->role bridge, then count distinct postings per role -----------
    roles_df = spark.createDataFrame(ROLE_PATTERNS, schema=["role", "pattern"])
    bridge = (
        fact.select("job_id", "title_lower")
        .join(F.broadcast(roles_df), F.col("title_lower").contains(F.col("pattern")))
        .select("job_id", "role")
        .distinct()
    )
    demand = (
        bridge.groupBy("role")
        .agg(F.countDistinct("job_id").alias("job_count"))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .orderBy(F.col("job_count").desc())
    )

    demand_path = f"s3://{bucket}/gold/demand_by_role/"
    (demand.write.mode("overwrite").partitionBy("snapshot_date").parquet(demand_path))
    print(f"[silver_to_gold] wrote {demand_path}")
    for row in demand.collect():
        print(f"    {row['role']:<28} {row['job_count']}")

    job.commit()
    print("[silver_to_gold] done")


if __name__ == "__main__":
    main()
