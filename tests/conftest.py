"""Pytest fixtures for the PySpark transform tests.

The Glue job scripts live in ``glue/jobs/`` (uploaded to S3 as standalone scripts, not a package),
so we add that directory to ``sys.path`` to import their Glue-free transform functions. A single
local[1] SparkSession is shared across the test session.
"""

import sys
from pathlib import Path

import pytest

JOBS_DIR = Path(__file__).resolve().parent.parent / "glue" / "jobs"
sys.path.insert(0, str(JOBS_DIR))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("jobmarket-aws-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# The Bronze record shape land_to_bronze.py writes (minus raw_json, which Silver drops). Declared
# explicitly rather than inferred: a fixture column that is entirely NULL — e.g. an absent `remote`
# flag — gives Spark nothing to infer from (CANNOT_DETERMINE_TYPE).
BRONZE_COLUMNS = [
    "source", "board_token", "source_job_id", "company", "title",
    "location", "remote", "apply_url", "posted_at", "description",
]


@pytest.fixture(scope="session")
def bronze(spark):
    """Factory: rows (tuples in BRONZE_COLUMNS order) -> a raw Bronze DataFrame."""
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    schema = StructType([
        StructField(name, BooleanType() if name == "remote" else StringType(), nullable=True)
        for name in BRONZE_COLUMNS
    ])

    def _make(rows):
        return spark.createDataFrame(list(rows), schema=schema)

    return _make
