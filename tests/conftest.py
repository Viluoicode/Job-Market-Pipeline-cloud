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
