"""Unit tests for the Silver -> Gold transforms (glue/jobs/silver_to_gold.py).

Builds a tiny Silver frame via ``build_silver`` (so the fixtures are realistic), then asserts:
  * fact_job_posting  — cross-source content dedup keeps one row per dedup_hash, choosing the
                        most recently observed copy (then the one with posted_at) deliberately.
  * demand_by_role    — single-label role classification counts distinct postings correctly.
  * role_opportunity  — the decision mart's demand/remote/top-company columns are right.

Rows are tuples in ``conftest.BRONZE_COLUMNS`` order, built via the ``bronze`` fixture.
"""

import hashlib

import pytest
from pyspark.sql import functions as F

import bronze_to_silver as b2s
import silver_to_gold as s2g

# Two Data Engineer roles at Databricks + one at Acme; one ML role; one DevOps role posted on two
# boards (same content) to exercise cross-source dedup.
DATASET = [
    ("greenhouse", "databricks", "1", "Databricks", "Senior Data Engineer", "Remote", True, "u", "2026-01-01T00:00:00+00:00", "a"),
    ("greenhouse", "databricks", "2", "Databricks", "Data Engineer", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "b"),
    ("lever", "acme", "3", "Acme", "Data Engineer", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "c"),
    ("greenhouse", "openai", "4", "OpenAI", "Machine Learning Engineer", "SF", True, "u", "2026-01-01T00:00:00+00:00", "d"),
    ("greenhouse", "foo", "5", "Foo", "DevOps Engineer", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "e"),
    ("lever", "foo", "6", "Foo", "devops engineer", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "f"),
]


SNAPSHOT = "2026-01-01"


@pytest.fixture(scope="module")
def fact(bronze):
    """The Gold fact table built from DATASET, cached for the whole module."""
    return s2g.build_fact(b2s.build_silver(bronze(DATASET), SNAPSHOT)).cache()


def test_build_fact_dedups_cross_source(fact):
    # 6 raw rows, but the DevOps posting on two boards collapses -> 5 unique content rows.
    assert fact.count() == 5


def _same_content_pair(bronze, posted_at_a, posted_at_b):
    """Two copies of one posting (same company/title/location -> same dedup_hash) on two boards.

    Returns (silver, lower_id, higher_id) where lower_id is the job_id the old orderBy("job_id")
    rule would keep, so each test can make that copy the WRONG one to keep.
    """
    rows = [
        ("greenhouse", "boarda", "101", "Acme", "Data Engineer", "Berlin", False, "u", posted_at_a, "x"),
        ("lever", "boardb", "202", "Acme", "Data Engineer", "Berlin", False, "u", posted_at_b, "y"),
    ]
    silver = b2s.build_silver(bronze(rows), SNAPSHOT)
    ids = sorted(hashlib.sha256(f"{src}|{board}|{sid}".encode()).hexdigest() for src, board, sid, *_ in rows)
    assert silver.select("dedup_hash").distinct().count() == 1      # precondition: true duplicates
    return silver, ids[0], ids[1]


def test_build_fact_keeps_most_recently_seen_duplicate(bronze):
    silver, lower_id, higher_id = _same_content_pair(
        bronze, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00")
    # The lower job_id copy was last observed a day earlier (e.g. its board failed today).
    silver = silver.withColumn(
        "last_seen_at",
        F.when(F.col("job_id") == lower_id, F.to_timestamp(F.lit("2025-12-31 00:00:00")))
         .otherwise(F.to_timestamp(F.lit("2026-01-01 00:00:00"))),
    )
    kept = s2g.build_fact(silver).collect()
    assert [r["job_id"] for r in kept] == [higher_id]


def test_build_fact_prefers_duplicate_with_posted_at(bronze):
    # Same last_seen_at; only the higher job_id copy reports posted_at.
    silver, lower_id, higher_id = _same_content_pair(bronze, None, None)
    silver = silver.withColumn(
        "posted_at",
        F.when(F.col("job_id") == higher_id, F.to_timestamp(F.lit("2025-12-20 00:00:00"))),
    )
    kept = s2g.build_fact(silver).collect()
    assert [r["job_id"] for r in kept] == [higher_id]
    assert kept[0]["posted_at"] is not None


def test_demand_by_role_counts(spark, fact):
    roles = s2g.make_roles_df(spark)
    demand = {r["role"]: r["job_count"] for r in s2g.build_demand_by_role(fact, roles, SNAPSHOT).collect()}
    assert demand["Data Engineer"] == 3            # "Senior Data Engineer" + two "Data Engineer"
    assert demand["ML / AI Engineer"] == 1
    assert demand["DevOps / SRE / Platform"] == 1  # deduped across the two boards
    assert "Backend Engineer" not in demand        # no matching titles -> absent


def test_role_patterns_match_whole_words_only(spark, bronze):
    # Substring matching put "html engineer" under ML ("ml engineer") and "platform engineering"
    # under DevOps ("platform engineer"); patterns must match whole words of the title.
    rows = [
        ("greenhouse", "x", "11", "X", "Senior HTML Engineer", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "g"),
        ("greenhouse", "x", "12", "X", "Customer Success Rep - Platform Engineering", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "h"),
        ("greenhouse", "x", "13", "X", "ML Engineer, Search", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "i"),
        ("greenhouse", "x", "14", "X", "Site Reliability Engineer (SRE)", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "j"),
        ("greenhouse", "x", "15", "X", "Senior Front-End Engineer", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "k"),
    ]
    fact = s2g.build_fact(b2s.build_silver(bronze(rows), SNAPSHOT))
    roles = s2g.make_roles_df(spark)
    demand = {r["role"]: r["job_count"] for r in s2g.build_demand_by_role(fact, roles, SNAPSHOT).collect()}
    assert demand == {
        "ML / AI Engineer": 1,            # only "ML Engineer, Search", not "HTML Engineer"
        "DevOps / SRE / Platform": 1,     # SRE title counted once; "Platform Engineering" excluded
        "Frontend Engineer": 1,           # hyphenated pattern still matches
    }


def test_each_posting_gets_one_role_by_priority(spark, bronze):
    rows = [
        ("greenhouse", "y", "21", "Y", "Data Engineer / Backend Developer", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "l"),
        ("greenhouse", "y", "22", "Y", "Software Engineer, Data Platform", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "m"),
        ("greenhouse", "y", "23", "Y", "Machine Learning Engineering Manager", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "n"),
        ("greenhouse", "y", "24", "Y", "Sr. Product Manager, Data Engineering", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "o"),
        ("greenhouse", "y", "25", "Y", "Software Engineer, Banking", "NYC", False, "u", "2026-01-01T00:00:00+00:00", "p"),
    ]
    fact = s2g.build_fact(b2s.build_silver(bronze(rows), SNAPSHOT))
    roles = s2g.make_roles_df(spark)
    demand = {r["role"]: r["job_count"] for r in s2g.build_demand_by_role(fact, roles, SNAPSHOT).collect()}
    assert demand == {
        "Data Engineer": 2,                 # first-listed family wins; qualifier form also counts
        "Engineering Manager": 1,           # manager outranks the ML specialty
        "Software Engineer (General)": 1,
    }                                       # product manager excluded -> 4 of 5 classified
    assert sum(demand.values()) == 4        # single label: role counts add up to postings


def test_role_opportunity_decision_columns(spark, fact):
    roles = s2g.make_roles_df(spark)
    opp = {r["role"]: r for r in s2g.build_role_opportunity(fact, roles, SNAPSHOT).collect()}
    de = opp["Data Engineer"]
    assert de["job_count"] == 3
    assert de["remote_count"] == 1                 # only "Senior Data Engineer" is remote
    assert de["remote_pct"] == 33.3
    assert de["remote_unknown_count"] == 0          # every fixture row states remote true/false
    assert de["remote_unknown_pct"] == 0.0
    assert de["top_company"] == "Databricks"       # 2 DE postings vs Acme's 1
    assert de["top_company_count"] == 2
    assert de["demand_rank"] == 1                  # DE has the highest demand


def test_role_opportunity_reports_unknown_remote_separately(spark, bronze):
    rows = [
        ("greenhouse", "z", "31", "Z", "Data Engineer", "Remote", True, "u", "2026-01-01T00:00:00+00:00", "q"),
        ("greenhouse", "z", "32", "Z", "Senior Data Engineer", "Paris", False, "u", "2026-01-01T00:00:00+00:00", "r"),
        ("greenhouse", "z", "33", "Z", "Staff Data Engineer", "Berlin", None, "u", "2026-01-01T00:00:00+00:00", "s"),
    ]
    fact = s2g.build_fact(b2s.build_silver(bronze(rows), SNAPSHOT))
    opp = s2g.build_role_opportunity(fact, s2g.make_roles_df(spark), SNAPSHOT).collect()
    de = {r["role"]: r for r in opp}["Data Engineer"]
    assert de["job_count"] == 3
    assert de["remote_count"] == 1
    assert de["remote_pct"] == 33.3                 # share of ALL postings: unknown is not remote
    assert de["remote_unknown_count"] == 1
    assert de["remote_unknown_pct"] == 33.3
