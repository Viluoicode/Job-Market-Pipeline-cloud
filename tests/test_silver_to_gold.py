"""Unit tests for the Silver -> Gold transforms (glue/jobs/silver_to_gold.py).

Builds a tiny Silver frame via ``build_silver`` (so the fixtures are realistic), then asserts:
  * fact_job_posting  — cross-source content dedup keeps one row per dedup_hash.
  * demand_by_role    — substring role classification counts distinct postings correctly.
  * role_opportunity  — the decision mart's demand/remote/top-company columns are right.

Rows are tuples in ``conftest.BRONZE_COLUMNS`` order, built via the ``bronze`` fixture.
"""

import pytest

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


def test_demand_by_role_counts(spark, fact):
    roles = s2g.make_roles_df(spark)
    demand = {r["role"]: r["job_count"] for r in s2g.build_demand_by_role(fact, roles, SNAPSHOT).collect()}
    assert demand["Data Engineer"] == 3            # "Senior Data Engineer" + two "Data Engineer"
    assert demand["Machine Learning Engineer"] == 1
    assert demand["DevOps Engineer"] == 1          # deduped across the two boards
    assert "Backend Engineer" not in demand        # no matching titles -> absent


def test_role_opportunity_decision_columns(spark, fact):
    roles = s2g.make_roles_df(spark)
    opp = {r["role"]: r for r in s2g.build_role_opportunity(fact, roles, SNAPSHOT).collect()}
    de = opp["Data Engineer"]
    assert de["job_count"] == 3
    assert de["remote_count"] == 1                 # only "Senior Data Engineer" is remote
    assert de["remote_pct"] == 33.3
    assert de["top_company"] == "Databricks"       # 2 DE postings vs Acme's 1
    assert de["top_company_count"] == 2
    assert de["demand_rank"] == 1                  # DE has the highest demand
