# AGENTS.md

Instructions for coding agents working in this repository. Read [README.md](README.md) for the overview and
[docs/architecture.md](docs/architecture.md) for the invariants.

## What this is

A serverless AWS data pipeline. Every night it collects job postings from 36 public job boards, lands raw JSON
in an S3 medallion lake (Bronze JSON, Silver Parquet, Gold Apache Iceberg), gates each run on data quality and
publishes Gold through Athena. Two readers depend on it: the Streamlit dashboard in `dashboard/` and
[Job Market Radar](https://github.com/Viluoicode/job-market-radar), a separate repository. All infrastructure is
Terraform and runs in AWS without a laptop.

## Layout

```text
ingestion/    Glue Python Shell job: public API connectors, source catalog, run manifest
glue/jobs/    Glue Spark jobs: Silver lifecycle and data quality, Gold marts (Iceberg), shared contract
infra/        Terraform for the whole stack (S3 backend, partial config in backend.hcl)
monitoring/   Lambdas: health monitor, lock janitor
dashboard/    Streamlit app reading the published snapshot through Athena
scripts/      Read-only checks: freshness, IAM permissions, run metrics, Iceberg parity
sql/          Athena queries
tests/        pytest: Spark transforms, ingestion, contract, Lambdas, dashboard, Iceberg writes, docs
docs/         Architecture, operations, data dictionary, security review, dated evidence
```

## Checks

Python 3.11 and Java 17 (PySpark 3.5.1 supports neither newer Python nor newer Java):

```powershell
python -m pip install -r requirements-dev.txt
python -m ruff check .
python -m pytest tests -q
terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra init -backend=false
terraform -chdir=infra validate
```

- `tests/test_iceberg_write.py` runs only when `ICEBERG_SPARK_JAR` points at an `iceberg-spark-runtime-3.5_2.12`
  jar; CI downloads one. Otherwise those tests are skipped, not passed.
- New test dependencies go into `requirements-dev.txt`; CI installs nothing else.
- CI also runs Checkov on `infra/` and fails on any new finding.

## Runtimes the code must run on

Tests run newer versions than AWS does. Write for the deployed ones:

- `ingestion/` runs on Glue Python Shell, **Python 3.9**. Ruff targets `py39`; no 3.10+ syntax.
- `glue/jobs/` runs on Glue 4.0: **PySpark 3.3, Python 3.10, Iceberg 1.0**. Avoid Spark APIs newer than 3.3.
  Iceberg writes are tested in both the current and the `spark33` API forms; keep both passing.
- `monitoring/` Lambdas run Python 3.12.

## Contracts that must not break

- **The run manifest is the commit point.** Ingestion writes every board's file first and the manifest last;
  later stages read only what the manifest lists.
- **The completion marker** `state/pipeline/latest.json` is written only after the Gold version record matches
  the run. It pins one Iceberg snapshot id per table, and readers query `FOR VERSION AS OF` those ids. A new
  Gold table also needs its `IsPresent` check in `infra/stepfunctions.tf`.
- **Downstream columns.** The Gold columns listed under "Downstream consumers" in
  [docs/data-dictionary.md](docs/data-dictionary.md) are read by Job Market Radar. Rename or remove one only
  together with a Radar change, and update the data dictionary with any Gold schema change.
- **Never guess data.** Salary comes only from structured source fields, never parsed from free text. A work
  mode, level or salary the source does not state stays NULL or "not stated".

## Security and AWS

- Never write the AWS account ID, a bucket name containing it, or an email address into a committed file.
  Use `<account-id>` in docs and evidence. `infra/terraform.tfvars` and `infra/backend.hcl` are not committed.
- Do not run `terraform apply` or change AWS resources without the maintainer's explicit go-ahead. Show the
  plan first.
- Least privilege: each stage has its own IAM role. After any IAM change, run `scripts/check_permissions.py`
  (read-only IAM simulation).
- Checkov stays at zero open findings. An accepted finding gets an inline `#checkov:skip=<id>:<reason>` and a
  line in [docs/security-review.md](docs/security-review.md).

## Docs and evidence

- Docs are UTF-8 (`tests/test_encoding.py` checks it); several are bilingual English/Vietnamese.
- Every number in README or docs comes from a dated file in `docs/evidence/`, with the account ID redacted.
  Do not state a result that has no evidence file.

## Git

The maintainer reviews, commits and pushes. Do not commit, push or open pull requests; leave changes in the
working tree and propose a commit message.
