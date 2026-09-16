# Job Market AWS Pipeline — cloud-native Medallion

A job-market data pipeline on AWS: public ATS feeds → **S3 Bronze** → **Glue/PySpark Silver**
→ **Gold marts** → **Glue Data Catalog / Athena**, provisioned with **Terraform**.
Sibling project to [SkillRadar](https://github.com/Viluoicode/SkillRadar): the same domain on a
managed cloud data stack instead of local DuckDB/dbt.

**Current focus:** automatic ingestion, trustworthy data freshness, and posting/storage lifecycle.
Dashboard development is deferred.

## Deployment status — scheduled run verified 2026-09-16

The ingestion/lifecycle upgrade is deployed in `ap-southeast-1`. The first automatic run
started at **01:00:16 on 2026-09-16 (UTC+7)** and completed at **01:07:29**, with all three Glue
jobs, seven Data Quality rules and the crawler successful. Its input explicitly identifies
`aws.events` / `Scheduled Event` from `jobmarket-aws-pipeline-schedule`.

The run fetched **7,575 records from all 36 boards**, committed **7,731 Silver lifecycle rows**,
and published **7,583 active/fresh, content-unique Gold postings across 309 companies**.
Silver retains prior observations; eligible recent observations can remain in Gold, so these
counts are not a simple funnel of the latest fetch alone. Arbeitnow remains incomplete because
of its two-page cap. The health check passed when the evidence was captured on 2026-09-16.

EventBridge is **ENABLED**, daily at **01:00 UTC+7** (`18:00 UTC`). Collection and processing
run entirely on AWS, independent of a local computer or Codex. `snapshot_date=2026-09-15` is
correct for this run because partition dates use the execution's UTC date. Same-day UTC reruns
replace that day's Gold partition while retaining separate Silver runs.

See the [operations runbook](docs/operations.md), [scheduled-run evidence](docs/evidence/scheduled-run-20260916.json)
and [Athena results](docs/sample_results.md). This proves the first scheduled run; continued
daily reliability still needs operational observation. July counts use a different methodology.

## Architecture

```text
EventBridge (daily, enabled on this deployment) or manual StartExecution
  → Step Functions acquires a DynamoDB execution lock
  → Glue Python Shell: ingest Greenhouse / Lever / Ashby / Arbeitnow
  → S3 Bronze objects + committed per-run manifest
  → Glue Spark: validate freshness, normalize, deduplicate, reconcile lifecycle, enforce DQ
  → immutable Silver run + lifecycle state pointer
  → Glue Spark: publish active/fresh Gold marts for the execution's UTC date
  → start crawler, wait and verify this crawl SUCCEEDED
  → publish completion marker, release lock
  → Athena queries (check freshness before consuming current data)
```

| Concern | Implementation |
| --- | --- |
| Collection | Glue Python Shell 3.9, 0.0625 DPU, 15-minute timeout; `httpx` + `boto3` |
| Storage | Private, encrypted S3 buckets for lake, scripts and Athena results |
| Transform | Two Glue PySpark jobs, 2× G.1X by default, 30-minute timeout each |
| Quality | Run manifest checks plus enforced AWS Glue Data Quality rules |
| Catalog/query | Glue crawler and catalog; Athena with a 10 GiB/query scan cap |
| Orchestration | Step Functions Standard, 90-minute ceiling, DynamoDB lock |
| Schedule | EventBridge, daily 18:00 UTC; ENABLED here, opt-in for new deployments |
| Infrastructure | Terraform with pinned AWS provider, CI fmt/validate |

See [architecture.md](docs/architecture.md) and the Vietnamese [learning guide](docs/LEARN.md).

## Ingestion and run identity

`ingestion/sources.json` contains 36 boards across four public APIs. Each board produces canonical
newline-delimited JSON with source, board, source ID, company, title, location, remote status,
description, application URL, source posting timestamp and raw payload. Every record also carries
the **run ID and actual fetch timestamp**.

```text
bronze/run_id=<run>/source=<source>/board=<token>/jobs.json
control/ingestion/run_id=<run>/manifest.json
```

The manifest commits only after board objects are stored. It distinguishes failed, successful
empty, complete and truncated boards. Network errors, 429 and 5xx are retried with backoff.
By default, at least 80% of boards and at least one posting are required; otherwise the pipeline
stops before Silver. Arbeitnow's two-page cap is explicitly marked incomplete if more pages exist.

Silver reads **only this manifest's objects**, checks that ingestion is at most 24 hours old, and
verifies run IDs, board counts and observation timestamps. It never re-labels accumulated Bronze
as fresh data. A committed run ID is immutable and safe to read again.

## Silver: identity, lifecycle and quality

| Field | Meaning |
| --- | --- |
| `job_id` | Lower SHA-256 of `source|board_token|source_job_id`; identity within a board |
| `dedup_hash` | Upper SHA-256 of normalized company/title/location; cross-source content dedup |
| `first_seen_at` | First real observation; preserved through edits and reactivation |
| `last_seen_at` | Latest successful fetch that actually contained the posting |
| `is_active` | False after absence from a complete board or 7 days without observation |
| `is_fresh` | Last observation within 26 hours of ingestion completion |
| `status_reason` | `observed`, `absent_from_complete_board`, `board_unverified`, `stale_observation` |

Failed or incomplete boards do not close jobs immediately and never advance `last_seen_at`.
A successful empty board can close its previous jobs. A posting that reappears becomes active
again with its original first-seen time. Within-run duplicates prefer the newest **observation**
before comparing the source's `posted_at`.

Silver writes `silver/runs/<run>/jobs/` and advances `state/silver/latest.json` only after the
data-quality gate and Parquet write succeed. DQ requires nonempty rows, unique/complete IDs,
complete content keys and remote flags, valid sources, and at least 90% company completeness.
Quality results are persisted under `quality/silver_jobs/<run>/`.

**Migration:** the first new run starts lifecycle tracking from real new observations. Legacy
Silver timestamps were synthetic, so importing them would create false history. Old daily Gold
partitions are preserved and should be treated as a different measurement methodology.

## Gold and publication

Gold includes only **active and fresh** postings, then deduplicates across sources by content.

| Mart | Grain and purpose |
| --- | --- |
| `fact_job_posting` | One content-unique posting, with company/date keys, first/last-seen timestamps and `posting_count=1` |
| `demand_by_role` | One role per snapshot; distinct posting count across eight role families |
| `role_opportunity` | One role per snapshot; rank, demand, remote share and top hiring company |

All marts retain the existing `snapshot_date` partition layout. A rerun overwrites only its
daily partition, including empty outputs. The crawler must report success for the current
execution before `state/pipeline/latest.json` is published.

Publication across the three marts is not transactional. The health check detects a newer
Silver run that has not fully published; current consumers must respect that check. Historical
queries and the original samples are in [sql/athena_analysis.sql](sql/athena_analysis.sql) and
[docs/sample_results.md](docs/sample_results.md).

## Run locally

```powershell
python -m pip install -r requirements.txt
python ingestion/land_to_bronze.py --out-dir out
# Small smoke test is local-only so it cannot close omitted production boards:
python ingestion/land_to_bronze.py --out-dir out --limit 3
```

For AWS, start the state machine after deploying the reviewed Terraform plan; ingestion is now
part of that execution. Do not run individual Glue stages concurrently or redrive an inner task.
See [operations.md](docs/operations.md) for the exact deploy/verification steps.

```powershell
aws stepfunctions start-execution --state-machine-arn (terraform -chdir=infra output -raw state_machine_arn) --region ap-southeast-1
python scripts/check_freshness.py --bucket (terraform -chdir=infra output -raw lake_bucket)
```

The health command is read-only. It fails for missing metadata, incomplete publication or
ingestion older than 26 hours, even if a transform ran recently.

## Tests and infrastructure checks

Use **Python 3.11 + Java 17**, matching CI. Tests use a local SparkSession; no AWS credentials
are required. API responses are mocked.

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
terraform -chdir=infra init -backend=false
terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra validate
```

Tests cover malformed/empty/failed/truncated boards, retry and manifest commit behavior,
stale/mismatched manifests, board-scoped identity, edits, closure, reactivation, stale expiry,
Gold freshness, role aggregates and health checks.

## Retention and cost controls

| Prefix/storage | Retention |
| --- | --- |
| `bronze/`, `control/ingestion/` | 30 days by default (`bronze_expiration_days`) |
| `quality/` | 90 days |
| Athena `results/` | 7 days by default |
| Glue scripts bucket `tmp/` | 7 days; abandoned multipart uploads after 1 day |
| `silver/`, `gold/`, `state/` | No automatic expiration; preserve lifecycle and historical snapshots |

Glue is the main compute expense. This deployment has `enable_schedule = true`; daily runs
introduce recurring charges. New deployments default to false. Default cron `0 18 * * ? *`
means 01:00 next day in UTC+7. Set the value back to false and apply to pause. An AWS Budget is defined, with email alerts only when `alert_email` is configured.
No Redshift or MWAA is required.

Do not destroy the stack to recover a failed run: S3 buckets use `force_destroy = true`, which
deletes history. Follow the lock-recovery procedure in the operations runbook instead.

## Existing serving material

The serving implementation below is retained from earlier work; it is outside this change's scope.

### Serving layer — `dashboard/app.py` (Streamlit over Athena)

The Gold marts become charts a recruiter or curriculum lead can read. The app queries Athena live
via `awswrangler`:

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py          # http://localhost:8501
```

- **Config, not constants** — database / workgroup / region come from env vars
  (`ATHENA_DATABASE`, `ATHENA_WORKGROUP`, `AWS_REGION`) with the current stack's values as
  defaults. The results bucket is deliberately absent: the workgroup sets
  `enforce_workgroup_configuration = true`, so Athena applies its own result location.
- **Read-only** — `ctas_approach=False`; awswrangler's default would `CREATE TABLE AS SELECT`
  (writing Parquet to S3 and registering a temp table).
- **Cost-aware** — every query filters the `snapshot_date` partition, the KPI row is a single
  combined query, and `@st.cache_data(ttl=900)` stops re-scanning on every rerender. A full page
  load scans **~36 KB** (~$0.0000002).

A static, self-contained variant of the same view is at [`docs/dashboard.html`](docs/dashboard.html)
· live: <https://claude.ai/code/artifact/2b3e2547-4053-4204-8ee8-f122b262b98e>.

## Layout

```text
infra/       Terraform: storage, IAM, ingestion, Glue, lock, Step Functions, schedule, Athena, budget
ingestion/   ATS connectors, run manifest and board catalog
glue/jobs/   Bronze→Silver lifecycle, Silver→Gold marts, shared run contract
scripts/     Read-only freshness check
tests/       Mocked ingestion and local Spark regression tests
sql/         Athena analytical and freshness queries
docs/        Architecture, learning guide, operations/deployment audit and historical samples
```
