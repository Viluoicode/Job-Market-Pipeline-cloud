# Job Market AWS Pipeline — cloud-native Medallion 

A cloud-native rebuild of the SkillRadar Medallion pipeline on AWS, provisioned end-to-end with
**Terraform**. Same domain (tech job postings), but the local DuckDB/dbt stack is replaced by an
S3 data lake, **Glue** (PySpark) transforms, the **Glue Data Catalog**, **Athena** SQL, and
**Step Functions** orchestration — the stack Vietnamese DE job posts ask for (AWS + IaC + Spark).

> Sibling project to **SkillRadar** (local-first: DuckDB + dbt + Streamlit). Building the *same*
> pipeline two ways is the point: it shows the tradeoffs between a local lakehouse and cloud-native.

> **New to the project / data engineering?** Start with the from-zero learning guide:
> [`docs/LEARN.md`](docs/LEARN.md) — explains every concept (data lake, Medallion, Spark, Athena,
> Terraform) and the full data flow for someone with no prior background.

## Architecture

```
Greenhouse / Lever / Ashby            (public ATS APIs)
        │  ingestion/land_to_bronze.py  (local script or Lambda)
        ▼
S3  bronze/   raw newline-delimited JSON, partitioned by source/date
        │  Glue job: bronze_to_silver.py  (PySpark: type + cross-source dedup)
        ▼
S3  silver/   typed, deduped Parquet, partitioned by snapshot_date
        │  Glue job: silver_to_gold.py    (PySpark: role classification + aggregates)
        ▼
S3  gold/     fact_job_posting/, demand_by_role/  (Parquet marts)
        │  Glue crawler  ->  Glue Data Catalog
        ▼
Athena (SQL over the catalog)        [optional later: Redshift Spectrum reads the same Gold]

Orchestration: Step Functions runs bronze->silver, silver->gold, then the crawler, in order.
Everything above is defined in infra/*.tf (Terraform).
```

## How each AWS piece maps to what you already know

| AWS service        | Role here                       | SkillRadar / .NET equivalent     |
| ------------------ | ------------------------------- | -------------------------------- |
| S3 (zones)         | Bronze/Silver/Gold storage      | local `data/` Parquet + DuckDB   |
| Glue (PySpark)     | Distributed Silver/Gold transform | dbt models / Python services   |
| Glue Data Catalog  | Table metadata over S3          | DuckDB/MotherDuck schema         |
| Glue Crawler       | Auto-discovers Gold schema      | (dbt knows the schema directly)  |
| Athena             | Serverless SQL over the lake    | DuckDB queries / dbt marts       |
| Step Functions     | Orchestration / DAG             | Prefect flow / Hangfire          |
| Terraform          | Infrastructure as Code          | (new — the headline P2 skill)    |

## How it works — logic & transforms

The whole project is one idea: move job postings through **three quality tiers** (Medallion —
Bronze → Silver → Gold), doing exactly one job at each hop. Below is the logic of each hop and the
tech that runs it. (For a from-zero explanation of every term, see [`docs/LEARN.md`](docs/LEARN.md).)

### Stage 1 · Ingestion → Bronze — `ingestion/land_to_bronze.py` (Python, `httpx` + `boto3`)

Pulls every board in [`ingestion/sources.json`](ingestion/sources.json) (36 boards across 4 public
ATS APIs — **Greenhouse, Lever, Ashby, Arbeitnow**; no keys needed). Each source returns a different
JSON shape; a per-source `fetch_*` connector normalizes them all into **one flat 11-column record**
(`source, board_token, source_job_id, company, title, location, remote, description, apply_url,
posted_at, raw_json`). Writes newline-delimited JSON to
`bronze/source=<source>/date=<YYYY-MM-DD>/jobs.json`.

- **Unification** is the point of Bronze: 4 shapes in, 1 schema out, so every later stage handles a
  single shape.
- **Resilient:** each board is wrapped in `try/except` — one failing API is logged and skipped, the
  batch keeps going.

### Stage 2 · Bronze → Silver — `glue/jobs/bronze_to_silver.py` (PySpark on Glue)

Reads all Bronze JSON (`recursiveFileLookup`), casts types (`remote`→boolean, `posted_at`→timestamp),
then computes the **two keys that are the heart of the pipeline**:

| Key | Formula | Purpose |
| --- | ------- | ------- |
| `job_id` | `lower(sha256(source \| source_job_id))` | Stable identity of **one posting from one source** → dedup **within a source** (re-crawls collapse). |
| `dedup_hash` | `upper(sha256(norm(company) \| norm(title) \| norm(location)))` | Content fingerprint → dedup **across sources** (same job on two boards → same hash). |

`norm()` = lower-case → replace every char outside `[a-z0-9 ]` with a space → collapse whitespace.
So `"Senior  Engineer!"` and `"senior engineer"` hash identically. *(This logic is a 1:1 port of
SkillRadar's `make_job_id` / `compute_dedup_hash` / `normalize_for_key`, so the two projects agree.)*

Then it keeps **one row per `job_id`** (latest `posted_at`, via `Window.partitionBy("job_id")` +
`row_number()=1`) and writes typed **Parquet** to `silver/jobs/snapshot_date=<date>/`.

#### Data quality gate (Silver)

Before Silver is published, `bronze_to_silver.py` runs an **AWS Glue Data Quality** ruleset
(`EvaluateDataQuality`, DQDL) over the deduped DataFrame — the cloud parallel of SkillRadar's dbt
tests:

| DQDL rule | dbt equivalent | Asserts |
| --------- | -------------- | ------- |
| `IsComplete "job_id"` | `not_null` | surrogate key always present |
| `IsUnique "job_id"` | `unique` | within-source dedup actually held |
| `IsComplete "dedup_hash"` | `not_null` | cross-source key present |
| `ColumnValues "source" in [...]` | `accepted_values` | only the 4 known sources |
| `Completeness "company" >= 0.9` | `not_null` (threshold) | company mostly populated |
| `RowCount > 0` | — | the run produced data |

Results publish to the **Glue Data Quality console** + CloudWatch and are persisted to
`quality/silver_jobs/snapshot_date=<date>/` for audit. With `--dq_enforce true` (default) any
failing rule **fails the job**, so bad data never reaches Silver. *Verified run (2026-07-27):
score **1.0**, 7/7 rules PASS.*

### Stage 3 · Silver → Gold — `glue/jobs/silver_to_gold.py` (PySpark on Glue)

Builds **three marts** (transform logic lives in importable functions — `build_fact`,
`build_demand_by_role`, `build_role_opportunity` — so `tests/` can exercise them on a local
SparkSession):

- **`fact_job_posting`** — dedups **cross-source** by keeping one representative per `dedup_hash`
  (`row_number()=1`), adds `title_lower`, `company_key = md5(company)`, `posted_date_key`, and the
  additive measure `posting_count = 1`. Grain = one active, content-unique posting.
- **`role_opportunity`** — the **decision mart**: per role, `demand_rank` + `job_count` +
  `remote_pct` + the single `top_company` (most postings for that role). One table that answers
  *"which role should I learn / apply for, how remote-friendly is it, and who hires for it?"* — the
  layer a candidate actually decides on. Powers the [dashboard](#dashboard).
- **`demand_by_role`** — classifies each posting into target roles and counts demand:
  1. `DEFAULT_ROLES` = 8 role families, each with lower-cased title substrings (e.g. *Data Engineer*
     ← `data engineer`, `etl engineer`, `analytics engineer`).
  2. **Bridge:** `broadcast`-join postings to roles where `title_lower CONTAINS pattern` (a posting
     can match several roles).
  3. **Count:** `groupBy(role).countDistinct(job_id)` → the final "which role is most in demand" number.

Both are Parquet partitioned by `snapshot_date`; the **Glue Crawler** then registers them in the
**Data Catalog** so **Athena** can query them.

### Data model (Gold)

```
fact_job_posting   grain = one deduped active posting; measure posting_count = 1
  job_id, dedup_hash, company_key, posted_date_key, first_seen_date_key, source, board_token,
  company, title, title_lower, location, is_remote, apply_url, posted_at, first_seen_at,
  last_seen_at, posting_count   + partition snapshot_date

role_opportunity   grain = one (role, snapshot) pair   [decision mart]
  demand_rank, role, job_count, remote_count, remote_pct, top_company, top_company_count
                                + partition snapshot_date

demand_by_role     grain = one (role, snapshot) pair
  role, job_count               + partition snapshot_date
```

Each run writes a fresh `snapshot_date` partition (MVP: `first_seen_at = last_seen_at = run time`,
`is_active = true`), so `demand_by_role` trends over time without mutating prior days. True SCD
history is a P2.5 extension.

### Orchestration — `infra/stepfunctions.tf` (Step Functions, Standard)

`BronzeToSilver` (`glue:startJobRun.sync`, blocks till done) → `SilverToGold` (`.sync`) →
`StartCrawler` → **poll loop** `Wait 30s → GetCrawler → Choice(State == READY?)` (the crawler has no
`.sync` integration, so it's polled). Any error is caught and routed to a `Failed` state.

### Dashboard

The `role_opportunity` mart drives a one-page **decision dashboard** — ranked role demand, remote
share, and top employer per role, all queried from Athena. Since the pipeline is AWS-console-first
(no app to run), the dashboard is a self-contained HTML page:
[`docs/dashboard.html`](docs/dashboard.html) · live: <https://claude.ai/code/artifact/2b3e2547-4053-4204-8ee8-f122b262b98e>.

### Tests — `tests/` (pytest + local SparkSession)

The Glue-free transform functions are unit-tested on a real (local) SparkSession: `job_id` stability
and within-source dedup, `dedup_hash` cross-source equality, role classification counts, and the
`role_opportunity` decision columns. They run in CI ([`.github/workflows/tests.yml`](.github/workflows/tests.yml),
Python 3.11 + Java 17) — `pip install -r requirements-dev.txt && pytest`.

### Verified end-to-end

A full AWS run (snapshot 2026-07-28) landed ~6,900 raw postings → **9,206** after cross-source dedup
(the lake accumulates snapshots), across **308** companies, **~35% remote**; top role **Machine
Learning Engineer (207)**. See [`docs/sample_results.md`](docs/sample_results.md) for the full Athena
output. *(The original 2026-06-30 run — 6,809 deduped — is kept there as the first documented run.)*

## Cost guardrails (read before `apply`)

Designed to run on the **Free Tier for cents**, but you control the spend:

- **Region** `ap-southeast-1` (Singapore) — closest to HCMC.
- **Glue** is the main cost: 2× G.1X workers, 30-min timeout, **run on-demand only**. Don't add a
  schedule until you mean to. A run over this dataset is a few cents.
- **Athena** = $5/TB scanned. Parquet + `snapshot_date` partitions keep queries well under 1 cent;
  the workgroup also caps each query at 10 GB.
- **Step Functions (Standard)** is effectively free at this scale. **Do NOT use MWAA** (managed
  Airflow) for a portfolio — it is ~$350/mo always-on.
- **Redshift** is intentionally NOT here. If you add it later, use **Serverless** and pause/delete
  it — it is the #1 surprise-bill source.
- **S3 lifecycle** expires raw bronze (30 d) and Athena results (7 d) so storage stays near zero.
- **Tear down when idle:** `terraform destroy`. It's all IaC — recreate in minutes.
- Set an **AWS Budget alert** ($5–10) as a backstop.

## Build order

1. **Prereqs** — AWS account + an admin IAM user/SSO, `aws configure`, install Terraform + the
   AWS CLI. Edit `infra/terraform.tfvars.example` -> `terraform.tfvars` with globally-unique bucket
   names.
2. **Provision infra**
   ```bash
   cd infra
   terraform init
   terraform plan
   terraform apply        # creates buckets, IAM, Glue jobs, Athena WG, Step Functions
   ```
3. **Land data** — point the ingestion at your lake bucket and run it:
   ```bash
   export LAKE_BUCKET="$(terraform -chdir=infra output -raw lake_bucket)"
   pip install boto3 httpx
   python ingestion/land_to_bronze.py     # writes raw JSON to bronze/
   ```
4. **Run the pipeline** — start the Step Functions state machine (runs both Glue jobs + crawler):
   ```bash
   aws stepfunctions start-execution \
     --state-machine-arn "$(terraform -chdir=infra output -raw state_machine_arn)"
   ```
   (Or run jobs individually from the Glue console while iterating.)
5. **Query** — open the Athena console, pick the `jobmarket-aws` workgroup + `*_gold` database, and
   run `sql/athena_analysis.sql`. Screenshot the results for your portfolio.
6. **Polish** — add a GitHub Actions workflow (`terraform fmt -check` + `validate` + `plan`),
   an architecture diagram, and a short write-up comparing this to SkillRadar.

### Extensions (P2.5)

- ✅ **Glue Data Quality** — *implemented.* A DQDL ruleset gates the Silver table inside
  `bronze_to_silver.py` (the cloud parallel of SkillRadar's dbt `not_null` / `unique` /
  `accepted_values` tests). See [Data quality gate](#data-quality-gate-silver) below.
- ✅ **EventBridge schedule** — *deployed* ([`infra/schedule.tf`](infra/schedule.tf)). An
  EventBridge rule triggers the Step Functions state machine on a cron (default daily 18:00 UTC).
  **Disabled by default** as a cost guardrail — set `enable_schedule = true` (and optionally
  `schedule_expression`) in `terraform.tfvars` to turn it on. A disabled rule costs nothing.
  *Verified 2026-07-27: rule live, state `DISABLED`, target = the pipeline state machine.*
- **Redshift Serverless + Spectrum** reading the same Gold Parquet (warehouse keyword).
- **Lambda** wrapper around the ingestion so the whole thing is serverless.

## Layout

```
infra/        Terraform — S3, IAM, Glue (jobs + crawler + catalog), Athena, Step Functions, EventBridge
glue/jobs/    PySpark ETL: bronze_to_silver.py (+ DQ gate), silver_to_gold.py (3 marts)
ingestion/    land_to_bronze.py — pull ATS feeds -> S3 bronze
sql/          athena_analysis.sql — example analytical queries
tests/        pytest + local SparkSession unit tests for the transforms
docs/         LEARN.md, architecture.md, sample_results.md, dashboard.html
```
