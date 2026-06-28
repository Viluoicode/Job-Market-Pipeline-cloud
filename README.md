# Job Market AWS Pipeline — cloud-native Medallion (P2)

A cloud-native rebuild of the SkillRadar Medallion pipeline on AWS, provisioned end-to-end with
**Terraform**. Same domain (tech job postings), but the local DuckDB/dbt stack is replaced by an
S3 data lake, **Glue** (PySpark) transforms, the **Glue Data Catalog**, **Athena** SQL, and
**Step Functions** orchestration — the stack Vietnamese DE job posts ask for (AWS + IaC + Spark).

> Sibling project to **SkillRadar** (local-first: DuckDB + dbt + Streamlit). Building the *same*
> pipeline two ways is the point: it shows the tradeoffs between a local lakehouse and cloud-native.

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

### Optional extensions (P2.5)

- **Redshift Serverless + Spectrum** reading the same Gold Parquet (warehouse keyword).
- **EventBridge** schedule to run the state machine daily.
- **Glue Data Quality** rules on the Silver tables (parallels SkillRadar's dbt tests).
- **Lambda** wrapper around the ingestion so the whole thing is serverless.

## Layout

```
infra/        Terraform — S3, IAM, Glue (jobs + crawler + catalog), Athena, Step Functions
glue/jobs/    PySpark ETL: bronze_to_silver.py, silver_to_gold.py
ingestion/    land_to_bronze.py — pull ATS feeds -> S3 bronze
sql/          athena_analysis.sql — example analytical queries
```
