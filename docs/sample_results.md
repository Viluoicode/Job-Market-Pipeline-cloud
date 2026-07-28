# Sample results — Athena over the Gold marts

Real output from a full end-to-end run on AWS (`ap-southeast-1`), snapshot **2026-06-30**.
Pipeline: `land_to_bronze.py` → S3 `bronze/` → Glue `bronze_to_silver` → Glue `silver_to_gold`
→ Glue Crawler → Athena. 6,900 raw postings landed; **6,809** after cross-source dedup.

Queries live in [`../sql/athena_analysis.sql`](../sql/athena_analysis.sql), run in the
`jobmarket-aws` workgroup against database `jobmarket_aws_gold`.

> **Re-verified 2026-07-27** — a fresh end-to-end run (land → Step Functions `SUCCEEDED` → Athena)
> wrote snapshot `2026-07-27`: **9,206** postings after cross-source dedup (~35% remote, 308
> companies); top role **Machine Learning Engineer (207)**, then Data Scientist (142), Full Stack
> (117), DevOps (116). The higher count vs. 2026-06-30 is expected — `bronze_to_silver` reads *all*
> accumulated `bronze/` partitions and dedups by `job_id`, so the lake grows as more snapshots land.
> The 2026-06-30 tables below are kept as the original documented run.

## Market overview (`fact_job_posting`)

| Metric | Value |
| --- | --- |
| Postings (deduped) | 6,809 |
| Remote | 2,434 (~36%) |
| Distinct companies | 161 |

## Decision mart (`role_opportunity`) — snapshot 2026-07-28

The mart the dashboard renders: per role, demand rank + remote share + top hiring company.

| Rank | Role | Demand | Remote % | Top employer (postings) |
| --- | --- | --- | --- | --- |
| 1 | Machine Learning Engineer | 207 | 43.5% | Mistral AI (27) |
| 2 | Data Scientist | 142 | 47.9% | Lyft (21) |
| 3 | Full Stack Engineer | 117 | 37.6% | Databricks (17) |
| 4 | DevOps Engineer | 116 | 38.8% | Palantir (21) |
| 5 | Backend Engineer | 78 | 41.0% | GitLab (22) |
| 6 | Data Engineer | 65 | 53.8% | OpenAI (9) |
| 7 | Mobile Engineer | 50 | 40.0% | Reddit (12) |
| 8 | Frontend Engineer | 17 | 41.2% | Airbnb (3) |

Read as a decision: ML/AI roles dominate demand; Data Engineer is the most remote-friendly of the
top roles (53.8%). Rendered in [`dashboard.html`](dashboard.html).

## Demand by role (`demand_by_role`)

Distinct postings whose title matches each target role's patterns.

| Rank | Role | Postings |
| --- | --- | --- |
| 1 | Machine Learning Engineer | 165 |
| 2 | Data Scientist | 116 |
| 3 | DevOps Engineer | 95 |
| 4 | Full Stack Engineer | 95 |
| 5 | Data Engineer | 56 |
| 6 | Backend Engineer | 54 |
| 7 | Mobile Engineer | 41 |
| 8 | Frontend Engineer | 13 |

ML/AI roles dominate — consistent with AI-heavy employers (OpenAI, Databricks, Mistral, Cohere)
being well represented in the source boards.

## Top hiring companies

| Company | Postings |
| --- | --- |
| Databricks | 759 |
| OpenAI | 723 |
| Stripe | 478 |
| MongoDB | 397 |
| Samsara | 323 |
| Palantir | 274 |
| Brex | 236 |
| Airbnb | 230 |
| Cloudflare | 213 |
| Elastic | 195 |

## Volume & remote split by source

| Source | Postings | Remote |
| --- | --- | --- |
| greenhouse | 4,472 | 1,255 |
| ashby | 1,480 | 1,104 |
| lever | 658 | 47 |
| arbeitnow | 199 | 28 |

> Note: counts shift slightly between runs as boards publish/close postings. Each run writes a new
> `snapshot_date` partition, so `demand_by_role` can be trended over time without mutating prior days.
