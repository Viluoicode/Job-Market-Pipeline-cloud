# Sample results — Athena over the Gold marts

## Scheduled run — 2026-09-16 01:00 UTC+7

After the automatic pipeline succeeded, Athena returned **7,583 postings across 309 companies**,
including **2,499 remote postings (33.0%)**. Ingestion fetched **7,575 records** from 36 boards;
Silver contains **7,731 rows** because it also retains lifecycle history. Gold can retain recent
observations from incomplete boards, so it is not limited to this run's raw observations.

The UTC snapshot date is **2026-09-15**. This scheduled run overwrote that daily Gold partition;
the manual acceptance figures below are preserved as a historical capture, not a separate
Athena partition or another day of market change. Arbeitnow still has a two-page cap.

| Rank | Role | Postings | Remote | Top employer (postings) |
| --- | --- | --- | --- | --- |
| 1 | Machine Learning Engineer | 174 | 57.5% | OpenAI (37) |
| 2 | DevOps Engineer | 109 | 22.0% | Palantir (22) |
| 3 | Data Scientist | 99 | 48.5% | OpenAI (17) |
| 4 | Full Stack Engineer | 98 | 33.7% | OpenAI (17) |
| 5 | Backend Engineer | 98 | 48.0% | GitLab (41) |
| 6 | Data Engineer | 56 | 48.2% | OpenAI (9) |
| 7 | Mobile Engineer | 35 | 37.1% | Robinhood (12) |
| 8 | Frontend Engineer | 12 | 41.7% | Reddit (3) |

These are title-based role groups within the configured source sample; one posting can match
multiple groups. Remote percentages reflect source metadata. Use the health check before
current-market queries. Full SQL and query IDs appear in
[scheduled-run-20260916.json](evidence/scheduled-run-20260916.json).

## Manual acceptance — 2026-09-15 (historical capture)

Verified through Athena after the full pipeline succeeded and `check_freshness.py` exited 0.
**7,578 raw observations → 7,612 Silver lifecycle rows → 7,510 active/fresh,
content-unique Gold postings**. There are **270 companies**, with
**2,496 remote postings (33.2%)**.
Silver includes 34 postings retained from the previous observation because their board could
not confirm absence; their original `last_seen_at` stays unchanged. They remain eligible only
within the 26-hour freshness window.
All 36 configured boards succeeded; Arbeitnow is capped at two pages and marked incomplete.
These figures describe the configured source sample. Roles use title matching, and a posting
can match more than one role family.

| Rank | Role | Postings | Remote | Top employer (postings) |
| --- | --- | --- | --- | --- |
| 1 | Machine Learning Engineer | 176 | 57.4% | OpenAI (37) |
| 2 | DevOps Engineer | 109 | 22.0% | Palantir (22) |
| 3 | Data Scientist | 100 | 49.0% | OpenAI (18) |
| 4 | Full Stack Engineer | 97 | 34.0% | OpenAI (17) |
| 5 | Backend Engineer | 96 | 49.0% | GitLab (41) |
| 6 | Data Engineer | 57 | 49.1% | OpenAI (9) |
| 7 | Mobile Engineer | 35 | 37.1% | Robinhood (12) |
| 8 | Frontend Engineer | 12 | 41.7% | Reddit (3) |

Use this table to prioritize roles and employers, then query `fact_job_posting` for current
application links. SQL is in [athena_analysis.sql](../sql/athena_analysis.sql), and execution/query
IDs are in [deployment evidence](evidence/deployment-20260915.json). This is a verified daily
snapshot, not a live guarantee that every application link remains open indefinitely.

## Historical method and results

> **Historical results, not current market freshness.** On 2026-09-14 the latest deployed Gold
> snapshot was still 2026-07-28 and scheduled execution was disabled. The old transform combined
> accumulated Bronze and marked all rows active, so 9,206 is not a verified count of currently
> open jobs. The new ingestion/lifecycle method changes that measurement; see
> [operations.md](operations.md). Do not interpret the migration count change as a market trend.


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
top roles (53.8%). These figures describe the historical snapshot above.

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
