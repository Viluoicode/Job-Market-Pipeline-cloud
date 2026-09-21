# Operations: ingestion, freshness and lifecycle

## Scheduled execution verified — 2026-09-16

The first daily run was triggered by **AWS EventBridge**, with input `source=aws.events`,
`detail-type=Scheduled Event` and the ARN of `jobmarket-aws-pipeline-schedule`.
It started at **01:00:16.275 UTC+7** on 2026-09-16 and completed successfully at
**01:07:29.609 UTC+7** (7 minutes 13 seconds). No local process or Codex task is needed to
start ingestion. The rule remains **ENABLED** at 18:00 UTC / 01:00 UTC+7 daily.

| Check | Observed result |
| --- | --- |
| All three Glue jobs | SUCCEEDED |
| Source coverage | 36 successful boards; 0 failed; Arbeitnow incomplete at two pages |
| Raw observations in this run | 7,575 |
| Silver lifecycle rows | 7,731, including retained history |
| Gold / Athena | 7,583 postings; 309 companies; 2,499 remote (33.0%); 8 role groups |
| Glue Data Quality | 7/7 PASS, score 1.0 |
| Crawler | SUCCEEDED for this execution |
| Publication | Silver and completion markers reference this same run |
| Concurrency lock | Released |

Execution name:
`bb0e63dd-4ba7-3e0f-6ae4-cc994a7f4da9_3a4e7b21-e329-5c8e-0adc-fb8108cff169`.
[scheduled-run-20260916.json](evidence/scheduled-run-20260916.json) contains the full ARN,
trigger event, Glue run IDs, DQ rules, marker values, health capture and Athena query IDs/results.
Health is a point-in-time observation; run the health command again before consuming data.

**Date convention:** 01:00 on 2026-09-16 in Vietnam is 18:00 on 2026-09-15 UTC. Therefore
`snapshot_date=2026-09-15` is expected. This scheduled run replaced the Gold partition written
by the previous evening's manual verification; the immutable Silver runs retain separate history.
Gold can include recent prior observations whose board could not confirm absence, until they
exceed the freshness window. Its row count need not be below the current raw fetch count.

<<<<<<< ours
The deployed pipeline now has evidence of an automatic daily run. Failure/staleness alerting
and a separately scheduled health check are still follow-up work; this evidence is not an
ongoing monitoring service. The acceptance record below documents the earlier manual runs.
=======
The deployed pipeline now has evidence of an automatic daily run. Failure/staleness alerting and an independent scheduled health check were deployed on
2026-09-17; see [monitoring.md](monitoring.md) for the current notification status and runbook.
The evidence file itself remains a point-in-time capture. The acceptance record below documents the earlier manual runs.
>>>>>>> theirs

## Deployment acceptance — 2026-09-15 (historical record)

Account `290488660407`, region `ap-southeast-1`. The existing stack was upgraded in place;
no infrastructure was destroyed. Ingestion now runs inside the state machine. EventBridge
`jobmarket-aws-pipeline-schedule` is **ENABLED**, `cron(0 18 * * ? *)` = **01:00 UTC+7 daily**.
The trigger, job execution and storage all run on AWS; the user's computer can be off.
The first expected scheduled tick is **2026-09-16 01:00 UTC+7**. It has not yet occurred at
acceptance time. The rule target and permission to call `states:StartExecution` were verified.

| Acceptance check | Observed result |
| --- | --- |
| Final execution | `lifecycle-verify-20260915-1413` — **SUCCEEDED** |
| Execution interval, UTC+7 | 2026-09-15 21:11:27.442000+07:00 → 2026-09-15 21:18:12.615000+07:00 |
| Ingestion | **7,578 records**, 36 successful boards, 0 failed |
| Coverage caveat | Arbeitnow remains incomplete (two-page cap); absence does not close its jobs |
| Silver | **7,612 rows**, all **7/7 Glue DQ rules PASS** |
| Gold / Athena | **7,510 postings**, 270 companies, 33.2% remote, 8 role groups |
| Publication | Current crawler SUCCEEDED, matching JSON object markers, lock released |
| Health | `check_freshness.py` exit **0**, actual ingestion at 2026-09-15T14:12:24.592677+00:00 |
| Lifecycle across real runs | 7,571 prior identities retained; 7,537 observed again; 0 first-seen changes; 0 dropped history rows |
| Tests | **44 pytest tests passed**, Python 3.11 / Java 17 / PySpark 3.5.1 |
| Retention | Bronze + manifests 30 days; quality 90 days; Athena/Glue temporary files 7 days; Silver/Gold/state retained |

Final execution ARN:
`arn:aws:states:ap-southeast-1:290488660407:execution:jobmarket-aws-pipeline:lifecycle-verify-20260915-1413`.
Query IDs, row results, job run IDs, lifecycle checks and the enabled rule are saved in
[deployment-20260915.json](evidence/deployment-20260915.json).

Two runtime issues were found and corrected during acceptance:

- Glue Python Shell adds runtime arguments without necessarily passing `--JOB_NAME`. The parser
  now accepts explicitly named Glue options and still rejects unknown application arguments.
  The failed initial run released its DynamoDB lock automatically.
- Step Functions' S3 SDK integration serialized an already serialized completion value again.
  `PublishCompletion` now passes the JSON object directly. The subsequent full execution produced
  a marker readable by the health command. A regression check rejects malformed metadata.

The final run also read the first new Silver run, confirming persisted lifecycle history and a
same-day Gold replacement. Historical July partitions remain separate from this measurement.
No dashboard changes were made. Scope is the configured 36 boards, not the entire job market;
role labels use title rules and remote status reflects each source's metadata.

## Historical deployment audit — before upgrade, 2026-09-14

These are direct AWS API observations in account `290488660407`, region `ap-southeast-1`.
At that audit, the change was prepared in code and had not yet been applied to AWS.

Local live ingestion verification on the same date fetched **7,547 postings from all 36 boards**:
36 successful, 0 failed, 1 successful empty board (Lever/mistral), and 1 incomplete feed
(Arbeitnow, page cap). Objects and the committed manifest were written only to local scratch
storage; this check did **not** refresh AWS. No source boards were removed to meet the threshold.

Validation completed for this change:

- **41 pytest tests passed** on Python 3.11, Java 17, PySpark 3.5.1.
- The local live dataset passed manifest/observation validation and produced **7,547 Silver
  rows → 7,452 content-unique Gold postings → 8 role groups**. Fetching took about 45 seconds.
- Terraform `fmt -check` and `validate` passed. The reviewed plan has **8 creates, 8 in-place
  updates, 0 destroys**; the EventBridge schedule stays DISABLED.
- AWS `ValidateStateMachineDefinition` returned OK with no diagnostics. A read-only `TestState`
  call confirmed the actual GetCrawler response and timestamp format.

These were pre-deployment checks only. The AWS acceptance and refreshed dataset are recorded
in the current deployment section above.

| Concern | Observed deployment | Implementation in this change |
| --- | --- | --- |
| Orchestration | Starts at `BronzeToSilver`; no ingestion task | Python Shell ingestion → Silver → Gold → verified crawler |
| Schedule | `jobmarket-aws-pipeline-schedule` **DISABLED**, `cron(0 18 * * ? *)` | Same opt-in schedule, now covers ingestion as well |
| Latest execution | `mart-verify-20260728-220715`, SUCCEEDED; 2026-07-28 15:07–15:13 UTC | Run identity/date passed explicitly to every stage |
| Latest Gold | `snapshot_date=2026-07-28`; fact objects modified at 15:10:07 UTC | Gold uses only active, sufficiently recent observations |
| Bronze | No objects under `bronze/` | Each run fetches its own Bronze before transforming |
| Retention | Bronze 30 days, Athena results 7 days | Also retain manifests 30 days, quality audit 90 days, Glue temp 7 days |
| Crawler | READY; last crawl SUCCEEDED on 2026-07-28 | Require successful crawl belonging to this execution |
| Script drift | S3 ETags/tags differ from local Terraform state | Downloaded both scripts: contents match repo after line-ending normalization |

As of this audit, Gold is approximately **48 days old**. The earlier 9,206-row result is a
historical accumulated-Bronze count, not evidence of 9,206 currently open jobs. The old transform
read every retained Bronze partition and assigned fresh `last_seen_at` timestamps to all rows.
Bronze expiration is working; it is not a substitute for scheduled ingestion.

Git audit: local `main` is `a9dd23e`, two dashboard-related commits ahead of `origin/main`
(`d1136a4`). Those commits and the pre-existing README edits are preserved. No dashboard work
is included in this change.

## Run contract

Step Functions derives `run_id` from its execution name and `snapshot_date` from the UTC
execution start. Use letters, digits, hyphens and underscores for execution names (up to 80
characters). The date remains fixed if processing crosses midnight.

Ingestion is a Glue **Python Shell 3.9** job, 0.0625 DPU, 15-minute timeout, using the existing
HTTP connectors. It loads `sources.json` from the scripts bucket, retries network errors,
429 and 5xx up to three attempts, and reports each board separately.

```text
bronze/run_id=<run>/source=<source>/board=<token>/jobs.json
control/ingestion/run_id=<run>/manifest.json
silver/runs/<run>/jobs/*.parquet
state/silver/latest.json
gold/<mart>/snapshot_date=<date>/*.parquet
state/pipeline/latest.json
```

Each Bronze record carries `run_id` and `fetched_at`. A manifest includes the expected boards,
per-board status, completeness, row count, object key, and observation time. It is written
after all board objects. At least 80% of boards must succeed and the run must contain at least
one posting; otherwise ingestion exits nonzero and transforms do not start. This threshold is
configurable through `ingestion_min_success_ratio`.

A failed board contributes no input file. A successful empty board is explicitly recorded.
Malformed payloads and postings without IDs fail the board. Arbeitnow's two-page cap marks the
feed incomplete while another page exists, so truncation never implies that unseen jobs closed.
`--limit` is restricted to local dry runs. Repeating a committed `run_id` reuses its manifest
and observation timestamps rather than fetching again.

Silver accepts only the named successful manifest, at most 24 hours old. It reads exactly the
listed nonempty objects with a fixed schema and FAILFAST JSON parsing, then verifies board counts,
run IDs and observation timestamps. It never scans historical Bronze to assemble today's market.

## Lifecycle semantics

Identity is now SHA-256 of `source|board_token|source_job_id`, preventing an ID reused by another
board from collapsing separate postings. Cross-source `dedup_hash` and role rules remain intact.

| Event | Silver result | Gold result |
| --- | --- | --- |
| Observed again | Preserve `first_seen_at`; update fields and `last_seen_at`; active/fresh | Included |
| Missing from successful, complete board | Inactive, reason `absent_from_complete_board` | Excluded |
| Board failed, removed, or incomplete | Preserve last observation; reason `board_unverified` | Included only while observation is within 26 hours |
| Still unobserved after 7 days | Inactive, reason `stale_observation` | Excluded |
| Previously inactive posting returns | Reactivate; preserve original first observation | Included |

The 26-hour freshness window and 7-day stale threshold are measured against **ingestion
completion**, not the time a transform happens to run. Wall-clock pipeline freshness is checked
separately by the health command below. Field changes are chosen by observation time before
source `posted_at`, which may remain unchanged when an employer edits a posting.

Each Silver run is immutable after its state pointer commits, and retains inactive rows as
lifecycle history. No S3 rule expires `silver/`, `gold/`, or `state/`. Therefore raw retention
does not erase posting history. If the current state pointer exists but its Parquet is missing,
the job fails instead of silently starting over.

**Migration:** the first new run bootstraps from newly fetched postings. It intentionally does
not infer history from legacy Silver, whose first/last-seen values were generated by transforms.
July partitions remain available as historical results but use a different counting method;
do not interpret the July-to-new-run count change as a market trend.

## Publish and concurrency

One conditional DynamoDB lock covers the whole execution. A competing execution fails with
`PipelineBusy` before ingestion. Each Glue job also allows only one concurrent run. Normal
success/failure releases the lock only when the stored owner matches the execution ARN.

Silver commits its own pointer after its enforced Glue Data Quality gate and Parquet write.
Gold reads the exact committed Silver run. Each mart overwrites only the specified daily
partition, including an empty result, so same-day reruns cannot leave an old nonempty mart
behind. Other days are preserved.

Crawler `READY` alone does not imply success. The workflow checks `LastCrawl.Status` and start
time before publishing `state/pipeline/latest.json`. The state machine has a 90-minute ceiling.
There is **no transaction across the three Gold marts**: an interrupted same-day write may leave
mixed files until rerun. Consumers must check the health marker before reading the current
snapshot; mismatching Silver/pipeline run IDs mean the current publish is incomplete.

Start a **new execution** after a failed run; do not redrive an inner task or run Glue jobs
directly, because that bypasses pipeline lock ownership. Committed ingestion/Silver reuse exists
for safe stage idempotency, not to authorize writes outside the lock.

## Deploy and verify

Keep `enable_schedule = false` for the first verification. From the repo root:

```powershell
terraform -chdir=infra init
terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra validate
terraform -chdir=infra plan '-out=pipeline.tfplan'
# Review resource changes, then deploy explicitly:
terraform -chdir=infra apply pipeline.tfplan

$pipelineArn = terraform -chdir=infra output -raw state_machine_arn
aws stepfunctions start-execution --state-machine-arn $pipelineArn --region ap-southeast-1
```

The first verification should show ingestion SUCCEEDED, a successful manifest, both Glue jobs
SUCCEEDED, Silver DQ passing, the crawler SUCCEEDED, and matching Silver/pipeline run IDs.
Inspect failed/incomplete board counts before enabling recurring execution.

```powershell
python -m pip install -r requirements.txt
python scripts/check_freshness.py --bucket (terraform -chdir=infra output -raw lake_bucket)
```

This read-only command exits **0 only when** a fully published run has ingestion at most 26
hours old. Missing metadata, expired manifests, old observations, and uncommitted Silver fail
the check. A recent transform completion does not reset data age.

After verification, set `enable_schedule = true` in the existing `infra/terraform.tfvars`,
review a new plan and apply. The default is 18:00 UTC = **01:00 next day, UTC+7**. Daily Glue
runs create recurring charges. The current deployment completed these steps on 2026-09-15;
its schedule is now enabled.
To pause, set it back to false and apply. No notifications are sent by the health command.

## Recover an aborted or timed-out execution

The lock has no TTL: expiry must not let a second pipeline write while an old Glue job is still
finishing. `StopExecution`, top-level timeout, or a failure releasing the lock may leave it held.

1. Read `LockId=pipeline` in the `jobmarket-aws-pipeline-lock` DynamoDB table and note its
   `ExecutionArn`.
2. Inspect that execution. If RUNNING, wait or deliberately stop it. Confirm **all three Glue
   jobs have no STARTING/RUNNING/STOPPING/WAITING runs** and the Gold crawler is READY. An aborted
   workflow does not guarantee its external jobs stopped.
3. Only after confirming quiescence, remove that one lock with a **conditional delete** requiring
   `ExecutionArn` to equal the inspected ARN. Do not truncate/delete the lock table.
4. Start a new execution; it will ingest again, reconcile from the latest valid Silver state,
   and repair the day's Gold marts. Run the freshness check before consuming the snapshot.

Never `terraform destroy` to recover a stalled run: this stack's buckets have
`force_destroy = true`, which also removes stored history.

## References

- [AWS Glue Python Shell job configuration](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html)
- [AWS Glue crawler API and LastCrawlInfo](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-crawling.html)
- [Step Functions error handling](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html)
