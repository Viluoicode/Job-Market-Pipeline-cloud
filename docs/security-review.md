# Security review / Quyền sau hardening — 2026-09-30

## Deployed changes / Thay đổi đã apply

Terraform now deploys separate `jobmarket-aws-silver-role` and `jobmarket-aws-gold-role`. The
shared `jobmarket-aws-glue-role` and AWSGlueServiceRole attachment were removed, and since the
Iceberg switch (November 2026) there is no crawler or crawler role. No runtime role can write the deployed scripts; only Step Functions among pipeline
runtime roles can write the completion marker. Administrative identities remain outside that claim.

| Role | Read | Write/delete/control |
| --- | --- | --- |
| Ingestion | Own script/config, ingestion manifests | Put Bronze/manifests, scoped Glue logs; no object delete |
| Silver | Bronze, manifests, previous Silver, Silver pointer; its script/shared contract | Put/Delete Silver runs and quality output; Put Silver pointer; own temp prefix |
| Gold | Silver runs, Silver pointer, manifests; its script/shared contract | Put/Delete `gold_iceberg/*` (Iceberg data and metadata); Put `state/gold_iceberg/*` (version record); Get/Create/Update tables only in the `_iceberg` database; own temp prefix. No write to the legacy Parquet `gold/` |
| Step Functions | Job status; the Gold version record | Control three jobs, Put/Delete only the pipeline lock key, Put completion marker |
| Daily EventBridge | No lake data | Start this state machine only |
| Health Lambda | Published metadata only | Its logs and JobMarket/Pipeline metrics only |
| Lock janitor Lambda | Lock item; owner execution and three job runs | Delete only the pipeline lock key (owner-conditional); its logs. No start/stop of executions or jobs |
| Dashboard reader | Gold Iceberg files under `gold_iceberg/`, metadata, pipeline lock, `_iceberg` Catalog | Athena SELECT workflow in its workgroup and result objects; no lake writes or Glue job execution |

Stage jobs retain bucket-level listing/metadata for their two buckets for Spark compatibility;
object reads/writes remain prefix-scoped. Glue log permissions cover `/aws-glue/*` in this account
and region; metrics are restricted to Glue/Glue Data Quality. Silver's DQ publish/read permission
covers Data Quality rulesets within this account/region, not all Glue actions. These are explicit
remaining scope choices, not claims of per-object/per-run isolation.

All three buckets enforce an insecure-transport Deny for non-service principals, have all public
access blocks enabled, and BucketOwnerEnforced ownership managed in Terraform. Lake versioning
was adopted without suspending it; scripts versioning is now enabled. Default SSE-S3 stays enabled.
No noncurrent versions were deleted or assigned a new expiration during this change. Follow-up,
22 November 2026: the agreed retention is 30 days for noncurrent versions on the lake and scripts
buckets, and 30 days for the shared `/aws-glue/*` log groups (imported into Terraform with
`skip_destroy`, so destroying this stack leaves them in place).

`allow_bucket_force_destroy` now defaults to false. Cleanup must deliberately opt in before deleting
nonempty buckets. This protects the Terraform cleanup path, not deletion by a privileged IAM user.
Step Functions trust is scoped to this account and exact state-machine ARN. Existing service-only
Glue/EventBridge/Lambda trusts remain; no unverified service-specific condition was added blindly.

The dashboard requires `AWS_DASHBOARD_ROLE_ARN`, assumes the reader for one hour, refreshes its
cached session before expiry, and never falls back to the operator session for data queries.
The reader trust lists explicitly configured IAM principals, not account-root or everyone.
The operator's AdministratorAccess was not removed; bootstrap credentials remain available to the
local operator. This is not OS/process isolation or an authorization design for public hosting.

Implementation: `infra/iam_runtime.tf`, `infra/iam.tf`, `infra/glue.tf`, `infra/s3.tf`,
`dashboard/data_access.py`, `dashboard/app.py`. Configure trusted principals using the ignored
`infra/terraform.tfvars`; see the example file. New deployments with an empty principal list do not
create a reader role and cannot launch the dashboard until configured.

## Same-day overwrite correction

The first run with split roles succeeded. The first same-day rerun exposed an S3 AccessDenied
on `gold/fact_job_posting_$folder$`: Spark needed a directory marker outside `fact_job_posting/*`.
The Gold policy now allows Get/Put/Delete on exactly the three mart-root `_$folder$` objects.
Other Gold markers remain denied. This change does not grant writes to the entire lake.
The failed attempt is retained in acceptance evidence; recovery uses a new full state-machine run.

[AWS documents these Hadoop directory placeholders](https://repost.aws/knowledge-center/emr-s3-empty-files).
A passing policy simulation alone cannot expose every runtime filesystem operation, which is why
the same-day execution is part of acceptance.

## Verification / Kiểm chứng

[Permission evidence](evidence/security-permissions-20260930.json) records 52 positive/negative IAM
simulations and live bucket configuration checks. Context keys are explicitly supplied so a missing
condition cannot create a misleading denied result. Simulations do not cover every account-level
policy or prove runtime compatibility. [Live acceptance](evidence/security-acceptance-20260930.json)
records the successful initial run, the failed overwrite attempt and successful full recovery,
seven DQ rules per successful run, and Athena reads using the restricted reader. All 70 local tests
passed; the final Terraform plan reported no changes. IAM Access Analyzer returned no findings
for the four new inline policies. The verifier never issues production writes/deletes as negative probes.

```powershell
python scripts/check_permissions.py --lake (terraform -chdir=infra output -raw lake_bucket) --scripts (terraform -chdir=infra output -raw scripts_bucket) --results (terraform -chdir=infra output -raw athena_results_bucket)
```

The command requires an operator with IAM simulation/configuration-read permissions. The dashboard
reader is intentionally not allowed to audit IAM. Gold still lacks an atomic transaction across its
three marts; publication checks and the lock reduce exposure but do not add storage transactions.

## IaC scan (Checkov) / Quét bảo mật hạ tầng — 2026-11-21

CI runs Checkov 3.4.6 (`bridgecrewio/checkov-action@v12`) on `infra/` with `soft_fail: false`.
The first scan reported 41 findings across 20 checks (279 passed). Every finding was reviewed: one
check was fixed, the rest are suppressed inline (`#checkov:skip=<id>:<reason>`) on the exact
resource. Result: 282 passed, 0 failed, 38 skipped. A new finding fails the pull request until it is
fixed or a reasoned suppression is reviewed.

**Fixed**

| Check | Change |
| --- | --- |
| CKV_AWS_300 abort failed multipart uploads | Bucket-wide lifecycle rule on all three buckets aborts incomplete uploads after 7 days. An interrupted Spark write otherwise leaves billed parts that never appear as objects. |

**Accepted with reasons** (the workshop stores public job postings; most items are enterprise
defaults whose cost or operating burden is not justified at this scale)

| Check | Resources | Reason |
| --- | --- | --- |
| CKV_AWS_145 S3 KMS, CKV_AWS_119 DynamoDB CMK, CKV_AWS_158 log group KMS, CKV_AWS_173 Lambda env KMS | buckets, lock table, Lambda logs/env | Data is encrypted at rest with AWS-owned/managed keys (SSE-S3 enforced). A customer KMS key costs per key and per request and protects no secret here. |
| CKV_AWS_26 SNS encryption | alert topic | CloudWatch alarms cannot publish to a topic encrypted with the AWS-managed `aws/sns` key (its key policy cannot grant CloudWatch). A customer key would add cost for non-sensitive alert text. |
| CKV_AWS_144 cross-region replication | buckets | Single-region workshop; versioning covers accidental overwrite/delete. Replication doubles storage. |
| CKV_AWS_18 S3 access logging | buckets | Needs a separate log bucket; CloudTrail already records management events. |
| CKV2_AWS_62 S3 event notifications | buckets | Nothing consumes object events; Step Functions orchestrates the pipeline. |
| CKV_AWS_21 versioning | Athena results bucket | Query output is reproducible and expires after 7 days; versioning would keep it. Lake and scripts buckets are versioned. |
| CKV_AWS_195 Glue security configuration | three jobs, crawler | Buckets already enforce SSE-S3 and TLS; a configuration without KMS adds no protection. |
| CKV_AWS_28 point-in-time recovery | lock table | It holds one transient lock item; restoring an old lock would be wrong. |
| CKV_AWS_115 reserved concurrency | both Lambdas | Small accounts cannot reserve concurrency; overlapping runs are safe (conditional delete / idempotent metrics). |
| CKV_AWS_116 dead-letter queue | both Lambdas | Scheduled asynchronous invocations; a failure raises an alarm and the next 15-minute run retries. |
| CKV_AWS_117 VPC | both Lambdas | They call only AWS public APIs; a VPC would need NAT or endpoints without isolation gain. |
| CKV_AWS_272 code signing | both Lambdas | Single-operator Terraform deployment from reviewed source. |
| CKV_AWS_50, CKV_AWS_284 X-Ray tracing | Lambdas, state machine | Short service calls; execution history, logs and alarms are sufficient. |
| CKV_AWS_285 execution logging | state machine | A Standard workflow keeps full execution history for 90 days; failures alarm through SNS. |
| CKV_AWS_338 one-year log retention | Lambda log groups | 14 days bounds cost; durable evidence lives in S3 manifests and state files. |

Revisit these decisions if the data becomes sensitive, the stack becomes multi-user, or an
organization policy requires customer-managed keys.

---

## Historical findings before this deployment

The remainder records the pre-hardening inspection and migration rationale. It does not describe
the current role assignments. In particular, the old shared-role/admin-reader recommendations have
now been implemented as described above; retention deletion and public hosting remain excluded.


Read-only comparison of Terraform with live IAM role policies, service role bindings, S3 settings,
SNS topic policy and Lambda invocation policy in the reference Singapore deployment. No permissions
were changed. This is a scoped configuration review, not a penetration test or full account audit.
Identity-policy grants below remain subject to explicit denies, organization policies and other
applicable controls; their effective combinations were not exhaustively simulated.

## Resource names

`lake`, `scripts`, `athena` below refer to this project's three S3 buckets, whose names end in
`-lake`, `-scripts`, `-athena`. Roles have the `jobmarket-aws-` prefix.
Read = GetObject, write = PutObject (can replace an object), delete = DeleteObject.
A resource ARN ending in `/*` covers every object under that bucket/prefix.

## Pre-hardening permissions / Quyền trước hardening

| Role | Used by | Read | Write / delete / control |
| --- | --- | --- | --- |
| `ingestion-role` | Glue Python Shell ingestion | scripts/ingestion/*; lake/control/ingestion/*; prefix-limited manifest listing; bucket location | Put lake/bronze/* and lake/control/ingestion/*; Glue logs. No object Delete grant in its inspected policy. |
| `glue-role` | BOTH Spark jobs AND Gold crawler | All objects/listing in all three project buckets | Get/Put/Delete all objects in all three buckets, including scripts and completion markers; unrestricted metric namespace. Also attaches AWSGlueServiceRole, which grants glue:* on * and additional S3/EC2/IAM-read/log permissions. |
| `sfn-role` | Pipeline state machine | Job-run and crawler status | Start/status/stop the three named Glue jobs; start/read one crawler; Put/Delete items in one lock table; Put only lake/state/pipeline/latest.json. No lake data-read grant. |
| `events-role` | Daily EventBridge rule | None needed on the lake | StartExecution on this one state machine. No S3 grant. |
| `health-monitor-role` | Health Lambda | Two latest state JSON files and control/ingestion/*/manifest.json | Own log streams/events; PutMetricData only in JobMarket/Pipeline. No lake-write or pipeline-start grant. |
| Local default CLI identity | Operator; dashboard if using the same default credential chain | AdministratorAccess is directly attached | Admin policy is not a dashboard read-only boundary. Dashboard does not select a separate role/profile in code. |

The five runtime roles have service principals in their trust policies and no permissions boundary.
Their trust statements currently have no SourceAccount/SourceArn conditions. Absence of a boundary
is not automatically a defect; principal, actions and resource scope still matter.

## S3 and notification checks

All three buckets: four Block Public Access settings true, BucketOwnerEnforced (ACLs disabled),
default AES256/SSE-S3 encryption. None has a bucket policy; therefore no bucket-level explicit Deny
for non-HTTPS requests. Lack of a bucket policy does not itself make a bucket public.

Lake versioning is Enabled, but Terraform has no versioning resource. Scripts/results versioning
returns no enabled status. Lake expiration covers current Bronze/manifests (30 days) and quality
(90 days), with no noncurrent-version expiration. Old versions may remain billable after expiration.
Terraform uses force_destroy=true for the project buckets, so destruction can remove stored history.

SNS permits CloudWatch Publish only with this account and project-prefixed alarm ARNs. Lambda's
resource policy permits InvokeFunction from the specific health-monitor EventBridge rule. These
are useful existing boundaries. Lambda does not need SNS Publish: the alarm publishes to SNS.

## Required hardening / Các điều cần chỉnh

1. **High priority: runtime script and marker integrity.** Remove write/delete on scripts/jobs/*,
   scripts/ingestion/* and lake/state/pipeline/* from ETL/crawler roles. A runtime role should not
   rewrite the code executed next time or forge the completion marker. Code deployment belongs to
   the deployment identity; completion belongs to Step Functions.
2. **High priority: separate crawler and ETL permissions.** Replace shared AWSGlueServiceRole with
   job-specific/custom policies. Narrow S3, Catalog, logs and metrics together. Editing only the
   inline S3 policy leaves the broad managed-policy permissions in place.
3. **High priority for dashboard usage: dedicated reader.** Keep administrator credentials for
   deliberate administration; use a separate short-lived reader session for Streamlit. Do not remove
   the operator's admin policy without planning deployment access and recovery.
4. **HTTPS enforcement:** add an explicit insecure-transport Deny on bucket and object ARNs for
   all three buckets. Handle AWS service-principal exceptions according to AWS guidance and verify
   service calls. SSE-S3 at rest and HTTPS in transit are separate controls.
5. **Ownership and retention in IaC:** adopt the already-enabled lake versioning into Terraform
   without suspending it; manage ownership settings; define noncurrent-version retention based on
   recovery needs. Consider script versioning. Changing retention can delete historical versions,
   so do not apply an arbitrary expiry during a security review.
6. **Trust and deletion guardrails:** add supported SourceAccount/SourceArn restrictions, especially
   the Step Functions trust policy, using account/region/name-derived ARNs to avoid Terraform cycles.
   Review lock LeadingKeys restriction to pipeline. Consider a deliberate cleanup switch/default
   force_destroy=false. It is an accidental-deletion guard, not a replacement for IAM restrictions.

## Migration target / Ma trận dùng để triển khai

| Principal | Data read | Data write/delete | Other required access |
| --- | --- | --- | --- |
| Ingestion | Its scripts/config; manifests | Put Bronze and manifests only | Prefix list, bucket location, scoped logs |
| Silver job | Bronze, manifests, previous Silver and Silver pointer; job scripts | Silver run output and quality output (Put/Delete required by overwrite); Put Silver pointer; scoped temp | Scoped logs and metrics; only Glue/DQ calls actually required |
| Gold job | Committed Silver, Silver pointer, manifests; job scripts | `gold_iceberg/*`, the Gold version record and scoped temp | Scoped logs and metrics; Iceberg table calls in its own database |
| Step Functions | Run status; Gold version record | Lock item and completion marker only | Control the three jobs |
| EventBridge | None | None | Start only this state machine |
| Health monitor | Publication metadata only | Own logs/metrics only | Keep existing namespace restriction |
| Lock janitor | Lock item; execution and job-run status | Delete the pipeline lock key only | DescribeExecution on this state machine's executions; GetJobRuns on three jobs |
| Dashboard reader | Gold, publication metadata; lock GetItem | Athena query output prefix only, no lake/script mutations | Athena Start/Get/GetResults/Stop on designated workgroup; read Gold Catalog metadata |
| Deployer | Infrastructure configuration and protected state | Upload code/config; reviewed infrastructure and IAM changes | PassRole limited to intended runtime roles/services; independent of dashboard |

Every S3 reader/writer also needs the appropriate bucket-level metadata/list permissions, scoped
by prefix where supported. Multipart uploads/cleanup and Glue DQ runtime dependencies must be
verified against real execution before a policy is considered complete. Spark overwrite requires
Delete on its output; removing all Delete actions would break reruns.

CloudWatch PutMetricData requires Resource=*; narrow by namespace where appropriate rather than
replacing it with an unsupported ARN. Read-only data consumption still writes Athena results.

## Validation before acceptance

- Review Terraform plan: no unexpected bucket replacement, deletion or schedule changes.
- Validate custom policies with IAM Access Analyzer; use policy simulation for positive and
  negative cases. Simulation is not a substitute for running the services.
- Verify ingestion cannot write Gold/scripts; crawler cannot write lake objects; reader cannot
  modify Bronze/Gold/scripts or start Glue; ETL cannot publish pipeline completion.
- Run the whole state machine and a same-day rerun, confirm DQ/crawler success, lock release and
  matching markers; check logs for AccessDenied. Refresh dashboard with the dedicated reader.
- Recheck all five alarms and metadata health. Keep a reviewed rollback plan during role migration.
- Do not issue real destructive API calls against production data to test a denied permission.

## Architecture drawing

Use diagrams.net: More Shapes → Networking → AWS library → Apply. Save the editable drawing and
export SVG/PNG. Put the AWS workload in a region boundary; keep local Streamlit and public APIs
outside. Do not invent VPC/private subnet/NAT resources absent from this deployment. Distinguish
orchestration arrows from data reads/writes. Show Bronze/Silver/Gold as prefixes of one lake bucket,
with scripts and Athena results as separate buckets. For the final drawing, show the deployed separate Silver, Gold and crawler roles and the
local dashboard assuming its reader role. The shared role above describes the pre-hardening state.

References:
- [draw.io AWS diagrams](https://www.drawio.com/docs/diagram-types/aws-diagrams/)
- [Official AWS icons](https://aws.amazon.com/architecture/icons/)
- [AWSGlueServiceRole](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSGlueServiceRole.html)
- [S3 transport conditions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html)
- [Step Functions role and trust](https://docs.aws.amazon.com/step-functions/latest/dg/procedure-create-iam-role.html)
