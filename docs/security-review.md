# Security review / Quyền sau hardening — 2026-09-30

## Deployed changes / Thay đổi đã apply

Terraform now deploys separate `jobmarket-aws-silver-role`, `jobmarket-aws-gold-role` and
`jobmarket-aws-crawler-role`. The shared `jobmarket-aws-glue-role` and AWSGlueServiceRole attachment
were removed. No runtime role can write the deployed scripts; only Step Functions among pipeline
runtime roles can write the completion marker. Administrative identities remain outside that claim.

| Role | Read | Write/delete/control |
| --- | --- | --- |
| Ingestion | Own script/config, ingestion manifests | Put Bronze/manifests, scoped Glue logs; no object delete |
| Silver | Bronze, manifests, previous Silver, Silver pointer; its script/shared contract | Put/Delete Silver runs and quality output; Put Silver pointer; own temp prefix |
| Gold | Silver runs, Silver pointer, manifests; its script/shared contract | Put/Delete only three Gold mart prefixes and their exact directory markers; own temp prefix |
| Crawler | Gold objects and intended Gold Catalog metadata | Create/update tables/partitions in the Gold database; logs; no S3 write/delete |
| Step Functions | Job/crawler status | Control three jobs/one crawler, Put/Delete only the pipeline lock key, Put completion marker |
| Daily EventBridge | No lake data | Start this state machine only |
| Health Lambda | Published metadata only | Its logs and JobMarket/Pipeline metrics only |
| Dashboard reader | Gold, metadata, pipeline lock, Gold Catalog | Athena SELECT workflow in its workgroup and result objects; no lake writes or Glue job execution |

Stage jobs retain bucket-level listing/metadata for their two buckets for Spark compatibility;
object reads/writes remain prefix-scoped. Glue log permissions cover `/aws-glue/*` in this account
and region; metrics are restricted to Glue/Glue Data Quality. Silver's DQ publish/read permission
covers Data Quality rulesets within this account/region, not all Glue actions. These are explicit
remaining scope choices, not claims of per-object/per-run isolation.

All three buckets enforce an insecure-transport Deny for non-service principals, have all public
access blocks enabled, and BucketOwnerEnforced ownership managed in Terraform. Lake versioning
was adopted without suspending it; scripts versioning is now enabled. Default SSE-S3 stays enabled.
No noncurrent versions were deleted or assigned a new expiration during this change. Old versions
continue to consume storage until an explicit recovery/retention policy is agreed.

`allow_bucket_force_destroy` now defaults to false. Cleanup must deliberately opt in before deleting
nonempty buckets. This protects the Terraform cleanup path, not deletion by a privileged IAM user.
Step Functions trust is scoped to this account and exact state-machine ARN. Existing service-only
Glue/EventBridge/Lambda trusts remain; no unverified service-specific condition was added blindly.

The dashboard requires `AWS_DASHBOARD_ROLE_ARN`, assumes the reader for one hour, refreshes its
cached session before expiry, and never falls back to the operator session for data queries.
The reader trust lists explicitly configured IAM principals, not account-root or everyone.
The operator's AdministratorAccess was not removed; bootstrap credentials remain available to the
local operator. This is not OS/process isolation or an authorization design for public hosting.

Implementation: `infra/iam_runtime.tf.json`, `infra/iam.tf`, `infra/glue.tf`, `infra/s3.tf`,
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
| Gold job | Committed Silver, Silver pointer, manifests; job scripts | Three Gold mart prefixes and scoped temp (Put/Delete for partition overwrite) | Scoped logs and metrics; required Glue calls |
| Crawler | Gold only | No S3 object write/delete | Read/update intended Catalog database/table/partition metadata; scoped logs |
| Step Functions | Run/crawler status | Lock item and completion marker only | Control the three jobs and one crawler |
| EventBridge | None | None | Start only this state machine |
| Health monitor | Publication metadata only | Own logs/metrics only | Keep existing namespace restriction |
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
