# Operations / Vận hành

The state machine is the production write path. Do not run individual Glue stages to repair a
production snapshot. Historical deployment observations live in [evidence](evidence/README.md).

## Deploy / Triển khai

Use Python 3.11, Java 17 for local Spark tests, AWS CLI v2, Terraform, and a configured AWS profile.
Review `infra/terraform.tfvars.example`; keep real emails and account settings out of Git.
For a new deployment only, copy the example to `infra/terraform.tfvars` and edit it.
Do not overwrite an existing deployment's tfvars or state.

Terraform sets a 30-day retention on Glue's shared `/aws-glue/*` log groups (`infra/logs.tf`). In an
account where Glue has already run, those groups exist: adopt them once before the first apply with
a temporary `import { for_each = local.glue_log_groups  to = aws_cloudwatch_log_group.glue[each.key]  id = each.key }`
block, then delete the block. They carry `skip_destroy`, so a destroy leaves them in place.

### Remote state / State trên S3 (one-time)

Terraform state lives in an S3 bucket created **by hand, outside this stack**, so destroying the
stack can never delete its own state. Locking uses the S3 lock file (`use_lockfile`, Terraform ≥ 1.10).
State bucket do bạn tạo tay một lần, không do stack quản lý; `backend.hcl` chứa tên bucket và không commit.

```powershell
$account = aws sts get-caller-identity --query Account --output text
$stateBucket = "jobmarket-aws-tfstate-$account-ap-southeast-1"
aws s3api create-bucket --bucket $stateBucket --region ap-southeast-1 `
  --create-bucket-configuration LocationConstraint=ap-southeast-1
aws s3api put-bucket-versioning --bucket $stateBucket --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket $stateBucket --public-access-block-configuration `
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
"bucket = `"$stateBucket`"" | Set-Content -Encoding utf8 infra/backend.hcl
```

Moving an existing local state: back up `infra/terraform.tfstate` outside the repo first, then run
`terraform -chdir=infra init -migrate-state -backend-config=backend.hcl` and answer `yes`.
A following `terraform plan` must show no unexpected changes. Every other machine runs the
`init -backend-config=backend.hcl` line below once; it then reads the same state.

```powershell
aws sts get-caller-identity
terraform -chdir=infra init -backend-config=backend.hcl
terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra validate
terraform -chdir=infra plan -out=workshop.tfplan
# Read the plan before applying; this creates billable resources.
terraform -chdir=infra apply workshop.tfplan
$pipelineArn = terraform -chdir=infra output -raw state_machine_arn
aws stepfunctions start-execution --state-machine-arn $pipelineArn --region ap-southeast-1
```

Initially keep `enable_schedule=false`. Verify ingestion, Silver DQ, Gold and crawler success,
matching state pointers, released lock and a fresh ingestion manifest. Then set
`enable_schedule=true`, review a new plan and apply. Default schedule: 18:00 UTC / 01:00 UTC+7.
Existing reference deployment already has this enabled; no laptop needs to stay on.

## Daily check / Kiểm tra mỗi ngày

```powershell
$pipelineArn = terraform -chdir=infra output -raw state_machine_arn
$lake = terraform -chdir=infra output -raw lake_bucket
aws events describe-rule --name jobmarket-aws-pipeline-schedule --region ap-southeast-1
aws stepfunctions list-executions --state-machine-arn $pipelineArn --max-results 3 --region ap-southeast-1
python scripts/check_freshness.py --bucket $lake --region ap-southeast-1
```

If using a different project prefix, substitute its rule name from Terraform outputs.
`check_freshness.py` exits 0 only for matching published identities and ingestion no older than
26 hours; it does not replace the dashboard's additional lock check. Inspect successful, failed
and incomplete board counts even when the command passes.

Trong AWS Console, chọn **Singapore** rồi:
1. EventBridge → Rules → pipeline schedule: kiểm tra ENABLED và lịch.
2. Step Functions → pipeline → Executions: mở lượt hôm nay, kiểm tra SUCCEEDED và input scheduled event.
3. S3 → lake → `state/pipeline/latest.json`: lấy run ID và snapshot UTC.
4. Mở `control/ingestion/run_id=<run>/manifest.json`: xem thời gian quan sát, số bản ghi và lỗi nguồn.
5. Mở dashboard theo [workshop](workshop.md), bấm Refresh; đối chiếu ngày UTC và ingestion age.

English: verify the enabled rule, today's execution and trigger input, the completion marker and
matching ingestion manifest, then refresh the dashboard. A list of S3 buckets alone proves none
of these. 01:00 on 28 September in Vietnam corresponds to snapshot 27 September UTC.

**Iceberg shadow phase (task 5.1 A).** After a scheduled run, compare the published Parquet marts
with the shadow Iceberg copy. Exit code 0 means `MATCH`; keep the dated JSON as evidence. A shadow
failure never blocks publication: read `state/gold_iceberg/run_id=<run>.json` for the failed table
and error, fix it, and let the next run retry.

```powershell
python scripts/compare_iceberg.py --output docs/evidence/iceberg-parity-<yyyymmdd>.json
```

Giai đoạn A: Parquet vẫn là dữ liệu chính thức. Chỉ chuyển dashboard sang Iceberg khi nhiều ngày
liên tiếp đều `MATCH`.

## Open the dashboard / Mở dashboard

There are three separate actions:

| Action | Where it runs | When you need it |
| --- | --- | --- |
| Deploy with Terraform | Creates/updates AWS resources | First setup or reviewed infrastructure/code changes |
| Run the pipeline | AWS: EventBridge → Step Functions → Glue | Automatically at 01:00 UTC+7; optional manual full execution |
| Open the dashboard | Your computer; reads published AWS data | Whenever you want to inspect results |

**VI:** Stack hiện tại đã deploy và bật lịch. Để xem dữ liệu, chỉ mở dashboard; không cần
`terraform apply` hay chạy lại Glue. Tắt máy không dừng lịch AWS. Dashboard không crawl dữ liệu.

Run PowerShell from the repository root (the folder containing this README, `infra/` and `dashboard/`).
Use an authenticated AWS profile authorized to assume the configured dashboard reader.

One-time local setup (reuse an existing suitable environment if you have one):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r dashboard/requirements.txt
```

Each new terminal session:

```powershell
# Optional: set AWS_PROFILE to your existing configured profile; otherwise use default.
# $env:AWS_PROFILE = 'your-profile'
aws sts get-caller-identity
$env:AWS_REGION = 'ap-southeast-1'
$env:LAKE_BUCKET = terraform -chdir=infra output -raw lake_bucket
$env:PIPELINE_LOCK_TABLE = terraform -chdir=infra output -raw pipeline_lock_table
$env:ATHENA_DATABASE = terraform -chdir=infra output -raw gold_database
$env:ATHENA_WORKGROUP = terraform -chdir=infra output -raw athena_workgroup
$env:AWS_DASHBOARD_ROLE_ARN = terraform -chdir=infra output -raw dashboard_reader_role_arn
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.address 127.0.0.1
```

Open **http://localhost:8501** and press **Refresh data**. Keep the terminal running; `Ctrl+C`
stops only the dashboard. Repeat the environment-variable block after opening a new terminal.
No Java/Spark installation is needed just to view the dashboard.

These `terraform output` commands read the existing deployment state from S3; they do not deploy anything.
On a new machine run `terraform -chdir=infra init -backend-config=backend.hcl` once first
(see [remote state](#remote-state--state-trên-s3-one-time)). A fresh clone has no AWS credentials. If outputs are unavailable, obtain
these six settings from the existing deployment's operator; do not create another stack just to view it.
If reusing a different venv, substitute its Python executable consistently in both commands.

The reader role must trust your authenticated principal. Missing credentials or an AssumeRole denial
must be resolved through that profile/trust configuration, not by removing the reader restriction.
The page blocks stale or in-progress publication. An idle page does not refresh automatically.
See [daily checks](#daily-check--kiểm-tra-mỗi-ngày) and [troubleshooting](#dashboard-troubleshooting--mở-giao-diện-và-xử-lý-lỗi) and [the lab guide](workshop.md)
for new deployments, manual runs, monitoring and cleanup.

## Dashboard troubleshooting / Mở giao diện và xử lý lỗi

Use the [startup commands above](#open-the-dashboard--mở-dashboard). Opening a dashboard is independent
of deploying resources or starting a pipeline execution.

| Symptom | Check / Cách xử lý |
| --- | --- |
| `localhost:8501` cannot open | Start Streamlit and keep its terminal running; read the URL printed in the terminal. |
| `No module named streamlit` | Install `dashboard/requirements.txt` using the same Python executable that starts the app. |
| Next run fails with `PipelineBusy` | Check the lock-janitor log; `waiting_for_glue` means a Glue run is still active. Otherwise follow the lock recovery steps. |
| Missing environment settings | Run the full environment-variable block again in this terminal; check each Terraform output succeeded. |
| Terraform has no outputs | Run `terraform -chdir=infra init -backend-config=backend.hcl` once (state is in S3, see Deploy); otherwise obtain output values from the operator. |
| Credentials expired / AssumeRole denied | Check `aws sts get-caller-identity`, your AWS profile/session and the reader's explicit trusted principal. |
| Pipeline in progress | Wait for the full execution to finish, then Refresh; the guard prevents reading during writes. |
| Older than 26 hours / mismatched publication | Follow the daily check and recovery steps; do not change timestamps or disable the guard. |
| Snapshot is yesterday | Normal at 01:00 UTC+7: the partition uses the previous UTC calendar date. |
| Address already in use | Reuse the existing dashboard or start with `--server.port 8502` and open the printed URL. |

For a deliberate manual ingestion, use the complete Step Functions execution command in Deploy.
It consumes AWS resources; it is unnecessary just to view current data. Never start individual
Glue stages to bypass the orchestration. `Ctrl+C` on the local dashboard does not pause AWS;
use the Pause procedure below when intentionally stopping future scheduled ingestion.

## Failure and recovery / Xử lý lỗi

A successful, complete board can confirm an absent job closed. A failed/incomplete board cannot;
Silver retains its previous observation, Gold excludes it once outside the 26-hour observation
window, and unobserved jobs become inactive after seven days. See [architecture](architecture.md).

A failed workflow should release its lock when caught. Check CloudWatch logs and fix the cause,
then start a **new complete execution**. Gold writes are not transactional across marts; an
interrupted same-day run can leave mixed files until repaired. Never use MAX(snapshot_date) as
proof of healthy publication. Do not redrive only an inner task or bypass lock ownership.

**Automatic lock recovery.** The `jobmarket-aws-lock-janitor` Lambda runs when an execution ends
FAILED/TIMED_OUT/ABORTED and every 15 minutes. It deletes the lock only when its owner execution
has ended, none of the three Glue jobs has an active run, and the crawler is READY; the delete is
conditional on the inspected owner ARN. It never stops jobs or starts executions. Check its
decision in `/aws/lambda/jobmarket-aws-lock-janitor` (`action`: `released`, `waiting_for_glue`,
`waiting_for_crawler`, `owner_running`, `no_lock`). A Glue run that outlives its execution keeps
the lock until it ends (at most its job timeout); then the next sweep releases it.

Tự động: janitor chỉ gỡ khóa khi execution đã kết thúc **và** Glue/crawler đã dừng. Sau khi log
báo `released`, bắt đầu một execution mới đầy đủ. Nếu alarm `lock-janitor-errors` báo lỗi, làm thủ công:

Manual fallback for an aborted/timed-out run with an orphaned lock:
1. Read `LockId=pipeline` in the lock table and record its `ExecutionArn`.
2. Confirm that execution ended, all three Glue jobs have no active runs, and the crawler is READY.
   Stopping Step Functions does not guarantee external Glue jobs stopped.
3. Delete only that lock with a DynamoDB condition requiring the inspected owner ARN to still match.
4. Start a new execution and verify freshness and dashboard publication.

Không xóa lock khi Glue còn ghi dữ liệu. Không dùng Terraform destroy để sửa lỗi.

## Pause / Tạm nghỉ

Set `enable_schedule=false`, plan and apply. This prevents new daily executions, but does not stop
an existing execution or remove storage/monitoring charges. The independent monitor will report
staleness after 26 hours; this is expected while paused. Resume by reviewing and applying true.

## Cleanup / Kết thúc lab

**Destructive when explicitly enabled:** bucket `force_destroy` now defaults to false. An opted-in cleanup can empty
Bronze, Silver, Gold and state. Export required evidence and data first. Never execute cleanup
against another project's resources or a shared stack.

1. Disable the daily schedule as above and wait for active pipeline/Glue/crawler work to finish.
2. Export evidence and required datasets outside the buckets being removed. Verify the export.
3. Confirm the AWS account, region, Terraform state and resource list belong to this lab.
4. Review a destroy plan; execute only when intentionally retiring the lab.

```powershell
aws sts get-caller-identity
terraform -chdir=infra state list
# In terraform.tfvars, set allow_bucket_force_destroy=true ONLY for intentional cleanup.
# First plan/apply that flag after reviewing its scope; no bucket data is deleted by the flag alone.
terraform -chdir=infra plan -out=enable-cleanup.tfplan
terraform -chdir=infra apply enable-cleanup.tfplan
terraform -chdir=infra plan -destroy -out=cleanup.tfplan
# Only after reviewing the destructive plan and verifying exports:
terraform -chdir=infra apply cleanup.tfplan
```

   The shadow Iceberg tables are created by the Gold job, not by Terraform; destroying the
   `_iceberg` Glue database removes them, and their files live under `gold_iceberg/` in the lake.
5. Verify stack resources are gone. Review residual Glue-created log groups, artifacts and resources
   created manually outside Terraform; remove only those owned by this lab after retaining evidence.
6. Stop the local Streamlit process, remove local downloaded data if no longer needed, and check
   Billing later for delayed charges. Keep private state backups secure; never commit them.

Cleanup is documented, **not executed** on the current reference deployment.
