# Operations / Vận hành

The state machine is the production write path. Do not run individual Glue stages to repair a
production snapshot. Historical deployment observations live in [evidence](evidence/README.md).

## Deploy / Triển khai

Use Python 3.11, Java 17 for local Spark tests, AWS CLI v2, Terraform, and a configured AWS profile.
Review `infra/terraform.tfvars.example`; keep real emails and account settings out of Git.
For a new deployment only, copy the example to `infra/terraform.tfvars` and edit it.
Do not overwrite an existing deployment's tfvars or state.

```powershell
aws sts get-caller-identity
terraform -chdir=infra init
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

## Failure and recovery / Xử lý lỗi

A successful, complete board can confirm an absent job closed. A failed/incomplete board cannot;
Silver retains its previous observation, Gold excludes it once outside the 26-hour observation
window, and unobserved jobs become inactive after seven days. See [architecture](architecture.md).

A failed workflow should release its lock when caught. Check CloudWatch logs and fix the cause,
then start a **new complete execution**. Gold writes are not transactional across marts; an
interrupted same-day run can leave mixed files until repaired. Never use MAX(snapshot_date) as
proof of healthy publication. Do not redrive only an inner task or bypass lock ownership.

For an aborted/timed-out run with an orphaned lock:
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

**Destructive:** this stack uses `force_destroy=true` on its project buckets. Destroy can empty
Bronze, Silver, Gold and state. Export required evidence and data first. Never execute cleanup
against another project's resources or a shared stack.

1. Disable the daily schedule as above and wait for active pipeline/Glue/crawler work to finish.
2. Export evidence and required datasets outside the buckets being removed. Verify the export.
3. Confirm the AWS account, region, Terraform state and resource list belong to this lab.
4. Review a destroy plan; execute only when intentionally retiring the lab.

```powershell
aws sts get-caller-identity
terraform -chdir=infra state list
terraform -chdir=infra plan -destroy -out=cleanup.tfplan
# Only after reviewing the destructive plan and verifying exports:
terraform -chdir=infra apply cleanup.tfplan
```

5. Verify stack resources are gone. Review residual Glue-created log groups, artifacts and resources
   created manually outside Terraform; remove only those owned by this lab after retaining evidence.
6. Stop the local Streamlit process, remove local downloaded data if no longer needed, and check
   Billing later for delayed charges. Keep private state backups secure; never commit them.

Cleanup is documented, **not executed** on the current reference deployment.
