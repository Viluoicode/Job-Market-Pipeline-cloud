# Workshop lab guide / Hướng dẫn thực hành

## 1. Mục tiêu / Outcome

**VI:** Hoàn thành một batch pipeline AWS tự chạy, theo dõi lifecycle và hiển thị dữ liệu thật qua
dashboard. Phạm vi là 36 board cấu hình sẵn, không phải sản phẩm tìm việc Việt Nam.

**EN:** Build and verify a scheduled AWS batch pipeline with lifecycle tracking and a one-page
analytics dashboard over its published output. The configured sample is not a whole-market census.

## 2. Chuẩn bị / Prerequisites

**VI:** Tài khoản AWS có quyền triển khai dịch vụ trong `infra/`; AWS CLI v2; Terraform phù hợp
`infra/versions.tf`; Python 3.11 và Java 17 nếu chạy Spark test. Cấu hình AWS profile bằng cơ chế của
tài khoản; không nhập access key vào code. Dashboard dùng credential chain của boto3.

**EN:** Use an AWS account authorized to deploy this stack, AWS CLI v2, Terraform compatible with
`infra/versions.tf`, Python 3.11 and Java 17 for Spark tests. Authenticate using your account's
supported profile mechanism. Keep credentials and tfvars outside source control.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
aws sts get-caller-identity
```

Nếu repo có sẵn môi trường, dùng môi trường đó; không cần tạo lại. / Reuse a suitable existing venv.

## 3. Hiểu kiến trúc / Understand the design

Read [proposal](proposal.md), [architecture](architecture.md) and [data dictionary](data-dictionary.md).

**VI:** EventBridge gọi Step Functions; lock tránh ghi đồng thời. Glue Python Shell tải API vào
Bronze; Glue Spark tạo Silver và Gold; crawler cập nhật Catalog; Athena đọc Parquet. Marker chỉ
công bố sau khi hoàn tất. Lambda độc lập kiểm tra dữ liệu mới và gửi metric/alarm qua CloudWatch/SNS.

**EN:** EventBridge starts Step Functions, which serializes writers with a DynamoDB lock. Python
Shell ingests Bronze; Spark builds Silver and Gold; a crawler updates the Catalog for Athena.
Completion metadata is published after success. An independent Lambda monitors publication health.

## 4. Triển khai và chạy / Deploy and run

**VI:** Làm theo [Deploy](operations.md#deploy--triển-khai). Với stack mới, giữ lịch tắt đến khi lượt
thủ công đạt; kiểm tra plan trước apply. Với stack đang chạy, kiểm chứng trước, không deploy lại vô cớ.

**EN:** Follow [Operations](operations.md). Keep the schedule disabled for the first manual run,
review the plan, deploy, start the state machine and inspect every stage. An existing healthy
reference stack does not need redeployment to run the dashboard.

Expected stages: ingestion → Silver and DQ → Gold → crawler → completion → lock release.
Kết quả mong đợi: từng stage thành công, manifest và state pointers cùng run ID.

## 5. Dashboard đọc AWS / Run the dashboard

From the repository root, with AWS access already configured:

```powershell
python -m pip install -r dashboard/requirements.txt
$env:AWS_REGION = 'ap-southeast-1'
$env:LAKE_BUCKET = terraform -chdir=infra output -raw lake_bucket
$env:PIPELINE_LOCK_TABLE = terraform -chdir=infra output -raw pipeline_lock_table
$env:ATHENA_DATABASE = terraform -chdir=infra output -raw gold_database
$env:ATHENA_WORKGROUP = terraform -chdir=infra output -raw athena_workgroup
python -m streamlit run dashboard/app.py --server.address 127.0.0.1
```

Open **http://localhost:8501**.

**VI:** Xem ngày UTC, tuổi ingestion và coverage trước. Lọc nguồn, role, địa điểm hoặc tiêu đề;
KPI, biểu đồ và bảng cùng thay đổi. Mở link gốc để kiểm tra một tin. Refresh kiểm tra publication
mới; trang để yên không tự refresh. Đang chạy pipeline hoặc dữ liệu cũ hơn 26 giờ sẽ bị chặn hiển thị.

**EN:** Read the UTC date, ingestion age and coverage first. Filter platform, role, reported location
or text; all cards and charts use the same filtered fact rows. Open an original posting. Refresh
rechecks publication. An idle page does not auto-refresh. A busy writer or stale data blocks display.

The dashboard runs locally; AWS ingestion continues when it closes. No public hosting is included.
Athena may write query results to its result bucket; the dashboard never writes the data lake.

## 6. Kiểm thử và bằng chứng / Test and verify

```powershell
python -m pytest tests -q
terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra validate
python scripts/check_freshness.py --bucket $env:LAKE_BUCKET --region $env:AWS_REGION
```

**VI:** Tests dùng mock API và Spark local; kiểm tra ingestion, lifecycle, Gold, monitoring và
publication của dashboard. Không dùng một test pass thay cho bằng chứng AWS. Theo
[kiểm tra mỗi ngày](operations.md#daily-check--kiểm-tra-mỗi-ngày), lấy execution ID, manifest,
DQ, query ID, số dòng và thời điểm. So với [kết quả đã đo](sample_results.md).

**EN:** Local tests cover mocked APIs, lifecycle, Gold, monitoring and dashboard publication gates.
They do not prove cloud deployment. Collect an execution ID, manifest, DQ outcome, query ID, counts
and timestamps from the actual stack and compare them with the dated reference results.

Demo sequence: explain the problem → architecture → scheduled execution → raw manifest →
Silver/Gold meaning → dashboard filters and original posting → monitoring → cost and cleanup.

## 7. Chi phí, bảo mật, cleanup / Cost, security and retirement

**VI:** Đọc [cost-and-security](cost-and-security.md). Budget không tự dừng chi tiêu. Chạy Glue hằng
ngày vẫn tiêu thụ credit. Khi kết thúc, dùng quy trình cleanup trong Operations và kiểm tra export
trước thao tác phá hủy.

**EN:** Review the cost assumptions and permission boundaries. Budgets notify rather than cap
spending. Daily Glue consumes billable resources even while credits cover invoices. Retire the
stack using the documented cleanup procedure only after exporting required evidence.

## 8. Bàn giao workshop / Submission boundary

Repo cung cấp code, tài liệu lab và bằng chứng. Người học cần tự hoàn thiện sơ đồ AWS-icon đã kiểm
chứng, giải thích quyết định thiết kế, quay video/demo, tạo estimate AWS Pricing Calculator và đưa
nội dung VI/EN vào website báo cáo theo template của chương trình. Chưa có URL website, video hay
estimate đã xuất bản trong repo; không đánh dấu các mục đó đã xong.

The repository supplies code, the lab and technical evidence. The learner still provides a verified
AWS-icon diagram, their design explanation, demo recording, a saved Pricing Calculator estimate
and the program's bilingual report website. Personal worklogs and internship evidence belong in
that report, not this code repository.

Requirements reviewed 28 September 2026:
[Workshop requirements](https://hcm-rules.awsfcaj.com/3-project/),
[Scoring criteria](https://hcm-rules.awsfcaj.com/5-scoring/).
