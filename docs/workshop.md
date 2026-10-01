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

Follow [Run the dashboard in README](../README.md#run-the-dashboard) for the canonical
one-time setup and per-terminal commands. Reuse your existing environment or create `.venv`.
The dashboard does not require Java or a local Spark runtime.

**VI:** M? PowerShell t?i root repo ? x?c th?c AWS ? ??t s?u bi?n m?i tr??ng theo README ?
ch?y Streamlit ? m? **http://localhost:8501** ? b?m **Refresh data**. Gi? terminal m?;
`Ctrl+C` ch? d?ng giao di?n, l?ch AWS v?n ch?y. Kh?ng c?n deploy l?i ?? xem d? li?u.

**EN:** The existing stack continues ingestion independently. Starting Streamlit only reads the
published snapshot; it does not start ingestion. A fresh clone needs the existing stack settings
and authorized AWS credentials because private Terraform state is not committed.

Before deployment, set `dashboard_reader_principal_arns` in private tfvars to the IAM user/role
launching the dashboard. That principal needs sts:AssumeRole on the reader. Credentials are
exchanged for a restricted one-hour session; the app stops if role assumption fails.
VI: Cấu hình ARN người/role được phép assume reader trong tfvars; không dùng ARN account-root.


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

### Architecture artifact placement

1. L?u file s?a ???c ? `docs/diagrams/architecture.drawio`; xu?t `architecture.png` (ho?c SVG)
   v?o c?ng th? m?c. T?o th? m?c khi c? artifact th?t; kh?ng commit ?nh placeholder.
2. Trong **README ? Architecture**, ??t ?nh ngay d??i heading, tr??c ?o?n gi?i th?ch lu?ng:

   ```markdown
   ![AWS architecture](docs/diagrams/architecture.png)
   ```

   Thay Mermaid b?ng ?nh ho?c ??a Mermaid v?o kh?i thu g?n; gi? ph?n m? t? v? link t?i li?u.
3. Trong `docs/architecture.md`, th?m ?nh `![AWS architecture](diagrams/architecture.png)`
   ngay d??i ti?u ?? ch?nh v? link `[Editable diagram](diagrams/architecture.drawio)`.
4. ??i chi?u b?n v? v?i deployment: AWS region Singapore; API v? local Streamlit ? ngo?i AWS;
   EventBridge ? Step Functions, DynamoDB lock, ba Glue jobs, S3 lake Bronze/Silver/Gold,
   crawler ? Catalog ? Athena, bucket scripts v? Athena results; health Lambda ? CloudWatch ? SNS.
   D?ng n?t kh?c nhau cho orchestration v? data; kh?ng t? th?m VPC/NAT/subnet ch?a tri?n khai.
5. IAM: th? hi?n role Silver, Gold, Crawler ri?ng v? dashboard assume reader role. ??i chi?u
   [ma tr?n quy?n](security-review.md), kh?ng d?ng role Glue chung c?a thi?t k? c?.

EN: Commit both editable diagram and export. Embed the export in README's Architecture section,
and link its source from the detailed architecture document. The diagram must reflect deployed
resources and the separate runtime roles, not an unimplemented target design.

### Final handoff checklist / Ki?m tra tr??c commit

- [x] Pipeline, daily schedule, lifecycle/DQ, monitoring and reader-role dashboard verified on AWS.
- [x] Security acceptance and same-day overwrite recovery documented (30 September).
- [x] Next scheduled execution and live dashboard checked (1 October); see
  [evidence](evidence/handoff-check-20261001.json). This is a dated observation, not a live guarantee.
- [x] Four PNG screenshots exist under `docs/screenshots/`.
- [ ] Add and personally verify the AWS-icon diagram as described above.
- [ ] Review screenshots at readable size; capture UTC date/freshness, label their snapshot,
  then embed overview/postings in README and filtered/verification in the lab/report.
- [ ] Supply demo video/link, saved Pricing Calculator estimate and bilingual report website
  for the workshop submission. Existing repo checks do not certify the entire program rubric.
- [ ] Review `git diff --check`, `git diff --stat`, `git status --short`, then stage chosen project
  files. Inspect `git diff --cached` before committing. Keep tfvars, state, credentials,
  local environments, logs and downloaded datasets out of Git.

The failed security test remains in historical evidence with its fix; do not erase it to make
all execution history appear successful. No extra production rerun is needed just for a commit.

## 9. Dashboard screenshots / Ảnh dashboard cần chụp

Use actual AWS data after Refresh; keep the snapshot date visible. Do not edit counts or hide a
coverage warning. Browser zoom 90–100%, a wide window, no open dropdowns/tooltips over charts.
Chụp PNG rõ chữ; không cần chụp cả desktop hoặc thanh tab có thông tin riêng tư.

| Suggested filename | What to capture / Nội dung |
| --- | --- |
| `dashboard-overview.png` | No filters. Title, UTC date/freshness, coverage, four KPIs and both charts. If one frame is too tall, split overview and charts instead of shrinking text. |
| `dashboard-postings.png` | Posting table with title, company, location, source, last observation and original link; show 8–12 readable rows. Scroll horizontally if needed for the link. |
| `dashboard-filtered.png` | Select Data Engineer (or Backend Engineer); keep the selected filter, changed KPIs/chart and part of the results visible. Use the same snapshot as the overview. |
| `dashboard-verification.png` | Expand Data scope and verification; capture run ID, raw count, Athena query ID and scan size. This image supports the report rather than the README hero. |

Save reviewed images in `docs/screenshots/`. Use overview + postings in README, filtered +
verification in the lab/report. Include a caption with capture date and UTC snapshot. A screenshot
is a point-in-time observation, not proof that daily scheduling remains healthy.

**VI:** Bộ ảnh dashboard chưa thay thế bằng chứng AWS. Khi quay demo, bổ sung execution Step
Functions, manifest/DQ và CloudWatch alarms theo mục 6. Không cố tình làm lỗi pipeline thật chỉ để
chụp cảnh báo; có thể minh họa trường hợp không có kết quả bằng một từ khóa không khớp.
