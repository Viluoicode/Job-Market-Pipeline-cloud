# Hướng dẫn tự vẽ và review kiến trúc AWS

Mục 7–8 của Giai đoạn 2. Đối chiếu Terraform tại baseline `92a6e4c`, ngày 24/09/2026.
Tài liệu này là hướng dẫn học và checklist review, không phải bản vẽ cuối do sinh viên thực hiện.
Không kiểm tra lại deployment live trong lần cập nhật tài liệu này; evidence vận hành có ngày
riêng tại [evidence](evidence/README.md).

## 1. Đầu ra cần bàn giao

Bạn tự tạo hai trang trong một file draw.io:

1. **System architecture:** AWS services, data/control/metadata flows, boundary, monitoring.
2. **Execution and failure handling:** thứ tự state machine, quality gate và phục hồi lỗi.

Lưu file editable tại `docs/diagrams/job-market.drawio`, export từng trang thành PNG hoặc SVG.
Tên gợi ý: `system-architecture.png`, `execution-flow.png`. Đây là đường dẫn dự kiến;
chưa có file bản vẽ trong repo lúc lập hướng dẫn.

Lấy icon từ [AWS Architecture Icons](https://aws.amazon.com/architecture/icons/).
[Rubric FCAJ](https://hcm-rules.awsfcaj.com/5-scoring/) yêu cầu người học tự thiết kế/vẽ,
giải thích và kiểm chứng implementation. Ghi nhận AI đã hỗ trợ review/hướng dẫn;
không nhận sơ đồ do AI tạo hoàn toàn là sản phẩm tự thiết kế.

## 2. Chuẩn bị và dựng bố cục

1. Mở [draw.io](https://app.diagrams.net/), tạo diagram trống và lưu trên máy.
2. Dùng icon AWS chính thức; kiểm tra tên service thay vì chọn icon có màu gần giống.
3. Vẽ khung AWS account, bên trong có region `Asia Pacific (Singapore) / ap-southeast-1`.
4. Đặt public APIs bên trái ngoài AWS, analyst/operator/email recipient ngoài boundary.
5. Bố trí hàng trên cho điều phối, hàng giữa cho data flow, hàng dưới cho monitoring.
6. IAM và AWS Budgets đặt như quản trị account; Terraform nằm ở môi trường operator.
7. Tạo legend: nét liền xanh = dữ liệu; nét đứt đen = điều khiển; nét chấm tím = metadata/metrics.
   Luôn có nhãn và kiểu nét để đọc được cả khi in đen trắng.

Không có VPC/subnet/NAT trong Terraform stack được review. Không thêm chúng vào bản vẽ hiện trạng.
Đây là scheduled EventBridge rules, không phải resource EventBridge Scheduler.

## 3. Đặt các service trước khi nối dây

| Nhóm | Icon/nhãn trên bản vẽ | Resource hoặc file đối chiếu |
| --- | --- | --- |
| Nguồn | Greenhouse, Lever, Ashby, Arbeitnow — 36 configured boards | `ingestion/sources.json` |
| Điều phối | Amazon EventBridge — daily rule | `infra/schedule.tf` |
| Điều phối | AWS Step Functions — Standard | `aws_sfn_state_machine.pipeline` |
| Khóa | Amazon DynamoDB — execution lock | `aws_dynamodb_table.pipeline_lock` |
| Ingestion | AWS Glue — Python Shell, 0.0625 DPU | `aws_glue_job.ingestion` |
| Transform | AWS Glue — BronzeToSilver + embedded Data Quality | `aws_glue_job.bronze_to_silver` |
| Transform | AWS Glue — SilverToGold | `aws_glue_job.silver_to_gold` |
| Storage | Amazon S3 — lake bucket | `aws_s3_bucket.lake` |
| Storage | Amazon S3 — scripts and temporary files | `aws_s3_bucket.scripts` |
| Storage | Amazon S3 — Athena results | `aws_s3_bucket.athena_results` |
| Metadata | AWS Glue Crawler; AWS Glue Data Catalog | `aws_glue_crawler.gold`, `aws_glue_catalog_database.gold` |
| Query | Amazon Athena — workgroup jobmarket-aws | `aws_athena_workgroup.main` |
| Giám sát | Amazon EventBridge — 15-minute rule; AWS Lambda | `infra/monitoring.tf` |
| Giám sát | Amazon CloudWatch — logs, metrics, 5 alarms | `infra/monitoring.tf`, `infra/glue.tf` |
| Thông báo | Amazon SNS — pipeline alerts | `aws_sns_topic.pipeline_alerts` |
| Quản trị | AWS IAM runtime roles; AWS Budgets | `infra/iam.tf`, `infra/budget.tf` và các role trong file khác |

Trong lake bucket, thể hiện các ô prefix: `bronze/`, `control/ingestion/`, `silver/runs/`,
`gold/`, `quality/`, `state/`. Chúng thuộc **một bucket**, không phải sáu bucket.
Gold có ba mart: fact_job_posting, demand_by_role, role_opportunity.

## 4. Nối luồng theo bảng này

Mũi tên dữ liệu hướng theo dữ liệu di chuyển; mũi tên điều khiển hướng từ bên gọi tới bên nhận.
Như vậy `S3 -> Athena` với nhãn “read Gold data” diễn tả dữ liệu được Athena đọc từ S3.

| Từ -> đến | Loại / nhãn | Điều cần giải thích |
| --- | --- | --- |
| Daily rule -> Step Functions | Control: StartExecution, 18:00 UTC | 01:00 hôm sau tại Việt Nam |
| Step Functions -> DynamoDB | Control: conditional acquire/release | DynamoDB không tự khởi động Glue |
| Step Functions -> từng Glue job | Control: StartJobRun.sync | Chạy tuần tự và chờ hoàn tất |
| APIs -> ingestion | Data: HTTPS responses | Ingestion chủ động fetch API; có retry lỗi tạm |
| Scripts S3 -> Glue jobs | Data: code/config | Cấu hình sources và script do Terraform upload |
| Ingestion -> Bronze + manifest | Data: records then committed manifest | Manifest được ghi sau các board objects |
| Bronze + manifest + previous Silver -> Silver job | Data: exact run inputs | Không quét toàn bộ raw lịch sử như input mới |
| Silver job -> Silver + quality + Silver pointer | Data: DQ-gated commit | Pointer chỉ advance sau quality gate và write thành công |
| Committed Silver + pointer -> Gold job | Data: exact committed run | Chỉ active/fresh, dedup nội dung |
| Gold job -> S3 Gold | Data: daily partition | Ba mart không có transaction chung |
| Step Functions -> crawler | Control: start, poll, verify | READY chưa đủ; kiểm tra LastCrawl của đúng execution |
| S3 Gold -> crawler -> Catalog | Data sample then metadata | Catalog giữ schema/partition, không giữ posting data |
| Analyst -> Athena | Control: SQL | Consumer kiểm tra health trước khi dùng snapshot hiện tại |
| Catalog -> Athena | Metadata: schema/partitions | Tách khỏi luồng dữ liệu S3 |
| S3 Gold -> Athena -> results S3 | Data: scan then query output | Workgroup enforce output location và scan cap |
| Step Functions -> pipeline marker | Data/control: publish completion | Sau khi crawl được xác minh thành công |
| Monitor rule -> Lambda | Control: every 15 minutes | Độc lập với daily execution |
| Manifest + state markers -> Lambda | Data: read metadata | Monitor không sửa lake hoặc chạy pipeline |
| Lambda -> CloudWatch | Metrics: Healthy, SourceSuccessRatio | Missing health metric được coi là breaching |
| Step Functions -> CloudWatch | Metrics: failed/timed-out/aborted | Ba execution alarms ngoài hai health/coverage alarms |
| Glue/Lambda -> CloudWatch | Logs | Chi tiết lỗi phục vụ operator |
| CloudWatch -> SNS -> email | Alarm action then notification | Email cần confirmed; test email có evidence riêng |

Scripts bucket có `tmp/` để Glue ghi temporary files. Nếu sơ đồ chính quá nhiều dây,
đưa nhánh scripts/temp và IAM vào ô chú giải “supporting resources”, không nhầm thành data source.
Budgets theo dõi billing và báo chi phí tới email; không nằm trên luồng crawl, không tự chặn chi phí.

## 5. Trang execution: vẽ theo state machine thật

Vẽ các bước: InitializeRun -> AcquireLock -> Ingest -> BronzeToSilver -> SilverToGold ->
MarkCrawlerStart -> StartCrawler -> WaitForCrawler -> GetCrawler -> CrawlerFinished ->
CrawlerSucceeded -> BuildCompletion -> PublishCompletion -> ReleaseLock -> Succeeded.

Thêm nhánh:

- AcquireLock bị conditional failure -> PipelineBusy -> Fail.
- Crawler chưa READY -> WaitForCrawler; READY nhưng crawl sai/thất bại -> CrawlerFailed.
- Lỗi được Catch trong RunPipeline -> ReleaseFailedLock -> Failed.
- Abort hoặc timeout toàn execution: có thể còn external job/lock; operator theo runbook.
  Không vẽ như mọi trường hợp đều tự release lock.

`RunPipeline` là Parallel state chỉ có **một branch** để gom Catch, không phải ba Glue job chạy song song.
Giới hạn workflow 90 phút không phải cam kết hoàn thành hoặc cam kết chi phí tối đa mỗi tháng.

## 6. Chú thích ngắn nên có trên bản vẽ

- Daily scheduled batch; no dependency on a personal computer.
- Bronze/manifest 30d; quality 90d; results/temp 7d; Silver/Gold/state retained.
- SSE-S3 + Block Public Access; shared Glue role still needs least-privilege hardening.
- UTC snapshot_date; display observations in Vietnam time for user inspection.
- Ingestion gate >=80% successful boards; coverage monitoring target 100%.
- Gold freshness 26h; Silver stale expiry 7d; capped feeds are marked incomplete.
- Completion marker is a health convention, not an atomic transaction across Gold marts.

Lake versioning được quan sát ngày 21/09 ở ngoài Terraform management. Không gắn nhãn
“fully managed backup/restore” hoặc “all security findings fixed” lên sơ đồ.

## 7. Tự giải thích trước khi nộp

| Câu hỏi | Ý chính bạn cần tự trình bày |
| --- | --- |
| Tắt laptop thì ai chạy? | EventBridge và Step Functions trên AWS |
| Tại sao có lock? | Tránh overlapping writers và state corruption |
| API lỗi có làm tin đóng ngay? | Không; giữ last real observation và áp dụng freshness/expiry |
| Gold ở đâu? | Parquet trong S3; Catalog chỉ metadata; Athena đọc S3 |
| Sao execution thành công vẫn cần monitor? | Dữ liệu có thể cũ, publication thiếu hoặc không có execution mới |
| Vì sao dùng Spark cho dataset nhỏ? | Managed Glue/DQ và mục tiêu học; chưa chứng minh tối ưu hơn single-node |
| Bảo mật còn thiếu gì? | Shared Glue permissions, TLS-only policy, state/restore controls |
| Chi phí nào chính? | Glue compute/crawler; credit không đồng nghĩa usage miễn phí |

## 8. Acceptance checklist và trạng thái

- [ ] File editable do bạn tự vẽ và hai ảnh export có thể đọc được.
- [ ] Các node/arrow khớp bảng mapping ở trên và Terraform hiện tại.
- [ ] Data/control/metadata phân biệt bằng legend và nhãn.
- [ ] Không có dịch vụ tương lai được thể hiện như đã deploy.
- [ ] Không nhầm bucket/prefix, passive lock/trigger, Catalog/storage.
- [ ] Failure path và publication limitation giải thích đúng.
- [ ] Bạn tự giải thích được tám câu hỏi ở mục 7.
- [ ] Review bản vẽ với execution thật tại thời điểm demo, ghi ngày/evidence.

Trạng thái 24/09: hướng dẫn và đối chiếu code đã hoàn thành; chưa có bản vẽ người học,
nên mục 7 và review cuối mục 8 vẫn mở. Review live tiếp theo là read-only; không cần chạy lại
Glue chỉ để chụp hình. Chưa commit/push tài liệu này.

English summary: This is an assisted drawing guide, not the learner's final architecture.
Create an editable system diagram and an execution/failure view using official AWS icons.
The final review must check service boundaries, three S3 buckets, separate data/control/metadata
paths, passive locking, DQ-gated publication, Athena's S3 reads and independent monitoring.
Source-code review is complete; learner authorship and final diagram/live evidence review remain pending.
