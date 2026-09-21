# Monitoring AWS và xem dữ liệu ngày 17/09/2026

## Trạng thái triển khai

Đã deploy ngày **17/09/2026**, region **Singapore / ap-southeast-1**:
Terraform thêm 16 tài nguyên, không sửa/xóa tài nguyên pipeline đang chạy.
Kiểm tra sau deploy: Terraform không còn thay đổi chờ áp dụng. Chưa commit/push phần monitoring;
mọi commit tiếp theo cần người dùng duyệt.

- Lambda `jobmarket-aws-health-monitor` đọc metadata, không có quyền sửa dữ liệu hoặc chạy pipeline.
- EventBridge `jobmarket-aws-health-monitor-schedule` chạy **mỗi 15 phút**, độc lập lịch ingestion 01:00.
- Hai metric trong namespace `JobMarket/Pipeline`, dimension `Project=jobmarket-aws`:
  `Healthy=1` và `SourceSuccessRatio=1.0` đã được ghi nhận trên CloudWatch.
- Đã thấy invocation do `aws.events` trong log Lambda, ngoài lần gọi kiểm chứng thủ công.
- SNS topic: `jobmarket-aws-pipeline-alerts`, dùng email AWS Budget đã được người dùng chọn.
- Email: **Đang chờ xác nhận email (PendingConfirmation)**. AWS chưa gửi cảnh báo tới inbox cho tới khi người nhận bấm Confirm subscription.
- Log Lambda giữ 14 ngày. Topic/alarm/Lambda có chi phí vận hành AWS riêng.

## Năm cảnh báo đang có

| Tên (prefix jobmarket-aws-) | Khi nào cảnh báo | Xử lý ban đầu |
| --- | --- | --- |
| pipeline-failed | Có execution FAILED; Sum trong 5 phút >= 1 | Mở Step Functions, xem bước lỗi |
| pipeline-timed-out | Có execution TIMED_OUT; Sum trong 5 phút >= 1 | Kiểm tra job còn chạy và khóa trước khi chạy lại |
| pipeline-aborted | Có execution ABORTED; Sum trong 5 phút >= 1 | Kiểm tra job và làm theo runbook phục hồi khóa |
| pipeline-unhealthy | Healthy < 1 hoặc mất metric trong 2 kỳ 15 phút | Kiểm tra Lambda logs, marker và tuổi ingestion |
| source-coverage-low | Tỷ lệ board thành công < 100% trong 2 kỳ 15 phút | Xem board lỗi trong manifest của lượt đã công bố |

Health check chặn ingestion cũ quá **26 giờ**, thời gian ở tương lai, metadata thiếu/sai,
và Silver mới chưa được Gold công bố hoàn chỉnh. Hai kỳ đánh giá giúp tránh báo động do khoảng
chuyển tiếp ngắn khi pipeline đang ghi dữ liệu. Độ trễ nhận cảnh báo còn phụ thuộc thời điểm
metric đến CloudWatch; đây không phải cam kết báo đúng sau 30 phút.

Metric Healthy bị thiếu được coi là lỗi, nên bộ giám sát ngừng chạy cũng bị phát hiện.
Ba metric execution là sự kiện thưa, vì vậy thiếu dữ liệu được coi là bình thường.
Coverage chỉ đo board thành công/lỗi; Arbeitnow bị giới hạn phân trang nhưng gọi API thành công
không tự làm giảm coverage. Nếu ingestion thất bại trước khi công bố manifest thì cảnh báo
execution xử lý; coverage không đại diện cho lượt thất bại chưa commit.

SNS gửi khi alarm chuyển sang ALARM; hai alarm health/coverage cũng gửi khi trở lại OK.
Các cảnh báo này không tự khởi động lại pipeline và không tự xóa khóa DynamoDB.

## Xác nhận email và kiểm chứng

1. Mở email dùng cho AWS Budget, tìm thư **AWS Notification - Subscription Confirmation**.
2. Kiểm tra topic kết thúc bằng `jobmarket-aws-pipeline-alerts`, bấm **Confirm subscription**.
3. Trong AWS SNS → Topics → topic trên → Subscriptions, trạng thái phải được xác nhận.

Đã kiểm chứng:

- **56 test passed**, gồm dữ liệu cũ/future, nguồn lỗi, metadata thiếu/sai, AccessDenied và lỗi ghi metric.
- Gọi Lambda trên AWS trả về `healthy=true`, coverage 100%; metric đã hiện trong CloudWatch.
- Alarm tạm `jobmarket-aws-monitoring-TEST` ghi nhận **CloudWatch → SNS action Succeeded**.
  Alarm thử đã được xóa, không làm pipeline dữ liệu thất bại.
- Việc SNS nhận action không chứng minh người dùng đã nhận/đọc email.
- Bằng chứng: [monitoring-20260917.json](evidence/monitoring-20260917.json).

## Xem tận mắt dữ liệu đã lấy hôm nay trên AWS

Luôn chọn đúng account `290488660407` và region **ap-southeast-1**.

### 1. Xem từng tin trong Athena

[Mở Athena](https://ap-southeast-1.console.aws.amazon.com/athena/home?region=ap-southeast-1#/query-editor)

1. Chọn workgroup **jobmarket-aws**, database **jobmarket_aws_gold**.
2. Mở **Saved queries**, chọn **Jobs observed today (Vietnam)** rồi Run.
3. Xem các cột company, title, location, apply_url và **observed_at_vietnam**.
4. Query giới hạn 100 hàng để xem thử; có thể thay LIMIT nếu cần xem thêm.

Saved query ID: `9f592987-4f80-4cd7-9170-c33d4ab3b221`.
Lần kiểm chứng thành công: `86cbf5c0-7aec-4350-8278-30eb7932519a` (xem Recent queries).
SQL trong repo: [inspect_today_jobs.sql](../sql/inspect_today_jobs.sql).
Ngày trong saved query tự đổi theo giờ Việt Nam; kết quả có thể rỗng trước lượt cập nhật đầu ngày.

Ngày 17/09 lúc kiểm chứng: **7.597 bản ghi raw**, **7.477 tin Gold được quan sát trong ngày**,
và **7.850 tin Gold tổng cộng**, thuộc **441 công ty**. Có 373 tin Gold giữ từ lượt trước.
Tin quan sát hôm nay không đồng nghĩa tin tuyển dụng được tạo mới hôm nay.
Ví dụ query hiển thị thời gian **2026-09-17 01:01:30** cho những tin Arbeitnow.

### 2. Xem dữ liệu gốc và manifest trên S3

[Mở Bronze của lượt 01:00 ngày 17/09](https://ap-southeast-1.console.aws.amazon.com/s3/buckets/jobmarket-aws-290488660407-ap-southeast-1-lake?region=ap-southeast-1&prefix=bronze%2Frun_id%3Dc86395ef-92e5-b0c2-a7af-48a3a3121c0a_404d2b08-be23-2011-32ea-a9eb2e781a11%2F&showversions=false)

Trong prefix đó, mở `source=.../board=.../jobs.json` để xem dữ liệu raw từng board.
Manifest của lượt nằm ở:

```text
control/ingestion/run_id=c86395ef-92e5-b0c2-a7af-48a3a3121c0a_404d2b08-be23-2011-32ea-a9eb2e781a11/manifest.json
```

Manifest ghi 36 board thành công, 0 lỗi; Arbeitnow `complete=false` vì giới hạn hai trang.
Thời gian UTC trong record/manifest phải cộng 7 giờ khi đối chiếu giờ Việt Nam.

### 3. Xem luồng chạy và cảnh báo

- [Step Functions](https://ap-southeast-1.console.aws.amazon.com/states/home?region=ap-southeast-1):
  state machine `jobmarket-aws-pipeline`, execution bắt đầu **01:00:16** ngày 17/09,
  kết thúc **01:08:14**, SUCCEEDED. Input có `source=aws.events` / `Scheduled Event`.
- [CloudWatch Alarms](https://ap-southeast-1.console.aws.amazon.com/cloudwatch/home?region=ap-southeast-1#alarmsV2:):
  lọc `jobmarket-aws-` để thấy 5 alarm.
- CloudWatch Logs: `/aws/lambda/jobmarket-aws-health-monitor` để xem từng lần kiểm tra.

`snapshot_date=2026-09-16` là đúng: 01:00 ngày 17/09 Việt Nam tương ứng 18:00 ngày 16/09 UTC.
Pipeline và monitoring đều tự chạy trên AWS khi máy cá nhân/Codex đóng.

## Cấu hình và vận hành

- `enable_monitor_email=true` đăng ký email hiện có; đặt false và apply để bỏ subscription đã xác nhận.
- `monitor_min_success_ratio=1.0` báo khi bất kỳ board nào lỗi trong manifest đã công bố.
- `freshness_hours=26` cấu hình giới hạn tuổi ingestion của monitor, đồng thời là cửa sổ Gold.
- Nếu pipeline lỗi/timeout/aborted, làm theo [operations.md](operations.md); không xóa khóa khi Glue còn chạy.
- Không dùng terraform destroy để dừng cảnh báo; việc đó có thể xóa data lake theo cấu hình hiện tại.

Tham khảo: [CloudWatch missing data](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-missing-data.html),
[Step Functions metrics](https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html).
