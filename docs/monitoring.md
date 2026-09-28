# Monitoring / Giám sát

## Current reference check — 28 September 2026

The health-monitor Lambda and its independent 15-minute EventBridge rule are deployed. All five
project alarms were **OK**, with actions enabled, at this check. The email SNS subscription is
**confirmed**. This verifies configuration, not receipt of a new email; no test email was sent.
Historical deployment/test evidence remains in [monitoring-20260917.json](evidence/monitoring-20260917.json).

Lambda reads control metadata, emits `Healthy` and `SourceSuccessRatio` in `JobMarket/Pipeline`,
and has no lake-write or pipeline-start permissions. Logs are retained for 14 days.

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


## Email verification / Kiểm tra email

For a new subscription, confirm the AWS email after checking the topic name. In SNS, verify that
SubscriptionArn is no longer PendingConfirmation. Do not infer inbox delivery from a successful
CloudWatch → SNS action; check the recipient's actual inbox when performing an authorized test.

Với triển khai mẫu, subscription đã xác nhận ở lần kiểm tra 28/09. Không cần đăng ký lại.
Email không được ghi trong evidence công khai.

## Operator checks / Kiểm tra vận hành

Use [the daily runbook](operations.md#daily-check--kiểm-tra-mỗi-ngày) for executions, S3 metadata
and dashboard verification. CloudWatch Logs group: `/aws/lambda/jobmarket-aws-health-monitor`.
For failures, inspect stage logs and follow the lock recovery procedure; alarms never remove locks
or automatically restart the data pipeline.

The saved Athena query **Jobs observed today (Vietnam)** uses Vietnam's timezone to select
observations. Its name does **not** mean jobs located in Vietnam. `sql/inspect_today_jobs.sql`
contains the query. Check publication health before running exploratory SQL.

## Configuration / Cấu hình

- `enable_monitor_email=true`: subscribe the configured alert email; confirmation is required.
- `monitor_min_success_ratio=1.0`: warn if any board failed in the published manifest.
- `freshness_hours=26`: monitor ingestion age; dashboard uses the workshop's fixed 26-hour limit.
- A paused daily pipeline will eventually trigger stale-data alarms; pausing does not stop monitoring.

See [CloudWatch missing-data behavior](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-missing-data.html)
and [Step Functions metrics](https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html).
