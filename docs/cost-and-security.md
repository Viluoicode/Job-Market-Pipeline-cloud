# Cost and security / Chi phí và bảo mật

## Budget assumptions / Giả định dự toán

This is a planning illustration, **not a measured bill or a Singapore price quote**. Review regional
rates in [AWS Pricing Calculator](https://calculator.aws/) before submission. Prices, account
eligibility, tax and credits can change. Capture and link the saved estimate in the workshop report.

Example workload: 30 daily runs/month; each Spark job uses two G.1X workers for five billed minutes;
Python Shell uses 0.0625 DPU for two minutes; crawler uses two DPUs for ten billed minutes. Replace
these assumptions with your observed billed duration. Glue startup/wall time is not automatically
billable DPU time. The AWS Glue pricing page illustrates $0.44/DPU-hour; regional rates can differ.

| Component | Example monthly calculation | Planning amount (USD) |
| --- | --- | --- |
| Two Spark jobs | 30 × 2 jobs × 2 DPU × 5/60 hours × $0.44 | 4.40 |
| Python Shell ingestion | 30 × 0.0625 × 2/60 × $0.44 | 0.03 |
| Crawler | 30 × 2 × 10/60 × $0.44 | 4.40 |
| Athena SQL | 1,000 queries × assumed 10 MB billable minimum × $5/TB | About 0.05 |
| S3, requests, logs, metrics/alarms, Step Functions, DynamoDB, Lambda, SNS, EventBridge | Provisional allowance; not a service-by-service quote | 5.00 |
| Illustrative total | Before credits/tax; excludes retries and public hosting | About 13.88 |

The $5 allowance must be replaced with per-service calculator entries for formal submission; this
example is not a spending ceiling. The existing $10 budget can alert below this illustration.
[AWS Glue pricing](https://aws.amazon.com/glue/pricing/) and
[Athena pricing](https://aws.amazon.com/athena/pricing/) explain billing models and examples.

**VI:** Credit 200 USD không biến dịch vụ thành miễn phí vô hạn. Theo dõi gross usage và credit còn
lại; không dự đoán số tháng dùng được từ một lượt chạy. EventBridge điều phối lịch; phần chi phí
đáng chú ý của lab nằm ở công việc lịch đó gọi, nhất là Glue và crawler.

**EN:** Credits offset eligible charges but do not cap usage. Track usage before credits as well as
remaining credit. A schedule can initiate billable work even while the laptop is off.

## Existing controls / Kiểm soát hiện có

- Daily batch instead of continuous compute; one writer and per-job concurrency limits.
- Glue timeouts, no job retries by default, API retry/backoff for transient source errors.
- Parquet and snapshot predicates; enforced Athena workgroup with 10 GiB per-query cutoff.
- Dashboard caches facts for 15 minutes; each interaction still checks publication metadata.
- Bronze/manifests 30 days, quality 90 days, Athena results/Glue temp 7 days.
- Silver, Gold and state have no automatic expiry: retained history continues to consume storage.
- Budget alerts are notifications, not a hard stop. Pausing the pipeline does not delete resources.

## Security / Bảo mật

Private buckets, SSE-S3 and HTTPS enforcement protect storage and transport. Separate stage roles
scope output writes and keep runtime code read-only. The local dashboard assumes an explicit
short-lived reader role; no admin fallback is used for data access. Terraform manages bucket
ownership and lake/scripts versioning. Never commit state, credentials or real tfvars.

The operator still has administrative access for deployment. IAM simulations and live acceptance
are scoped checks, not production certification. See [security-review](security-review.md).
Versioning preserves old versions: current-object expiration alone does not bound all storage.
No new noncurrent-version expiry has been enabled; account for retained versions in cost estimates.

For a dedicated dashboard reader, scope permissions to the specific resources:

| Service | Required capability |
| --- | --- |
| Athena | Start/Get/Stop query execution and GetQueryResults on the configured workgroup |
| Glue Catalog | Read database/table/partition metadata for the Gold database |
| Lake S3 | Read `gold/`, `state/`, `control/ingestion/`; required bucket listing/location |
| Result S3 | Read/write Athena result objects and required bucket metadata; never lake write access |
| DynamoDB | Consistent GetItem on the one pipeline-lock table |

Additional KMS permissions are needed if the deployment is changed to customer-managed keys.
These capabilities are now defined in the deployed dashboard-reader policy.
Use a short-lived profile for a supervised local demo. Public hosting, user authentication and
multi-user authorization are outside this workshop release.

**VI:** Không chia sẻ profile hay mở server local ra Internet. Dữ liệu từ website công khai vẫn cần
được dùng đúng điều kiện nguồn; không diễn giải metadata thiếu thành dữ kiện chắc chắn.

**EN:** Do not expose the local demo server or share credentials. Respect source terms and data
limitations. Keep a separate reviewer-visible record of remaining security trade-offs.

Detailed live IAM/S3 audit, current and target permission matrices: [Security review, 30 September 2026](security-review.md). The report distinguishes deployed controls from pre-hardening findings.
