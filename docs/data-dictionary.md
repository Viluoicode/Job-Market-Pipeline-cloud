# Data dictionary / Ý nghĩa dữ liệu

## Layers / Các tầng

| Dataset | Grain / Mỗi dòng biểu diễn | Purpose / Mục đích |
| --- | --- | --- |
| Bronze | One source observation within a run / Một quan sát nguồn trong một lần chạy | Original payload and normalized ingestion fields |
| Silver | One source + board + source-job identity in an immutable run | Lifecycle, including inactive history |
| `fact_job_posting` | One content-deduplicated, active and fresh posting per UTC snapshot | Dashboard base dataset |
| `demand_by_role` | One title-rule role family per snapshot | Distinct posting counts; roles can overlap |
| `role_opportunity` | One role family per snapshot | Demand rank, remote share and top company; not personal fit |

## Key fields / Trường quan trọng

| Field | Meaning / Ý nghĩa |
| --- | --- |
| `job_id` | Hash of source, board and original ID; source identity, not universal vacancy ID |
| `dedup_hash` | Content-based duplicate grouping; may merge similar postings or miss edited duplicates |
| `source` / `board_token` | API platform / configured company board or aggregator |
| `company`, `title`, `location` | Source-reported values; locations are not standardized cities |
| `apply_url` | Original external job link; availability must be checked there |
| `posted_at` | Publication date supplied by the source, possibly missing |
| `first_seen_at` | First observation under the current lifecycle model |
| `last_seen_at` | Last actual ingestion observation; transforming old data does not refresh this |
| `is_active` (Silver) | Active according to lifecycle rules, not a guarantee of employer availability |
| `is_fresh` (Silver) | Last observation within 26 hours of ingestion completion |
| `is_remote` | Remote indicated by source; false includes unknown and is not confirmed onsite |
| `snapshot_date` | Workflow start date in UTC; Glue Catalog exposes this partition as a string |
| `posting_count` | 1 per fact record; sum yields fact posting count |

Gold does not contain the full job description. Use the original posting link.
Gold không lưu JD đầy đủ; mở link gốc để xem nội dung và tình trạng tuyển dụng.

## Dashboard metrics / Chỉ số

All cards, charts and the table share the same source, role, location, text and remote filters.
Mọi chỉ số dùng chung bộ lọc. Unique postings là số dòng fact, không phải số vị trí tuyển thật.
Companies và locations là số giá trị khác nhau theo nguồn; `Unknown` hiển thị giá trị thiếu.
Marked remote = số tin có `is_remote=true` / số tin sau lọc. Tập rỗng hiển thị 0 và thông báo rõ.

Role classification reproduces the eight Gold title-pattern families. A posting can match multiple
families, so bars must not be summed as unique jobs. Unclassified postings stay visible.
The page shows one published snapshot, not a daily-new-jobs count or a market growth chart.

## Date example / Ví dụ ngày

A run at 01:00 UTC+7 on 28 September has UTC snapshot date 27 September. This is expected.
Ngày snapshot nhỏ hơn ngày trên đồng hồ Việt Nam một ngày không đồng nghĩa với dữ liệu bị chậm.
Check the actual ingestion timestamp and publication health.
