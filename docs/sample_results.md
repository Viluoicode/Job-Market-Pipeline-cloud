# Verified results / Kết quả đã kiểm chứng

## 28 September 2026 — workshop dashboard acceptance

Read-only check at **07:42 UTC / 14:42 UTC+7**. This is a dated observation, not a live status badge.

| Measurement | Observed value |
| --- | --- |
| Daily rule | ENABLED, `cron(0 18 * * ? *)` |
| Latest scheduled execution | 28/09, 01:00:16 → 01:08:56 UTC+7, SUCCEEDED |
| Last three executions | SUCCEEDED |
| UTC snapshot | `2026-09-27` |
| Raw observations | 7,838 |
| Successful / failed boards | 36 / 0 |
| Incomplete boards | 1 (Arbeitnow pagination cap) |
| Gold unique postings | 8,157 |
| Companies as reported | 467 |
| Marked remote | 2,481 (30.4%) |
| Dashboard Athena scan | 981,745 bytes |
| Query ID | `55428c11-d652-44b2-ae6b-2fba64082f58` |

[Machine-readable evidence](evidence/workshop-20260928.json) records publication metadata,
schedule, recent execution identifiers and sample records. The reader checked the execution lock
and matching publication metadata before and after its Athena query.

**Diễn giải:** Gold có thể nhiều dòng hơn số bản ghi tải trong lần hiện tại vì giữ lại quan sát gần
đây từ nguồn chưa xác minh đầy đủ. Đây không phải số job mới đăng hôm nay. Một platform không phải
một board; 36 board thuộc bốn platform. Role có thể chồng lặp và mẫu không đại diện toàn thị trường.

**Interpretation:** Gold may retain recent observations from unverified sources; it need not be
smaller than today's ingestion. These are current-snapshot counts, not newly posted job counts.
Title families overlap and remote metadata is incomplete.

## Historical acceptance

Earlier measurements remain in [evidence](evidence/README.md). July's accumulated-input method and
September's lifecycle method are not trend-comparable. Same-day reruns replace a Gold partition;
historical captured numbers are not necessarily still queryable as separate snapshots.

For today's result, use [the daily checks](operations.md#daily-check--kiểm-tra-mỗi-ngày) and refresh
the dashboard. Never treat a historical evidence file as proof of current health.
