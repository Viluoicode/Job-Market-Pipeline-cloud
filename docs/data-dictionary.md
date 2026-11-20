# Data dictionary / Ý nghĩa dữ liệu

## Layers / Các tầng

| Dataset | Grain / Mỗi dòng biểu diễn | Purpose / Mục đích |
| --- | --- | --- |
| Bronze | One source observation within a run / Một quan sát nguồn trong một lần chạy | Original payload and normalized ingestion fields |
| Silver | One source + board + source-job identity in an immutable run | Lifecycle, including inactive history |
| `fact_job_posting` | One content-deduplicated, active and fresh posting per UTC snapshot | Dashboard base dataset |
| `demand_by_role` | One title-rule role family per snapshot | Distinct posting counts; each posting is in at most one role, so counts add up |
| `role_opportunity` | One role family per snapshot | Demand rank, `remote_pct` and `remote_unknown_pct` (both shares of all postings), top company; not personal fit |

## Key fields / Trường quan trọng

| Field | Meaning / Ý nghĩa |
| --- | --- |
| `job_id` | Hash of source, board and original ID; source identity, not universal vacancy ID |
| `dedup_hash` | Content-based duplicate grouping; may merge similar postings or miss edited duplicates. Gold keeps the most recently seen copy (then one with `posted_at`) |
| `source` / `board_token` | API platform / configured company board or aggregator |
| `company`, `title`, `location` | Source-reported values; locations are not standardized cities |
| `apply_url` | Original external job link; availability must be checked there |
| `posted_at` | Publication date supplied by the source, possibly missing |
| `first_seen_at` | First observation under the current lifecycle model |
| `last_seen_at` | Last actual ingestion observation; transforming old data does not refresh this |
| `is_active` (Silver) | Active according to lifecycle rules, not a guarantee of employer availability |
| `is_fresh` (Silver) | Last observation within 26 hours of ingestion completion |
| `is_remote` | Tri-state: `true` = fully remote, `false` = onsite or hybrid, `NULL` = the source gave no signal (unknown). See [Work mode](#work-mode--hình-thức-làm-việc) |
| `snapshot_date` | Workflow start date in UTC; Glue Catalog exposes this partition as a string |
| `posting_count` | 1 per fact record; sum yields fact posting count |

Gold does not contain the full job description. Use the original posting link.
Gold không lưu JD đầy đủ; mở link gốc để xem nội dung và tình trạng tuyển dụng.

## Dashboard metrics / Chỉ số

All cards, charts and the table share the same source, role, location, text and work-mode filters.
Mọi chỉ số dùng chung bộ lọc. Unique postings là số dòng fact, không phải số vị trí tuyển thật.
Companies và locations là số giá trị khác nhau theo nguồn; `Unknown` hiển thị giá trị thiếu.
Remote (of all postings) = số tin `is_remote=true` / tổng số tin sau lọc; Unknown không bao giờ được tính là
remote. Tỷ lệ Onsite / hybrid và Unknown nằm trong tooltip của thẻ. Tập rỗng hiển thị 0 và thông báo rõ.

## Work mode / Hình thức làm việc

`is_remote` is set at ingestion from the strongest signal each source provides:

| Source | Signal used (first match wins) | Unknown when |
| --- | --- | --- |
| Lever | `workplaceType` (remote / hybrid / onsite), then "remote"/"hybrid"/"on-site" in location or commitment | no workplace type and no keyword |
| Ashby | `workplaceType`, then `isRemote`, then location text. Ashby sets `isRemote=true` on Hybrid jobs, so the workplace type must win | none of the three present |
| Greenhouse | location text only ("remote" → true; "hybrid"/"on-site"/"in office" → false) | location names only a place |
| Arbeitnow | `remote` boolean | flag absent |

On the 2026-11-20 crawl (8,099 postings): 21.1% remote, 31.4% onsite/hybrid, 47.5% unknown, mostly
Greenhouse. `remote_pct` divides by **all** postings (a lower bound). Dividing by known postings only
would give 40.1% because Greenhouse reports the flag mainly when a job is remote. Hybrid counts as
not remote. Silver rows written before this rule carry `false` for unknown until their board is
observed again.

## Role families / Nhóm role

Each posting gets **at most one** role family: the first family, in the priority order below, with a
regex matching the lower-cased title. Titles matching an exclusion (product/program/account manager,
recruiter, assistant, counsel, account executive, UX/market researcher) get none. Patterns match
whole words, so "HTML Engineer" is not "ML Engineer". A family can match the role as the job
("Backend Engineer") or as a qualifier ("Software Engineer, Backend"). Gold
(`glue/jobs/silver_to_gold.py`) and the dashboard (`dashboard/model.py`) keep identical rules;
`tests/test_dashboard.py` fails if they diverge.

Mỗi tin thuộc tối đa 1 nhóm, theo thứ tự ưu tiên dưới đây (cụ thể trước, chung chung sau).

| Priority | Role family | Typical titles | Share of 8,103 live postings (2026-11-20) |
| ---: | --- | --- | ---: |
| 1 | Forward Deployed / Solutions | Forward Deployed Engineer, Solutions Architect, Sales Engineer | 10.9% |
| 2 | Engineering Manager | Engineering Manager, Director of Engineering | 2.5% |
| 3 | Research Scientist / Engineer | Research Engineer, Research Scientist, Researcher | 0.8% |
| 4 | ML / AI Engineer | Machine Learning Engineer, Applied AI Engineer | 2.4% |
| 5 | Data Scientist | Data Scientist, Applied Scientist | 1.4% |
| 6 | Data Engineer | Data Engineer, Analytics Engineer, Software Engineer – Data Platform | 1.1% |
| 7 | Data Analyst / BI | Data Analyst, Business Intelligence Analyst | 0.2% |
| 8 | Security Engineer | Security Engineer, Detection Engineer, Software Engineer – Security | 1.8% |
| 9 | DevOps / SRE / Platform | SRE, Platform Engineer, Software Engineer – Infrastructure | 3.0% |
| 10 | Mobile Engineer | iOS / Android Engineer, Software Engineer – iOS | 1.0% |
| 11 | Full Stack Engineer | Full Stack Engineer, Software Engineer – Full Stack | 1.2% |
| 12 | Frontend Engineer | Frontend Engineer, Web Engineer | 0.4% |
| 13 | Backend Engineer | Backend Engineer, Java Engineer, Distributed Systems | 2.6% |
| 14 | Technical Support Engineer | Technical Support Engineer, Support Engineer | 1.1% |
| 15 | Software Engineer (General) | Software Engineer, Member of Technical Staff | 9.7% |
| – | Not matched | Sales, marketing, legal, hardware/mechanical engineering… | 59.9% |

Priority 6/8/9 also have a second, lower-priority rule for the qualifier form (checked after
Backend), so "Backend Engineer – Data Platform" stays Backend while "Software Engineer – Data
Platform" becomes Data Engineer. Not matched is mostly non-engineering work; it stays in totals and
the table. The role chart excludes it, the filter labels it "Not matched to the 15 role families",
and matched/unmatched counts are shown for the current filter. With one selected role category,
the left chart shows counts by source platform.

The page shows one published snapshot, not a daily-new-jobs count or a market growth chart.

## Date example / Ví dụ ngày

A run at 01:00 UTC+7 on 28 September has UTC snapshot date 27 September. This is expected.
Ngày snapshot nhỏ hơn ngày trên đồng hồ Việt Nam một ngày không đồng nghĩa với dữ liệu bị chậm.
Check the actual ingestion timestamp and publication health.
