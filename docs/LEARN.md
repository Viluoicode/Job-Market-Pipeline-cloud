# Học từ ZERO — Job Market AWS Pipeline

> **Cập nhật 16/09/2026:** đã xác minh lượt tự chạy đầu tiên, từ **01:00:16 đến 01:07:29**.
> EventBridge trên AWS tự kích hoạt; không cần máy cá nhân bật hoặc mở Codex.
> 36/36 board thành công; Athena có **7.583 tin active/đủ mới thuộc 309 công ty**.
> Cả ba job Glue, 7/7 quy tắc DQ và crawler đều đạt; kiểm tra độ mới đạt khi ghi nhận bằng chứng.
> `snapshot_date=2026-09-15` là đúng vì pipeline dùng ngày UTC, tương ứng 01:00 ngày 16/09 ở Việt Nam.
> Đọc [operations.md](operations.md) và [bằng chứng lượt tự chạy](evidence/scheduled-run-20260916.json).
> Các số liệu tháng 7 bên dưới là lịch sử, không được dùng để suy ra tin còn mở hôm nay.

## Luồng hiện tại trên AWS

1. Step Functions chốt `run_id` và ngày UTC, lấy khóa DynamoDB để tránh chạy chồng.
2. Glue Python Shell tự lấy dữ liệu ATS. Mỗi board có trạng thái riêng; lỗi API khác với board rỗng.
3. Ghi Bronze theo run/board rồi mới ghi manifest. Mặc định cần ít nhất 80% board thành công.
4. Silver chỉ đọc file của manifest đó và chặn ingestion cũ quá 24 giờ. Mỗi tin có `fetched_at` thật.
5. Lifecycle giữ `first_seen_at`, chỉ cập nhật `last_seen_at` khi thấy tin; đóng tin vắng mặt từ board
   lấy đầy đủ. Board lỗi hoặc bị giới hạn phân trang không làm đóng tin ngay; quá 7 ngày thì hết hạn.
6. Gold chỉ đếm tin active và được quan sát trong 26 giờ tính đến lúc ingestion hoàn tất.
7. Crawler phải thành công đúng lượt này rồi mới ghi completion marker. Lệnh
   `python scripts/check_freshness.py --bucket <lake>` kiểm tra độ mới theo đồng hồ hiện tại.

`job_id` mới dùng `source|board_token|source_job_id` để tránh trùng ID giữa các board.
Silver lưu lịch sử theo run; Bronze hết hạn sau 30 ngày không xóa lịch sử này. Lượt đầu sau nâng cấp
chỉ khởi tạo lịch sử từ quan sát mới vì timestamp của Silver cũ là thời gian transform.
Lịch đã bật sau khi kiểm tra end-to-end. Chưa phát triển thêm dashboard. Athena/SQL hiện đã
trả lời được nhu cầu theo nghề, tỷ lệ remote, công ty tuyển nhiều và danh sách link ứng tuyển.

## Tài liệu nền tảng của phiên bản ban đầu (tham khảo lịch sử)


> Tài liệu này dành cho **người chưa biết gì** về project. Đọc từ trên xuống, bạn sẽ hiểu:
> project làm gì, các khái niệm nền tảng, dữ liệu chảy thế nào, dùng những dịch vụ AWS nào,
> và cách tự chạy lại. Không cần biết trước về AWS, Spark hay data engineering.
>
> 📄 Tài liệu liên quan: [README.md](../README.md) (tổng quan ngắn) ·
> [architecture.md](architecture.md) (bản tham khảo tiếng Anh, súc tích cho nhà tuyển dụng).

## Mục lục

1. [Project này là gì? Giải quyết vấn đề gì?](#1-project-này-là-gì-giải-quyết-vấn-đề-gì)
2. [Kiến thức nền tảng (giải thích từ zero)](#2-kiến-thức-nền-tảng-giải-thích-từ-zero)
3. [Bức tranh tổng thể](#3-bức-tranh-tổng-thể)
4. [Dòng chảy dữ liệu — chi tiết 3 tầng](#4-dòng-chảy-dữ-liệu--chi-tiết-3-tầng)
5. [Hạ tầng AWS — từng dịch vụ làm gì](#5-hạ-tầng-aws--từng-dịch-vụ-làm-gì)
6. [Cách chạy project](#6-cách-chạy-project)
7. [Cấu trúc thư mục — đọc file nào trước](#7-cấu-trúc-thư-mục--đọc-file-nào-trước)
8. [So sánh với SkillRadar](#8-so-sánh-với-skillradar)
9. [Từ điển thuật ngữ](#9-từ-điển-thuật-ngữ)
10. [Câu hỏi thường gặp & lỗi hay gặp](#10-câu-hỏi-thường-gặp--lỗi-hay-gặp)
11. [Trạng thái project & bước tiếp theo](#11-trạng-thái-project--bước-tiếp-theo)

---

## 1. Project này là gì? Giải quyết vấn đề gì?

### Bài toán
Bạn muốn biết **thị trường tuyển dụng tech đang cần nghề gì nhất** — Data Engineer, Frontend,
Machine Learning...? Bao nhiêu tin là remote? Công ty nào tuyển mạnh? Để trả lời, ta cần:

1. **Thu thập** tin tuyển dụng từ nhiều nguồn trên internet.
2. **Làm sạch** (mỗi nguồn có định dạng khác nhau, nhiều tin bị trùng).
3. **Phân tích** (gán nghề cho từng tin, rồi đếm).

### Cách project giải quyết
Project xây một **data pipeline** (đường ống dữ liệu) trên **AWS** (đám mây của Amazon),
mô phỏng đúng cách các công ty thật làm:

- Lấy tin từ **4 nguồn ATS công khai** (Greenhouse, Lever, Ashby, Arbeitnow — không cần API key).
- Đưa qua **3 tầng xử lý** (kiến trúc Medallion: Bronze → Silver → Gold).
- Cuối cùng có thể **gõ SQL** để ra số liệu "nghề nào hot nhất".

> **ATS** = Applicant Tracking System = phần mềm quản lý tuyển dụng mà công ty dùng để đăng tin
> và nhận hồ sơ (vd Greenhouse, Lever). Chúng có sẵn API công khai để lấy danh sách job.

### Vì sao project tồn tại (mục đích thật)
Đây là một **portfolio project** — bằng chứng kỹ năng để xin việc Data Engineer. Nó chứng minh
người làm biết: **AWS + Terraform (Infrastructure as Code) + PySpark (Spark) + SQL**, đúng những
từ khoá mà tin tuyển DE ở Việt Nam yêu cầu.

Nó là **bản song sinh** của project [SkillRadar](https://github.com/Viluoicode/SkillRadar) —
cùng bài toán nhưng làm bằng công cụ local (DuckDB + dbt + Streamlit). Làm **cùng một thứ theo
hai cách** chính là điểm mạnh: chứng tỏ hiểu cả hai trường phái và biết khi nào dùng cái nào
(xem [mục 8](#8-so-sánh-với-skillradar)).

---

## 2. Kiến thức nền tảng (giải thích từ zero)

Đọc phần này một lần, mọi thứ phía sau sẽ dễ hiểu.

### 2.1. Data pipeline là gì?
Là một chuỗi bước **tự động** đưa dữ liệu từ nguồn thô → thành dạng dùng được để phân tích.
Giống dây chuyền nhà máy: nguyên liệu vào một đầu, sản phẩm ra đầu kia.

### 2.2. Data lake là gì?
Một **kho chứa dữ liệu khổng lồ, giá rẻ**, đổ vào đủ loại định dạng (JSON, Parquet, ảnh, log...).
Ở đây kho đó là **Amazon S3** — hiểu nôm na là "ổ cứng vô hạn trên mây", tổ chức theo thư mục.

### 2.3. Kiến trúc Medallion (Bronze / Silver / Gold) — trái tim của project
Một quy ước chia data lake thành **3 tầng chất lượng tăng dần**:

| Tầng | Ý nghĩa | Ở project này |
|---|---|---|
| 🥉 **Bronze** | Dữ liệu **thô**, y nguyên như lấy về | JSON từ các API tuyển dụng |
| 🥈 **Silver** | Dữ liệu **sạch**, đã ép kiểu, đã khử trùng | Parquet đã chuẩn hoá |
| 🥇 **Gold** | Dữ liệu **sẵn sàng phân tích** | Bảng "số tin theo nghề" |

Vì sao chia tầng? Để **mỗi bước làm một việc**, dễ kiểm lỗi, và nếu logic phân tích sai thì sửa ở
Gold mà không phải kéo lại dữ liệu thô từ internet.

### 2.4. ETL là gì?
**Extract – Transform – Load** = Lấy ra – Biến đổi – Nạp vào. Chính là việc đi từ tầng này sang
tầng kia (Bronze→Silver→Gold).

### 2.5. JSON vs Parquet — vì sao đổi định dạng?
- **JSON** (ở Bronze): text dễ đọc bằng mắt, nhưng nặng và đọc chậm.
- **Parquet** (ở Silver/Gold): định dạng **theo cột**, nén tốt, query nhanh hơn nhiều lần. Khi chỉ
  cần 2/20 cột, Parquet chỉ đọc đúng 2 cột đó → rẻ và nhanh. Đây là định dạng chuẩn của data lake.

### 2.6. Partition (phân vùng) là gì?
Là cách **chia dữ liệu thành thư mục con theo giá trị**, ví dụ:
```
bronze/source=greenhouse/date=2026-06-29/jobs.json
silver/jobs/snapshot_date=2026-06-29/...
```
Khi bạn hỏi "dữ liệu ngày 29" thì hệ thống chỉ đọc đúng thư mục `date=2026-06-29`, bỏ qua phần
còn lại → **nhanh và rẻ**. Quy ước tên `key=value` được Athena/Spark hiểu tự động.

### 2.7. Spark / PySpark là gì?
**Apache Spark** là công cụ xử lý dữ liệu lớn **phân tán** (chia việc cho nhiều máy chạy song
song). **PySpark** là cách viết Spark bằng Python. Trong project, 2 file ở `glue/jobs/` là PySpark.
Bạn viết thao tác trên "bảng" (DataFrame) như SQL, Spark lo phần chạy song song.

### 2.8. AWS Glue là gì?
Dịch vụ **Spark được quản lý sẵn** của AWS (serverless). Bạn chỉ đưa script PySpark, Glue tự cấp
máy chạy rồi tự tắt — không phải dựng cụm Spark thủ công. Tốn tiền theo thời gian chạy.

### 2.9. Glue Data Catalog & Crawler là gì?
- **Catalog** = "danh bạ bảng": ghi nhớ rằng trong S3 có bảng tên `fact_job_posting`, gồm các cột
  nào, kiểu gì. Bản thân dữ liệu vẫn nằm ở S3, catalog chỉ là **metadata** (mô tả).
- **Crawler** = con bot tự **quét** thư mục Gold trên S3, suy ra schema (tên cột, kiểu) và **ghi
  vào Catalog**. Nhờ đó Athena biết cách đọc.

### 2.10. Athena là gì?
Dịch vụ **gõ SQL thẳng lên file trong S3**, không cần database server. "Serverless query".
Tính tiền theo **lượng dữ liệu quét** ($5/TB) — nên Parquet + partition giúp rẻ gần như 0.

### 2.11. Step Functions là gì? (orchestration)
**Orchestration** = "nhạc trưởng" điều phối các bước chạy đúng thứ tự. Step Functions là dịch vụ
vẽ **sơ đồ luồng** (state machine): chạy job Silver → xong mới chạy Gold → rồi chạy Crawler.
Nếu một bước lỗi, nó dừng và báo.

### 2.12. Infrastructure as Code (IaC) & Terraform là gì?
Thay vì vào AWS Console **bấm chuột** tạo từng thứ (dễ sai, khó lặp lại), ta **mô tả hạ tầng bằng
code**. **Terraform** đọc các file `.tf` rồi tự gọi AWS tạo đúng những gì đã khai báo. Lợi ích:
- Gõ `terraform apply` → dựng toàn bộ trong vài phút.
- Gõ `terraform destroy` → xoá sạch, về 0 đồng.
- Code lưu trong git → ai cũng tái tạo y hệt.

### 2.13. Serverless & "trả tiền theo dùng"
Hầu hết dịch vụ ở đây (S3, Glue, Athena, Step Functions) là **serverless**: không có server chạy
24/7. Không chạy = gần như không mất tiền. Đây là lý do project chạy được với **vài cent**.

---

## 3. Bức tranh tổng thể

```
   Greenhouse · Lever · Ashby · Arbeitnow        ← 4 nguồn ATS công khai (không cần key)
              │
              │  ingestion/land_to_bronze.py      ← Python (httpx) kéo tin về
              ▼
   🥉 S3 bronze/    JSON thô, partition theo source/date
              │
              │  glue/jobs/bronze_to_silver.py    ← PySpark: ép kiểu + khử trùng trong-nguồn
              ▼
   🥈 S3 silver/    Parquet sạch, partition theo snapshot_date
              │
              │  glue/jobs/silver_to_gold.py      ← PySpark: gộp trùng chéo-nguồn + gán nghề + đếm
              ▼
   🥇 S3 gold/      fact_job_posting/ + demand_by_role/  (Parquet)
              │
              │  Glue Crawler  →  Glue Data Catalog (danh bạ bảng)
              ▼
   Athena (gõ SQL ra số liệu)   →  sql/athena_analysis.sql  →  screenshot cho portfolio

   ▲ Điều phối toàn bộ 3 mũi tên Glue ở trên: Step Functions (chạy đúng thứ tự).
   ▲ Toàn bộ hạ tầng (S3, Glue, Athena, Step Functions, IAM, Budget): khai báo trong infra/*.tf (Terraform).
```

Sơ đồ Mermaid đầy đủ có trong [architecture.md](architecture.md).

---

## 4. Dòng chảy dữ liệu — chi tiết 3 tầng

### 🥉 Tầng Bronze — thu thập & chuẩn hoá thô
**File:** [`ingestion/land_to_bronze.py`](../ingestion/land_to_bronze.py) · **chạy bằng:** Python
thường (chạy được trên máy bạn, không cần AWS).

Việc nó làm:
1. Đọc danh sách 36 board trong [`ingestion/sources.json`](../ingestion/sources.json).
2. Với mỗi board, gọi đúng API của nguồn đó (mỗi nguồn có 1 hàm `fetch_*`).
3. **Chuẩn hoá** mọi tin về **một khuôn 11 cột giống nhau** (hàm `_record`):
   `source, board_token, source_job_id, company, title, location, remote, description,
   apply_url, posted_at, raw_json`.
4. Ghi ra **newline-delimited JSON** (mỗi dòng 1 tin) tại
   `bronze/source=<nguồn>/date=<ngày>/jobs.json`.

> ⭐ **Bài học thiết kế:** 4 nguồn có cấu trúc API khác nhau hoàn toàn, nhưng Bronze gom hết về
> **một schema thống nhất**. Nhờ vậy các tầng sau chỉ xử lý 1 khuôn duy nhất.
>
> ⭐ **Resilient ingestion:** mỗi board được bọc `try/except` — một nguồn lỗi thì log rồi bỏ qua,
> không làm sập cả mẻ.

### 🥈 Tầng Silver — làm sạch & khử trùng trong-nguồn
**File:** [`glue/jobs/bronze_to_silver.py`](../glue/jobs/bronze_to_silver.py) · **chạy bằng:**
PySpark trên AWS Glue (chỉ chạy ở Chế độ B — deploy AWS).

Việc nó làm:
1. **Đọc tất cả** file JSON dưới `bronze/` (mọi nguồn, mọi ngày) gộp thành 1 bảng.
2. **Ép kiểu**: `remote` → boolean, `posted_at` (chuỗi) → timestamp thật...
3. **Tạo 2 "vân tay" (hash)** — phần quan trọng nhất:

   - **`job_id` = `lower(sha256(source | source_job_id))`**
     → định danh duy nhất của **một tin từ một nguồn**. Crawl lại ngày mai vẫn ra cùng `job_id` →
     biết là *cùng tin*, không đếm 2 lần. Đây là khử trùng **trong cùng nguồn**.

   - **`dedup_hash` = `upper(sha256(norm(company) | norm(title) | norm(location)))`**
     → "vân tay theo nội dung". Cùng một job đăng ở 2 nguồn khác nhau → `job_id` khác nhưng
     `dedup_hash` **giống** → tầng Gold sẽ gộp lại. Đây là khử trùng **chéo nguồn**.

   Hàm `norm()` chuẩn hoá chuỗi để so khớp: hạ chữ thường → đổi ký tự lạ thành dấu cách → gộp
   khoảng trắng. Nhờ đó `"Senior  Engineer!"` và `"senior engineer"` ra cùng hash.

4. **Giữ 1 dòng cho mỗi `job_id`** (bản mới nhất) bằng kỹ thuật cửa sổ
   `Window.partitionBy("job_id")` + `row_number() = 1`.
5. **Kiểm chất lượng (Data Quality gate)** — trước khi ghi, chạy bộ rule **AWS Glue Data Quality**
   (viết bằng DQDL) trên bảng Silver. Đây là bản "cloud" của dbt tests bên SkillRadar:
   `job_id` không null & duy nhất (= `not_null` + `unique`), `dedup_hash` không null, `source` chỉ
   thuộc 4 nguồn hợp lệ (= `accepted_values`), `company` điền ≥ 90%. Nếu **có rule fail → job dừng**
   (dữ liệu xấu không lọt vào Silver). Kết quả hiện lên **Glue Data Quality console** + lưu ra
   `quality/silver_jobs/`. *(Lượt verify 2026-07-27: score 1.0, 7/7 rule PASS.)*
6. Ghi **Parquet** ra `silver/jobs/snapshot_date=<ngày>/`.

> 💡 `snapshot_date` = "ảnh chụp thị trường" của ngày chạy. Mỗi lần chạy tạo 1 partition mới,
> không đụng vào ngày cũ → so sánh xu hướng theo thời gian được.

### 🥇 Tầng Gold — gộp trùng chéo-nguồn, gán nghề, đếm
**File:** [`glue/jobs/silver_to_gold.py`](../glue/jobs/silver_to_gold.py) · **chạy bằng:** PySpark
trên AWS Glue.

Tạo **2 bảng kết quả**:

**Bảng 1 — `fact_job_posting`** (mỗi tin đã gộp trùng = 1 dòng):
- Dùng `dedup_hash` + `row_number() = 1` để **mỗi nội dung-job chỉ giữ 1 đại diện** (gộp trùng
  chéo nguồn).
- Thêm cột phân tích: `title_lower`, `company_key`, `posted_date_key`, và `posting_count = 1`
  (cột "đo lường" để Athena chỉ việc `SUM(posting_count)` ra tổng).

**Bảng 2 — `demand_by_role`** (đếm nhu cầu mỗi nghề) — qua 3 bước:
1. **Bảng tra nghề** `DEFAULT_ROLES`: 8 nghề, mỗi nghề kèm vài từ khoá xuất hiện trong tên job.
   Ví dụ thấy `"data engineer"` hoặc `"etl engineer"` trong tiêu đề → đó là **Data Engineer**.

   | Nghề | Một số từ khoá nhận diện |
   |---|---|
   | Backend Engineer | backend engineer, backend developer |
   | Frontend Engineer | frontend engineer, ui engineer |
   | Full Stack Engineer | full stack, fullstack |
   | Data Engineer | data engineer, etl engineer, analytics engineer |
   | Data Scientist | data scientist, machine learning scientist |
   | Machine Learning Engineer | machine learning engineer, ml engineer, ai engineer |
   | DevOps Engineer | devops, sre, platform engineer |
   | Mobile Engineer | ios engineer, android engineer, mobile developer |

2. **Bắc cầu job ↔ nghề**: join mỗi job với bảng nghề, điều kiện `title_lower` *chứa* từ khoá.
   Một job khớp nhiều nghề thì ra nhiều dòng. (`broadcast` = mẹo tối ưu vì bảng nghề rất nhỏ.)
3. **Đếm**: `groupBy("role").countDistinct("job_id")` → mỗi nghề có bao nhiêu tin. Sắp giảm dần.
   **Đây chính là con số cuối cùng** để vẽ biểu đồ "nghề nào hot nhất".

**Bảng 3 — `role_opportunity`** (bảng "quyết định"): lấy `demand_by_role` rồi *làm giàu* thêm các
cột mà người đi xin việc thật sự cần để quyết định — `demand_rank` (xếp hạng), `remote_pct` (%
tin remote của nghề đó), và `top_company` (công ty tuyển nhiều nhất cho nghề đó). Một bảng trả lời
gọn: *"nên học/ứng tuyển nghề nào, có dễ remote không, ai đang tuyển?"* — đây là thứ
[dashboard](dashboard.html) vẽ ra.

> 🧪 **Kiểm thử (tests):** logic 3 bảng trên được tách thành hàm PySpark thuần (`build_fact`,
> `build_demand_by_role`, `build_role_opportunity`) trong `glue/jobs/`, nên `tests/` chạy được
> `pytest` trên SparkSession local để kiểm: `job_id` ổn định, `dedup_hash` khớp chéo nguồn, phân
> loại nghề đúng, và các cột của bảng quyết định đúng. Chạy tự động trong CI (Python 3.11 + Java 17).

### Tóm tắt 3 tầng

| Tầng | Định dạng | Việc chính | Khử trùng | Chạy ở đâu |
|---|---|---|---|---|
| 🥉 Bronze | JSON thô | gom 4 nguồn về 1 khuôn | — | máy local hoặc AWS |
| 🥈 Silver | Parquet | ép kiểu + tạo 2 hash | trong-nguồn (`job_id`) | AWS Glue |
| 🥇 Gold | Parquet | gán nghề + đếm | chéo-nguồn (`dedup_hash`) | AWS Glue |

---

## 5. Hạ tầng AWS — từng dịch vụ làm gì

Tất cả khai báo bằng Terraform trong thư mục [`infra/`](../infra). Mỗi file `.tf` lo một mảng:

| Dịch vụ AWS | Vai trò trong project | File khai báo |
|---|---|---|
| **S3** (3 bucket) | Kho lưu: `lake` (bronze/silver/gold), `athena-results`, `scripts` | [`s3.tf`](../infra/s3.tf) |
| **IAM** (2 role) | Cấp quyền cho Glue & Step Functions (least-privilege) | [`iam.tf`](../infra/iam.tf) |
| **Glue** | 2 job PySpark + Catalog database + Crawler | [`glue.tf`](../infra/glue.tf) |
| **Athena** | Workgroup `jobmarket-aws`, chặn quét > 10GB/query | [`athena.tf`](../infra/athena.tf) |
| **Step Functions** | State machine điều phối pipeline | [`stepfunctions.tf`](../infra/stepfunctions.tf) |
| **AWS Budgets** | Cảnh báo chi phí ($10/tháng, báo ở 80%) | [`budget.tf`](../infra/budget.tf) |

**Vài chi tiết hay:**
- **Tên bucket tự sinh, không trùng toàn cầu:** `<project>-<account_id>-<region>-{lake,athena,scripts}`
  (xem [`locals.tf`](../infra/locals.tf)). Không phải nghĩ tên thủ công.
- **`scripts_upload.tf`** tự đẩy 2 file PySpark lên S3, dùng `etag = filemd5(...)` → sửa script là
  tự upload lại.
- **Step Functions** chạy: `BronzeToSilver` (.sync, chờ xong) → `SilverToGold` (.sync) →
  `StartCrawler` → vòng lặp `Wait 30s → GetCrawler → kiểm tra READY chưa`. (Crawler không có tích
  hợp `.sync` nên phải tự poll.)
- **An toàn chi phí:** `force_destroy = true` trên bucket để `terraform destroy` xoá sạch kể cả khi
  còn file. S3 lifecycle tự xoá `bronze/` sau 30 ngày, kết quả Athena sau 7 ngày.

**Các lệnh Terraform cốt lõi:**
```
terraform init      # tải provider AWS (lần đầu)
terraform validate  # kiểm cú pháp .tf (không tốn tiền)
terraform plan      # xem trước sẽ tạo gì (chưa tạo)
terraform apply     # thực sự tạo hạ tầng trên AWS
terraform output    # in ra tên bucket, ARN... sau khi apply
terraform destroy   # xoá sạch toàn bộ
```

---

## 6. Cách chạy project

### 🟢 Chế độ A — chạy LOCAL, không cần AWS, không tốn tiền (nên làm trước)
Mục đích: thấy dữ liệu thật chảy ra, hiểu code, an toàn.

```powershell
# (1 lần) cài thư viện Python
python -m pip install -r requirements.txt

# kéo tin tuyển dụng thật về thư mục out\  (tầng Bronze)
python ingestion/land_to_bronze.py --out-dir out

# (tuỳ chọn) chỉ lấy 5 board đầu cho nhanh:
python ingestion/land_to_bronze.py --out-dir out --limit 5
```
→ tạo `out/bronze/source=.../date=.../jobs.json`. Mở bằng VS Code để xem từng tin.

> ⚠️ Hai file Silver/Gold là PySpark, **không chạy được trên máy thường** — ở Chế độ A chỉ **đọc
> hiểu logic**. Chúng chạy thật ở Chế độ B.

### 🔵 Chế độ B — deploy thật lên AWS (tốn vài cent)
Cần: tài khoản AWS + IAM admin, đã cài **AWS CLI** và **Terraform**, đã chạy `aws configure`.

```powershell
# B1. Dựng hạ tầng
cd infra
copy terraform.tfvars.example terraform.tfvars   # mở file, điền email thật để nhận cảnh báo budget
terraform init
terraform plan          # xem trước
terraform apply         # gõ yes → AWS tạo S3, IAM, Glue, Athena, Step Functions, Budget

# B2. Đổ dữ liệu lên S3 bronze
cd ..
$env:LAKE_BUCKET = terraform -chdir=infra output -raw lake_bucket
python ingestion/land_to_bronze.py

# B3. Chạy cả pipeline (Bronze→Silver→Gold→Crawler) bằng 1 lệnh
aws stepfunctions start-execution --state-machine-arn (terraform -chdir=infra output -raw state_machine_arn)
#    → theo dõi trong AWS Console > Step Functions tới khi SUCCEEDED

# B4. Query: vào AWS Console > Athena > chọn workgroup "jobmarket-aws" + database gold
#    → dán nội dung sql/athena_analysis.sql → chạy → chụp màn hình kết quả

# B5. XONG → xoá sạch để khỏi tốn tiền
cd infra
terraform destroy       # gõ yes
```

### "UI" của project nằm ở đâu?
Project **không có dashboard riêng** (khác SkillRadar có Streamlit). "Giao diện" chính là **các màn
hình AWS Console** sau khi deploy (Chế độ B): sơ đồ **Step Functions** sáng dần khi chạy, bảng kết
quả **Athena**, trình duyệt thư mục **S3**, lịch sử job **Glue**. Đây là thứ chụp lại làm portfolio.

---

## 7. Cấu trúc thư mục — đọc file nào trước

Đọc theo đúng dòng chảy dữ liệu để hiểu nhanh nhất:

```
Job Market AWS Pipeline/
├── README.md                       # tổng quan ngắn (đọc đầu tiên)
├── docs/
│   ├── LEARN.md                    # ← file này (học từ zero)
│   └── architecture.md             # tham khảo tiếng Anh, súc tích
│
├── ingestion/
│   ├── land_to_bronze.py           # ② Bronze: kéo tin về (dễ hiểu nhất, đọc sớm)
│   └── sources.json                # ③ danh sách 36 board
│
├── glue/jobs/
│   ├── bronze_to_silver.py         # ④ Silver: làm sạch + 2 hash khử trùng
│   └── silver_to_gold.py           # ⑤ Gold: gán nghề + đếm
│
├── sql/
│   └── athena_analysis.sql         # ⑥ câu hỏi phân tích cuối cùng
│
└── infra/                          # ⑦ hạ tầng AWS (Terraform) — đọc sau cùng
    ├── s3.tf  iam.tf  glue.tf  athena.tf  stepfunctions.tf  budget.tf
    ├── variables.tf  locals.tf  outputs.tf  providers.tf  versions.tf
    └── terraform.tfvars.example    # mẫu cấu hình → copy thành terraform.tfvars
```

Thứ tự gợi ý: ① README → ② `land_to_bronze.py` → ③ `sources.json` → ④ `bronze_to_silver.py`
→ ⑤ `silver_to_gold.py` → ⑥ `athena_analysis.sql` → ⑦ `infra/` → [architecture.md](architecture.md).

---

## 8. So sánh với SkillRadar

Hai project **cùng bài toán, khác bộ công nghệ** — cố ý làm vậy để chứng minh hiểu cả hai trường phái.

| | 🟦 **SkillRadar** (local-first) | 🟩 **Job Market AWS Pipeline** (cloud-native) |
|---|---|---|
| Chạy ở đâu | Máy cá nhân / MotherDuck | AWS |
| Lưu trữ | DuckDB + Parquet | S3 |
| Biến đổi | dbt (SQL) + Python | Glue PySpark |
| Query | DuckDB | Athena |
| Điều phối | Prefect | Step Functions |
| Hạ tầng | cài tay | Terraform (IaC) |
| Giao diện | Streamlit dashboard | AWS Console (không dashboard) |

**Phần GIỐNG nhau (cố ý port y nguyên để 2 bản so sánh được):** cùng 4 nguồn ATS & 36 board; cùng
logic `job_id`, `dedup_hash`, hàm chuẩn hoá chuỗi, và bảng `DEFAULT_ROLES` phân loại nghề.

> ⚠️ **Hai repo riêng biệt.** Project AWS này nằm ở repo riêng (không push chung vào repo
> SkillRadar). Nên link chéo 2 README cho nhau khi đưa nhà tuyển dụng xem.

---

## 9. Từ điển thuật ngữ

| Thuật ngữ | Nghĩa ngắn gọn |
|---|---|
| **ATS** | Phần mềm tuyển dụng (Greenhouse, Lever...) — nguồn lấy tin |
| **Data lake** | Kho dữ liệu lớn giá rẻ (ở đây là S3) |
| **Medallion** | Quy ước chia data lake thành Bronze/Silver/Gold |
| **Bronze/Silver/Gold** | 3 tầng chất lượng: thô / sạch / sẵn sàng phân tích |
| **ETL** | Extract-Transform-Load: lấy ra, biến đổi, nạp vào |
| **Parquet** | Định dạng file theo cột, nén tốt, query nhanh |
| **Partition** | Chia dữ liệu thành thư mục con theo giá trị (vd `date=...`) |
| **Spark / PySpark** | Công cụ xử lý dữ liệu lớn phân tán / viết bằng Python |
| **Glue** | Spark được AWS quản lý sẵn (serverless) |
| **Catalog** | "Danh bạ bảng" — metadata mô tả dữ liệu trong S3 |
| **Crawler** | Bot quét S3 để tự suy ra schema và ghi vào Catalog |
| **Athena** | Gõ SQL thẳng lên file S3, không cần server |
| **Step Functions** | "Nhạc trưởng" điều phối các bước theo thứ tự |
| **state machine** | Sơ đồ luồng các bước trong Step Functions |
| **IAM** | Hệ thống phân quyền của AWS (role, policy) |
| **IaC / Terraform** | Khai báo hạ tầng bằng code thay vì bấm tay |
| **Serverless** | Không có server chạy 24/7; không dùng = gần như không mất tiền |
| **snapshot_date** | "Ảnh chụp" thị trường của ngày chạy pipeline |
| **dedup** | Khử trùng lặp (deduplication) |
| **hash** | Chuỗi "vân tay" sinh từ dữ liệu để so sánh nhanh |
| **broadcast join** | Mẹo join nhanh khi 1 bảng rất nhỏ |

---

## 10. Câu hỏi thường gặp & lỗi hay gặp

**Hỏi: Tôi chạy được Silver/Gold trên máy không?**
Không (trừ khi tự dựng Spark). Chúng cần môi trường Glue/Spark của AWS → chạy ở Chế độ B. Ở local
bạn đọc hiểu logic là đủ.

**Hỏi: `pip` báo "not recognized"?**
Trên Windows dùng `python -m pip install ...` thay cho `pip ...`.

**Hỏi: Lỗi `UnicodeEncodeError` khi chạy ingestion?**
Đã xử lý sẵn trong code (ép stdout về UTF-8 ở đầu file). Nếu vẫn gặp, kiểm tra Python ≥ 3.7.

**Hỏi: Chạy AWS có đắt không?**
Một lượt pipeline tốn **vài cent**. Glue là phần tốn chính. Nhớ `terraform destroy` khi xong và đặt
Budget alert. **Không** dùng Redshift/MWAA/schedule (những thứ gây hoá đơn bất ngờ).

**Hỏi: Quên `terraform destroy` thì sao?**
S3/Athena/Step Functions khi *không chạy* gần như không tốn tiền; tốn nhất là nếu để Glue chạy lặp.
Vì project không gắn schedule nên nó không tự chạy. Vẫn nên destroy cho sạch.

**Hỏi: Có cần API key cho 4 nguồn tuyển dụng không?**
Không. Cả 4 đều là API công khai.

---

## 11. Trạng thái project & bước tiếp theo

### Project đã hoàn chỉnh chưa? — Đã deploy & verify end-to-end thật trên AWS ✅
**Toàn bộ pipeline đã chạy thật trên AWS (không chỉ kiểm cú pháp):**
- ✅ Ingestion **chạy thật** (kéo ~6.900 tin từ 4 nguồn, land lên S3 `bronze/`).
- ✅ Terraform **`apply` thành công** — 30 resource thật (S3, IAM, 2 Glue job, Crawler, Athena WG,
  Step Functions, Budget) ở `ap-southeast-1`.
- ✅ 2 job Glue PySpark **đã chạy thật trên Glue/Spark** (không còn là "chỉ py_compile").
- ✅ Step Functions **SUCCEEDED nhiều lượt** (gần nhất snapshot **2026-07-27**), Gold marts
  `fact_job_posting/` + `demand_by_role/` sinh ra trong S3 và **query được bằng Athena**.

> ✅ **Đã nghiệm thu end-to-end.** Lượt verify mới nhất (snapshot 2026-07-27): **9.206** tin sau
> dedup chéo-nguồn, **~35% remote**, **308** công ty; top nghề **Machine Learning Engineer (207)**.
> (Số cao hơn snapshot 2026-06-30 vì `bronze_to_silver` đọc *toàn bộ* bronze tích luỹ trong S3 rồi
> dedup theo `job_id` — đúng thiết kế.) Xem thêm [sample_results.md](sample_results.md).

### Việc nên làm tiếp
1. **Chụp màn hình** Step Functions (graph SUCCEEDED) / Athena (bảng kết quả) / S3 (cây gold) /
   Glue (lịch sử job) → bỏ vào `docs/screenshots/` cho portfolio (folder hiện đang trống).
2. `terraform destroy` sau khi chụp xong để **dừng chi phí** (infra để idle vẫn tốn lặt vặt).
3. (Tuỳ chọn) triển khai một trong các mở rộng P2.5 bên dưới để làm project nổi bật hơn.

### Mở rộng (P2.5)
- ✅ **Glue Data Quality** — *đã làm.* Bộ rule DQDL kiểm chất lượng bảng Silver ngay trong
  `bronze_to_silver.py` (song song với dbt tests bên SkillRadar). Xem [mục 4 · Tầng Silver](#-tầng-silver--làm-sạch--khử-trùng-trong-nguồn).
- ✅ **EventBridge schedule** — *đã deploy* ([`infra/schedule.tf`](../infra/schedule.tf)). Rule
  EventBridge chạy state machine theo lịch (mặc định hằng ngày 18:00 UTC). **Tắt sẵn mặc định** cho
  an toàn tiền — đặt `enable_schedule = true` trong `terraform.tfvars` để bật (rule tắt thì 0đ).
  *(Verify 2026-07-27: rule live, trạng thái DISABLED, target trỏ đúng state machine.)*
- ⏳ Còn lại (tuỳ chọn): Redshift Serverless + Spectrum · Lambda bọc ingestion để pipeline hoàn
  toàn serverless.
