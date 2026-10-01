# Documentation map / Bản đồ tài liệu

README giới thiệu repo; tài liệu dưới đây giải thích cách học, triển khai và kiểm chứng.
The root README is the entry point; each document below has a distinct responsibility.

| File | Nhiệm vụ / Responsibility |
| --- | --- |
| [proposal.md](proposal.md) | Bài toán, giá trị thực hành, phạm vi và tiêu chí hoàn thành / Problem and scope |
| [architecture.md](architecture.md) | Luồng dữ liệu, hợp đồng, lifecycle và quyết định thiết kế / Design and invariants |
| [workshop.md](workshop.md) | Thực hành VI/EN, chạy dashboard, demo và bàn giao / Bilingual lab guide |
| [data-dictionary.md](data-dictionary.md) | Ý nghĩa bảng, trường và cách đọc chỉ số / Data and metric semantics |
| [operations.md](operations.md) | Deploy, kiểm tra mỗi ngày, recovery, pause, cleanup / Operator runbook |
| [monitoring.md](monitoring.md) | Health model, alarm và kiểm tra SNS / Monitoring reference |
| [security-review.md](security-review.md) | Kiểm tra IAM/S3 thực tế, ma trận quyền hiện tại và đề xuất / Deployed permissions audit |
| [cost-and-security.md](cost-and-security.md) | Dự toán, kiểm soát chi phí, quyền và giới hạn / Cost and security |
| [sample_results.md](sample_results.md) | Kết quả đã đo, có ngày và query ID / Dated measured results |
| [evidence/](evidence/README.md) | Bằng chứng máy đọc được; không sửa lịch sử / Immutable acceptance records |

Không giữ roadmap sản phẩm, thử nghiệm nguồn Việt Nam hay ghi chú cá nhân trong tài liệu dự án.
Evidence là quan sát tại một thời điểm, không phải trạng thái hiện tại. Không commit credentials,
email cá nhân, Terraform state, dữ liệu tải thử hay môi trường Python.
