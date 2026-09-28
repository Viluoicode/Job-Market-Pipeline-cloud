# Proposal — Job Market AWS Pipeline

## Tiếng Việt

### Bài toán
Một bài thực hành data engineering cần chứng minh được toàn bộ đường đi từ API tới báo cáo,
không chỉ chạy một script tải dữ liệu. Tin tuyển dụng là bộ dữ liệu mẫu phù hợp: schema khác nhau,
nguồn có thể lỗi, tin thay đổi hoặc biến mất, và dữ liệu cũ có thể bị nhầm là dữ liệu mới.

Đối tượng của dự án là người học AWS và người phân tích muốn khảo sát **tập nguồn được cấu hình**.
Giá trị của workshop là một quy trình có thể chạy lại, truy nguyên con số và phát hiện dữ liệu không
đủ tin cậy. Không định vị đây là sản phẩm tìm việc tốt hơn các website tuyển dụng.

### Giải pháp và phạm vi chốt
Thu thập hằng ngày từ 36 board của Greenhouse, Lever, Ashby và Arbeitnow; lưu Bronze, chuẩn hóa
và theo dõi lifecycle trong Silver, xuất ba mart Gold. Athena và dashboard một trang giúp xem
số tin, doanh nghiệp, nhóm vai trò, địa điểm và link nguồn. Pipeline tự chạy trên AWS khi máy cá nhân tắt.

Hoàn thành khi có: execution tự động thành công, manifest và publication khớp, dữ liệu còn mới,
quality gate đạt, cảnh báo độc lập, dashboard đọc dữ liệu thật, hướng dẫn deploy/verify/cleanup và
bằng chứng có ngày kiểm tra. Không mở rộng nguồn Việt Nam, CV matching, recommendation, chatbot,
đăng nhập người dùng hay dịch vụ tìm việc công khai trong phiên bản này.

### Giá trị và giới hạn
Người xem có thể giải thích **con số đến từ đâu, được quan sát khi nào, và còn đáng tin không**.
Đây là giá trị thực hành về kỹ thuật và vận hành; không suy diễn thành đại diện thị trường lao động.
Role dựa vào tiêu đề, remote theo metadata nguồn, và tin còn hiển thị có thể đã đóng trên website gốc.

### Kế hoạch bàn giao
1. Khóa phạm vi và kiểm chứng deployment — bằng chứng trong [kết quả](sample_results.md).
2. Hoàn thiện dashboard, kiểm thử và tài liệu — theo [hướng dẫn thực hành](workshop.md).
3. Người học kiểm chứng kiến trúc bằng icon AWS, trình bày quyết định thiết kế và quay demo.
4. Đưa nội dung vào website báo cáo riêng; nghỉ chạy hoặc cleanup sau buổi đánh giá.

[Kiến trúc](architecture.md) giải thích lựa chọn dịch vụ. [Chi phí và bảo mật](cost-and-security.md)
chứa dự toán minh họa và giới hạn. Nguồn lỗi được phân biệt với tin đóng; dữ liệu quá cũ bị dashboard
chặn; chi phí được giới hạn bằng lịch chạy, timeout, retention và ngân sách cảnh báo.

## English

### Problem statement
A data engineering workshop must demonstrate a repeatable API-to-report workflow. Job postings
provide realistic schema differences, source failures, changing records and freshness risks.
The audience is AWS learners and analysts exploring the configured sample of boards.

### Proposed solution
Run daily ingestion for 36 boards across four public API platforms. Preserve observations in
Bronze, reconcile identity and lifecycle in Silver, and publish three Gold marts. Athena and a
one-page dashboard expose measurable results with traceable publication metadata.

### Value, scope and acceptance
The deliverable demonstrates automated collection, data quality, lifecycle, publication checks,
monitoring and reproducible operation. It is not a commercial job-search replacement or a
representative Vietnam labor-market dataset. Vietnam expansion and personal matching are excluded.
Acceptance requires a successful scheduled execution, matching publication metadata, fresh data,
passing quality checks, usable dashboard, tests and dated evidence. The learner supplies the final
architecture explanation and demo recording. See the linked architecture, lab and cost documents.
