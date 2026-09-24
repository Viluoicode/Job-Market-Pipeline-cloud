# Proposal: Job Market Intelligence Pipeline

## 1. Tóm tắt đề xuất

Người học, người tìm việc và đơn vị đào tạo đều cần trả lời một câu hỏi quan trọng: **thị trường
đang thực sự cần những vai trò nào, ở đâu và theo hình thức làm việc nào?** Dữ liệu để trả lời câu
hỏi này tồn tại trong các tin tuyển dụng công khai, nhưng đang phân tán trên nhiều hệ thống, thay
đổi liên tục và không thể sử dụng trực tiếp như một nguồn dữ liệu đáng tin cậy.

Job Market Intelligence Pipeline được đề xuất như một nền tảng dữ liệu tự động: thu thập các tin
tuyển dụng công khai mỗi ngày, chuẩn hóa chúng, theo dõi vòng đời, kiểm tra độ mới và chất lượng,
sau đó xuất ra các tập dữ liệu có thể kiểm chứng bằng SQL. Giá trị cốt lõi của giải pháp không phải
là “crawl được nhiều tin”, mà là biến các quan sát rời rạc thành **thông tin có nguồn gốc, có thời
điểm và đủ tin cậy để hỗ trợ quyết định**.

## 2. Problem statement

### Bối cảnh

Nhu cầu nghề nghiệp và kỹ năng thay đổi nhanh. Người học phải chọn hướng phát triển trước khi có đủ
thông tin; người tìm việc phải tìm kiếm trên nhiều website; đơn vị đào tạo cần điều chỉnh nội dung
nhưng thường dựa vào khảo sát, nhận định cá nhân hoặc dữ liệu chậm hơn thị trường.

Online job postings có thể cung cấp tín hiệu gần thời gian thực và chi tiết theo công việc, kỹ
năng, địa điểm và doanh nghiệp. World Bank xem loại dữ liệu này như một “nhiệt kế” gần thời gian
thực cho nhu cầu lao động; ILO cũng nhấn mạnh rằng việc dự báo có hệ thống nhu cầu kỹ năng là cần
thiết để giảm skills mismatch. Tuy nhiên, chính các tin tuyển dụng online cũng có giới hạn: chúng
không đại diện toàn bộ thị trường và phải được thu thập, chuẩn hóa, diễn giải một cách cẩn thận.

Nguồn tham khảo:

- [World Bank — Bridging the Skills Gap with real-time vacancy data](https://www.worldbank.org/en/news/feature/2025/07/25/bridging-the-skills-gap-how-real-time-job-vacancy-data-can-reshape-employment-strategies-in-argentina-and-uruguay)
- [ILO — Skills mismatches](https://www.ilo.org/skills-mismatches)
- [OECD — Limits and uses of online job-posting data](https://www.oecd.org/en/publications/policy-options-for-labour-market-challenges-in-amsterdam-and-other-dutch-cities_181c0fff-en/full-report/component-7.html)

### Vấn đề trung tâm

**Hiện chưa có một nguồn dữ liệu thống nhất, được cập nhật tự động và có thể kiểm chứng để biến các
tin tuyển dụng phân tán thành tín hiệu đáng tin cậy về nhu cầu việc làm trong phạm vi nguồn được
theo dõi.**

Vấn đề này không chỉ là thiếu một danh sách việc làm tổng hợp. Một dataset dùng cho quyết định còn
phải trả lời được:

- Tin này được lấy từ nguồn nào và trong lần chạy nào?
- Pipeline thực sự nhìn thấy tin lần cuối khi nào?
- Tin còn hoạt động, đã biến mất hay chỉ tạm thời không xác minh được vì nguồn lỗi?
- Hai nguồn đang nói về cùng một cơ hội hay hai cơ hội khác nhau?
- Dữ liệu hiện tại có đủ mới và đủ nguồn để sử dụng không?
- Kết quả phân tích có tái tạo và kiểm chứng được hay không?

Nếu không trả lời được các câu hỏi trên, số lượng tin lớn vẫn có thể tạo ra một bức tranh sai về
thị trường.

## 3. Vì sao cách làm hiện tại chưa đủ?

### Tìm kiếm thủ công trên từng website

Người dùng phải mở nhiều nguồn, lặp lại từ khóa và tự so sánh kết quả. Cách này phù hợp để tìm một
tin cụ thể nhưng không phù hợp để đo nhu cầu, theo dõi thay đổi theo ngày hoặc tạo báo cáo có thể
lặp lại.

### Gộp các tin vào một file hoặc database

Việc gom dữ liệu giải quyết phân tán nhưng chưa giải quyết độ tin cậy. Nếu mỗi lần transform lại
gắn thời gian mới cho dữ liệu cũ, hệ thống sẽ biến một posting lịch sử thành posting “vừa được quan
sát”. Nếu API lỗi mà hệ thống coi mọi tin không xuất hiện là đã đóng, kết quả lifecycle cũng sai.

### Chỉ nhìn trạng thái pipeline thành công

Một workflow có thể chạy thành công trên input cũ hoặc publish thiếu một phần. Trạng thái kỹ thuật
`SUCCEEDED` không tự chứng minh dữ liệu mới, đủ nguồn và nhất quán.

### Chỉ xây dashboard

Dashboard làm dữ liệu dễ nhìn hơn nhưng không sửa được dữ liệu sai. Khi freshness, identity,
lifecycle và quality chưa đáng tin cậy, giao diện đẹp chỉ khiến kết luận sai dễ được tin hơn.

## 4. Hậu quả nếu không giải quyết

### Với sinh viên và người tìm việc

- Chọn kỹ năng học dựa trên độ nổi tiếng thay vì nhu cầu quan sát được.
- Mất thời gian với tin đã cũ hoặc đã đóng.
- Không nhận ra nhóm vai trò, doanh nghiệp hoặc cơ hội remote đang nổi bật trong tập nguồn.

### Với đơn vị đào tạo và cố vấn nghề nghiệp

- Nội dung đào tạo phản ứng chậm hơn nhu cầu tuyển dụng.
- Khó giải thích vì sao ưu tiên một nhóm kỹ năng hoặc nghề nghiệp.
- Không có dữ liệu lặp lại để đánh giá nhận định theo thời gian.

### Với data analyst và người vận hành

- Mất thời gian làm sạch lại nhiều nguồn trước mỗi phân tích.
- Không truy nguyên được một con số về đúng source, run và thời điểm quan sát.
- Có nguy cơ công bố báo cáo mới từ dữ liệu cũ mà không phát hiện.

## 5. Giải pháp được đề xuất

Job Market Intelligence Pipeline cung cấp một quy trình dữ liệu hằng ngày với năm năng lực bắt
buộc:

1. **Tự động thu thập:** lấy dữ liệu định kỳ từ các job board công khai mà không phụ thuộc máy cá
   nhân.
2. **Chuẩn hóa và truy nguyên:** đưa các nguồn khác schema về một cấu trúc chung, giữ source,
   board, run ID và thời gian quan sát thật.
3. **Theo dõi lifecycle:** phân biệt posting được quan sát, biến mất khỏi một board đầy đủ, chưa
   thể xác minh do nguồn lỗi/incomplete, hết hạn và xuất hiện trở lại.
4. **Kiểm soát chất lượng và freshness:** chỉ công bố dữ liệu sau quality gate; cảnh báo khi
   pipeline lỗi, thiếu nguồn hoặc dữ liệu quá cũ.
5. **Cung cấp dữ liệu ra quyết định:** xuất các bảng về posting hiện hành, nhu cầu theo vai trò,
   tỷ lệ remote và doanh nghiệp tuyển nhiều để truy vấn bằng SQL.

Giải pháp hiện theo dõi 36 board thuộc Greenhouse, Lever, Ashby và Arbeitnow. Đây là một mẫu nguồn
có kiểm soát để chứng minh phương pháp, không phải tuyên bố bao phủ toàn bộ thị trường lao động.

## 6. Tại sao giải pháp này cần được sử dụng?

### Nó thay thế cảm tính bằng bằng chứng có thể kiểm tra

Mỗi kết quả có thể lần ngược về posting, source, manifest và execution. Người dùng không chỉ nhận
một con số mà còn có thể kiểm tra dữ liệu nào tạo ra con số đó.

### Nó phân biệt “không còn tin” với “không thu thập được tin”

Đây là khác biệt quan trọng giữa một crawler và một hệ thống dữ liệu đáng tin cậy. Khi nguồn lỗi
hoặc bị giới hạn phân trang, pipeline không tự kết luận các posting trước đó đã đóng.

### Nó đo độ mới của dữ liệu, không chỉ đo độ mới của job xử lý

Freshness dựa trên thời gian quan sát nguồn. Một transform vừa chạy không thể biến dữ liệu cũ thành
dữ liệu thị trường hiện tại.

### Nó giảm công việc lặp lại

Thay vì mỗi analyst tự crawl, làm sạch và deduplicate trước từng báo cáo, pipeline tạo một nguồn
dữ liệu chung được cập nhật, kiểm thử và giám sát tự động.

### Nó tạo nền móng cho nhiều sản phẩm khác

Cùng một Gold layer có thể phục vụ báo cáo định kỳ, dashboard, cảnh báo cơ hội, phân tích kỹ năng
hoặc nghiên cứu xu hướng sau khi phạm vi nguồn và phương pháp đo đủ ổn định. Không cần xây lại quá
trình ingestion và quality cho từng giao diện.

### Nó phù hợp với quy mô và ngân sách hiện tại

Kiến trúc chạy theo lịch và trả phí theo mức sử dụng, không duy trì server hoặc data warehouse luôn
bật. Retention, worker limits, Athena scan limit và AWS Budget giúp kiểm soát chi phí khi project
vẫn còn ở quy mô portfolio.

## 7. Người dùng mục tiêu và quyết định được hỗ trợ

| Người dùng | Quyết định cần hỗ trợ | Giá trị hiện tại |
| --- | --- | --- |
| Sinh viên/người chuyển nghề | Nên ưu tiên nhóm vai trò nào; cơ hội remote ra sao | Báo cáo được analyst diễn giải từ dữ liệu mới |
| Người tìm việc | Công ty và vị trí nào đang xuất hiện trong tập nguồn | Posting hiện hành cùng URL ứng tuyển và thời gian quan sát |
| Đơn vị đào tạo/cố vấn | Nội dung nào cần được theo dõi hoặc nghiên cứu sâu hơn | Nhu cầu theo role và doanh nghiệp tuyển nhiều |
| Data analyst | Tạo phân tích lặp lại mà không xây ingestion từ đầu | Ba Gold marts truy vấn bằng Athena |
| Người vận hành | Có thể tin và sử dụng publication mới nhất hay không | Execution history, health metric, alarm và manifest |

Analyst và operator là người dùng trực tiếp của phiên bản hiện tại. Sinh viên và người tìm việc là
người hưởng lợi thông qua báo cáo được diễn giải. Một giao diện self-service công khai chưa thuộc
phạm vi đã bàn giao.

## 8. Bằng chứng khả thi hiện có

Đây không chỉ là ý tưởng trên tài liệu:

- Pipeline đã được triển khai tại AWS region `ap-southeast-1`.
- EventBridge tự kích hoạt workflow mỗi ngày lúc 01:00 UTC+7.
- Các scheduled execution đã chạy thành công khi máy cá nhân không hoạt động.
- Mỗi run đi qua ingestion, lifecycle transformation, bảy data-quality checks, Gold publication
  và Glue Crawler.
- Health monitor độc lập kiểm tra publication mỗi 15 phút và phát CloudWatch alarms.
- Dữ liệu Gold có thể được kiểm tra bằng Athena và truy nguyên về manifest cùng Bronze object.

Các con số theo ngày là dữ liệu có thời hạn. Bằng chứng acceptance được lưu tại
[`docs/evidence/`](evidence/README.md); trạng thái hiện tại phải được kiểm tra theo
[operations.md](operations.md).

## 9. Phạm vi và giới hạn

Proposal không tuyên bố rằng hệ thống:

- đại diện toàn bộ thị trường lao động;
- dự đoán chắc chắn nghề nào sẽ tăng trong tương lai;
- chứng minh một posting online tương ứng đúng một vacancy thực tế;
- thay thế khảo sát lao động hoặc dữ liệu thống kê chính thức;
- đã cung cấp trải nghiệm tìm việc self-service cho người dùng cuối.

Online postings có độ chi tiết và tốc độ cao nhưng có thể thiên lệch về những doanh nghiệp, ngành
và công việc tuyển dụng online. Vì vậy sản phẩm nên được sử dụng như **một nguồn tín hiệu nhu cầu
trong phạm vi đã công bố**, kết hợp với các nguồn khác khi đưa ra quyết định quan trọng.

## 10. Tiêu chí để giải pháp đáng được sử dụng

Giải pháp chỉ nên được dùng cho một publication khi:

- scheduled execution gần nhất hoàn thành;
- manifest đạt ngưỡng nguồn và không che giấu board lỗi/incomplete;
- bảy quality rules vượt qua;
- Silver và pipeline completion marker cùng `run_id`;
- observation chưa vượt cửa sổ freshness;
- truy vấn được posting và URL nguồn để kiểm chứng;
- mọi báo cáo ghi rõ thời gian quan sát và phạm vi 36 board.

Các điều kiện này biến “dữ liệu đã crawl” thành “dữ liệu đủ điều kiện sử dụng”.

## 11. Thông điệp đề xuất

> Thị trường việc làm thay đổi nhanh, nhưng dữ liệu phục vụ quyết định vẫn phân tán, thiếu lịch sử
> và dễ bị hiểu sai. Job Market Intelligence Pipeline tạo ra một nguồn dữ liệu tuyển dụng được cập
> nhật tự động, có thể truy nguyên, có lifecycle và có kiểm soát chất lượng. Giải pháp giúp analyst,
> người học và đơn vị đào tạo quan sát nhu cầu trong phạm vi nguồn một cách nhanh hơn và có bằng
> chứng hơn, đồng thời công khai rõ giới hạn để tránh biến dữ liệu online thành kết luận quá mức.

## 12. Kết luận

Lý do cần sử dụng giải pháp không nằm ở số lượng AWS service hay số lượng posting thu thập được.
Nhu cầu thực sự là một **quy trình biến dữ liệu tuyển dụng thay đổi liên tục thành thông tin có thể
tin, kiểm tra và sử dụng lại**. Project đã chứng minh nền tảng kỹ thuật cho quy trình đó; bước phát
triển tiếp theo nên tập trung vào mở rộng giá trị phân tích và cách đưa insight đến người dùng mà
không làm giảm tính trung thực của dữ liệu.
