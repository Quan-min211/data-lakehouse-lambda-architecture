# CHƯƠNG 5: KẾT LUẬN VÀ ĐỊNH HƯỚNG PHÁT TRIỂN

Chương này tổng kết những gì đề tài đã làm được, những gì chưa làm được, và hướng đi tiếp theo ở khóa luận tốt nghiệp. Các nhận định trong chương dựa trên kết quả của Chương 3 và Chương 4. Chương không đưa ra số liệu mới, trừ kết quả chạy bộ kiểm thử đơn vị ngày 2026-11-16.

## 5.1. TỔNG KẾT KẾT QUẢ ĐẠT ĐƯỢC CỦA ĐỀ TÀI

### 5.1.1. Mức độ hoàn thành so với mục tiêu và tiêu chí nghiệm thu

Mục 1.3.1 đặt ra tám sản phẩm. Bảng 5.1 đối chiếu từng sản phẩm với hiện trạng mã nguồn. Một sản phẩm được coi là hoàn thành khi mã nguồn có đủ chức năng nêu trong mục tiêu và có kiểm thử hoặc thực nghiệm đi kèm. Những phần còn thiếu được ghi ở cột cuối, không gộp vào đánh giá chung.

**Bảng 5.1: Đối chiếu tám sản phẩm của đề tài với hiện trạng mã nguồn**

| STT | Sản phẩm | Mức độ | Bằng chứng | Phần còn thiếu |
| :---: | :---: | :---: | :---: | :---: |
| 1 | Đường thu thập dữ liệu | Hoàn thành | WebSocket Binance → Kafka, DLQ, phát hiện mất chuỗi, bù dữ liệu qua REST (Mục 3.2) | Số lần mất chuỗi không được ghi xuống bảng kiểm toán; chưa có đợt chạy liên tục dài ngày được ghi nhận |
| 2 | Tập dữ liệu gốc Bronze | Hoàn thành | Bảng Iceberg phân vùng ẩn theo ngày trên MinIO; ghi bản ghi thô trước kiểm tra chất lượng (Mục 3.4) | Ghi Bronze là tùy chọn; chức năng time travel có sẵn nhưng chưa được dùng |
| 3 | Tầng tốc độ | Hoàn thành | Spark Structured Streaming, micro-batch 5 giây, watermark 1 phút, ghi ClickHouse và Redis (Mục 3.3) | Kho trạng thái RocksDB chưa có hiệu lực do checkpoint cũ |
| 4 | Tầng xử lý lô | Hoàn thành | Bronze → Silver → Gold, kho Silver tích lũy, watermark tăng đơn điệu, Dagster 15 phút (Mục 3.4, 3.5) | Lõi xử lý viết bằng Python thuần; mô hình dbt không nằm trên đường chạy |
| 5 | Tầng phục vụ | Hoàn thành | 8 điểm cuối FastAPI, Query Merger ba trường hợp, kiểm thử đơn vị cho cả Trường hợp 3 (Mục 3.6) | Độ trễ của Trường hợp 3 chưa được đo |
| 6 | Cổng chất lượng và bộ tiêm lỗi | Hoàn thành | Bảy luật ngữ nghĩa, bảng cách ly, bộ tiêm lỗi ngưỡng cộng dồn; Precision, Recall, F1 bằng 1,0 (Mục 4.5) | Chỉ có luật cho lỗi về giá trị, chưa có luật cho giá lệch thị trường |
| 7 | Giao diện giám sát | Hoàn thành | Streamlit hiển thị nến, VWAP, nhãn đối soát, chất lượng dữ liệu và cảnh báo (Mục 3.7) | – |
| 8 | Bộ thực nghiệm và báo cáo | Hoàn thành một phần | Bốn thực nghiệm và bốn kịch bản hỗn loạn có nhật ký CSV (Chương 4) | Tầng tốc độ được mô phỏng trong các bằng chứng cho RQ2; chưa đo độ tươi dữ liệu |

*Nguồn: nhóm thực hiện đối chiếu với mã nguồn và các Mục 3.2 đến 4.6.*

Chất lượng mã nguồn được kiểm chứng bằng bộ kiểm thử đơn vị. Ngày 2026-11-16, lệnh `python scripts/run_tests.py` chạy 7 nhóm kiểm thử với tổng cộng 89 kiểm thử, cho kết quả 7/7 nhóm đạt, không có lỗi. Bộ kiểm thử không cần Docker. Nó bao gồm các trường hợp biên đã phát hiện qua đợt kiểm toán ngày 2026-11-12: cây nến bị ghi đè bởi dữ liệu thiếu, watermark lùi lại, và offset Kafka được ghi nhận trước khi dữ liệu được ghi xong.

Về mục tiêu tổng quát, hệ thống đã được thiết kế, hiện thực và đánh giá định lượng như dự kiến. Phần đánh giá có những giới hạn được trình bày ở Mục 5.2. Cũng cần nói rõ mức độ đạt được của SLA dưới 5 giây. Chu kỳ micro-batch 5 giây của tầng tốc độ là một tham số cấu hình. Độ trễ đầu-cuối từ lúc giao dịch xảy ra trên sàn tới lúc xuất hiện trong `speed_agg` chưa được đo trực tiếp.

### 5.1.2. Tổng hợp câu trả lời cho ba câu hỏi nghiên cứu

Bảng 4.11 đã tổng hợp chi tiết. Phần này nêu lại ngắn gọn, kèm so sánh với giả thuyết ban đầu.

**RQ1 – Kiến trúc Lambda có giảm độ trễ truy vấn so với Batch-only không?** Giả thuyết ban đầu là có. Kết quả không ủng hộ giả thuyết này. Query Merger có P50 62,84 ms, chậm hơn truy vấn trực tiếp `batch_agg` 2,72 ms, và chênh lệch giữa ba chiến lược nhỏ hơn biến động giữa hai đợt đo. Kết luận đúng là phép hợp nhất theo watermark chỉ thêm chi phí khoảng vài mili-giây so với truy vấn một bảng. Lợi ích thật của Lambda là độ tươi của dữ liệu: số liệu của phút vừa qua có trong `speed_agg` sau một chu kỳ micro-batch thay vì sau một chu kỳ lô 15 phút [17, Ch. 1]. Thực nghiệm chưa đo lợi ích này.

**RQ2 – Speed View sai lệch bao nhiêu và tự đối soát diễn ra thế nào?** Khi 20% đến 25% giao dịch bị trễ quá watermark hoặc bị mất, khối lượng của khung nhìn tốc độ lệch khoảng 20% đến 26%. Đường xử lý lô đưa độ lệch này về 0 trong mọi kịch bản và mọi đợt chạy. Cơ chế đối soát vì vậy được kiểm chứng tốt ở mức logic. Độ lớn sai lệch của tầng tốc độ khi chạy thật trên luồng thật thì chưa được đo, vì ở cả hai nguồn bằng chứng tầng tốc độ đều được mô phỏng.

**RQ3 – Gộp tệp cải thiện hiệu năng đọc bao nhiêu lần?** Trên bảng Bronze 118 tệp, gộp tệp bin-packing giảm 91,5% số tệp, tăng tốc đọc khoảng 1,5 lần ở trạng thái bộ đệm nóng, và giảm độ lệch chuẩn của thời gian đọc khoảng 28 lần, với chi phí 17,54 giây. Con số 2,1 lần trong đề cương và trong các đợt đo cũ bị thổi phồng bởi lần đọc lạnh đầu tiên.

So với đề cương, kết quả của cả ba câu hỏi đều thấp hơn hoặc hẹp hơn kỳ vọng. Một phần khoảng cách này đến từ việc đề cương đã ghi các con số từ những đợt đo mà sau đó được chứng minh là có lỗi phương pháp. Báo cáo chọn trình bày kết quả đã đối chiếu lại với nhật ký và mã nguồn, thay vì giữ các con số ban đầu.

### 5.1.3. Đánh giá tính ứng dụng của kiến trúc

Kiến trúc của đề tài phù hợp với các bài toán có ba đặc điểm: dữ liệu phát sinh liên tục, có sự kiện đến trễ và trùng lặp, và cần cả số liệu tức thời lẫn số liệu đã đối soát. Giám sát giao dịch trên sàn tiền mã hóa là một ví dụ. Các ví dụ khác gồm theo dõi thanh khoản của một quỹ đầu tư hay đối chiếu giao dịch giữa hệ thống nội bộ và đối tác. Ba thành phần của đề tài có thể dùng lại độc lập với bài toán tiền mã hóa:

- **Watermark tăng đơn điệu và thuật toán hợp nhất ba trường hợp.** Đây là một cách cụ thể để hiện thực tầng phục vụ của Lambda, vốn được mô tả ở mức nguyên lý trong mô hình gốc [17, Ch. 1].
- **Kho Silver tích lũy dùng để tính lại các cửa sổ bị ảnh hưởng.** Thành phần này giải quyết xung đột giữa việc đọc dữ liệu theo từng đợt và cơ chế thay cả dòng của `ReplacingMergeTree`.
- **Bảng kiểm toán đối soát theo từng cửa sổ.** Bảng này cho phép đo lệch pha giữa hai khung nhìn trong vận hành, không chỉ trong thực nghiệm.

Ở trạng thái hiện tại, hệ thống chưa sẵn sàng cho môi trường sản xuất. Nó chạy trên một máy, Kafka không có bản sao, mật khẩu mặc định của MinIO nằm trong Dockerfile, và chưa từng chạy liên tục dài ngày. Giá trị chính của đề tài là một kiến trúc tham chiếu có mã nguồn, kiểm thử và nhật ký thực nghiệm đầy đủ để người khác kiểm tra lại.

## 5.2. CÁC HẠN CHẾ VÀ THÁCH THỨC CÒN TỒN TẠI

### 5.2.1. Hạn chế về tài nguyên phần cứng đơn máy

Mười bốn dịch vụ chạy trên một máy tính xách tay 16 GB RAM, với tổng trần bộ nhớ của các container là 22 364 MiB. Hệ thống chỉ chạy được khi các dịch vụ không cùng chạm trần, và người vận hành phải chọn đúng tập dịch vụ cần khởi động (Mục 3.8.2). Ba hệ quả trực tiếp:

- **Không đánh giá được khả năng chịu lỗi phân tán.** Kafka chạy một broker với hệ số nhân bản 1, nên cấu hình xác nhận `acks = all` của producer thực chất tương đương `acks = 1`.
- **Môi trường đo chưa được kiểm soát hết.** Bộ nhớ của máy ảo WSL2 do cấu hình Windows quyết định và không được ghi lại. Compose không đặt giới hạn CPU cho dịch vụ nào, nên các tiến trình Spark và ClickHouse cạnh tranh nhau trên cùng các nhân.
- **Quy mô dữ liệu bị giới hạn.** Các truy vấn của Thực nghiệm 1 chỉ trả về 5 đến 105 nến, và bảng của Thực nghiệm 3 chỉ khoảng 106 MB. Ở quy mô này, độ trễ bị chi phối bởi chi phí cố định của mỗi lượt khứ hồi chứ không phải lượng dữ liệu.

### 5.2.2. Chi phí duy trì mã nguồn kép và độ phức tạp vận hành

Đây là điểm yếu cố hữu của Lambda mà Kreps đã nêu [15]. Trong đề tài, logic tổng hợp nến tồn tại ở hai dạng: biểu thức Spark trong `metrics_calculator.py` cho tầng tốc độ, và hàm Python cho tầng lô. Đề tài giảm rủi ro hai bản cài đặt lệch nhau bằng cách cho chúng dùng chung các mô-đun tính nến, phát hiện đột biến và tiêu chí đối soát. Tuy nhiên, chưa có thực nghiệm nào chạy cả hai tầng thật trên cùng một luồng dữ liệu sạch để kiểm chứng rằng chúng cho cùng kết quả. Khi thay đổi một chỉ số tài chính, kỹ sư vẫn phải sửa và kiểm thử ở cả hai nơi.

Độ phức tạp vận hành thể hiện qua những lỗi chỉ phát hiện được khi kiểm toán. Ba ví dụ: kho trạng thái RocksDB bị khóa bởi checkpoint cũ, watermark có thể lùi lại khi đọc sai phiên bản, và offset Kafka được ghi nhận trước khi dữ liệu được ghi xong. Cả ba lỗi đều không làm hệ thống dừng. Chúng làm số liệu sai một cách lặng lẽ. Đây là loại lỗi đặc trưng của một hệ thống có hai đường xử lý và một tầng ghép ở giữa.

### 5.2.3. Hạn chế về phạm vi dữ liệu và phương pháp thực nghiệm

**Về dữ liệu.** Dữ liệu của tầng lô là 1 275 230 bản ghi tổng hợp sinh từ phân phối thống kê của nến Binance thật, không phải luồng giao dịch thật. Đề cương dự kiến dùng hơn 5 triệu giao dịch `BTCUSDT` thật. Các thực nghiệm chỉ dùng `BTCUSDT` và `ETHUSDT` của một sàn duy nhất. Hệ thống chưa từng chạy luồng WebSocket thật liên tục trong nhiều ngày, nên các hiện tượng chỉ xuất hiện theo thời gian dài, như trạng thái phình to hay kết nối lại nhiều lần, chưa được quan sát.

**Về phương pháp.** Chương 4 đã chỉ ra năm khoảng trống:

- Trường hợp 3 của Query Merger chưa được đo độ trễ, vì watermark giả lập nằm ngoài khoảng của mọi truy vấn.
- Độ tươi của dữ liệu, vốn là lợi ích chính của Lambda, chưa được đo.
- Ở cả Thực nghiệm 2 lẫn các kịch bản hỗn loạn, tầng tốc độ được mô phỏng.
- Thời gian hội tụ đầu-cuối chưa được đo. Bảng kiểm toán ghi thời lượng lượt chạy lô vào cột này và luôn ghi 0 vào cột mất chuỗi.
- Phân phối độ trễ có hai cụm mà nguyên nhân chưa được xác định.

**Về khả năng tái lập.** Bốn ảnh container dùng nhãn `latest`, `requirements.txt` chỉ ràng buộc phiên bản tối thiểu, và trạng thái ClickHouse lúc đo không được lưu lại. Một người chạy lại thực nghiệm sẽ có cùng mã nguồn nhưng chưa chắc có cùng phiên bản phần mềm hay cùng dữ liệu trong ClickHouse.

## 5.3. ĐỊNH HƯỚNG PHÁT TRIỂN CHO KHÓA LUẬN TỐT NGHIỆP

Các định hướng được xếp theo thứ tự ưu tiên. Ưu tiên cao nhất là lấp các khoảng trống đo lường, vì nếu không lấp chúng, mọi mở rộng về sau đều kế thừa cùng những giới hạn về bằng chứng.

### 5.3.1. Hoàn thiện phương pháp đánh giá

- **Đo Trường hợp 3.** Cho phép tham số `--watermark-offset-hours` của `bench_latency.py` nhận giá trị thập phân, hoặc thêm tham số tính theo phút. Sau đó đặt watermark giả lập vào giữa khoảng 30 phút của Q3, để đo chi phí của ba lượt truy vấn và phép khử trùng tại ranh giới.
- **Đo độ tươi và độ trễ đầu-cuối.** Gắn dấu thời gian lúc ghi vào `speed_agg`, rồi đo khoảng cách tới `trade_time` của giao dịch cuối trong cửa sổ. Đây là phép đo trực tiếp cho SLA dưới 5 giây.
- **Đo sai lệch thật của tầng tốc độ.** Chạy cả tầng tốc độ lẫn tầng lô thật trên cùng một luồng Kafka có lỗi đã biết, rồi đọc `volume_drift_pct` và `vwap_error` trong bảng kiểm toán. Cùng phép đo này, chạy trên luồng sạch, sẽ kiểm chứng hai bản cài đặt logic tổng hợp cho cùng kết quả.
- **Ghi đầy đủ bảng kiểm toán.** Chuyển số lần mất chuỗi từ tầng thu thập xuống cột `sequence_gap_count`, và tính thời gian hội tụ theo định nghĩa đầu-cuối ở Mục 2.2.3.
- **Bảo đảm khả năng tái lập.** Ghim phiên bản mọi ảnh container và gói Python, ghi lại cấu hình WSL2, và lưu ảnh chụp trạng thái ClickHouse cùng mỗi đợt đo.

### 5.3.2. Mở rộng dữ liệu: đa cặp giao dịch và đa sàn

Bộ thu thập đã hỗ trợ chọn động mười cặp có khối lượng lớn nhất. Bước tiếp theo là chạy luồng thật liên tục ít nhất vài ngày trên các cặp này, để quan sát hành vi dài hạn của kho trạng thái, watermark và gộp tệp. Mở rộng sang nhiều sàn đòi hỏi hai thay đổi trong hợp đồng dữ liệu. Thứ nhất, khóa khử trùng phải thêm mã sàn, vì mã giao dịch chỉ duy nhất trong một sàn. Thứ hai, dữ liệu của mỗi sàn phải được chuẩn hóa về cùng định dạng `TradeEvent`. Dữ liệu đa sàn cũng mở ra một dạng đối soát mới: so khớp giá và khối lượng của cùng một cặp giữa các sàn.

### 5.3.3. Phát hiện bất thường thị trường nâng cao

Bộ phát hiện đột biến hiện tại dùng hai ngưỡng cố định, 2% cho biên độ đóng – mở và 3% cho biên độ cao – thấp trong một phút. Ngưỡng cố định không thích nghi với mức biến động riêng của từng cặp: Bảng 1.1 cho thấy biên độ 24 giờ của `ZECUSDT` gần gấp bốn lần `BTCUSDT`. Hướng phát triển là thay ngưỡng cố định bằng ngưỡng tính từ phân phối lịch sử của từng cặp trong khung nhìn lô, rồi thử các mô hình học máy phát hiện bất thường cho các hành vi như giao dịch rửa (wash trading). Kiến trúc Lambda có lợi thế ở đây: tầng lô cung cấp dữ liệu lịch sử đã làm sạch để huấn luyện, còn tầng tốc độ áp dụng mô hình trên luồng.

### 5.3.4. Kiểm toán trước khi công bố và triển khai phân tán

Hiện tại, kết quả của một lượt chạy lô được ghi thẳng vào `batch_agg` và watermark tiến lên ngay. Nếu lượt chạy có lỗi logic, số liệu sai được công bố là `Reconciled`. Mẫu ghi – kiểm toán – công bố (Write-Audit-Publish) khắc phục điều này bằng cách ghi kết quả vào một nhánh tách biệt, kiểm tra các ràng buộc, rồi mới công bố. Đặc tả Iceberg hỗ trợ tham chiếu snapshot dạng nhánh và nhãn [4], nên có thể hiện thực mẫu này ở tầng Bronze và Silver. Với ClickHouse, cách tương ứng là chỉ tiến watermark sau khi bảng kiểm toán của lượt chạy không có cửa sổ bất thường.

Về hạ tầng, bước cuối là đưa hệ thống lên một cụm nhiều nút. Kafka cần ít nhất ba broker với hệ số nhân bản 3 để `acks = all` có ý nghĩa, và Spark cần chạy trên cụm thật thay cho chế độ `local`. Đóng gói bằng một nền tảng điều phối container sẽ thay cho Docker Compose. Chỉ khi đó các câu hỏi về khả năng mở rộng và chịu lỗi mới đo được. Một khi tầng xử lý luồng đủ mạnh để phát lại lịch sử, cũng nên đánh giá lại lựa chọn giữa Lambda và Kappa [14, Ch. 12], [15], vì chi phí duy trì mã nguồn kép ở Mục 5.2.2 là cái giá mà Kappa tránh được.
