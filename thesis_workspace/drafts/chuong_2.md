# CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ NỀN TẢNG CÔNG NGHỆ CỐT LÕI

Chương này trình bày các khái niệm mà Chương 3 dùng để thiết kế hệ thống và Chương 4 dùng để đánh giá nó. Mỗi mục gồm hai phần: phần lý thuyết dẫn theo tài liệu tham khảo, và phần đối chiếu cho biết hệ thống của đề tài áp dụng lý thuyết đó đến đâu. Phần đối chiếu dựa trên mã nguồn. Khi mã nguồn khác với mô hình lý thuyết, chương ghi rõ điểm khác.

## 2.1. TIẾN HÓA CỦA KIẾN TRÚC DỮ LIỆU DOANH NGHIỆP

### 2.1.1. Từ kho dữ liệu, hồ dữ liệu đến Lakehouse

Kho dữ liệu (Data Warehouse – DWH) là mô hình đầu tiên được dùng rộng rãi cho phân tích. Dữ liệu từ các hệ thống nghiệp vụ được trích xuất, biến đổi rồi mới nạp vào kho, theo một lược đồ đã thiết kế trước. Kimball và Ross mô tả cách tổ chức phổ biến nhất là mô hình chiều, gồm bảng sự kiện chứa số đo và các bảng chiều chứa ngữ cảnh [13, Ch. 1]. Vì lược đồ được áp đặt ngay lúc ghi (schema-on-write), dữ liệu trong kho sạch và truy vấn nhanh. Cái giá là sự cứng nhắc. Mọi nguồn mới phải được mô hình hóa trước khi nạp, và quy trình nạp thường chạy theo lô hằng đêm.

Hồ dữ liệu ra đời để gỡ sự cứng nhắc đó. Dữ liệu thô được ghi thẳng lên kho lưu trữ đối tượng giá rẻ dưới các định dạng tệp mở như Parquet, còn lược đồ chỉ được áp khi đọc (schema-on-read). Armbrust và cộng sự [9] chỉ ra rằng cách làm này dẫn tới một kiến trúc hai tầng: hồ dữ liệu giữ bản gốc, còn một phần dữ liệu tiếp tục được nạp vào kho để phục vụ báo cáo. Kiến trúc hai tầng gặp bốn vấn đề: độ tin cậy kém vì hai bản dữ liệu dễ lệch nhau, dữ liệu trong kho bị cũ so với hồ, các ứng dụng học máy khó truy cập kho, và tổng chi phí cao vì phải lưu hai lần.

Lakehouse là đề xuất của Armbrust và cộng sự để hợp nhất hai tầng [9]. Ý tưởng là giữ dữ liệu trên kho lưu trữ đối tượng ở định dạng mở, nhưng đặt lên trên một tầng siêu dữ liệu quản lý tệp nào thuộc phiên bản nào của bảng. Tầng siêu dữ liệu này mang lại các tính năng vốn chỉ kho dữ liệu mới có: giao dịch bảo đảm tính nguyên tử, nhất quán, cô lập và bền vững (Atomicity, Consistency, Isolation, Durability – ACID), kiểm soát lược đồ, và lịch sử phiên bản. Delta Lake là một hiện thực cụ thể của ý tưởng này. Nó ghi nhật ký giao dịch ngay trên kho lưu trữ đối tượng để nhiều tiến trình có thể đọc và ghi cùng một bảng một cách an toàn [7].

Hệ thống của đề tài theo hướng Lakehouse ở tầng lưu trữ gốc: dữ liệu giao dịch được ghi vào bảng Apache Iceberg trên MinIO, một kho lưu trữ đối tượng tương thích giao diện S3. Đề tài không truy vấn trực tiếp hồ dữ liệu để phục vụ người dùng. Các khung nhìn đã tổng hợp được đưa sang cơ sở dữ liệu phân tích ClickHouse, vì yêu cầu độ trễ dưới 5 giây của tầng phục vụ khó đạt được khi phải đọc tệp từ kho lưu trữ đối tượng cho mỗi truy vấn. Thực nghiệm gộp tệp ở Chương 4 cho thấy một lượt đọc bảng Bronze trên MinIO mất khoảng 1,2 đến 1,9 giây, trong khi một truy vấn ClickHouse chỉ mất vài chục mili-giây.

### 2.1.2. Kiến trúc Medallion và cách đề tài hiện thực

Kiến trúc Medallion là một mẫu thiết kế tổ chức dữ liệu trong Lakehouse thành ba tầng theo mức chất lượng tăng dần: Bronze chứa dữ liệu thô, Silver chứa dữ liệu đã được kiểm định, và Gold chứa dữ liệu đã được làm giàu, sẵn sàng cho nghiệp vụ [12]. Databricks coi đây là một khuyến nghị thiết kế chứ không phải quy tắc bắt buộc [12]. Giá trị của mẫu nằm ở chỗ mỗi tầng có một hợp đồng rõ ràng. Tầng sau luôn tính lại được từ tầng trước, nên khi logic làm sạch thay đổi, chỉ cần chạy lại từ Bronze.

Bảng 2.1 đối chiếu định nghĩa của từng tầng với cách đề tài hiện thực. Điểm cần lưu ý là lõi của đường Bronze → Silver → Gold trong `src/batch_layer/spark_batch_jobs.py` được viết bằng Python thuần. Spark và Iceberg chỉ tham gia ở bước ghi Bronze tùy chọn và ở bước gộp tệp. Thư mục `dbt_project/` chứa ba mô hình `bronze_crypto_trades`, `silver_cleaned_trades` và `gold_market_aggregates` mô tả cùng các phép biến đổi bằng SQL, nhưng chỉ là bản tham chiếu, không nằm trên đường chạy của hệ thống.

**Bảng 2.1: Đối chiếu ba tầng Medallion với hiện thực của đề tài**

| Tầng | Định nghĩa theo [12] | Hiện thực trong đề tài | Nơi lưu trữ |
| :---: | :---: | :---: | :---: |
| Bronze | Dữ liệu thô, giữ nguyên như khi nhận | Toàn bộ bản ghi thô, kể cả bản ghi lỗi và bản trùng, ghi trước khi kiểm tra chất lượng | Bảng Iceberg `iceberg_catalog.bronze.crypto_trades` trên MinIO (chỉ khi bật `--write-iceberg`) |
| Silver | Dữ liệu đã kiểm định, làm sạch | Bản ghi qua `clean_and_deduplicate()`: đạt mọi luật chất lượng, khử trùng theo (`symbol`, `trade_id`) | Bộ nhớ tiến trình; ở chế độ đọc Kafka thêm kho tích lũy `SilverTradeStore` dạng JSONL |
| Gold | Dữ liệu tổng hợp phục vụ nghiệp vụ | Nến một phút OHLCV, VWAP, cờ đột biến | Bảng ClickHouse `lakehouse.batch_agg` |

*Nguồn: [12]; src/batch_layer/spark_batch_jobs.py, src/batch_layer/silver_store.py, src/batch_layer/iceberg_utils.py, src/data_quality/dq_checks.py.*

Bản ghi bị cổng chất lượng từ chối không bị xóa mà được chuyển sang bảng cách ly `lakehouse.dq_quarantine` kèm mã lỗi (Mục 2.6.2). Tầng Bronze nhận dữ liệu trước bước kiểm tra chất lượng. Nhờ vậy, khi luật kiểm tra thay đổi, các bản ghi từng bị từ chối vẫn còn trong Bronze để xử lý lại.

## 2.2. KIẾN TRÚC LAMBDA VÀ MÔ HÌNH TOÁN HỌC ĐỐI SOÁT

### 2.2.1. Nguyên lý ba tầng của kiến trúc Lambda

Marz và Warren bắt đầu từ một định nghĩa tổng quát: mọi truy vấn trên một hệ thống dữ liệu đều là một hàm trên toàn bộ dữ liệu [17, Ch. 1]. Chạy hàm đó trên toàn bộ lịch sử mỗi khi có truy vấn thì quá chậm. Kiến trúc Lambda tách hàm này thành ba phần [17, Ch. 1]:

$$\text{batch view} = f(\text{all data})$$

$$\text{realtime view} = g(\text{realtime view}, \text{new data})$$

$$\text{query} = h(\text{batch view}, \text{realtime view})$$

Tầng xử lý lô giữ tập dữ liệu gốc (master dataset) ở dạng chỉ ghi thêm, không bao giờ sửa, và định kỳ tính lại khung nhìn lô từ đầu. Tầng tốc độ chỉ xử lý phần dữ liệu mới mà tầng lô chưa kịp đưa vào, và cập nhật khung nhìn thời gian thực theo kiểu tăng dần. Tầng phục vụ trả lời truy vấn bằng cách kết hợp hai khung nhìn. Khi một chu kỳ lô hoàn tất, phần tương ứng của khung nhìn thời gian thực trở nên thừa và có thể bỏ đi [17, Ch. 1].

Tính bất biến của tập dữ liệu gốc là nền tảng của mô hình. Marz và Warren lập luận rằng nó tạo ra khả năng chịu lỗi của con người (human fault tolerance): nếu một lỗi lập trình tạo ra khung nhìn sai, chỉ cần sửa mã và tính lại từ dữ liệu gốc, vì dữ liệu gốc không bị lỗi đó làm hỏng [17, Ch. 1]. Tầng tốc độ được phép phức tạp và kém chính xác hơn, vì mọi sai sót của nó chỉ tồn tại cho tới chu kỳ lô kế tiếp.

Đề tài hiện thực ba hàm trên như sau. Hàm f là đường Bronze → Silver → Gold của tầng lô, ghi vào `batch_agg`. Hàm g là truy vấn Spark Structured Streaming của tầng tốc độ, ghi vào `speed_agg`. Hàm h là thuật toán `AutoCorrectingQueryMerger`. Mô hình gốc không nói rõ "phần dữ liệu tầng lô chưa xử lý" bắt đầu từ đâu. Đề tài làm rõ điểm này bằng mốc watermark W của tầng lô, ghi trong bảng `lakehouse.system_watermark`. Với một truy vấn trên khoảng nửa mở [T~start~, T~end~):

$$\text{query}(T_{\text{start}}, T_{\text{end}}) = \text{batch}\left[T_{\text{start}}, \min(W, T_{\text{end}})\right) \cup \text{speed}\left[\max(W, T_{\text{start}}), T_{\text{end}}\right)$$

Khi T~end~ ≤ W, vế thứ hai rỗng và kết quả lấy hoàn toàn từ khung nhìn lô. Khi T~start~ ≥ W, vế thứ nhất rỗng. Hai vế không bao giờ chồng lên nhau vì cùng dùng W làm ranh giới, nên một cây nến không thể bị đếm hai lần. Để mệnh đề này đúng, W phải tăng đơn điệu. Hàm `ClickHouseBatchSync.update_watermark()` chỉ ghi giá trị mới khi nó lớn hơn giá trị hiện tại. Chi tiết của thuật toán được trình bày ở Mục 3.6.

### 2.2.2. Đối sánh Lambda, Kappa và Medallion thuần

Năm 2014, Kreps đặt câu hỏi về sự cần thiết của tầng lô [15]. Ông chỉ ra cái giá của Lambda: cùng một logic phải được cài đặt hai lần trên hai hệ thống khác nhau, rồi phải giữ cho hai bản cài đặt luôn cho cùng kết quả. Phương án ông đề xuất, sau này gọi là kiến trúc Kappa, chỉ giữ một đường xử lý luồng [15]. Nhật ký sự kiện được lưu đủ lâu trong Kafka. Muốn tính lại lịch sử, người vận hành khởi động một phiên bản thứ hai của công việc xử lý luồng, cho nó đọc nhật ký từ đầu và ghi ra một bảng đầu ra mới. Khi phiên bản mới đuổi kịp hiện tại, ứng dụng chuyển sang đọc bảng mới, và phiên bản cũ bị dừng [15]. Kafka phù hợp với vai trò này vì nó lưu sự kiện dưới dạng nhật ký chỉ ghi thêm, chia theo phân vùng, và cho phép người đọc tự chọn vị trí bắt đầu (offset) [16]. Kleppmann cũng nhận xét rằng khi một hệ thống xử lý luồng đủ mạnh để phát lại lịch sử, ranh giới giữa xử lý lô và xử lý luồng bắt đầu mờ đi [14, Ch. 12].

Bảng 2.2 đối sánh ba mô hình theo các tiêu chí liên quan trực tiếp đến bài toán đối soát. Bảng bổ sung cho Bảng 1.3 ở Chương 1: Bảng 1.3 so sánh các hướng tiếp cận theo nhu cầu người dùng, còn Bảng 2.2 so sánh theo cơ chế bên trong.

**Bảng 2.2: Đối sánh cơ chế của Lambda, Kappa và Medallion thuần**

| Tiêu chí | Lambda | Kappa | Medallion thuần |
| :---: | :---: | :---: | :---: |
| Số đường xử lý | Hai (lô và luồng) | Một (luồng) | Một (lô, theo tầng) |
| Số lần cài đặt logic tổng hợp | Hai | Một | Một |
| Nguồn sự thật | Tập dữ liệu gốc bất biến | Nhật ký sự kiện | Tầng Bronze |
| Cơ chế tái xử lý | Chạy lại tầng lô trên dữ liệu gốc | Phát lại nhật ký sang bảng đầu ra mới | Chạy lại từ Bronze |
| Giới hạn tái xử lý | Dung lượng lưu trữ đối tượng | Thời gian lưu giữ của nhật ký | Dung lượng lưu trữ đối tượng |
| Chuẩn đối chiếu cho kết quả tức thời | Có: khung nhìn lô | Không có đường thứ hai | Không có kết quả tức thời |
| Độ trễ dữ liệu mới nhất | Vài giây | Vài giây | Một chu kỳ lô |

*Nguồn: nhóm thực hiện tổng hợp từ [12], [14, Ch. 12], [15], [16], [17, Ch. 1].*

Hai dòng quyết định lựa chọn của đề tài là "chuẩn đối chiếu" và "giới hạn tái xử lý". Bài toán của đề tài không chỉ cần số liệu đúng mà cần đo được số liệu tức thời sai bao nhiêu. Chỉ Lambda có hai đường độc lập để so với nhau. Về tái xử lý, topic `crypto_trades_raw` chỉ lưu giữ 24 giờ (`retention.ms = 86 400 000` trong `configs/kafka/topics.yaml`), nên một hệ thống Kappa với cấu hình này không tính lại được dữ liệu cũ hơn một ngày. Đổi lại, đề tài chịu chi phí cài đặt logic hai lần mà Kreps nêu. Đề tài giảm chi phí này bằng cách cho hai tầng dùng chung mã: tính nến trong `metrics_calculator.py`, phát hiện đột biến trong `spike_detector.py`, và tiêu chí khớp số liệu trong `src/utils/reconciliation.py`.

### 2.2.3. Mô hình toán học đánh giá độ lệch đối soát

Đơn vị đối soát là cửa sổ một phút của một cặp giao dịch, xác định bởi cặp khóa (`symbol`, `window_start`). Một giao dịch có thời gian sự kiện t (mili-giây) thuộc cửa sổ bắt đầu tại:

$$\text{window\_start}(t) = \left\lfloor \frac{t}{60000} \right\rfloor \times 60000$$

Trong cửa sổ có N giao dịch với giá P~i~ và khối lượng Q~i~, giá trung bình gia quyền theo khối lượng (Volume-Weighted Average Price – VWAP) và tổng khối lượng V là:

$$\text{VWAP} = \frac{\sum_{i=1}^{N} P_i Q_i}{\sum_{i=1}^{N} Q_i} \qquad V = \sum_{i=1}^{N} Q_i$$

Định nghĩa đầy đủ của nến, gồm giá mở cửa, cao nhất, thấp nhất, đóng cửa và khối lượng (Open, High, Low, Close, Volume – OHLCV), được trình bày ở Mục 3.1.2. Gọi các đại lượng của khung nhìn tốc độ có chỉ số speed và của khung nhìn lô có chỉ số batch. Đề tài định nghĩa năm chỉ số đối soát.

Sai số tuyệt đối của VWAP trên một cửa sổ:

$$\varepsilon_{\text{VWAP}} = \left| \text{VWAP}_{\text{speed}} - \text{VWAP}_{\text{batch}} \right|$$

Độ lệch tương đối của khối lượng trên một cửa sổ:

$$\Delta V_{\%} = \frac{\left| V_{\text{speed}} - V_{\text{batch}} \right|}{V_{\text{batch}}} \times 100\%$$

Tỷ lệ bản ghi trùng lặp trên một lượt xử lý, với N~raw~ là số bản ghi nhận vào và N~distinct~ là số bản ghi còn lại sau khử trùng:

$$R_{\text{dup}} = \frac{N_{\text{raw}} - N_{\text{distinct}}}{N_{\text{raw}}} \times 100\%$$

Tỷ lệ mất chuỗi trên một dãy n mã giao dịch đã sắp tăng dần, dựa trên tính chất mã giao dịch tổng hợp của Binance là dãy số nguyên liên tiếp:

$$R_{\text{gap}} = \frac{\sum_{i=2}^{n} \max\left(0, \text{ID}_i - \text{ID}_{i-1} - 1\right)}{\text{ID}_{\max} - \text{ID}_{\min} + 1} \times 100\%$$

Thời gian hội tụ T~conv~ là khoảng thời gian cần để đưa một cửa sổ có lệch pha về trạng thái khớp với khung nhìn lô. Về lý thuyết, T~conv~ nên được đo từ lúc cửa sổ đóng tới lúc tầng phục vụ trả về giá trị đã đối soát. Mã nguồn hiện chưa đo theo định nghĩa đó, như Bảng 2.3 trình bày.

Từ hai chỉ số đầu, hàm `classify_window()` trong `src/utils/reconciliation.py` gán cho mỗi cửa sổ một trong bốn trạng thái. Nếu cả hai khung nhìn có dữ liệu, cửa sổ là `MATCHED` khi ε~VWAP~ < 10^−4^ và ΔV~%~ < 10^−4^ %. Ngưỡng VWAP bằng đúng bước làm tròn bốn chữ số thập phân của cả hai tầng. Nếu không đạt cả hai ngưỡng, cửa sổ là `RECONCILED_AND_HEALED`, nghĩa là khung nhìn tốc độ đã lệch và khung nhìn lô đã sửa. Cửa sổ chỉ có ở tầng lô là `BATCH_ONLY`, chỉ có ở tầng tốc độ là `PROVISIONAL_ONLY`. Cùng một hàm được dùng cho bảng kiểm toán và cho điểm cuối `/api/reconciliation`, nên một cửa sổ luôn nhận cùng một trạng thái ở cả hai nơi.

Để đánh giá trên nhiều cửa sổ, Thực nghiệm 2 tổng hợp sai số VWAP trên N cửa sổ có ở cả hai khung nhìn, với ε~k~ là ε~VWAP~ của cửa sổ thứ k:

$$\text{MAE} = \frac{1}{N} \sum_{k=1}^{N} \varepsilon_k \qquad \text{MAPE} = \frac{100\%}{N} \sum_{k=1}^{N} \frac{\varepsilon_k}{\text{VWAP}_{\text{batch},k}} \qquad \text{RMSE} = \sqrt{\frac{1}{N} \sum_{k=1}^{N} \varepsilon_k^2}$$

Sai số tuyệt đối trung bình (Mean Absolute Error – MAE) và sai số bình phương trung bình căn (Root Mean Squared Error – RMSE) có cùng đơn vị với giá, nên chỉ so sánh được giữa các cửa sổ của cùng một cặp. Sai số phần trăm tuyệt đối trung bình (Mean Absolute Percentage Error – MAPE) không phụ thuộc thang giá, nên dùng được để so sánh giữa `BTCUSDT` và `ETHUSDT`. RMSE phạt nặng các sai số lớn, nên khi RMSE lớn hơn nhiều so với MAE, sai số tập trung ở một vài cửa sổ.

**Bảng 2.3: Phạm vi tính toán thực tế của các chỉ số đối soát trong mã nguồn**

| Chỉ số | Nơi tính | Phạm vi | Ghi chú |
| :---: | :---: | :---: | :---: |
| ε~VWAP~, ΔV~%~ | `sync_reconciliation_audit()` trong `spark_batch_jobs.py` | Từng cửa sổ, mỗi lượt chạy lô | So nến mới tính với `speed_agg` trên ClickHouse |
| R~dup~ | `sync_reconciliation_audit()` | Cả lượt chạy lô | Lấy từ `duplicates_removed / total_records` |
| R~gap~ | `run_chaos_smoke_tests.py` (TC1) | Một kịch bản kiểm thử | Bảng kiểm toán luôn ghi `sequence_gap_count = 0` |
| T~conv~ | `run_chaos_smoke_tests.py`; bảng kiểm toán | Kịch bản kiểm thử; lượt chạy lô | Kịch bản: thời gian tính trong bộ nhớ; bảng kiểm toán: thời lượng lượt chạy lô |
| MAE, MAPE, RMSE | `bench_reprocess.py` | Một giờ nến mỗi cặp | Khung nhìn tốc độ được mô phỏng bằng nhiễu |

*Nguồn: src/batch_layer/spark_batch_jobs.py, src/utils/reconciliation.py, scripts/benchmarks/run_chaos_smoke_tests.py, scripts/benchmarks/bench_reprocess.py.*

Bảng 2.3 cho thấy khoảng cách giữa mô hình và hiện thực. Tầng thu thập có phát hiện mất chuỗi: `binance_ws.py` so mã giao dịch mới với mã trước đó của cùng cặp và đếm số lần nhảy cóc. Tuy nhiên, số đếm này không được chuyển xuống bảng kiểm toán. T~conv~ theo định nghĩa đầu-cuối cũng chưa được đo. Đây là hai hạng mục được nêu lại ở phần hạn chế của Chương 5.

## 2.3. ĐỊNH DẠNG BẢNG MỞ APACHE ICEBERG VÀ BÀI TOÁN QUẢN TRỊ TỆP NHỎ

### 2.3.1. Định dạng bảng mở: Iceberg, Delta Lake và Hudi

Một bảng trong hồ dữ liệu kiểu cũ thực chất là một thư mục chứa tệp Parquet. Cách quản lý theo thư mục có hai điểm yếu. Một tiến trình đọc có thể thấy một lần ghi đang làm dở, vì danh sách tệp thay đổi trong khi nó đang liệt kê. Ngoài ra, mọi thao tác cần biết bảng gồm những tệp nào đều phải liệt kê thư mục, và trên kho lưu trữ đối tượng, liệt kê là thao tác chậm. Định dạng bảng mở giải quyết cả hai bằng cách ghi danh sách tệp của bảng vào siêu dữ liệu, rồi thay đổi bảng bằng cách thay siêu dữ liệu một cách nguyên tử [4], [7].

Ba định dạng phổ biến là Apache Iceberg, Delta Lake và Apache Hudi. Delta Lake ghi một nhật ký giao dịch gồm các tệp JSON đánh số tăng dần trong thư mục `_delta_log` của bảng, định kỳ tóm lại thành tệp điểm kiểm (checkpoint) [7]. Hudi tổ chức các thao tác trên bảng theo một dòng thời gian (timeline) và cung cấp hai kiểu bảng: chép khi ghi (Copy-on-Write) và gộp khi đọc (Merge-on-Read), trong đó kiểu sau tối ưu cho cập nhật thường xuyên [3]. Iceberg dùng một cây siêu dữ liệu nhiều tầng và một danh mục (catalog) giữ con trỏ tới tệp siêu dữ liệu hiện hành [4].

**Bảng 2.4: Đối sánh ba định dạng bảng mở**

| Tiêu chí | Apache Iceberg | Delta Lake | Apache Hudi |
| :---: | :---: | :---: | :---: |
| Cơ chế siêu dữ liệu | Cây: tệp siêu dữ liệu → danh sách manifest → manifest → tệp dữ liệu [4] | Nhật ký giao dịch JSON kèm điểm kiểm Parquet [7] | Dòng thời gian các thao tác trên bảng [3] |
| Điểm cam kết nguyên tử | Thay con trỏ trong catalog [4] | Ghi tệp nhật ký có số thứ tự kế tiếp [7] | Hoàn tất một thao tác trên dòng thời gian [3] |
| Phân vùng | Phân vùng ẩn qua hàm biến đổi cột [4] | Phân vùng theo giá trị cột [7] | Phân vùng theo đường dẫn [3] |
| Thế mạnh chính | Đặc tả độc lập với công cụ xử lý | Tích hợp sâu với Spark | Cập nhật và đọc tăng dần |

*Nguồn: nhóm thực hiện tổng hợp từ [3], [4], [7].*

Đề tài chọn Iceberg vì ba lý do. Bảng chỉ có thao tác ghi thêm, nên lợi thế cập nhật của Hudi không cần thiết. Phân vùng ẩn cho phép truy vấn theo cột thời gian gốc mà không phải nhớ cột phân vùng. Cuối cùng, danh mục REST tách việc quản lý bảng khỏi Spark, nên các công cụ khác có thể đọc cùng một bảng.

### 2.3.2. Cây siêu dữ liệu, cơ chế giao dịch và time travel của Iceberg

Đặc tả Iceberg định nghĩa bảng qua bốn loại tệp [4]. Tệp siêu dữ liệu của bảng (`*.metadata.json`) chứa lược đồ, đặc tả phân vùng, thuộc tính bảng và danh sách các snapshot. Mỗi snapshot trỏ tới một danh sách manifest, liệt kê các tệp manifest tạo nên trạng thái của bảng tại thời điểm đó. Mỗi tệp manifest liệt kê các tệp dữ liệu, kèm giá trị phân vùng và thống kê theo cột như số dòng, giá trị nhỏ nhất và lớn nhất. Nhờ thống kê này, bộ lập kế hoạch truy vấn loại được những tệp chắc chắn không chứa dữ liệu cần tìm mà không phải mở tệp.

Một lần ghi diễn ra theo cơ chế kiểm soát đồng thời lạc quan (Optimistic Concurrency Control – OCC) [4]. Tiến trình ghi tạo các tệp dữ liệu và tệp siêu dữ liệu mới trên cơ sở phiên bản hiện hành, rồi yêu cầu danh mục đổi con trỏ từ phiên bản cũ sang phiên bản mới. Thao tác đổi con trỏ là nguyên tử và chỉ thành công khi phiên bản hiện hành vẫn là phiên bản mà tiến trình đã dựa vào. Nếu một tiến trình khác đã cam kết trước, tiến trình này phải thử lại trên phiên bản mới. Tiến trình đọc luôn đọc một snapshot cố định, nên không bao giờ thấy một lần ghi làm dở. Vì các snapshot cũ vẫn còn trong siêu dữ liệu cho tới khi bị dọn, người dùng có thể đọc lại bảng ở một snapshot trong quá khứ, gọi là time travel [4].

Phân vùng ẩn là đặc điểm thứ ba. Thay vì tạo một cột phân vùng riêng, Iceberg lưu phép biến đổi từ cột gốc sang giá trị phân vùng, ví dụ `days(ts)`. Truy vấn lọc trên cột gốc được tự động chuyển thành điều kiện trên phân vùng [4].

Bảng Bronze của đề tài được tạo bởi hằng số `BRONZE_CREATE_DDL` trong `src/batch_layer/iceberg_utils.py`. Bảng dùng định dạng Iceberg phiên bản 2, tệp Parquet nén Snappy, phân vùng ẩn `days(trade_time_ts)` theo ngày UTC, kích thước tệp mục tiêu 128 MB và chế độ phân phối khi ghi `hash`. Danh mục là Iceberg REST tại `http://iceberg-rest:8181`, kho dữ liệu đặt tại `s3://warehouse/` trên MinIO. Lớp `IcebergManager` có sẵn hàm đọc Bronze theo một `snapshot-id` cụ thể và hàm liệt kê lịch sử snapshot, tức là hệ thống có khả năng time travel. Tuy vậy, hiện chưa có luồng xử lý hay script thực nghiệm nào gọi các hàm này.

### 2.3.3. Bài toán tệp nhỏ, kỹ thuật gộp tệp và bảo trì bảng

Vì mỗi lần cam kết tạo tệp dữ liệu mới, một bảng nhận nhiều lần ghi nhỏ sẽ tích tụ nhiều tệp nhỏ. Mục 1.2.2 đã phân tích tác hại của hiện tượng này đối với việc đọc. Lời giải là gộp định kỳ các tệp nhỏ thành ít tệp lớn hơn. Iceberg cung cấp thủ tục `rewrite_data_files` cho Spark với chiến lược mặc định là bin-packing: các tệp trong cùng một phân vùng được xếp vào những nhóm có tổng kích thước gần với kích thước mục tiêu, rồi mỗi nhóm được viết lại thành tệp mới [5]. Kết quả được cam kết như một snapshot mới, nên các truy vấn đang chạy trên snapshot cũ không bị ảnh hưởng. Thủ tục còn có chiến lược `sort`, sắp xếp lại dữ liệu theo một thứ tự cho trước trong lúc viết lại [5].

Gộp tệp chưa giải phóng dung lượng, vì các tệp cũ vẫn được snapshot cũ tham chiếu. Ba thủ tục bảo trì khác giải quyết phần còn lại [5]. `expire_snapshots` xóa các snapshot quá hạn cùng những tệp chỉ chúng tham chiếu. `remove_orphan_files` xóa các tệp nằm trong thư mục bảng nhưng không được siêu dữ liệu nào tham chiếu, ví dụ tệp của một lần ghi bị hỏng giữa chừng. `rewrite_manifests` gộp lại các tệp manifest để bước lập kế hoạch truy vấn duyệt ít tệp siêu dữ liệu hơn.

Đề tài hiện thực các thủ tục này trong `src/batch_layer/compaction.py` và `src/batch_layer/maintenance.py`. Hàm `CompactionJob.run_bin_packing_compaction()` gọi `rewrite_data_files` với chiến lược `binpack`, kích thước mục tiêu 128 MB, ngưỡng tệp nhỏ 64 MB và tối đa hai nhóm tệp được viết lại đồng thời. Hàm `run_table_maintenance()` chạy lần lượt gộp tệp, hết hạn snapshot cũ hơn 7 ngày nhưng giữ lại ít nhất 3 snapshot, xóa tệp mồ côi cũ hơn 3 ngày, và viết lại manifest. Một bước lỗi được ghi nhận rồi bỏ qua, không chặn các bước sau. Dagster gọi chuỗi này qua tài sản `iceberg_small_files_compaction` theo lịch hai giờ một lần.

## 2.4. LÝ THUYẾT XỬ LÝ LUỒNG VÀ QUẢN LÝ DỮ LIỆU BẤT THƯỜNG PHÂN TÁN

### 2.4.1. Thời gian sự kiện, cửa sổ và watermark

Mô hình Dataflow của Akidau và cộng sự phân biệt hai trục thời gian [2]. Thời gian sự kiện là lúc sự việc thực sự xảy ra. Thời gian xử lý là lúc hệ thống quan sát được nó. Khoảng lệch giữa hai trục không cố định: nó thay đổi theo tải mạng, tải của hệ thống và các sự cố trên đường truyền. Hệ quả là dữ liệu đến không theo thứ tự thời gian sự kiện, và hệ thống không bao giờ biết chắc mình đã nhận đủ dữ liệu của một khoảng thời gian [2], [14, Ch. 11]. Một kết quả gom theo thời gian xử lý thay đổi tùy theo thời điểm chạy. Một kết quả gom theo thời gian sự kiện thì tất định, nhưng phải trả lời câu hỏi khi nào chốt.

Để gom dữ liệu vô hạn, luồng được chia thành các cửa sổ. Cửa sổ cố định (tumbling window) có độ dài bằng nhau, nối tiếp và không chồng lên nhau. Cửa sổ trượt có độ dài cố định nhưng các cửa sổ chồng lên nhau. Cửa sổ phiên được xác định bởi các khoảng lặng giữa các sự kiện [2], [14, Ch. 11]. Nến một phút là cửa sổ cố định theo thời gian sự kiện, với công thức `window_start` đã nêu ở Mục 2.2.3.

Watermark là cách hệ thống ước lượng tiến độ trên trục thời gian sự kiện [2]. Trong Spark Structured Streaming, watermark được tính từ thời gian sự kiện lớn nhất đã thấy trừ đi một ngưỡng trễ do người dùng đặt [6]:

$$W_{\text{stream}} = \max(t_{\text{event}}) - \delta$$

Một cửa sổ kết thúc trước W~stream~ được coi là đã đủ dữ liệu. Trạng thái của nó có thể được giải phóng, và các sự kiện đến sau thuộc cửa sổ đó có thể bị bỏ qua. Bảo đảm này chỉ chặt theo một chiều: sự kiện trễ không quá δ chắc chắn được tính, còn sự kiện trễ hơn δ có thể bị loại nhưng không chắc chắn bị loại [6]. Ngưỡng δ vì vậy là nơi diễn ra sự đánh đổi giữa độ đúng, độ trễ và chi phí mà Akidau và cộng sự nêu [2]. δ lớn làm kết quả đúng hơn nhưng giữ nhiều trạng thái hơn và chốt kết quả muộn hơn.

Tầng tốc độ của đề tài đặt δ = 1 phút cho cửa sổ một phút (`window_aggregator.py`). Cần phân biệt W~stream~ này với watermark W của tầng lô ở Mục 2.2.1. W~stream~ nằm trong trạng thái của truy vấn Spark và quyết định khi nào tầng tốc độ bỏ qua sự kiện trễ. W của tầng lô nằm trong ClickHouse và quyết định tầng phục vụ đọc từ bảng nào. Ở chế độ đọc Kafka, tầng lô cũng chừa một khoảng trễ: ứng viên của W bằng thời gian sự kiện lớn nhất trừ 60 giây (`BATCH_WATERMARK_LATENESS_SECONDS`), làm tròn xuống phút, để không chốt một phút còn có thể nhận thêm giao dịch.

### 2.4.2. Mô hình Spark Structured Streaming và quản lý trạng thái

Spark là bộ máy xử lý dữ liệu phân tán, chia dữ liệu thành các phân hoạch và xử lý song song bằng các tác vụ [22]. Structured Streaming là giao diện xử lý luồng của Spark, xây dựng trên ý tưởng coi luồng dữ liệu như một bảng không giới hạn được nối thêm dòng liên tục [8]. Người dùng viết truy vấn như trên một bảng tĩnh bằng DataFrame hoặc SQL. Spark tự chuyển truy vấn đó thành dạng tăng dần và tối ưu nó bằng cùng bộ tối ưu Catalyst của Spark SQL [8]. Ở chế độ mặc định, truy vấn được thực thi theo từng micro-batch: sau mỗi chu kỳ kích hoạt, Spark đọc phần dữ liệu mới, cập nhật trạng thái và ghi kết quả [6], [8].

Kết quả được ghi theo một trong ba chế độ đầu ra [6]. Chế độ `append` chỉ ghi các dòng đã chốt, nên với phép gom theo cửa sổ, một cửa sổ chỉ xuất hiện sau khi watermark vượt qua nó. Chế độ `update` ghi lại mọi dòng thay đổi trong chu kỳ, nên một cửa sổ có thể được ghi nhiều lần với giá trị ngày càng đầy đủ. Chế độ `complete` ghi lại toàn bộ bảng kết quả mỗi chu kỳ. Structured Streaming bảo đảm xử lý đúng một lần khi nguồn phát lại được và đích ghi có tính lũy đẳng [8]. Tuy nhiên, đích `foreachBatch` do người dùng tự viết chỉ được bảo đảm ghi ít nhất một lần [6].

Các phép gom theo cửa sổ và khử trùng là phép toán có trạng thái. Trạng thái được lưu trong kho trạng thái (state store) và được ghi điểm kiểm vào thư mục checkpoint để phục hồi sau sự cố [6]. Spark có hai bộ cung cấp kho trạng thái: bộ mặc định giữ trạng thái trong bộ nhớ heap của máy ảo Java (Java Virtual Machine – JVM), và bộ dựa trên RocksDB giữ trạng thái ngoài heap [6]. Spark 3.5 bổ sung phép `dropDuplicatesWithinWatermark`, khử bản ghi trùng theo một tập cột và chỉ giữ trạng thái khử trùng trong phạm vi watermark [6].

Tầng tốc độ của đề tài (`src/speed_layer/spark_streaming.py`, `window_aggregator.py`) dùng các cơ chế trên như sau:

- **Chu kỳ và giới hạn đọc.** Micro-batch chạy 5 giây một lần, mỗi lần đọc tối đa 50 000 bản ghi Kafka (`maxOffsetsPerTrigger`) để chặn đợt tăng tải đột ngột.
- **Lọc, watermark và khử trùng.** Bản ghi phải có mã giao dịch không âm, mã cặp khác rỗng, giá và khối lượng dương, thời gian dương. Sau đó truy vấn đặt watermark 1 phút, khử trùng theo cặp khóa (`symbol`, `trade_id`) bằng `dropDuplicatesWithinWatermark`, rồi gom theo cửa sổ một phút của `event_time` và theo `symbol`.
- **Chế độ đầu ra và đích ghi.** Truy vấn chạy ở chế độ `update` với đích `foreachBatch`, ghi sang ClickHouse và Redis. Vì `foreachBatch` chỉ bảo đảm ít nhất một lần, và chế độ `update` ghi một cửa sổ nhiều lần, bảng `speed_agg` phải chịu được việc nhận nhiều phiên bản của cùng một cửa sổ. Mục 2.5.2 cho thấy động cơ `ReplacingMergeTree` đáp ứng yêu cầu này.
- **Kho trạng thái.** Trên Linux, mã nguồn chọn bộ cung cấp RocksDB. Tuy nhiên, bộ cung cấp bị khóa theo checkpoint: volume `speed-checkpoints` hiện có được tạo với bộ cung cấp mặc định, nên container vẫn chạy kho trạng thái trên heap cho tới khi checkpoint được tạo lại.
- **Chính sách khi mất dữ liệu.** Tùy chọn `failOnDataLoss` được đặt là `true`: nếu Kafka đã xóa các offset mà checkpoint còn cần, truy vấn dừng thay vì lặng lẽ bỏ qua dữ liệu.

### 2.4.3. Năm bài toán biên của luồng dữ liệu thị trường

Lý thuyết ở Mục 2.4.1 mô tả lệch thời gian ở dạng tổng quát. Với luồng giao dịch tiền mã hóa, các bất thường có những dạng cụ thể hơn. Đề tài phân tích năm bài toán biên, tức các tình huống ở rìa của hoạt động bình thường mà hệ thống vẫn phải xử lý đúng. Bảng 2.5 liệt kê từng bài toán cùng cơ chế xử lý trong mã nguồn và phần còn chưa giải quyết.

**Bảng 2.5: Năm bài toán biên và cơ chế xử lý trong hệ thống**

| Bài toán | Biểu hiện | Cơ chế trong mã nguồn | Giới hạn còn lại |
| :---: | :---: | :---: | :---: |
| 1. Mất gói WebSocket, đứt chuỗi mã giao dịch | Kết nối không báo lỗi nhưng thiếu giao dịch | `binance_ws.py` phát hiện mã nhảy cóc theo từng cặp và bù khoảng thiếu tối đa 500 mã qua REST `/api/v3/aggTrades` | Số lần đứt chuỗi không được ghi xuống bảng kiểm toán |
| 2. Sự kiện đến trễ, sai thứ tự | Thời gian xử lý lệch thời gian sự kiện | Gom theo thời gian sự kiện; watermark 1 phút ở tầng tốc độ; tầng lô tính lại không đặt watermark | Tầng tốc độ bỏ qua sự kiện trễ quá ngưỡng cho tới chu kỳ lô kế tiếp |
| 3. Bản ghi trùng tại ranh giới giao thức | Cùng giao dịch đến từ WebSocket và REST, hoặc do gửi lại | Khử trùng theo (`symbol`, `trade_id`) ở cả hai tầng | Tầng tốc độ chỉ khử trùng trong phạm vi watermark |
| 4. Bùng tải khi thị trường biến động mạnh | Số giao dịch mỗi giây tăng đột ngột | Giới hạn 50 000 bản ghi mỗi micro-batch; trần bộ nhớ của container | Kiểm thử TC4 chỉ ở mức logic, chưa đo trên Spark |
| 5. Ghi đè số liệu tạm thời bằng số liệu đã xác minh | Khung nhìn tốc độ sai cần được thay | Ưu tiên bản ghi tầng lô khi khử trùng theo (`symbol`, `window_start`); watermark tăng đơn điệu | Không có |

*Nguồn: nhóm thực hiện phân tích từ src/ingestion/binance_ws.py, src/speed_layer/window_aggregator.py, src/data_quality/dq_checks.py, src/serving_layer/query_merger.py.*

Bài toán 2 và 3 có một điểm tinh tế liên quan đến thứ tự. Trong mẫu 500 giao dịch thật nêu ở Mục 1.1.1, có 119 cặp giao dịch liên tiếp trùng mili-giây. Nếu chỉ sắp theo thời gian, giá mở cửa và đóng cửa của một cây nến có thể phụ thuộc vào thứ tự đến. Cả hai tầng của đề tài đều sắp theo cặp khóa (`trade_time`, `trade_id`), nên kết quả không phụ thuộc thứ tự đến. Bài toán 5 là bài toán mà thuật toán hợp nhất truy vấn được thiết kế để giải: một cây nến tạm thời không bị sửa tại chỗ, mà bị thay bởi cây nến của tầng lô ngay khi W vượt qua nó.

## 2.5. CÔNG NGHỆ KHO PHỤC VỤ PHÂN TÍCH OLAP VÀ HOT CACHE

### 2.5.1. Lưu trữ theo cột và thực thi vector hóa trong ClickHouse

Cơ sở dữ liệu giao dịch truyền thống lưu dữ liệu theo dòng: các giá trị của một bản ghi nằm cạnh nhau. Cách lưu này phù hợp khi mỗi thao tác đọc hoặc ghi một vài bản ghi đầy đủ. Truy vấn phân tích thì ngược lại: nó đọc nhiều bản ghi nhưng chỉ vài cột. Lưu trữ theo cột đặt các giá trị của cùng một cột cạnh nhau, nên truy vấn chỉ đọc những cột nó cần [14, Ch. 3]. Các giá trị cùng cột có cùng kiểu và thường lặp lại, nên nén tốt hơn nhiều so với dữ liệu theo dòng [14, Ch. 3]. Abadi và cộng sự cho thấy ưu thế của kho cột không đến từ riêng cách bố trí dữ liệu. Nó còn đến từ các kỹ thuật thực thi đi kèm như xử lý trực tiếp trên dữ liệu nén và trì hoãn việc ghép các cột thành dòng. Vì vậy, một kho dòng không thể đạt hiệu năng tương đương chỉ bằng cách mô phỏng bố cục cột [1].

ClickHouse là cơ sở dữ liệu xử lý phân tích trực tuyến (Online Analytical Processing – OLAP) mã nguồn mở theo hướng lưu trữ cột [20]. Bộ máy truy vấn của nó xử lý dữ liệu theo từng khối giá trị của một cột thay vì từng dòng, và tận dụng các chỉ thị xử lý nhiều dữ liệu trong một lệnh của CPU [20]. Cách thực thi vector hóa này làm giảm chi phí gọi hàm trên mỗi giá trị, vốn chiếm phần lớn thời gian khi xử lý từng dòng.

### 2.5.2. Họ động cơ bảng MergeTree và ReplacingMergeTree

Động cơ bảng chính của ClickHouse là họ MergeTree [11], [20]. Mỗi lần chèn tạo ra một phần dữ liệu (part) mới, được sắp xếp sẵn theo khóa sắp xếp của bảng. Ở chế độ nền, ClickHouse liên tục gộp các phần nhỏ thành phần lớn hơn. Các biến thể của MergeTree tận dụng bước gộp này để biến đổi dữ liệu [20]. Với `ReplacingMergeTree`, khi gộp, các dòng có cùng khóa sắp xếp được thay bằng một dòng duy nhất. Nếu bảng khai báo cột phiên bản, dòng được giữ lại là dòng có phiên bản lớn nhất [11].

Việc khử trùng này có hai giới hạn quan trọng. ClickHouse không bảo đảm khi nào bước gộp diễn ra, và chỉ khử trùng các dòng nằm trong những phần được gộp với nhau [11]. Vì vậy, một truy vấn thông thường có thể thấy nhiều phiên bản của cùng một khóa. Từ khóa `FINAL` buộc ClickHouse khử trùng ngay lúc đọc, đổi lại truy vấn tốn thêm chi phí xử lý [11].

Bốn bảng `speed_agg`, `batch_agg`, `reconciliation_audit_metrics` và `system_watermark` của đề tài đều dùng `ReplacingMergeTree`. Ba bảng đầu dùng `ReplacingMergeTree(created_at)` với khóa sắp xếp (`symbol`, `window_start`) và phân vùng theo ngày. `system_watermark` dùng `ReplacingMergeTree(updated_at)` với khóa (`layer`). Lựa chọn này giải quyết yêu cầu nêu ở Mục 2.4.2. Mỗi lần tầng tốc độ ghi lại một cửa sổ, hoặc tầng lô chạy lại, ClickHouse nhận một phiên bản mới. Phiên bản có `created_at` lớn nhất là phiên bản đúng. Lớp `ClickHouseQueryClient` của tầng phục vụ đọc với `FINAL`, nên luôn thấy phiên bản mới nhất kể cả khi bước gộp nền chưa chạy. Ghi lại cùng một dữ liệu không làm sai kết quả, tức thao tác ghi có tính lũy đẳng ở cấp khóa.

Có một hệ quả phải xử lý ở phía ghi. `ReplacingMergeTree` thay cả dòng chứ không cộng dồn. Nếu tầng lô tính một cây nến từ một phần giao dịch rồi ghi đè lên cây nến đầy đủ, cây nến đầy đủ bị mất. Đây là lý do tầng lô ở chế độ đọc Kafka phải tính lại mỗi cửa sổ bị ảnh hưởng từ toàn bộ giao dịch đã biết trong `SilverTradeStore` (Mục 3.4). Bảng `system_watermark` cũng vậy: khi gộp, ClickHouse giữ dòng có `updated_at` mới nhất, không phải dòng có `watermark_time` lớn nhất. Tính tăng đơn điệu của W vì vậy phải được bảo đảm ở phía ghi, bằng hàm `update_watermark()` nêu ở Mục 2.2.1.

### 2.5.3. Vai trò của Redis làm bộ đệm nóng

Redis là kho khóa – giá trị trong bộ nhớ. Khi dung lượng dùng chạm giới hạn `maxmemory`, Redis loại bớt khóa theo chính sách đã cấu hình. Với chính sách `allkeys-lru`, khóa bị loại là khóa ít được truy cập gần đây nhất, chọn theo một thuật toán xấp xỉ dựa trên lấy mẫu [19]. Chính sách này phù hợp khi một phần nhỏ dữ liệu được đọc nhiều hơn hẳn phần còn lại [19].

Trong hệ thống, Redis chạy với `--maxmemory 256mb --maxmemory-policy allkeys-lru --save ""` trong container giới hạn 384 MB. Tùy chọn `--save ""` tắt việc lưu xuống đĩa, nên dữ liệu trong Redis mất khi container khởi động lại. Điều này chấp nhận được vì Redis chỉ giữ bản sao. Mỗi micro-batch, `RedisSpeedWriter` ghi mỗi cây nến vào khóa `speed:candle:{symbol}:{epoch_ms}` và cập nhật khóa `speed:candle:{symbol}:latest` trỏ tới cây nến mới nhất của cặp đó. Mọi khóa có thời gian tồn tại (Time-To-Live – TTL) 3 600 giây. Bản gốc của mọi cây nến vẫn nằm trong `speed_agg` trên ClickHouse, và thao tác ghi ClickHouse luôn diễn ra trước, độc lập với Redis.

Vai trò của Redis trong đề tài hẹp hơn mô hình "bộ đệm cho tầng phục vụ" thường gặp. Chỉ điểm cuối `/api/speed/latest` đọc Redis. Thuật toán hợp nhất truy vấn chỉ đọc ClickHouse, nên các con số độ trễ của Thực nghiệm 1 không bao gồm Redis. Khi Redis lỗi, `RedisSpeedWriter` tạm ngừng ghi trong 30 giây rồi tự thử kết nối lại, và tầng tốc độ vẫn tiếp tục ghi ClickHouse bình thường.

## 2.6. QUẢN TRỊ CHẤT LƯỢNG DỮ LIỆU CHỦ ĐỘNG (DATA QUALITY ENGINEERING)

### 2.6.1. Hợp đồng dữ liệu và cổng kiểm soát chất lượng

Hợp đồng dữ liệu là bản cam kết giữa bên phát dữ liệu và bên dùng dữ liệu về cấu trúc và ý nghĩa của từng trường. Trong đề tài, bên phát là tầng thu thập, còn bên dùng là tầng tốc độ và tầng lô. Hợp đồng được viết bằng JSON Schema phiên bản Draft 2020-12, một ngôn ngữ mô tả cấu trúc của tài liệu JSON được đặc tả bởi Wright và cộng sự [21]. Tệp `datasets/schemas/trade_event_schema.json` quy định kiểu và ràng buộc của từng trường. Ví dụ, `trade_id` là số nguyên không âm, `symbol` chỉ gồm chữ in hoa và chữ số, `price` và `quantity` tối thiểu 10^−8^, và `fault_type` chỉ nhận một trong năm giá trị lỗi hoặc rỗng.

Lược đồ là hợp đồng được công bố, không phải trình kiểm tra chạy lúc vận hành. Không có đoạn mã nào gọi một bộ kiểm tra JSON Schema. Ràng buộc được thực thi ở hai nơi: bộ lọc Spark của tầng tốc độ (Mục 2.4.2) và cổng kiểm soát chất lượng của tầng lô trong `src/data_quality/dq_checks.py`. Hàm `clean_and_deduplicate()` chạy hai bước. Bước chuẩn hóa kiểm tra năm trường bắt buộc và ép kiểu. Bước kiểm định áp các luật ngữ nghĩa theo thứ tự cố định và trả về mã lỗi đầu tiên vi phạm: `trade_id_negative`, `symbol_empty`, `price_not_finite`, `quantity_not_finite`, `price_non_positive`, `quantity_non_positive`, `trade_time_non_positive`. Hai luật `*_not_finite` đứng trước các luật về dấu vì phép so sánh `NaN <= 0` trả về sai, nên một giá `NaN` sẽ lọt qua nếu chỉ kiểm tra dấu. Bản ghi đạt mọi luật được khử trùng theo (`symbol`, `trade_id`). Khi trùng khóa, bản ghi có `ingestion_time` lớn hơn được giữ.

### 2.6.2. Mô hình cách ly dữ liệu lỗi (Quarantine Pattern)

Khi gặp bản ghi hỏng, một đường dữ liệu có ba lựa chọn. Dừng hẳn để báo lỗi thì một bản ghi xấu làm ngừng cả hệ thống. Bỏ qua trong im lặng thì không ai biết dữ liệu đã mất. Lựa chọn thứ ba là cách ly: tách bản ghi lỗi sang một nơi riêng kèm lý do, để đường chính tiếp tục chạy và người vận hành vẫn điều tra được sau đó.

Đề tài áp dụng cách ly ở hai điểm. Ở tầng thu thập, bản tin WebSocket không phân tích được thành JSON hoặc thiếu trường bắt buộc được đóng gói nguyên văn vào `DLQEvent`, rồi ghi vào hàng đợi chứa bản ghi lỗi (Dead-Letter Queue – DLQ) là topic `crypto_trades_dlq`. Ở tầng lô, bản ghi vi phạm luật chất lượng được ghi vào bảng `lakehouse.dq_quarantine` cùng mã lỗi `dq_error`, mã lượt chạy `batch_run_id` và bản ghi gốc `raw_record`. Mọi cột dữ liệu của bảng cách ly có kiểu chuỗi, để giữ nguyên cả những giá trị sai kiểu. Bản ghi trùng không được đưa vào bảng cách ly. Chúng không phải dữ liệu lỗi mà là dữ liệu thừa, chỉ được đếm vào `duplicates_removed`.

### 2.6.3. Kiểm thử hỗn loạn và tiêm lỗi có kiểm soát

Basiri và cộng sự định nghĩa kỹ thuật hỗn loạn (chaos engineering) là thực nghiệm trên một hệ thống phân tán nhằm tạo niềm tin vào khả năng chịu đựng của nó trước các điều kiện bất lợi [10]. Cách làm gồm bốn nguyên tắc [10]. Thứ nhất, định nghĩa trạng thái ổn định của hệ thống qua các chỉ số quan sát được, rồi đặt giả thuyết rằng trạng thái này được giữ. Thứ hai, đưa vào các sự kiện mô phỏng sự cố có thật. Thứ ba, chạy thực nghiệm trên môi trường càng gần môi trường thật càng tốt. Thứ tư, tự động hóa để thực nghiệm chạy được thường xuyên.

Đề tài áp dụng tư tưởng này ở quy mô nhỏ, theo hai cách. Cách thứ nhất là bộ tiêm lỗi `FaultInjector` (`src/ingestion/fault_injector.py`), đặt giữa bộ thu thập và Kafka. Bộ tiêm lỗi rút một số ngẫu nhiên u ∈ [0, 1) cho mỗi sự kiện và so với các ngưỡng cộng dồn t~1~ = s, t~2~ = s + l, t~3~ = s + l + o, t~4~ = s + l + o + d. Ở đây s, l, o, d là tỷ lệ của bốn loại lỗi trong Bảng 2.6. Mỗi loại lỗi chiếm một đoạn rời nhau của [0, 1), nên xác suất thực tế của mỗi loại bằng đúng tham số cấu hình khi s + l + o + d ≤ 1. Mỗi bản ghi bị tiêm lỗi mang cờ `is_injected = true` và tên lỗi `fault_type`. Nhờ đó, thực nghiệm biết chính xác bản ghi nào là lỗi, tức có sẵn nhãn đúng để chấm điểm cổng chất lượng.

**Bảng 2.6: Bốn loại lỗi của bộ tiêm lỗi và tình huống thực tế được mô phỏng**

| Loại lỗi | Hành vi trong mã nguồn | Tình huống mô phỏng | Cơ chế phát hiện |
| :---: | :---: | :---: | :---: |
| `schema_invalid` | Thay bản ghi gốc bằng bản có giá âm và khối lượng 0 | Lỗi tuần tự hóa, dữ liệu hỏng trên đường truyền | Luật `price_non_positive` của cổng chất lượng |
| `late_data` | Thay bản ghi gốc bằng bản có `trade_time` lùi 60 đến 300 giây | Sự kiện đến trễ so với thời gian sự kiện | Watermark ở tầng tốc độ; tầng lô tính theo thời gian sự kiện |
| `out_of_order` | Giữ bản ghi trong bộ đệm, mỗi sự kiện sau có xác suất 30% xả một bản | Sự kiện đến sai thứ tự | Gom theo thời gian sự kiện ở cả hai tầng |
| `duplicate` | Gửi thêm một bản sao cùng mã giao dịch | Gửi lại theo cơ chế ít nhất một lần, ranh giới REST – WebSocket | Khử trùng theo (`symbol`, `trade_id`) |

*Nguồn: src/ingestion/fault_injector.py; thực nghiệm chất lượng dữ liệu sinh thêm lỗi `negative_price` trước khi qua bộ tiêm lỗi (scripts/benchmarks/bench_fault.py).*

Với các nhãn đúng này, cổng chất lượng được đánh giá như một bộ phân loại nhị phân. Lớp dương là bản ghi lỗi cần cách ly, tức `schema_invalid` và `negative_price`. Gọi TP là số bản ghi lỗi bị cách ly, FP là số bản ghi hợp lệ bị cách ly nhầm, FN là số bản ghi lỗi lọt vào Silver, và TN là số bản ghi hợp lệ được cho qua. Các bản ghi `late_data` và `out_of_order` hợp lệ về nội dung nên thuộc lớp âm. Bốn chỉ số được tính như sau:

$$\text{Precision} = \frac{TP}{TP + FP} \qquad \text{Recall} = \frac{TP}{TP + FN} \qquad F_1 = \frac{2 \times \text{Precision} \times \text{Recall}}{\text{Precision} + \text{Recall}}$$

$$\text{Accuracy} = \frac{TP + TN}{N_{\text{total}}}$$

Trong `bench_fault.py`, mẫu số N~total~ của Accuracy là tổng số bản ghi đưa vào, gồm cả các bản trùng đã bị khử. Bản trùng không thuộc ô nào của ma trận nhầm lẫn, nên Accuracy có thể nhỏ hơn 1 ngay cả khi Precision và Recall đều bằng 1. Chương 4 trình bày kết quả này.

Cách thứ hai là bốn kịch bản kiểm thử hỗn loạn trong script `run_chaos_smoke_tests.py`: mất chuỗi mã giao dịch, sự kiện trễ gấp đôi watermark, bản ghi trùng hàng loạt, và bùng tải gấp mười lần. Mỗi kịch bản đặt một giả thuyết về trạng thái ổn định, ví dụ ΔV~%~ trở về 0 sau đối soát, rồi kiểm tra giả thuyết đó. So với các nguyên tắc của Basiri và cộng sự, các kịch bản này mới đáp ứng nguyên tắc thứ nhất, thứ hai và thứ tư. Chúng chạy trong một tiến trình Python trên dữ liệu tự sinh, với ClickHouse được thay bằng dữ liệu giả lập, chứ chưa chạy trên các container thật. Nguyên tắc thứ ba vì vậy chưa được đáp ứng, như đã nêu ở Mục 1.3.3.
