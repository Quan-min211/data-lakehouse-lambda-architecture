# CHƯƠNG 1: TỔNG QUAN VÀ ĐẶT VẤN ĐỀ

Chương này trả lời ba câu hỏi: vì sao bài toán giám sát thị trường tiền mã hóa cần một kiến trúc dữ liệu riêng, các cách tiếp cận hiện có thiếu điều gì, và đề tài giải quyết phần thiếu đó bằng phương pháp nào. Mọi số liệu trong chương được lấy từ dữ liệu do chính hệ thống thu thập và từ nhật ký thực nghiệm trong thư mục `results/logs/`. Chương 4 trình bày đầy đủ các số liệu thực nghiệm.

## 1.1. BỐI CẢNH VÀ TÍNH CẤP THIẾT CỦA ĐỀ TÀI

### 1.1.1. Đặc thù giao dịch 24/7/365 và tính biến động cao của thị trường tiền mã hóa

Thị trường chứng khoán truyền thống có phiên giao dịch. Khi phiên đóng cửa, dữ liệu ngừng phát sinh, và khoảng lặng sau giờ đóng cửa là lúc các hệ thống xử lý cuối ngày chạy để chốt sổ, đối chiếu và lập báo cáo. Sàn tiền mã hóa không có khoảng lặng đó. Bộ khớp lệnh của sàn Binance chạy liên tục mọi giờ trong ngày, mọi ngày trong năm. Khái niệm "một ngày giao dịch" chỉ còn là một quy ước theo múi giờ UTC, và hệ thống của đề tài ghi mọi dấu thời gian theo UTC vì lý do này.

Mật độ giao dịch cũng rất cao. Bảng 1.1 tổng hợp ảnh chụp thống kê 24 giờ của Binance, do script thu thập của đề tài lấy qua điểm cuối REST `/api/v3/ticker/24hr`. Script lọc các cặp niêm yết bằng USDT, loại token đòn bẩy và đồng ổn định giá, rồi sắp xếp giảm dần theo khối lượng quy đổi ra USDT.

**Bảng 1.1: Thống kê 24 giờ của năm cặp giao dịch USDT có khối lượng quy đổi lớn nhất trên Binance**

| Cặp giao dịch | Số giao dịch trong 24 giờ | Trung bình giao dịch mỗi giây | Khối lượng quy đổi (triệu USDT) | Thay đổi giá 24 giờ (%) | Biên độ (High − Low) / Low (%) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| `BTCUSDT` | 2 817 526 | 32,61 | 1 070,02 | −0,657 | 2,64 |
| `ETHUSDT` | 3 513 803 | 40,67 | 600,16 | −0,821 | 4,20 |
| `SOLUSDT` | 660 361 | 7,64 | 170,90 | −1,203 | 4,83 |
| `ZECUSDT` | 513 269 | 5,94 | 144,98 | −5,457 | 10,27 |
| `NEARUSDT` | 544 997 | 6,31 | 141,24 | −2,260 | 8,94 |

*Nguồn: datasets/raw/tickers/ticker_24hr_snapshot.json, khung 24 giờ từ 2026-10-16 08:16 đến 2026-10-17 08:16 UTC; cột trung bình mỗi giây và cột biên độ do nhóm tính từ các trường `count`, `highPrice`, `lowPrice`.*

Riêng cặp `BTCUSDT` có hơn 2,8 triệu giao dịch trong một ngày, tức trung bình khoảng 33 giao dịch mỗi giây. Con số trung bình này che đi hình dạng thật của luồng dữ liệu. Tệp `datasets/raw/aggtrades/btcusdt_aggtrades.json` chứa 500 giao dịch tổng hợp liên tiếp do hệ thống thu thập trong khoảng 60 giây, từ 08:15:38 đến 08:16:38 UTC ngày 2026-10-17. Trong mẫu này, trung bình mỗi giây có 8,2 giao dịch tổng hợp, nhưng giây đông nhất có 75 giao dịch, gấp khoảng 9 lần trung bình. Có 119 cặp giao dịch liên tiếp mang cùng một dấu thời gian tính đến mili-giây. Luồng dữ liệu vì vậy đến theo cụm, và thứ tự giữa các giao dịch cùng mili-giây phải được xác định bằng một quy tắc riêng chứ không thể dựa vào thời gian. Ngay trong khung 24 giờ ở Bảng 1.1, giá `ZECUSDT` dao động trong biên độ hơn 10% và giảm 5,457%, trong khi `BTCUSDT` chỉ biến động 2,64%. Một hệ thống không giới hạn lượng dữ liệu đọc vào mỗi lần có thể bị tràn bộ nhớ (Out-Of-Memory – OOM) khi gặp một đợt tăng tải đột ngột, đúng vào lúc người dùng cần nó nhất.

Ba đặc điểm trên quyết định hình dạng của hệ thống. Không có giờ đóng cửa nên không có thời điểm "đã đủ dữ liệu" để chạy xử lý lô một lần cho cả ngày. Mật độ cao và không đều buộc tầng thu thập phải có cơ chế giới hạn tốc độ đọc; tầng tốc độ của đề tài giới hạn mỗi micro-batch đọc tối đa 50 000 bản ghi Kafka. Biến động lớn trong thời gian ngắn khiến cảnh báo phải được tính trên từng cây nến một phút; một cây nến bị gắn cờ đột biến khi giá đóng cửa lệch giá mở cửa từ 2% hoặc biên độ trong phút đạt từ 3% trở lên.

### 1.1.2. Nhu cầu phân tích thời gian thực kết hợp kiểm toán lịch sử

Một tổ chức tham gia thị trường tiền mã hóa có hai nhóm người dùng dữ liệu, với hai yêu cầu kéo về hai hướng ngược nhau.

Nhóm thứ nhất là bộ phận giám sát rủi ro. Họ cần biết giá và khối lượng của phút đang diễn ra, cần được cảnh báo ngay khi giá trượt mạnh, và chấp nhận một con số gần đúng nếu con số đó đến kịp. Một cảnh báo đến sau năm phút thì không còn giá trị phòng vệ. Đề tài đặt cam kết chất lượng dịch vụ (Service Level Agreement – SLA) cho nhóm này là độ trễ đầu-cuối dưới 5 giây.

Nhóm thứ hai là bộ phận kiểm toán, quyết toán và nghiên cứu chiến lược. Họ dùng dữ liệu lịch sử để chốt số liệu, kiểm thử lại chiến lược trên dữ liệu quá khứ (backtesting) và trả lời câu hỏi của cơ quan quản lý. Với nhóm này, một cây nến sai vài phần nghìn không phải "gần đúng". Đó là sai. Họ cần kết quả tất định: chạy lại trên cùng tập dữ liệu phải ra cùng một con số, mọi giao dịch đến trễ phải được tính, và mọi giao dịch trùng phải bị loại.

**Bảng 1.2: Đối sánh hai nhóm nhu cầu khai thác dữ liệu thị trường**

| Tiêu chí | Giám sát tức thời | Kiểm toán lịch sử |
| :---: | :---: | :---: |
| Người dùng chính | Bộ phận giám sát rủi ro, nhà phân tích thị trường | Bộ phận kiểm toán, quyết toán, nghiên cứu chiến lược |
| Độ trễ chấp nhận được | Dưới 5 giây | Một chu kỳ xử lý lô (15 phút trong hệ thống) |
| Yêu cầu về độ chính xác | Gần đúng, ưu tiên kịp thời | Chính xác và tái lập được |
| Xử lý giao dịch đến trễ | Bỏ qua nếu trễ quá ngưỡng watermark 1 phút | Phải tính đủ theo thời gian sự kiện |
| Nhãn trạng thái dữ liệu | `Provisional` (tạm thời) | `Reconciled` (đã đối soát) |
| Bảng phục vụ trong ClickHouse | `lakehouse.speed_agg` | `lakehouse.batch_agg` |

*Nguồn: tổng hợp từ yêu cầu nghiệp vụ của đề tài; giá trị cấu hình lấy từ src/speed_layer/spark_streaming.py, dagster_project/repository.py và configs/clickhouse/init.sql.*

Mâu thuẫn giữa hai cột của Bảng 1.2 không phải do công nghệ chưa đủ nhanh. Nó nằm ở bản chất của dữ liệu không giới hạn và đến không theo thứ tự. Akidau và cộng sự [2] chỉ ra rằng một hệ thống xử lý luồng luôn phải cân bằng ba đại lượng: độ đúng, độ trễ và chi phí. Muốn trả lời sớm, hệ thống phải chốt kết quả trước khi biết chắc mọi dữ liệu của khoảng thời gian đó đã đến. Muốn trả lời đúng, nó phải chờ, và không ai biết trước phải chờ bao lâu.

### 1.1.3. Tính cấp thiết của kiến trúc dữ liệu lai hỗ trợ đối soát tự động

Cách làm phổ biến là dựng hai hệ thống tách rời: một luồng xử lý trực tuyến cho bảng điều khiển và một quy trình xử lý lô chạy định kỳ cho báo cáo. Cách này đáp ứng được cả hai nhóm người dùng, nhưng sinh ra một vấn đề mới. Cùng một phút giao dịch giờ đây có hai con số. Người dùng phải tự biết nên tin con số nào, và không ai đo được hai con số đó lệch nhau bao nhiêu.

Kiến trúc Lambda do Marz và Warren đề xuất [17] đặt nền móng cho lời giải. Tầng xử lý lô tính lại khung nhìn từ tập dữ liệu gốc bất biến, tầng tốc độ bù phần dữ liệu mới mà tầng lô chưa kịp xử lý, và tầng phục vụ hợp nhất hai khung nhìn khi trả lời truy vấn. Mô hình gốc để ngỏ hai chi tiết mà bài toán tài chính không thể bỏ qua. Thứ nhất, ranh giới giữa phần "đã đối soát" và phần "tạm thời" phải được xác định rõ ràng và chỉ được tiến về phía trước. Nếu ranh giới lùi lại, một phút đã công bố là chính xác có thể bị thay bằng số liệu tạm thời. Thứ hai, ở phút nằm ngay ranh giới, nếu phép hợp nhất lấy cả hai nguồn thì khối lượng giao dịch bị cộng hai lần.

Đề tài giải quyết hai chi tiết này bằng một mốc thời gian đối soát duy nhất, gọi là watermark W của tầng lô. Mốc W được tầng lô ghi vào bảng `lakehouse.system_watermark` và chỉ được ghi khi giá trị mới lớn hơn giá trị hiện tại. Thuật toán hợp nhất truy vấn `AutoCorrectingQueryMerger` ở tầng phục vụ đọc W cho mỗi truy vấn và tách khoảng thời gian được hỏi thành phần trước W (lấy từ `batch_agg`) và phần từ W trở đi (lấy từ `speed_agg`). Khi chu kỳ lô kế tiếp hoàn tất và W tiến lên, các phút từng mang nhãn `Provisional` tự động được trả lời bằng số liệu `Reconciled`. Không cần ai can thiệp. Đây là ý nghĩa của từ "tự sửa sai" (auto-correcting) trong tên đề tài, và cũng là lý do một kiến trúc lai có đối soát tự động là cần thiết, chứ không chỉ là một lựa chọn thiết kế.

## 1.2. GIỚI HẠN CỦA CÁC TIẾP CẬN TRUYỀN THỐNG VÀ VẤN ĐỀ NGHIÊN CỨU

### 1.2.1. Phân tích, đánh giá các công trình và hướng tiếp cận có liên quan

Mục này xét bốn hướng tiếp cận theo cùng một câu hỏi: hướng đó trả lời được nhu cầu nào trong Bảng 1.2, và phải trả giá gì.

**Xử lý lô thuần túy (Batch-only).** Đây là con đường của kho dữ liệu (Data Warehouse – DWH) truyền thống: dữ liệu được trích xuất, biến đổi và nạp theo chu kỳ, thường là hằng đêm [13, Ch. 1]. Armbrust và cộng sự [9] mô tả bước tiến tiếp theo là Lakehouse, đặt một định dạng bảng mở có giao dịch đảm bảo tính nguyên tử, nhất quán, cô lập và bền vững (Atomicity, Consistency, Isolation, Durability – ACID) lên trên lưu trữ đối tượng giá rẻ. Lakehouse giải quyết tốt bài toán lưu trữ và độ chính xác, nhưng không thay đổi độ trễ. Dữ liệu mới nhất mà người dùng thấy luôn cũ bằng một chu kỳ lô. Với chu kỳ 15 phút như hệ thống của đề tài, một cú sập giá ở phút thứ hai của chu kỳ chỉ xuất hiện trên báo cáo sau gần 15 phút.

**Xử lý luồng thuần túy (Streaming-only).** Spark Structured Streaming [8] và mô hình Dataflow [2] cho phép gom cửa sổ theo thời gian sự kiện và trả kết quả sau vài giây. Để bộ nhớ trạng thái không phình vô hạn, hệ thống phải đặt ngưỡng watermark: sự kiện trễ hơn ngưỡng có thể bị loại [6]. Đây là sự đánh đổi có chủ đích, và nó để lại một hậu quả lâu dài. Phút nào đã mất sự kiện thì sai vĩnh viễn, vì không có tầng nào tính lại. Các sai lệch khác cũng tích tụ theo thời gian: giao dịch bị lặp ở thời điểm kết nối lại, giao dịch bị rơi khi WebSocket mất gói. Ta gọi hiện tượng này là trôi dạt trạng thái (state drift). Nguy hiểm hơn, hệ thống không có chuẩn nào để tự phát hiện mình đã trôi.

**Kiến trúc Lambda.** Marz và Warren [17] giữ cả hai đường. Tầng lô cho kết quả đúng, tầng tốc độ cho kết quả kịp thời, tầng phục vụ ghép chúng lại. Kreps [15] và Kleppmann [14, Ch. 12] nêu hai điểm yếu của mô hình: cùng một logic biến đổi phải viết và bảo trì ở hai hệ thống khác nhau, và việc ghép đầu ra của hai đường không hề đơn giản. Cả hai phê bình đều đúng với bài toán này. Phép ghép chính là nơi xảy ra lỗi cộng hai lần nêu ở Mục 1.1.3.

**Kiến trúc Kappa.** Hướng này do Kreps đề xuất [15]: bỏ hẳn tầng lô và giữ một đường xử lý luồng duy nhất. Muốn tính lại lịch sử, hệ thống phát lại nhật ký sự kiện từ đầu qua cùng mã xử lý luồng [15], [14, Ch. 12]. Hướng này dựa vào nhật ký bền vững kiểu Kafka [16], nên khả năng tái xử lý bị giới hạn bởi thời gian lưu giữ của nhật ký. Topic `crypto_trades_raw` của đề tài giữ dữ liệu 24 giờ (`retention.ms = 86 400 000`). Với cấu hình đó, một kiến trúc Kappa chỉ tái xử lý được dữ liệu của ngày gần nhất. Kéo dài thời gian lưu giữ thì chi phí lưu trữ của broker tăng theo, trong khi lưu trữ đối tượng rẻ hơn nhiều cho cùng lượng dữ liệu.

**Bảng 1.3: Đối sánh các hướng tiếp cận kiến trúc xử lý dữ liệu thị trường**

| Tiêu chí | Batch-only | Streaming-only | Kappa | Lambda (đề tài lựa chọn) |
| :---: | :---: | :---: | :---: | :---: |
| Độ trễ dữ liệu mới nhất | Một chu kỳ lô | Vài giây | Vài giây | Vài giây (tầng tốc độ) |
| Độ chính xác lịch sử | Cao | Mất phần sự kiện trễ quá watermark | Cao nếu phát lại toàn bộ nhật ký | Cao (tầng lô tính lại) |
| Xử lý sự kiện đến trễ | Tính đủ ở chu kỳ sau | Loại bỏ vĩnh viễn | Chỉ khi phát lại | Tầng lô tính lại theo thời gian sự kiện |
| Tái xử lý lịch sử | Chạy lại trên kho | Không có | Giới hạn bởi thời gian lưu giữ nhật ký | Chạy lại trên tập dữ liệu gốc bất biến |
| Đo được độ lệch giữa hai khung nhìn | Không áp dụng | Không có chuẩn đối chiếu | Không có chuẩn đối chiếu | Có |
| Chi phí chính | Độ trễ cao | Sai lệch tích tụ | Lưu giữ nhật ký dài | Hai đường xử lý, phép ghép phức tạp |

*Nguồn: nhóm thực hiện tổng hợp từ [2], [9], [14], [15], [16], [17] và cấu hình configs/kafka/topics.yaml của hệ thống.*

Đề tài chọn Lambda vì đây là hướng duy nhất trong Bảng 1.3 có một chuẩn đối chiếu sẵn có: tầng lô. Tuy vậy, đề tài không chấp nhận nguyên trạng hai điểm yếu nêu trên. Điểm yếu thứ nhất được giảm bằng cách dùng chung mã: cả tầng tốc độ lẫn tầng xử lý lô cùng gọi bộ phát hiện đột biến `SpikeDetector` với cùng hai ngưỡng, và tiêu chí khớp số liệu được đặt tại một mô-đun duy nhất `src/utils/reconciliation.py`. Điểm yếu thứ hai, phép ghép, trở thành đối tượng nghiên cứu chính của đề tài. Lưu trữ gốc đi theo hướng Lakehouse [9] với định dạng bảng Apache Iceberg [4] trên MinIO.

### 1.2.2. Vấn đề tệp nhỏ (Small File Problem) trên Data Lake khi nạp dữ liệu liên tục

Định dạng bảng Apache Iceberg [4] bảo đảm tính nguyên tử bằng ảnh chụp (snapshot): mỗi lần ghi tạo ra các tệp dữ liệu Parquet mới cùng một snapshot mới trỏ tới chúng, thay vì ghi nối vào tệp đã có. Cơ chế này cho phép đọc và ghi đồng thời mà không khóa bảng, nhưng có một hệ quả phụ. Ghi càng thường xuyên với lượng dữ liệu mỗi lần càng ít thì bảng càng nhiều tệp, và tệp càng nhỏ.

Trong hệ thống, khi chế độ ghi Iceberg được bật, bảng Bronze `iceberg_catalog.bronze.crypto_trades` nhận một lần ghi nối sau mỗi chu kỳ lô. Dagster lập lịch tầng lô 15 phút một lần, tức 96 lần ghi mỗi ngày, và mỗi lần ghi tạo ít nhất một tệp mới cho mỗi phân vùng ngày có dữ liệu. Bảng được cấu hình kích thước tệp mục tiêu 128 MB, nhưng một lượt 15 phút của vài cặp giao dịch chỉ có vài megabyte dữ liệu. Số tệp tăng tuyến tính theo thời gian chạy, còn kích thước mỗi tệp không bao giờ chạm tới mục tiêu.

Nhiều tệp nhỏ làm chậm việc đọc theo ba cách. Mỗi tệp trên lưu trữ đối tượng tốn ít nhất một yêu cầu HTTP riêng, và chi phí mở kết nối, đọc phần chân tệp chứa siêu dữ liệu, giải mã lược đồ được lặp lại cho từng tệp. Spark chia dữ liệu đầu vào thành các phân hoạch và chạy một tác vụ cho mỗi phân hoạch [22], nên hàng trăm tệp vài trăm kilobyte có thể sinh ra hàng trăm tác vụ ngắn mà chi phí lập lịch lấn át thời gian tính toán. Cuối cùng, mỗi tệp dữ liệu có một mục trong tệp kê khai (manifest) của Iceberg [4], nên bước lập kế hoạch truy vấn phải duyệt nhiều siêu dữ liệu hơn.

Đề tài tái tạo hiện tượng này có kiểm soát bằng script `prepare_small_files.py`. Script ghi nối lặp lại từng khối 25 bản ghi vào bảng Bronze. Trước khi gộp tệp, bảng có 118 tệp Parquet với kích thước trung bình 0,97 MB, nhỏ hơn mục tiêu 128 MB khoảng 132 lần. Câu hỏi đặt ra là thủ tục gộp tệp bin-packing (`rewrite_data_files`) của Iceberg [5] cải thiện tốc độ đọc được bao nhiêu, và với chi phí nào.

### 1.2.3. Vấn đề lệch pha dữ liệu (Data Divergence) và đánh đổi giữa độ trễ và độ chính xác

Khung nhìn tốc độ (Speed View) và khung nhìn lô (Batch View) được tính từ cùng một luồng giao dịch, nhưng hiếm khi cho cùng một kết quả. Hiện tượng hai khung nhìn lệch nhau trên cùng một cây nến được gọi là lệch pha dữ liệu (data divergence). Đề tài đo lệch pha bằng hai đại lượng chính trên mỗi cửa sổ một phút. Đại lượng thứ nhất là sai số tuyệt đối của giá trung bình gia quyền theo khối lượng (Volume-Weighted Average Price – VWAP):

$$\varepsilon_{\text{VWAP}} = \left| \text{VWAP}_{\text{speed}} - \text{VWAP}_{\text{batch}} \right|$$

Đại lượng thứ hai là độ lệch tương đối của tổng khối lượng giao dịch:

$$\Delta V_{\%} = \frac{\left| V_{\text{speed}} - V_{\text{batch}} \right|}{V_{\text{batch}}} \times 100\%$$

Lệch pha có ba nguyên nhân chính, và cả ba đều xuất phát từ đặc điểm của luồng dữ liệu thị trường mô tả ở Mục 1.1.1.

Nguyên nhân thứ nhất là sự kiện đến trễ. Tầng tốc độ gom cửa sổ theo thời gian sự kiện với ngưỡng watermark 1 phút. Một giao dịch đến trễ hơn ngưỡng đó bị loại khỏi cửa sổ của nó, nên khối lượng và VWAP của phút đó trên `speed_agg` thiếu phần giao dịch này. Nguyên nhân thứ hai là bản ghi trùng lặp. Khi bộ thu thập kết nối lại hoặc chuyển từ bù dữ liệu qua REST sang luồng WebSocket, cùng một giao dịch có thể được gửi hai lần. Tầng tốc độ khử trùng theo cặp khóa (`symbol`, `trade_id`), nhưng chỉ trong phạm vi watermark. Nguyên nhân thứ ba là mất chuỗi: WebSocket có thể mất gói mà không báo lỗi kết nối, để lại khoảng trống trong dãy mã giao dịch.

Tầng lô không chịu ba giới hạn này. Nó đọc lại toàn bộ giao dịch đã biết của mỗi cửa sổ bị ảnh hưởng, khử trùng trên toàn tập và không đặt ngưỡng watermark khi gom nến. Vì vậy khung nhìn lô được dùng làm chuẩn đối chiếu (ground truth). Các kịch bản kiểm thử hỗn loạn của đề tài cho thấy độ lớn của lệch pha khi lỗi xảy ra. Khi 40 trong 200 giao dịch bị làm trễ 120 giây, tức gấp đôi ngưỡng watermark, khối lượng của khung nhìn tốc độ lệch 21,15% so với khung nhìn lô ở đợt chạy chính thức, và từ 19,70% đến 22,18% qua sáu đợt chạy. Khi 50 trong 200 mã giao dịch liên tiếp bị cắt bỏ, độ lệch là 23,58%, và từ 23,41% đến 25,90% qua sáu đợt. Các kịch bản này mô phỏng hành vi loại bỏ của tầng tốc độ ở mức logic, không chạy Spark (xem Mục 1.3.3).

Có thể giảm lệch pha bằng cách nới ngưỡng watermark. Cái giá là bộ nhớ trạng thái phải giữ nhiều cửa sổ mở hơn, và mỗi cây nến được chốt muộn hơn [2], [6]. Đề tài không chọn cách đó. Thay vào đó, đề tài giữ watermark ngắn để bảo vệ SLA, chấp nhận lệch pha có giới hạn trên khung nhìn tốc độ, và giao việc sửa sai cho tầng lô cùng thuật toán hợp nhất truy vấn.

### 1.2.4. Xác định ba câu hỏi nghiên cứu cốt lõi (RQ1, RQ2, RQ3)

Từ ba vấn đề trên, đề tài đặt ra ba câu hỏi nghiên cứu:

- **RQ1:** Kiến trúc Lambda có giúp giảm độ trễ truy vấn phân tích so với cách tiếp cận chỉ dùng tầng lô (Batch-only) hay không?
- **RQ2:** Khung nhìn tốc độ sai lệch bao nhiêu so với khung nhìn lô, và quá trình tự đối soát (auto-reconciliation) diễn ra như thế nào?
- **RQ3:** Kỹ thuật gộp tệp nhỏ (compaction) trên Apache Iceberg cải thiện hiệu năng đọc bao nhiêu lần sau khi dữ liệu được nạp liên tục?

Mỗi câu hỏi được gắn với một kịch bản đo, một bộ chỉ số và một tệp nhật ký cụ thể, như trong Bảng 1.4. Nhờ vậy câu trả lời có thể kiểm chứng lại từ dữ liệu thô. Với RQ2, sai số giữa hai khung nhìn được đo bằng sai số tuyệt đối trung bình (Mean Absolute Error – MAE), sai số phần trăm tuyệt đối trung bình (Mean Absolute Percentage Error – MAPE) và sai số bình phương trung bình căn (Root Mean Squared Error – RMSE), cùng hai đại lượng lệch pha đã định nghĩa ở Mục 1.2.3 và thời gian hội tụ T~conv~.

**Bảng 1.4: Ánh xạ câu hỏi nghiên cứu sang kịch bản đo và chỉ số đánh giá**

| Câu hỏi | Kịch bản đo | Chỉ số chính | Tệp nhật ký kết quả |
| :---: | :---: | :---: | :---: |
| RQ1 | `bench_latency.py`: ba loại truy vấn Q1, Q2, Q3 × ba chiến lược (`batch_only`, `speed_only`, `lambda`) | Độ trễ phân vị P50, P95, P99 (ms) | `bench_latency.csv` |
| RQ2 | `bench_reprocess.py` và bốn kịch bản `run_chaos_smoke_tests.py` | MAE, MAPE, RMSE, ΔV~%~, ε~VWAP~, T~conv~ | `bench_reprocess_summary.csv`, `chaos_smoke_tests.csv` |
| RQ3 | `bench_compaction.py` ở chế độ `mode = real` | Số tệp, độ trễ đọc, thời gian gộp tệp | `bench_compaction.csv` |

*Nguồn: scripts/benchmarks/ và results/logs/ trong kho mã nguồn của đề tài.*

Ngoài ba câu hỏi chính, đề tài đánh giá thêm cổng kiểm soát chất lượng dữ liệu bằng `bench_fault.py`. Cổng này là tiền đề cho RQ2: nếu bản ghi lỗi lọt vào tầng lô thì "chuẩn đối chiếu" không còn đáng tin.

## 1.3. MỤC TIÊU, ĐỐI TƯỢNG VÀ PHẠM VI NGHIÊN CỨU

### 1.3.1. Mục tiêu của đề tài và kết quả dự kiến đạt được

Mục tiêu tổng quát của đề tài là thiết kế, hiện thực và đánh giá định lượng một hệ thống Data Lakehouse theo kiến trúc Lambda. Hệ thống phải phục vụ dữ liệu thị trường tiền mã hóa với độ trễ dưới 5 giây, đồng thời tự động thay số liệu tạm thời bằng số liệu đã đối soát khi tầng lô hoàn tất.

Mục tiêu tổng quát được cụ thể hóa thành sáu mục tiêu kỹ thuật:

1. Xây dựng phân hệ thu thập nhận luồng giao dịch WebSocket từ Binance, chuẩn hóa theo một hợp đồng dữ liệu thống nhất và ghi vào Kafka, có hàng đợi chứa bản ghi lỗi (Dead-Letter Queue – DLQ).
2. Hiện thực tầng tốc độ bằng Spark Structured Streaming, tính nến OHLCV (Open, High, Low, Close, Volume) và VWAP một phút với chu kỳ micro-batch 5 giây.
3. Hiện thực tầng xử lý lô theo mô hình Bronze → Silver → Gold, có cổng kiểm soát chất lượng và bảng cách ly bản ghi lỗi, lưu tập dữ liệu gốc trên Apache Iceberg và MinIO.
4. Thiết kế thuật toán hợp nhất truy vấn `AutoCorrectingQueryMerger` định tuyến theo watermark của tầng lô, không cộng trùng dữ liệu tại ranh giới, và cung cấp thuật toán này qua giao diện lập trình ứng dụng (Application Programming Interface – API) viết bằng FastAPI.
5. Xây dựng bộ tiêm lỗi có kiểm soát và thực hiện bốn thực nghiệm định lượng cùng bốn kịch bản kiểm thử hỗn loạn để trả lời RQ1, RQ2, RQ3.
6. Đóng gói toàn bộ hệ thống bằng Docker Compose và vận hành được trên một máy tính cá nhân 16 GB RAM.

Bảng 1.5 liệt kê tám sản phẩm dự kiến, mỗi sản phẩm gắn với vị trí mã nguồn tương ứng.

**Bảng 1.5: Danh mục kết quả và sản phẩm dự kiến của đề tài**

| STT | Sản phẩm | Mô tả | Vị trí trong mã nguồn |
| :---: | :---: | :---: | :---: |
| 1 | Đường thu thập dữ liệu | WebSocket Binance → Kafka, phát hiện mất chuỗi và bù dữ liệu qua REST, DLQ | `src/ingestion/` |
| 2 | Tập dữ liệu gốc Bronze | Bảng Iceberg bất biến trên MinIO, phân vùng theo ngày | `src/batch_layer/iceberg_utils.py` |
| 3 | Tầng tốc độ | Nến OHLCV, VWAP, cờ đột biến một phút; ghi ClickHouse và Redis | `src/speed_layer/` |
| 4 | Tầng xử lý lô | Bronze → Silver → Gold, ghi `batch_agg` và tiến watermark; điều phối bằng Dagster | `src/batch_layer/`, `dagster_project/` |
| 5 | Tầng phục vụ | API FastAPI với thuật toán hợp nhất truy vấn theo watermark | `src/serving_layer/` |
| 6 | Cổng chất lượng và bộ tiêm lỗi | Kiểm tra ràng buộc, cách ly bản ghi lỗi, tiêm lỗi có kiểm soát | `src/data_quality/`, `src/ingestion/fault_injector.py` |
| 7 | Giao diện giám sát | Bảng điều khiển Streamlit hiển thị nến, đối soát và cảnh báo | `dashboard/` |
| 8 | Bộ thực nghiệm và báo cáo | Bốn thực nghiệm, bốn kịch bản hỗn loạn, nhật ký CSV và biểu đồ | `scripts/benchmarks/`, `results/` |

*Nguồn: cấu trúc kho mã nguồn của đề tài.*

### 1.3.2. Đối tượng nghiên cứu và cấu trúc sự kiện thị trường

Đối tượng nghiên cứu là luồng giao dịch khớp lệnh tổng hợp (`aggTrade`) mà Binance phát công khai qua WebSocket. Cặp `BTCUSDT` là đối tượng chính của mọi thực nghiệm, `ETHUSDT` được dùng làm cặp đối chứng. Ở chế độ vận hành, bộ thu thập tự chọn các cặp USDT có khối lượng quy đổi lớn nhất theo cách mô tả ở Mục 1.1.1, mặc định mười cặp (`TOP_N_COINS = 10`).

Mỗi bản tin được chuẩn hóa thành một bản ghi `TradeEvent` gồm sáu trường nghiệp vụ: mã giao dịch `trade_id`, mã cặp `symbol`, giá `price`, khối lượng `quantity`, thời điểm khớp lệnh `trade_time` tính bằng mili-giây và cờ `is_buyer_maker` cho biết bên mua có phải bên tạo lập thanh khoản hay không. Ba trường siêu dữ liệu đi kèm là `ingestion_time` (thời điểm hệ thống nhận gói tin), `is_injected` và `fault_type` (đánh dấu bản ghi do bộ tiêm lỗi tạo ra). Dưới đây là bản ghi đầu tiên trong tập mẫu `datasets/sample/btcusdt_trades_sample.json`:

```json
{"trade_id": 851917845, "symbol": "BTCUSDT", "price": 65060.16,
 "quantity": 0.0941, "trade_time": 1787486037765, "is_buyer_maker": false,
 "ingestion_time": 1787486753538, "is_injected": true, "fault_type": "late_data"}
```

Bản ghi này cho thấy vì sao hợp đồng dữ liệu tách hai mốc thời gian. Giao dịch được khớp lúc `trade_time` nhưng hệ thống nhận được sau đó khoảng 716 giây, vì nó là một bản ghi đến trễ do bộ tiêm lỗi tạo ra. Tập mẫu gồm 1 000 giao dịch `BTCUSDT` trong khoảng 12 phút ngày 2026-08-23, trong đó 123 bản ghi được gắn nhãn `late_data`.

Đơn vị phân tích của đề tài không phải từng giao dịch mà là cây nến một phút, xác định bởi cặp khóa (`symbol`, `window_start`). Đây cũng là khóa sắp xếp của các bảng `speed_agg` và `batch_agg` trong ClickHouse, và là khóa mà thuật toán hợp nhất dùng để khử trùng. Đặc tả đầy đủ của hợp đồng dữ liệu và các công thức OHLCV, VWAP được trình bày ở Mục 3.1.

### 1.3.3. Phạm vi nghiên cứu và giới hạn thực nghiệm

Đề tài nằm trong phạm vi kỹ thuật dữ liệu: thu thập, lưu trữ, xử lý, đối soát, phục vụ và đo đạc hiệu năng. Đề tài không xây dựng chương trình giao dịch tự động, không dự báo giá, không xử lý sổ lệnh (order book) và không thực hiện giao dịch bằng tiền thật. Hệ thống chỉ đọc dữ liệu công khai của một sàn là Binance.

Kết quả thực nghiệm cần được đọc trong các giới hạn sau:

- **Một máy, một nút.** Toàn bộ 14 dịch vụ chạy bằng Docker Compose trên một máy tính cá nhân 16 GB RAM. Kafka chạy một broker với hệ số nhân bản 1, nên đề tài không đánh giá khả năng chịu lỗi của cụm phân tán.
- **Quy mô dữ liệu truy vấn nhỏ.** Các truy vấn của thực nghiệm độ trễ trả về từ 5 đến 105 cây nến, nên độ trễ chủ yếu phản ánh chi phí khứ hồi tới ClickHouse hơn là lượng dữ liệu được quét (phân tích chi tiết ở Chương 4).
- **Khung nhìn tốc độ mô phỏng trong thực nghiệm tái xử lý.** Ở chế độ mặc định, `bench_reprocess.py` sinh khung nhìn tốc độ từ khung nhìn lô cộng nhiễu có hạt giống cố định, nên sai số đo được phản ánh tham số nhiễu chứ không phải sai số thật của Spark.
- **Kiểm thử hỗn loạn ở mức logic.** Bốn kịch bản trong `run_chaos_smoke_tests.py` chạy trong một tiến trình Python trên dữ liệu tự sinh. Hành vi loại bỏ của tầng tốc độ được mô phỏng bằng quy tắc, ví dụ bỏ các giao dịch trễ quá 60 giây. Phía tầng lô dùng đúng mã khử trùng, mã gộp nến và thuật toán hợp nhất của hệ thống, nhưng ClickHouse được thay bằng dữ liệu giả lập trong bộ nhớ. Thời gian hội tụ T~conv~ vì vậy là thời gian tính toán trong bộ nhớ, không phải thời gian đầu-cuối qua một chu kỳ lô thật.
- **Phiên bản phụ thuộc chưa ghim hết.** Bốn ảnh container bên ngoài (MinIO, AWS CLI, Iceberg REST, ClickHouse) dùng nhãn `latest`, và `requirements.txt` chỉ ràng buộc phiên bản tối thiểu, nên hai lần dựng ở hai thời điểm có thể khác nhau.

## 1.4. PHƯƠNG PHÁP NGHIÊN CỨU VÀ ĐÓNG GÓP CỦA ĐỀ TÀI

### 1.4.1. Phương pháp nghiên cứu khoa học thiết kế hệ thống (Design Science Research)

Đề tài theo phương pháp nghiên cứu khoa học thiết kế (Design Science Research – DSR). Phương pháp này phù hợp vì sản phẩm chính của đề tài là một hệ thống, tức một tạo tác (artifact) được thiết kế để giải một vấn đề thực tế, và giá trị của nó được chứng minh bằng đo đạc chứ không chỉ bằng lập luận. Đề tài áp dụng quy trình DSR của Peffers và cộng sự [18], gồm sáu bước: xác định vấn đề, xác định mục tiêu của giải pháp, thiết kế và phát triển, trình diễn, đánh giá, và truyền đạt. Quy trình cho phép quay lại bước thiết kế khi kết quả đánh giá chưa đạt. Bảng 1.6 cho thấy mỗi bước tương ứng với phần nào của đề tài.

**Bảng 1.6: Ánh xạ các bước của phương pháp DSR vào quá trình thực hiện đề tài**

| Bước DSR | Hoạt động trong đề tài | Trình bày tại |
| :---: | :---: | :---: |
| 1. Xác định vấn đề | Phân tích mâu thuẫn độ trễ – độ chính xác, lệch pha dữ liệu và tệp nhỏ | Mục 1.1, 1.2 |
| 2. Xác định mục tiêu giải pháp | Sáu mục tiêu kỹ thuật, ba câu hỏi nghiên cứu | Mục 1.2.4, 1.3.1 |
| 3. Thiết kế và hiện thực | Kiến trúc năm phân hệ, thuật toán hợp nhất truy vấn | Chương 3 |
| 4. Trình diễn | Chạy toàn bộ đường dữ liệu bằng `scripts/run_lambda_pipeline.py` | Chương 3, Chương 4 |
| 5. Đánh giá | Bốn thực nghiệm định lượng, bốn kịch bản kiểm thử hỗn loạn | Chương 4 |
| 6. Truyền đạt | Báo cáo tiểu luận và kho mã nguồn kèm nhật ký thực nghiệm | Toàn văn |

*Nguồn: nhóm thực hiện ánh xạ sáu bước của quy trình DSR trong [18] vào đề tài.*

Giá trị của vòng lặp nằm ở chỗ kết quả đánh giá được phép quay lại sửa thiết kế. Đề tài có hai ví dụ cụ thể. Ví dụ thứ nhất: rà soát bộ tiêm lỗi cho thấy nó so sánh số ngẫu nhiên với các ngưỡng không cộng dồn, nên tỷ lệ lỗi thực tế thấp hơn nhiều so với tham số cấu hình. Với tỷ lệ cấu hình 4%, 5%, 4%, 7% cho bốn loại lỗi, tỷ lệ thực tế chỉ còn 4%, 1%, 0%, 2%. Sau khi sửa sang ngưỡng cộng dồn, các đợt đo cũ `bm_fault_20261027_*` bị loại khỏi số liệu chính thức và thực nghiệm được chạy lại. Ví dụ thứ hai: đợt kiểm toán mã nguồn ngày 2026-11-12 phát hiện rằng khi tầng lô đọc Kafka theo từng đợt, một cây nến đầy đủ có thể bị thay bằng cây nến chỉ chứa phần giao dịch mới. Nhóm bổ sung kho Silver tích lũy `SilverTradeStore` để mỗi lượt chạy tính lại cửa sổ bị ảnh hưởng từ toàn bộ giao dịch đã biết, đồng thời buộc watermark chỉ được tiến lên. Bộ kiểm thử đơn vị được mở rộng thành bảy nhóm kiểm thử để giữ các bản sửa này.

### 1.4.2. Phương pháp thực nghiệm đo đạc định lượng có kiểm soát (Controlled Benchmarking)

Bước đánh giá của DSR được thực hiện bằng thực nghiệm có kiểm soát. Mỗi thực nghiệm giữ cố định mọi yếu tố trừ yếu tố cần so sánh, và tuân theo bốn nguyên tắc:

- **Lặp lại và thống kê phân vị.** Mỗi kịch bản độ trễ chạy 30 lần cho mỗi chiến lược. Kết quả được báo cáo bằng các phân vị P50, P95, P99 thay vì chỉ giá trị trung bình, vì độ trễ có phân phối lệch.
- **Mốc thời gian lấy từ dữ liệu.** Mốc "hiện tại" của thực nghiệm độ trễ là giá trị `window_start` lớn nhất trong `batch_agg`, không phải đồng hồ hệ thống. Nhờ vậy cùng một tập dữ liệu luôn cho cùng các khoảng truy vấn.
- **Hạt giống ngẫu nhiên cố định.** Thực nghiệm chất lượng dữ liệu dùng hạt giống `42 + iteration`, nên ma trận nhầm lẫn tái lập được giữa các đợt chạy.
- **Nhật ký thô dạng CSV.** Mọi lần đo được ghi vào `results/logs/*.csv` trước khi tổng hợp. Với thực nghiệm gộp tệp, chỉ các dòng `mode = real` (đo trực tiếp trên bảng Iceberg qua REST Catalog và MinIO) mới được dùng để kết luận.

**Bảng 1.7: Tổng hợp các thực nghiệm định lượng của đề tài**

| Thực nghiệm | Script | Yếu tố so sánh | Quy mô một đợt | Chỉ số |
| :---: | :---: | :---: | :---: | :---: |
| 1. Độ trễ truy vấn (RQ1) | `bench_latency.py` | Ba chiến lược truy vấn | 30 lần × 3 kịch bản × 3 chiến lược × 2 cặp | P50, P95, P99 (ms) |
| 2. Độ chính xác tái xử lý (RQ2) | `bench_reprocess.py` | Khung nhìn tốc độ và khung nhìn lô | 1 giờ nến của 2 cặp | MAE, MAPE, RMSE |
| 3. Hiệu quả gộp tệp (RQ3) | `bench_compaction.py` | Trước và sau gộp tệp | 20 lần đọc mỗi pha | Số tệp, độ trễ đọc |
| 4. Cổng chất lượng dữ liệu | `bench_fault.py` | Bản ghi sạch và bản ghi lỗi tiêm | 3 lượt × 5 000 sự kiện, tỷ lệ lỗi 20% | Precision, Recall, F1 |
| Kiểm thử hỗn loạn (RQ2) | `run_chaos_smoke_tests.py` | Trước và sau đối soát | 4 kịch bản lỗi phân tán | ΔV~%~, R~gap~, R~dup~, T~conv~ |

*Nguồn: scripts/benchmarks/ và nhật ký results/logs/; đợt chính thức 2026-11-11 cho Thực nghiệm 1 đến 4, 2026-11-08 cho kiểm thử hỗn loạn (xem Bảng 4.3).*

Toàn bộ chuỗi thực nghiệm chạy được bằng một lệnh `python scripts/benchmarks/run_all_benchmarks.py`. Cả hai nguồn bằng chứng cho RQ2 đều có giới hạn đã nêu ở Mục 1.3.3. Thực nghiệm 2 đo trên khung nhìn tốc độ được mô phỏng bằng nhiễu. Kiểm thử hỗn loạn kiểm chứng logic đối soát trên dữ liệu có lỗi, nhưng hành vi của tầng tốc độ cũng được mô phỏng. Vì vậy câu trả lời cho RQ2 là một kết luận về tính đúng của cơ chế đối soát, chưa phải một phép đo sai số của Spark Structured Streaming khi chạy thật.

### 1.4.3. Các đóng góp khoa học và giá trị ứng dụng thực tiễn của đề tài

Về mặt khoa học, đề tài có ba đóng góp.

Thứ nhất, đề tài đề xuất và hiện thực thuật toán hợp nhất truy vấn `AutoCorrectingQueryMerger`. Thuật toán chia mọi truy vấn thành ba trường hợp theo vị trí của watermark W: toàn bộ trước W, toàn bộ từ W trở đi, hoặc cắt ngang W. Ở trường hợp thứ ba, phần `[T_start, W)` lấy từ khung nhìn lô và phần `[W, T_end)` lấy từ khung nhìn tốc độ. Kết quả được khử trùng theo (`symbol`, `window_start`), ưu tiên bản ghi của tầng lô, nên không cây nến nào bị đếm hai lần. Thuật toán còn trả về độ lệch VWAP tại ranh giới để người dùng biết phần tạm thời sai khác bao nhiêu.

Thứ hai, đề tài đưa ra một bộ chỉ số đối soát gồm ε~VWAP~, ΔV~%~, tỷ lệ mất chuỗi R~gap~, tỷ lệ trùng lặp R~dup~ và thời gian hội tụ T~conv~. Mỗi lượt chạy lô ghi ε~VWAP~ và ΔV~%~ của từng cửa sổ, cùng R~dup~ của cả lượt, vào bảng `lakehouse.reconciliation_audit_metrics` để kiểm toán. R~gap~ và T~conv~ hiện chỉ được tính đầy đủ trong kịch bản kiểm thử hỗn loạn. Trong bảng kiểm toán, cột mất chuỗi luôn bằng 0, và cột thời gian hội tụ chỉ ghi thời lượng của lượt chạy lô.

Thứ ba, đề tài cung cấp một bộ số liệu thực nghiệm có thể tái lập, kể cả những kết quả không như kỳ vọng ban đầu. Ở đợt đo chính thức, thuật toán hợp nhất có độ trễ P50 là 62,84 ms, so với 60,12 ms khi truy vấn trực tiếp `batch_agg`. Như vậy chi phí của phép hợp nhất chỉ vài mili-giây, nhưng ưu thế độ trễ ghi nhận ở một đợt đo trước đó không tái lập được. Cổng chất lượng dữ liệu đạt Precision, Recall và F1 đều bằng 1,0. Gộp tệp giảm số tệp Parquet từ 118 xuống 10 (giảm 91,5%) và tăng tốc đọc khoảng 1,5 lần ở trạng thái bộ đệm nóng. Bốn kịch bản kiểm thử hỗn loạn đều đạt ở mức logic, với độ lệch khối lượng trở về 0% sau đối soát.

Về mặt thực tiễn, đề tài cung cấp một kiến trúc tham chiếu hoàn toàn bằng phần mềm nguồn mở, chạy được trên một máy tính cá nhân, cho bài toán giám sát và đối soát dữ liệu thời gian thực. Kiến trúc này áp dụng được cho các luồng dữ liệu có cùng đặc điểm: phát sinh liên tục, có sự kiện đến trễ và trùng lặp, và cần cả số liệu tức thời lẫn số liệu chuẩn tắc. Kho mã nguồn cũng là nền tảng để nhóm phát triển tiếp ở khóa luận tốt nghiệp.

## 1.5. CẤU TRÚC CỦA BÁO CÁO TIỂU LUẬN

### 1.5.1. Tóm tắt nội dung năm chương của báo cáo

Báo cáo được tổ chức theo mạch "vấn đề → nền tảng → giải pháp → kiểm chứng → kết luận":

- **Chương 1: Tổng quan và đặt vấn đề.** Trình bày bối cảnh, giới hạn của các hướng tiếp cận hiện có, ba câu hỏi nghiên cứu, mục tiêu, phạm vi và phương pháp.
- **Chương 2: Cơ sở lý thuyết và nền tảng công nghệ cốt lõi.** Trình bày sự tiến hóa từ kho dữ liệu đến Lakehouse, kiến trúc Lambda và mô hình toán học đối soát, định dạng bảng Apache Iceberg, lý thuyết xử lý luồng theo thời gian sự kiện, kho phân tích ClickHouse và kỹ thuật kiểm soát chất lượng dữ liệu.
- **Chương 3: Thiết kế kiến trúc hệ thống và hiện thực hóa pipeline.** Trình bày yêu cầu nghiệp vụ, kiến trúc năm phân hệ và cách hiện thực từng tầng, từ thu thập, tốc độ, lô, điều phối, phục vụ, giao diện đến đóng gói container.
- **Chương 4: Triển khai thực nghiệm, đánh giá hiệu năng và kiểm toán đối soát.** Trình bày môi trường thực nghiệm, kết quả bốn thực nghiệm, bốn kịch bản kiểm thử hỗn loạn và phân tích các đánh đổi kỹ thuật, qua đó trả lời RQ1, RQ2, RQ3.
- **Chương 5: Kết luận và định hướng phát triển.** Tổng kết kết quả so với mục tiêu, nêu các hạn chế còn tồn tại và định hướng cho khóa luận tốt nghiệp.

### 1.5.2. Hướng dẫn khai thác tài liệu kỹ thuật và mã nguồn

Mọi mô tả kỹ thuật trong báo cáo đều dẫn về một tệp cụ thể trong kho mã nguồn, để người đọc có thể đối chiếu. Bảng 1.8 cho biết mỗi thư mục chứa gì và được trình bày ở mục nào.

**Bảng 1.8: Ánh xạ cấu trúc kho mã nguồn với nội dung báo cáo**

| Thư mục hoặc tệp | Nội dung | Trình bày tại |
| :---: | :---: | :---: |
| `src/ingestion/` | Bộ thu thập WebSocket, Kafka producer, mô hình `TradeEvent`, bộ tiêm lỗi | Mục 3.2 |
| `src/speed_layer/` | Spark Structured Streaming, gom cửa sổ, ghi ClickHouse và Redis | Mục 3.3 |
| `src/batch_layer/`, `src/data_quality/` | Đường Bronze → Silver → Gold, cổng chất lượng, gộp tệp Iceberg | Mục 3.4 |
| `dagster_project/`, `dbt_project/` | Điều phối Dagster; mô hình dbt chỉ dùng làm tham chiếu, không nằm trên đường chạy | Mục 3.5 |
| `src/serving_layer/` | API FastAPI, thuật toán hợp nhất truy vấn, đọc watermark | Mục 3.6 |
| `dashboard/`, `src/utils/alerting.py` | Giao diện Streamlit và cảnh báo | Mục 3.7 |
| `docker-compose.yml`, `Dockerfile.*`, `configs/` | 14 dịch vụ container và cấu hình Kafka, Iceberg, ClickHouse, Redis | Mục 3.8, Phụ lục |
| `scripts/benchmarks/`, `results/logs/` | Script thực nghiệm và nhật ký kết quả thô | Chương 4 |
| `tests/` | Bộ kiểm thử đơn vị và kiểm thử tích hợp | Chương 3, Chương 4 |

*Nguồn: cấu trúc kho mã nguồn của đề tài.*

Người đọc muốn tái lập kết quả có thể dùng ba lệnh sau, chạy từ thư mục gốc của kho mã:

- `python scripts/run_lambda_pipeline.py`: khởi động các container cần thiết và chạy toàn bộ đường dữ liệu.
- `python scripts/run_tests.py`: chạy bảy nhóm kiểm thử đơn vị, không cần Docker.
- `python scripts/benchmarks/run_all_benchmarks.py`: chạy lại toàn bộ thực nghiệm và ghi nhật ký vào `results/logs/`.

Khi mô tả trong tài liệu phụ của kho mã lệch với mã nguồn, báo cáo này theo mã nguồn.
