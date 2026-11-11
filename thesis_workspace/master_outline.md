# ĐỀ CƯƠNG CHI TIẾT TIỂU LUẬN CHUYÊN NGÀNH (MASTER OUTLINE)

> **Đề tài:** THIẾT KẾ VÀ TRIỂN KHAI HỆ THỐNG DATA LAKEHOUSE THEO KIẾN TRÚC LAMBDA HỖ TRỢ ĐỐI SOÁT DỮ LIỆU THỜI GIAN THỰC CHO THỊ TRƯỜNG TIỀN MÃ HÓA  
> *(Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring)*  
> **Cơ quan chủ quản:** Trường Đại học Sư phạm Kỹ thuật TP. Hồ Chí Minh (HCMUTE)  
> **Đơn vị đào tạo:** Khoa Công Nghệ Thông Tin  
> **Bộ môn quản lý:** BỘ MÔN KỸ THUẬT DỮ LIỆU (Data Engineering)  
> **Loại đề tài:** Tiểu luận chuyên ngành (Định hướng tiếp nối Khóa luận tốt nghiệp)  
> **Sinh viên thực hiện:**  
> 1. Nguyễn Đặng Quốc Anh — MSSV: `23133004`  
> 2. Phạm Minh Quân — MSSV: `23133060`  
> **Giảng viên hướng dẫn:** ThS. Đoàn Minh Trí  
> **Mục đích tài liệu:** Đề cương chi tiết phân rã đến cấp tiểu mục 3 chữ số (X.Y.Z), tuân thủ chặt chẽ cấu trúc đóng cuốn chuẩn Mẫu 5 và Mẫu 4 của Khoa CNTT, gắn kèm danh sách tệp nguồn trực tiếp (**[Các file code liên quan: ...]**) để phục vụ trích xuất đặc tả kỹ thuật (*Technical Specs*) và biên soạn bản thảo từng chương (*Drafts*).

---

## TỔNG QUAN CẤU TRÚC ĐÓNG CUỐN VÀ 5 CHƯƠNG NỘI DUNG

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│               CẤU TRÚC TIỂU LUẬN CHUYÊN NGÀNH THEO QUY CHUẨN MẪU 5 KHOA CNTT           │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ CẤU TRÚC TIỀN ĐỀ (FRONT MATTER)                                                        │
│   • Trang bìa chính & Trang bìa phụ (Bộ môn Kỹ thuật Dữ liệu)                           │
│   • Phiếu nhận xét của GVHD & Phiếu nhận xét của GVPB (Mẫu 6)                           │
│   • Lời cảm ơn                                                                         │
│   • Đề cương chi tiết có chữ ký GVHD & SV (Mẫu 4, Kế hoạch 15 tuần)                    │
│   • Mục lục, Danh mục hình vẽ, Danh mục bảng biểu, Danh mục viết tắt                    │
│   • Tóm tắt tiểu luận (Abstract Tiếng Việt & Tiếng Anh)                                │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 5 CHƯƠNG NỘI DUNG CHÍNH:                                                               │
│   CHƯƠNG 1: TỔNG QUAN VÀ ĐẶT VẤN ĐỀ (PHẦN MỞ ĐẦU)                                     │
│   CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ NỀN TẢNG CÔNG NGHỆ CỐT LÕI                              │
│   CHƯƠNG 3: THIẾT KẾ KIẾN TRÚC HỆ THỐNG VÀ HIỆN THỰC HÓA PIPELINE (SYSTEM DESIGN)      │
│   CHƯƠNG 4: TRIỂN KHAI THỰC NGHIỆM, ĐÁNH GIÁ HIỆU NĂNG VÀ KIỂM TOÁN ĐỐI SOÁT           │
│   CHƯƠNG 5: KẾT LUẬN VÀ ĐỊNH HƯỚNG PHÁT TRIỂN (CONCLUSION & FUTURE WORK)               │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ TÀI LIỆU THAM KHẢO & PHỤ LỤC (A, B, C, D, E)                                           │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# CẤU TRÚC TIỀN ĐỀ (FRONT MATTER — QUY CHUẨN ĐÓNG CUỐN MẪU 5)

* **Trang bìa chính & Trang bìa phụ:** 
  * Trình bày theo đúng biểu mẫu Mẫu 5 của Khoa Công nghệ Thông tin – HCMUTE.
  * Thể hiện đầy đủ: Tên Trường, Tên Khoa, **BỘ MÔN KỸ THUẬT DỮ LIỆU**, Tên đề tài (Tiếng Việt và Tiếng Anh), Chuyên ngành Kỹ thuật Dữ liệu, Danh sách sinh viên thực hiện (kèm MSSV), Cán bộ hướng dẫn (ThS. Đoàn Minh Trí), Địa danh và Năm học 2026-2027.
* **Phiếu nhận xét của Giáo viên hướng dẫn & Phiếu nhận xét của Giáo viên phản biện:**
  * Soạn thảo và đóng cuốn theo biểu mẫu Mẫu 6 của Khoa CNTT.
  * Bao gồm phần đánh giá tinh thần, thái độ làm việc, khối lượng và chất lượng kết quả đạt được, thang điểm chi tiết và chữ ký xác nhận của GVHD và GVPB.
* **Lời cảm ơn:**
  * Bày tỏ lòng biết ơn chân thành đến Ban Giám hiệu Trường ĐH Sư phạm Kỹ thuật TP.HCM, quý Thầy Cô Khoa CNTT, các Thầy Cô Bộ môn Kỹ thuật Dữ liệu đã truyền đạt kiến thức nền tảng.
  * Đặc biệt tri ân sự định hướng học thuật và chỉ dẫn tận tình của ThS. Đoàn Minh Trí trong suốt quá trình triển khai đề tài.
* **Đề cương chi tiết có chữ ký của GVHD và Sinh viên thực hiện:**
  * Trình bày hoàn chỉnh theo Mẫu 4 của Khoa CNTT.
  * Tích hợp **Bảng Kế hoạch thực hiện 15 tuần** chi tiết (Phân bổ công việc từ Tuần 1 khảo sát, Tuần 2-4 thiết kế hạ tầng, Tuần 5-8 hiện thực Ingestion/Speed/Batch, Tuần 9-11 Serving Layer & Dashboard, Tuần 12-14 chạy 4 Benchmark & Chaos Smoke Tests, đến Tuần 15 nghiệm thu đóng cuốn).
* **Mục lục:**
  * Phân cấp tự động tiêu đề đến cấp tiểu mục 3 chữ số (X.Y.Z) kèm số trang chính xác.
* **Danh mục hình vẽ:**
  * Bảng tra cứu danh sách toàn bộ các biểu đồ kiến trúc, sơ đồ UML (Use Case, Component, Class, Sequence, Deployment) và biểu đồ kết quả benchmark thực nghiệm.
* **Danh mục bảng biểu:**
  * Bảng tra cứu danh sách các bảng cấu hình phần cứng, bảng DDL thực thể dữ liệu, ma trận đối sánh, ma trận nhầm lẫn và bảng số liệu định lượng.
* **Danh mục ký hiệu và chữ viết tắt:**
  * Bảng giải nghĩa các thuật ngữ chuyên ngành sắp xếp theo thứ tự bảng chữ cái ABC (*ACID, API, CDC, CEX, DWH, HFT, JSON, JVM, K8s, MAE, MAPE, OCC, OHLCV, OLAP, OOM, REST, RMSE, RPS, SLA, UTC, VLDB, VWAP, WAP, WebSocket...*).
* **Tóm tắt tiểu luận (Abstract Tiếng Việt & Tiếng Anh):**
  * Tóm lược cô đọng: (1) Tính cấp thiết và vấn đề nghiên cứu trong giám sát thị trường crypto 24/7; (2) Hướng tiếp cận kết hợp kiến trúc Lambda với Apache Iceberg Lakehouse và ClickHouse; (3) Thuật toán đối soát tự động Auto-Correcting Query Merger; (4) Tóm tắt các kết quả thực nghiệm định lượng nổi bật (P50 = 50.92ms, F1 = 1.0, Speedup Compaction = 2.10x).

---

# CHƯƠNG 1: TỔNG QUAN VÀ ĐẶT VẤN ĐỀ (PHẦN MỞ ĐẦU)

### 1.1. Bối cảnh và Tính cấp thiết của đề tài
* **1.1.1. Đặc thù giao dịch 24/7/365 và tính biến động cao của thị trường tiền mã hóa**
  * Mô tả: Phân tích tính liên tục không ngừng nghỉ của thị trường crypto (không có giờ đóng cửa như chứng khoán truyền thống), tần suất giao dịch cao (HFT) và các đợt biến động đột ngột (Flash Crash, Pump & Dump).
  * **[Các file code liên quan: datasets/raw/tickers/ticker_24hr_snapshot.json, docs/data/data.md, references/references.bib]**

* **1.1.2. Nhu cầu phân tích thời gian thực kết hợp kiểm toán lịch sử**
  * Mô tả: Phân tích hai luồng nhu cầu mâu thuẫn trong doanh nghiệp tài chính: Giám sát tức thời (SLA < 5s) phục vụ cảnh báo rủi ro vs. Kiểm toán lịch sử chính xác tuyệt đối phục vụ quyết toán và backtesting.
  * **[Các file code liên quan: report_materials/01_de_cuong_va_ke_hoach/de_cuong_chi_tiet_tieu_luan.md, SHORT_REPORT.md]**

* **1.1.3. Tính cấp thiết của kiến trúc dữ liệu lai hỗ trợ đối soát tự động**
  * Mô tả: Nhu cầu xây dựng mô hình kết hợp (Hybrid) có khả năng tự động đối soát, bù đắp sai số giữa luồng tính toán gần đúng (Speed View) và luồng dữ liệu chuẩn tắc (Batch View).
  * **[Các file code liên quan: src/serving_layer/query_merger.py, docs/md/caitan_lambda_architecture.md]**

### 1.2. Giới hạn của các tiếp cận truyền thống và Vấn đề nghiên cứu
* **1.2.1. Phân tích, đánh giá các công trình và hướng tiếp cận có liên quan (Batch-only, Streaming-only, Kappa)**
  * Mô tả: Phân tích tổng quan các công trình nghiên cứu và kiến trúc xử lý dữ liệu hiện có trong doanh nghiệp; đánh giá ưu nhược điểm, độ trễ và rủi ro trôi dạt trạng thái (State Drift) của Batch-only, Streaming-only, và kiến trúc Kappa nhằm đáp ứng đúng yêu cầu "Phân tích những công trình có liên quan" của Mẫu 4 và Mẫu 5.
  * **[Các file code liên quan: docs/co_so_ly_luan_chuong_2.md, references/references.bib]**

* **1.2.2. Vấn đề tệp nhỏ (Small File Problem) trên Data Lake khi nạp streaming liên tục**
  * Mô tả: Hiện tượng streaming vi mô tạo ra hàng ngàn tệp Parquet kích thước nhỏ (<2MB), gây suy giảm nghiêm trọng hiệu năng đọc của Object Storage và quá tải tầng siêu dữ liệu.
  * **[Các file code liên quan: src/batch_layer/compaction.py, docs/md/smallfile.md, scripts/benchmarks/bench_compaction.py]**

* **1.2.3. Vấn đề lệch pha dữ liệu (Data Divergence) và đánh đổi giữa độ trễ và độ chính xác**
  * Mô tả: Hiện tượng chênh lệch thể tích giao dịch ($\Delta V$) và giá trung bình gia quyền ($\varepsilon_{\text{VWAP}}$) giữa tầng stream và tầng batch do sự kiện trễ, trùng lặp.
  * **[Các file code liên quan: src/serving_layer/query_merger.py, scripts/benchmarks/bench_reprocess.py]**

* **1.2.4. Xác định 3 câu hỏi nghiên cứu cốt lõi (RQ1, RQ2, RQ3)**
  * Mô tả:
    * RQ1: Kiến trúc Lambda có giúp giảm độ trễ truy vấn phân tích so với Batch-only hay không?
    * RQ2: Speed View có sai lệch bao nhiêu so với Batch View và khả năng tự phục hồi (Auto-Reconciliation) diễn ra như thế nào?
    * RQ3: Kỹ thuật nén tệp nhỏ (Compaction) trên Apache Iceberg cải thiện hiệu năng đọc bao nhiêu lần sau streaming?
  * **[Các file code liên quan: AGENTS.md, report_materials/01_de_cuong_va_ke_hoach/de_cuong_chi_tiet_tieu_luan.md, SHORT_REPORT.md]**

### 1.3. Mục tiêu, Đối tượng và Phạm vi nghiên cứu
* **1.3.1. Mục tiêu của đề tài và Kết quả dự kiến đạt được**
  * Mô tả: Trình bày mục tiêu tổng quát, các mục tiêu kỹ thuật cụ thể và danh mục 8 kết quả/sản phẩm dự kiến đạt được (khớp mục 2 và mục 7 Phần Mở đầu của Mẫu 4).
  * **[Các file code liên quan: AGENTS.md, report_materials/01_de_cuong_va_ke_hoach/proposal_tlcn_lambda.md]**

* **1.3.2. Đối tượng nghiên cứu và cấu trúc sự kiện thị trường**
  * Mô tả: Luồng dữ liệu giao dịch khớp lệnh thực tế của cặp `BTCUSDT` từ sàn Binance với các trường `trade_id`, `price`, `quantity`, `trade_time`, `buyer_maker`.
  * **[Các file code liên quan: src/ingestion/models.py, datasets/schemas/trade_event_schema.json, datasets/sample/btcusdt_trades_sample.json]**

* **1.3.3. Phạm vi nghiên cứu và giới hạn thực nghiệm**
  * Mô tả: Phạm vi Data Engineering (thu thập, lưu trữ, xử lý, đối soát, phục vụ, đo đạc hiệu năng), không mở rộng sang thuật toán giao dịch tài chính tự động (Trading Bot).
  * **[Các file code liên quan: docker-compose.yml, requirements.txt]**

### 1.4. Phương pháp nghiên cứu và Đóng góp của đề tài
* **1.4.1. Phương pháp nghiên cứu khoa học thiết kế hệ thống (Design Science Research)**
  * Mô tả: Quy trình lặp từ xác định vấn đề, thiết kế kiến trúc, cài đặt mã nguồn, đo đạc thực nghiệm đến đánh giá kết quả.
  * **[Các file code liên quan: docs/process/, report_materials/INDEX_VIET_BAO_CAO.md]**

* **1.4.2. Phương pháp thực nghiệm đo đạc định lượng có kiểm soát (Controlled Benchmarking)**
  * Mô tả: Xây dựng các kịch bản tải tự động, đo lường độ trễ (P50/P95/P99), sai số đối soát và tỷ lệ nén tệp bằng mã Python chuẩn hóa lưu nhật ký CSV.
  * **[Các file code liên quan: scripts/benchmarks/run_all_benchmarks.py, results/logs/]**

* **1.4.3. Các đóng góp khoa học và giá trị ứng dụng thực tiễn của đề tài**
  * Mô tả: Cung cấp kiến trúc tham chiếu nguồn mở hoàn chỉnh cho bài toán giám sát đối soát thời gian thực; thuật toán Auto-Correcting Query Merger bảo đảm Zero Double-Counting; bộ số liệu benchmark định lượng thực tế.
  * **[Các file code liên quan: SHORT_REPORT.md, report_materials/01_de_cuong_va_ke_hoach/ke_hoach_hoan_thanh_100_phan_tram.md]**

### 1.5. Cấu trúc của báo cáo tiểu luận
* **1.5.1. Tóm tắt nội dung 5 chương của báo cáo**
  * Mô tả: Khái quát ngắn gọn mạch logic xuyên suốt từ Chương 1 đến Chương 5.
  * **[Các file code liên quan: report_materials/01_de_cuong_va_ke_hoach/de_cuong_chi_tiet_tieu_luan.md]**

* **1.5.2. Hướng dẫn khai thác tài liệu kỹ thuật và mã nguồn**
  * Mô tả: Hướng dẫn cấu trúc thư mục repo, cách đối chiếu giữa báo cáo và code trong `src/`, `configs/`, `scripts/`.
  * **[Các file code liên quan: README.md, report_materials/INDEX_VIET_BAO_CAO.md]**

---

# CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ NỀN TẢNG CÔNG NGHỆ CỐT LÕI

### 2.1. Tiến hóa của Kiến trúc Dữ liệu Doanh nghiệp
* **2.1.1. Từ Data Warehouse truyền thống đến Data Lake phân tán**
  * Mô tả: Phân tích ưu/nhược điểm của DWH (Schema-on-Write, chi phí cao, thiếu linh hoạt với dữ liệu bán cấu trúc) và Data Lake (lưu trữ phi cấu trúc rẻ nhưng dễ biến thành Data Swamp).
  * **[Các file code liên quan: docs/co_so_ly_luan_chuong_2.md, references/references.bib]**

* **2.1.2. Sự ra đời của Data Lakehouse: Hợp nhất ưu thế DWH và Data Lake**
  * Mô tả: Nguyên lý Data Lakehouse (Armbrust et al., 2021) kết hợp khả năng lưu trữ đối tượng giá rẻ với giao dịch ACID, schema enforcement và tối ưu hóa truy vấn.
  * **[Các file code liên quan: references/references.bib, docs/co_so_ly_luan_chuong_2.md]**

* **2.1.3. Nguyên lý kiến trúc Medallion (Bronze - Silver - Gold)**
  * Mô tả: Chuẩn hóa dữ liệu theo 3 tầng: Bronze (Raw Append-only), Silver (Cleaned, Quarantined, Deduplicated), Gold (Aggregated, Business-ready).
  * **[Các file code liên quan: dbt_project/models/schema.yml, dbt_project/models/bronze/bronze_crypto_trades.sql, dbt_project/models/silver/silver_cleaned_trades.sql, dbt_project/models/gold/gold_market_aggregates.sql]**

### 2.2. Kiến trúc Lambda và Mô hình Toán học Đối soát Dữ liệu
* **2.2.1. Nguyên lý 3 tầng của Kiến trúc Lambda**
  * Mô tả: Lý thuyết của Nathan Marz & James Warren (2015): Tách biệt Batch Layer (Master Dataset, Batch Views chính xác tuyệt đối), Speed Layer (Low Latency, Speed Views xấp xỉ), Serving Layer (Merge Views).
  * **[Các file code liên quan: references/references.bib, docs/md/caitan_lambda_architecture.md, docs/md/sys-arch.md]**

* **2.2.2. So sánh đối sánh toàn diện: Lambda vs Kappa vs Medallion Lakehouse**
  * Mô tả: Lập ma trận so sánh về độ trễ, độ phức tạp bảo trì mã nguồn, khả năng tái lập dữ liệu lịch sử và chi phí phần cứng.
  * **[Các file code liên quan: docs/co_so_ly_luan_chuong_2.md, references/references.bib]**

* **2.2.3. Mô hình toán học đánh giá sai số và độ lệch đối soát**
  * Mô tả: Định nghĩa các công thức toán học:
    * Tỷ lệ lỗ hổng chuỗi: $R_{\text{gap}} = \frac{\sum \max(0, \text{ID}_i - \text{ID}_{i-1} - 1)}{\text{ID}_{\max} - \text{ID}_{\min} + 1} \times 100\%$
    * Trôi dạt thể tích: $\Delta V_{\%} = \frac{|\sum V_{\text{Speed}} - \sum V_{\text{Batch}}|}{\sum V_{\text{Batch}}} \times 100\%$
    * Sai số VWAP: $\varepsilon_{\text{VWAP}} = |\text{VWAP}_{\text{Speed}} - \text{VWAP}_{\text{Batch}}|$
    * Tỷ lệ trùng lặp: $R_{\text{dup}} = \frac{\text{COUNT}_{\text{raw}} - \text{COUNT}(\text{DISTINCT})}{\text{COUNT}_{\text{raw}}} \times 100\%$
    * Thời gian hội tụ: $T_{\text{conv}} = t_{\text{Batch\_Merged}} - t_{\text{Window\_Close}}$
  * **[Các file code liên quan: .agents/skills/crypto-lambda-lakehouse-reconciliation-audit/SKILL.md, src/serving_layer/query_merger.py, scripts/benchmarks/bench_reprocess.py]**

### 2.3. Định dạng Bảng Mở Apache Iceberg và Bài toán Quản trị Tệp Nhỏ
* **2.3.1. So sánh các định dạng bảng mở thế hệ mới (Iceberg, Delta Lake, Hudi)**
  * Mô tả: Đánh giá cơ chế Metadata, tính độc lập engine xử lý, hỗ trợ đồng thời Batch/Streaming, và tính mở của Apache Iceberg.
  * **[Các file code liên quan: configs/iceberg/tables.yaml, docs/co_so_ly_luan_chuong_2.md, references/references.bib]**

* **2.3.2. Cấu trúc siêu dữ liệu phân tầng (Metadata Tree) của Apache Iceberg**
  * Mô tả: Cơ chế phân tầng gồm: Iceberg Catalog Pointer $\rightarrow$ Metadata File (`.metadata.json`) $\rightarrow$ Manifest List (`snap-*.avro`) $\rightarrow$ Manifest File (`*.avro`) $\rightarrow$ Data Files (`*.parquet`).
  * **[Các file code liên quan: src/batch_layer/iceberg_utils.py, configs/iceberg/tables.yaml]**

* **2.3.3. Cơ chế giao dịch ACID Snapshot Isolation và Time Travel**
  * Mô tả: Kỹ thuật kiểm soát đồng thời lạc quan (OCC - Optimistic Concurrency Control), khả năng truy vấn dữ liệu tại mốc snapshot quá khứ (Time Travel) phục vụ kiểm toán.
  * **[Các file code liên quan: src/batch_layer/iceberg_utils.py, src/batch_layer/maintenance.py, tests/test_dagster_pipeline.py]**

* **2.3.4. Bản chất của bài toán Small Files và kỹ thuật Compaction (Bin-Packing)**
  * Mô tả: Phân tích cơ chế nén gộp nhiều file nhỏ thành các file chuẩn kích thước (128MB/512MB) bằng thuật toán Bin-Packing mà không làm khóa bảng đọc.
  * **[Các file code liên quan: src/batch_layer/compaction.py, docs/md/smallfile.md, scripts/benchmarks/prepare_small_files.py]**

### 2.4. Lý thuyết Xử lý Luồng và Quản lý Dữ liệu Bất thường Phân tán
* **2.4.1. Mô hình xử lý luồng Spark Structured Streaming**
  * Mô tả: Cơ chế Micro-batch Processing, Stream-as-Table duality, lập kế hoạch thực thi tối ưu với Catalyst Optimizer.
  * **[Các file code liên quan: src/speed_layer/spark_streaming.py, configs/spark/spark-defaults.conf, references/references.bib]**

* **2.4.2. Khái niệm Event-Time, Processing-Time và cơ chế Watermarking**
  * Mô tả: Lý thuyết Dataflow Model (Akidau et al., 2015), xử lý dựa trên thời gian phát sinh sự kiện (`trade_time`), Watermark chặn biên giới hạn trạng thái trong bộ nhớ.
  * **[Các file code liên quan: src/speed_layer/window_aggregator.py, references/references.bib]**

* **2.4.3. Cửa sổ thời gian Tumbling Window và quản lý trạng thái phân tán với RocksDB**
  * Mô tả: Kỹ thuật phân cửa sổ cố định không chồng lấn (1 phút, 5 phút); cơ chế RocksDB State Store hạn chế tràn bộ nhớ RAM JVM.
  * **[Các file code liên quan: src/speed_layer/spark_streaming.py, src/speed_layer/window_aggregator.py, docker-compose.yml]**

* **2.4.4. Năm bài toán biên phân tán trong dòng dữ liệu thị trường tài chính**
  * Mô tả: Phân tích 5 hiện tượng thực tế: (1) WebSocket Silent Drops & Sequence Gaps; (2) Out-of-Order & Clock Drift; (3) Trùng lặp sự kiện tại Protocol Seam; (4) Flash Crash & Backpressure; (5) Ghi đè trạng thái Serving Layer khi có Reorganization.
  * **[Các file code liên quan: .agents/skills/crypto-lambda-lakehouse-reconciliation-audit/SKILL.md, docs/presentation/4errors.html]**

### 2.5. Công nghệ Kho Phục vụ Phân tích OLAP và Hot Cache
* **2.5.1. Cơ chế lưu trữ theo cột (Columnar Storage) và Vectorized Execution của ClickHouse**
  * Mô tả: Khả năng nén dữ liệu theo cột, xử lý chỉ thị SIMD, tốc độ tổng hợp hàng triệu dòng trong vài mili-giây.
  * **[Các file code liên quan: configs/clickhouse/init.sql, references/references.bib]**

* **2.5.2. Các dòng động cơ bảng ClickHouse (`MergeTree`, `ReplacingMergeTree`)**
  * Mô tả: Cơ chế gộp nền (Background Merge), tự động khử trùng lặp theo khóa sắp xếp với `ReplacingMergeTree` phục vụ Speed Views.
  * **[Các file code liên quan: configs/clickhouse/init.sql, src/serving_layer/clickhouse_client.py]**

* **2.5.3. Vai trò của Redis Hot Cache trong tối ưu hóa độ trễ Serving Layer**
  * Mô tả: Caching các nến gần nhất (In-Memory Key-Value) với TTL cấu hình, giảm tải truy vấn lặp lại cho ClickHouse.
  * **[Các file code liên quan: configs/redis/redis.yaml, src/speed_layer/redis_writer.py, docker-compose.yml]**

### 2.6. Quản trị Chất lượng Dữ liệu Chủ động (Data Quality Engineering)
* **2.6.1. Khái niệm Data Contracts và cơ chế Data Quality Gate**
  * Mô tả: Ràng buộc hợp đồng dữ liệu giữa nhà sản xuất (Producer) và người tiêu dùng (Consumer) thông qua JSON Schema nghiêm ngặt.
  * **[Các file code liên quan: datasets/schemas/trade_event_schema.json, src/data_quality/dq_checks.py, src/ingestion/models.py]**

* **2.6.2. Mô hình cách ly dữ liệu lỗi (Quarantine Pattern)**
  * Mô tả: Cơ chế không làm sập pipeline (Fail-safe): Tách dữ liệu sai hỏng vào Dead-Letter-Queue/Quarantine Table kèm mã lỗi (`err_code`) để kiểm toán và tái xử lý sau.
  * **[Các file code liên quan: src/data_quality/quarantine.py, configs/clickhouse/init.sql, scripts/test_dq_quarantine.py]**

* **2.6.3. Nguyên lý kiểm thử hỗn loạn (Chaos Testing & Fault Injection)**
  * Mô tả: Chủ động tiêm các lỗi mô phỏng (Late, Duplicate, Corrupt, Sequence Gap) vào dòng dữ liệu để định lượng khả năng phòng thủ của hệ thống.
  * **[Các file code liên quan: src/ingestion/fault_injector.py, .agents/skills/crypto-lambda-lakehouse-reconciliation-audit/SKILL.md, scripts/benchmarks/run_chaos_smoke_tests.py]**

---

# CHƯƠNG 3: THIẾT KẾ KIẾN TRÚC HỆ THỐNG VÀ HIỆN THỰC HÓA PIPELINE

### 3.1. Phân tích Yêu cầu Nghiệp vụ và Luồng Dữ liệu Tổng thể
* **3.1.1. Đặc tả luồng dữ liệu WebSocket sàn Binance và Data Contract JSON Schema**
  * Mô tả: Cấu trúc bản tin raw trade (`aggTrade`/`@trade`), kiểu dữ liệu các trường, và hợp đồng dữ liệu xác thực JSON Schema.
  * **[Các file code liên quan: src/ingestion/models.py, datasets/schemas/trade_event_schema.json, docs/process/01_ingestion_data_contract.md]**

* **3.1.2. Định nghĩa các chỉ số tài chính nghiệp vụ (OHLCV, VWAP, Price Spike)**
  * Mô tả: Công thức đóng nến Open/High/Low/Close/Volume theo cửa sổ thời gian; thuật toán tính VWAP $\sum (P_i \cdot V_i)/\sum V_i$; ngưỡng phát hiện đột biến giá tức thời.
  * **[Các file code liên quan: src/speed_layer/metrics_calculator.py, src/speed_layer/spike_detector.py, docs/presentation/giai_thich_thuat_toan_gbm.md]**

* **3.1.3. Thiết kế kiến trúc tổng thể 5 phân hệ**
  * Mô tả: Sơ đồ tương tác liên tầng: Ingestion $\rightarrow$ Speed Layer $\rightarrow$ Batch Layer $\rightarrow$ Serving Layer $\rightarrow$ Presentation Layer. **Bao gồm Biểu đồ Use Case tổng quát (UML Use Case Diagram) cho các tác nhân hệ thống và Biểu đồ Thành phần (UML Component Diagram) mô tả kiến trúc 5 phân hệ dữ liệu.**
  * **[Các file code liên quan: docs/md/sys-arch.md, docker-compose.yml, SHORT_REPORT.md]**

### 3.2. Hiện thực Phân hệ Thu thập Dữ liệu (Ingestion Layer) & Fault Injector
* **3.2.1. Xây dựng Binance WebSocket Live Stream Client bất đồng bộ**
  * Mô tả: Lập trình async client kết nối WebSocket Binance, xử lý heartbeat Ping/Pong, tự động kết nối lại khi rớt mạng, chuyển đổi múi giờ UTC.
  * **[Các file code liên quan: src/ingestion/binance_ws.py, scripts/stream_binance_live.py, scripts/fetch_binance_data.py]**

* **3.2.2. Xây dựng Kafka Producer nạp dữ liệu phân vùng vào topic `crypto_trades_raw`**
  * Mô tả: Khởi tạo Producer với cấu hình bảo đảm tin cậy (acks=1/all, retries, idempotent producer), phân mảnh key theo cặp giao dịch `symbol`.
  * **[Các file code liên quan: src/ingestion/kafka_producer.py, configs/kafka/topics.yaml, scripts/verify_kafka_messages.py]**

* **3.2.3. Xây dựng Module Fault Injector tiêm lỗi chủ động phục vụ đánh giá**
  * Mô tả: Lớp `FaultInjector` can thiệp tiêm lỗi xác suất: Duplication (10%), Late Arrival 1-5m (10%), Out-of-Order, Schema Violation, gắn cờ `is_injected=true`.
  * **[Các file code liên quan: src/ingestion/fault_injector.py, scripts/benchmarks/bench_fault.py]**

* **3.2.4. Xây dựng Historical Backfill Client khắc phục lỗ hổng chuỗi (Sequence Gap Healing)**
  * Mô tả: Lập trình REST backfill worker gọi Binance API truy xuất các trade bị mất khi phát hiện gián đoạn `trade_id`, áp dụng exponential backoff chống vi phạm Rate Limit (HTTP 429). **Sử dụng Biểu đồ Lớp (UML Class Diagram) cho HistoricalBackfillClient và Biểu đồ Tuần tự (UML Sequence Diagram) thể hiện luồng phát hiện gap và kích hoạt worker bù đắp.**
  * **[Các file code liên quan: src/ingestion/historical_backfill.py, .agents/skills/crypto-lambda-lakehouse-reconciliation-audit/SKILL.md]**

### 3.3. Hiện thực Phân hệ Xử lý Tốc độ (Speed Layer)
* **3.3.1. Thiết kế Spark Structured Streaming Pipeline đọc luồng Kafka**
  * Mô tả: Cấu hình Spark streaming reader đọc Kafka với `maxOffsetsPerTrigger`, failOnDataLoss, khởi tạo checkpoint phân tán bảo toàn offset.
  * **[Các file code liên quan: src/speed_layer/spark_streaming.py, src/speed_layer/trade_schema.py, Dockerfile.speed]**

* **3.3.2. Hiện thực hóa Tumbling Window, Watermark và tính toán chỉ số trực tiếp**
  * Mô tả: Lớp `WindowAggregator` và `StreamingMetricsCalculator` gom nhóm theo `window(trade_time, '1 minute')`, áp watermark 1-2 phút, tính toán OHLCV và VWAP trực tiếp trên Micro-batch.
  * **[Các file code liên quan: src/speed_layer/window_aggregator.py, src/speed_layer/metrics_calculator.py, tests/test_speed_layer.py]**

* **3.3.3. Module phát hiện đột biến giá và khối lượng (Price Spike Detector)**
  * Mô tả: Lớp `PriceSpikeDetector` tính toán tỷ lệ chênh lệch giá và độ lệch chuẩn khối lượng trong cửa sổ trượt, kích hoạt thông báo cảnh báo tức thời.
  * **[Các file code liên quan: src/speed_layer/spike_detector.py, src/utils/alerting.py]**

* **3.3.4. Ghi dữ liệu vi mô vào ClickHouse (`speed_agg`) và đồng bộ Redis Hot Cache**
  * Mô tả: Cơ chế ghi streaming song song: Bulk write DataFrame vào ClickHouse bảng `speed_agg` với cờ trạng thái `Provisional`, và ghi bản ghi mới nhất vào Redis key-value với TTL 3600s. **Sử dụng Biểu đồ Lớp (UML Class Diagram) cho ClickHouseStreamingWriter/RedisSpeedWriter và Biểu đồ Tuần tự (UML Sequence Diagram) cho tiến trình ghi vi mô song song.**
  * **[Các file code liên quan: src/speed_layer/clickhouse_writer.py, src/speed_layer/redis_writer.py, configs/clickhouse/init.sql, configs/redis/redis.yaml]**

### 3.4. Hiện thực Phân hệ Lưu trữ Bất biến và Xử lý Lô (Batch Layer)
* **3.4.1. Tầng Bronze: Ghi dữ liệu thô bất biến vào bảng Apache Iceberg trên MinIO S3**
  * Mô tả: Khởi tạo bảng Iceberg qua REST Catalog trên bucket MinIO `s3://warehouse/`, ghi nhận luồng sự kiện thô bất biến (Append-only) với phân vùng theo ngày.
  * **[Các file code liên quan: src/batch_layer/iceberg_utils.py, src/batch_layer/spark_batch_jobs.py, configs/iceberg/tables.yaml, Dockerfile.batch]**

* **3.4.2. Tầng Silver: Làm sạch, khử trùng lặp (Deduplication) và Data Quality Gate**
  * Mô tả: Spark Batch áp dụng bộ kiểm tra `DataQualityChecker`: Kiểm tra giá dương, khối lượng hợp lệ, khử trùng lặp chính xác tuyệt đối theo `trade_id`.
  * **[Các file code liên quan: src/batch_layer/spark_batch_jobs.py, src/data_quality/dq_checks.py, tests/test_data_quality.py]**

* **3.4.3. Module Quarantine: Phân loại và cách ly dữ liệu sai hỏng**
  * Mô tả: Lớp `QuarantineManager` tự động lọc bản ghi lỗi, ghi vào bảng `quarantine_trades` kèm `err_code` (`ERR_PRICE_NONPOSITIVE`, `ERR_DUPLICATE_TRADE_ID`, v.v.) và thu thập metric qua `DQMetricsCollector`.
  * **[Các file code liên quan: src/data_quality/quarantine.py, src/data_quality/dq_metrics.py, configs/clickhouse/init.sql]**

* **3.4.4. Tầng Gold: Spark Batch tính toán Ground Truth OHLCV/VWAP và đồng bộ ClickHouse `batch_agg`**
  * Mô tả: Xử lý tổng hợp toàn diện nến OHLCV và VWAP từ dữ liệu Silver đã làm sạch (không bị giới hạn bởi Watermark), đồng bộ vào ClickHouse `batch_agg` với trạng thái `Reconciled` và cập nhật `system_watermark`.
  * **[Các file code liên quan: src/batch_layer/spark_batch_jobs.py, src/batch_layer/clickhouse_sync.py, configs/clickhouse/init.sql]**

* **3.4.5. Cơ chế quản lý vòng đời bảng Iceberg: Compaction nén tệp nhỏ và dọn dẹp Snapshot**
  * Mô tả: Lớp `IcebergCompactor` và `IcebergMaintenance` thực hiện bin-packing gộp tệp nhỏ, chạy `expire_snapshots` dọn rác metadata và `remove_orphan_files` tối ưu dung lượng MinIO S3.
  * **[Các file code liên quan: src/batch_layer/compaction.py, src/batch_layer/maintenance.py, scripts/benchmarks/bench_compaction.py]**

### 3.5. Điều phối Luồng Xử lý Tự động với Dagster và Biến đổi dbt
* **3.5.1. Thiết lập Software-Defined Assets trong Dagster**
  * Mô tả: Khai báo DAG tài sản dữ liệu độc lập phụ thuộc: `bronze_crypto_trades` $\rightarrow$ `silver_cleaned_trades` $\rightarrow$ `gold_market_aggregates` $\rightarrow$ `clickhouse_batch_sync` $\rightarrow$ `iceberg_small_files_compaction` $\rightarrow$ `system_watermark_sync`.
  * **[Các file code liên quan: dagster_project/repository.py, configs/dagster/workspace.yaml, Dockerfile.dagster]**

* **3.5.2. Lập lịch tự động (Schedules) và đồng bộ System Watermark**
  * Mô tả: Cấu hình Dagster cron schedule tự động kích hoạt batch pipeline định kỳ, ghi nhận chính xác mốc Batch Watermark vào hệ thống để Serving Layer nhận biết.
  * **[Các file code liên quan: dagster_project/repository.py, src/batch_layer/clickhouse_sync.py, tests/test_dagster_pipeline.py]**

* **3.5.3. Tích hợp mô hình biến đổi dữ liệu dbt-spark (Medallion models)**
  * Mô tả: Cấu hình `dbt-spark` qua Thrift Server, triển khai các model SQL khai báo `bronze_crypto_trades.sql`, `silver_cleaned_trades.sql`, `gold_market_aggregates.sql`.
  * **[Các file code liên quan: dbt_project/dbt_project.yml, dbt_project/profiles.yml, dbt_project/models/bronze/bronze_crypto_trades.sql, dbt_project/models/silver/silver_cleaned_trades.sql, dbt_project/models/gold/gold_market_aggregates.sql, scripts/verify_dbt.py]**

### 3.6. Hiện thực Tầng Phục vụ (Serving Layer) & Thuật toán Auto-Correcting Query Merger
* **3.6.1. Thiết kế FastAPI Gateway và hệ thống Pydantic Schemas**
  * Mô tả: Khởi tạo REST API với các route `/api/v1/market/ohlcv`, `/api/v1/watermark`, `/api/v1/reconciliation/status`, xác thực tham số đầu vào và mô hình hóa dữ liệu trả về với Pydantic.
  * **[Các file code liên quan: src/serving_layer/api_routes.py, src/serving_layer/schemas.py, Dockerfile.serving]**

* **3.6.2. Thuật toán Auto-Correcting Query Merger phân tách 3 trường hợp truy vấn theo Watermark**
  * Mô tả: Lớp `AutoCorrectingQueryMerger` so sánh khoảng truy vấn $[T_{\text{start}}, T_{\text{end}}]$ với System Watermark $W$: Case 1 (Lịch sử: $T_{\text{end}} \le W$), Case 2 (Thời gian thực: $T_{\text{start}} > W$), Case 3 (Lai ghép: $T_{\text{start}} \le W < T_{\text{end}}$). **Sử dụng Biểu đồ Lớp (UML Class Diagram) cho AutoCorrectingQueryMerger và Biểu đồ Tuần tự (UML Sequence Diagram) thể hiện quy trình phân luồng và hợp nhất 3 trường hợp truy vấn.**
  * **[Các file code liên quan: src/serving_layer/query_merger.py, tests/test_query_merger.py]**

* **3.6.3. Cơ chế khử trùng lặp ưu tiên Batch (Zero Double-Counting) và tính sai lệch đối soát $\Delta$**
  * Mô tả: Thuật toán `_dedupe_prefer_batch` bảo đảm không đếm trùng tại mốc biên $W$; tính toán trực tiếp sai số $\Delta = |\text{VWAP}_{\text{speed}} - \text{VWAP}_{\text{batch}}|$ và độ lệch khối lượng $\Delta V$.
  * **[Các file code liên quan: src/serving_layer/query_merger.py, src/serving_layer/clickhouse_client.py]**

* **3.6.4. Module đọc và quản lý trạng thái mốc thời gian (Watermark Reader)**
  * Mô tả: Lớp `WatermarkReader` truy vấn trực tiếp ClickHouse `lakehouse.system_watermark` xác định mốc Ground Truth mới nhất kèm cơ chế xử lý ngoại lệ an toàn (fallback Epoch 1970 UTC).
  * **[Các file code liên quan: src/serving_layer/watermark_reader.py, src/serving_layer/clickhouse_client.py]**

### 3.7. Hiện thực Giao diện Giám sát (Dashboard UI) và Cảnh báo Chủ động
* **3.7.1. Xây dựng Dashboard tương tác thời gian thực với Streamlit**
  * Mô tả: Giao diện web Streamlit đa thành phần, cơ chế auto-refresh 3s hiển thị bảng trạng thái dịch vụ, đồng hồ System Watermark và thanh chọn tham số giao dịch.
  * **[Các file code liên quan: dashboard/app.py, dashboard/components/theme.py, dashboard/components/metrics_cards.py, Dockerfile.dashboard]**

* **3.7.2. Trực quan hóa nến OHLCV, đường VWAP và nhãn đối soát**
  * Mô tả: Biểu đồ nến Plotly tương tác, hiển thị đường chỉ báo VWAP và tô màu phân biệt rõ ràng giữa nến `Provisional` (màu cam/vàng) và nến `Reconciled` (màu xanh).
  * **[Các file code liên quan: dashboard/components/candlestick.py, dashboard/components/reconciliation_view.py, dashboard/components/watermark_status.py]**

* **3.7.3. Bảng điều khiển giám sát Data Quality Gate, Quarantine và cảnh báo Price Spike**
  * Mô tả: Component `dq_panel.py` hiển thị số lượng bản ghi cách ly theo mã lỗi; component `system_status.py` và `alerting.py` cảnh báo khi có biến động giá bất thường.
  * **[Các file code liên quan: dashboard/components/dq_panel.py, dashboard/components/system_status.py, src/utils/alerting.py]**

### 3.8. Đóng gói Hệ thống và Quản lý Tài nguyên Cục bộ (Resource Budgeting)
* **3.8.1. Thiết kế kiến trúc dịch vụ phân tán trên Docker Compose**
  * Mô tả: File `docker-compose.yml` định nghĩa 14 container trên mạng ảo `lakehouse-net`. **Sử dụng Biểu đồ Triển khai (UML Deployment Diagram) mô tả chi tiết 14 dịch vụ container, cổng giao tiếp mạng và gắn kết ổ đĩa volume trên mạng ảo `lakehouse-net`.**
  * **[Các file code liên quan: docker-compose.yml, .env.example, scripts/verify_services.ps1]**

* **3.8.2. Chiến lược phân bổ ngân sách bộ nhớ (Memory Budget ~8GB trên máy 16GB RAM)**
  * Mô tả: Bảng cgroups memory limits chi tiết cho từng container, bảo đảm hệ thống vận hành mượt mà không gây hiện tượng Out-Of-Memory (OOM) hay nghẽn bộ nhớ máy chủ cục bộ.
  * **[Các file code liên quan: docker-compose.yml, development.md, AGENTS.md]**

* **3.8.3. Giải pháp khắc phục lỗi môi trường Windows với Linux Container cho Spark**
  * Mô tả: Đóng gói tiến trình Spark Streaming và Spark Batch chạy bên trong Linux Container chuyên biệt (`Dockerfile.speed`, `Dockerfile.batch`), loại bỏ hoàn toàn lỗi tương thích nhị phân `winutils.exe` và Hadoop Native Libraries trên Windows Host.
  * **[Các file code liên quan: Dockerfile.speed, Dockerfile.batch, Dockerfile.spark-thrift, docker-compose.yml]**

---

# CHƯƠNG 4: TRIỂN KHAI THỰC NGHIỆM, ĐÁNH GIÁ HIỆU NĂNG VÀ KIỂM TOÁN ĐỐI SOÁT

### 4.1. Thiết lập Môi trường Kiểm thử và Kịch bản Đo đạc
* **4.1.1. Cấu hình phần cứng, thông số phần mềm và bộ dữ liệu thực nghiệm**
  * Mô tả: Đặc tả phần cứng thực nghiệm (Intel/AMD multi-core, 16GB RAM, NVMe SSD), phiên bản Docker, hệ điều hành và bộ dữ liệu hơn 5 triệu bản ghi giao dịch `BTCUSDT`.
  * **[Các file code liên quan: docker-compose.yml, development.md, datasets/sample/btcusdt_trades_sample.json, scripts/check_system.py]**

* **4.1.2. Phương pháp luận thu thập số liệu và ghi nhật ký tự động vào CSV/Plots**
  * Mô tả: Quy trình chạy benchmark tự động qua `run_all_benchmarks.py`, lưu trữ cấu trúc log CSV trong `results/logs/` và vẽ biểu đồ trực quan hóa bằng Matplotlib/Seaborn trong `results/plots/`.
  * **[Các file code liên quan: scripts/benchmarks/run_all_benchmarks.py, scripts/benchmarks/plot_benchmarks.py, results/logs/, results/plots/]**

### 4.2. Thực nghiệm 1: Đo lường và Đánh giá Độ trễ Truy vấn (Query Latency Benchmark)
* **4.2.1. Thiết kế kịch bản thử nghiệm: Batch-only vs Speed-only vs Lambda Merge**
  * Mô tả: Kịch bản `bench_latency.py` đo 3 loại truy vấn: Q1 (Historical), Q2 (Realtime), Q3 (Hybrid) qua 100 lần lặp độc lập trên 3 chiến lược.
  * **[Các file code liên quan: scripts/benchmarks/bench_latency.py, src/serving_layer/query_merger.py, src/serving_layer/clickhouse_client.py]**

* **4.2.2. Phân tích kết quả phân phối độ trễ P50, P95, P99 và thông lượng truy vấn**
  * Mô tả: Đánh giá chi tiết bảng số liệu P50/P95/P99 ms và Throughput (RPS): Speed-only đạt tốc độ tức thời (<15ms), Batch-only chậm hơn do quét dữ liệu lớn, Lambda Merge đạt mức cân bằng tối ưu (~20-40ms).
  * **[Các file code liên quan: results/logs/bench_latency.csv, results/plots/benchmark1_latency.png, SHORT_REPORT.md]**

* **4.2.3. Đánh giá trả lời câu hỏi nghiên cứu RQ1 (Latency Reduction)**
  * Mô tả: Khẳng định định lượng mức độ cắt giảm độ trễ của kiến trúc Lambda so với Batch-only truyền thống trong các kịch bản hỗn hợp.
  * **[Các file code liên quan: scripts/benchmarks/bench_latency.py, SHORT_REPORT.md, report_materials/04_bao_cao_thuc_nghiem_va_benchmark/SHORT_REPORT.md]**

### 4.3. Thực nghiệm 2: Đánh giá Độ chính xác và Khả năng Tự sửa sai (Reprocessing Correctness Benchmark)
* **4.3.1. Thiết kế kịch bản đo độ lệch đối soát giữa Speed View và Batch View**
  * Mô tả: Kịch bản `bench_reprocess.py` và `seed_benchmark2.py` tiêm các đợt sự kiện trễ và trùng lặp, tiến hành đối chiếu kết quả tính nhanh tức thời và kết quả xử lý lô.
  * **[Các file code liên quan: scripts/benchmarks/bench_reprocess.py, scripts/seed_benchmark2.py, src/serving_layer/query_merger.py]**

* **4.3.2. Phân tích sai số định lượng: MAE, MAPE, RMSE của Close Price, VWAP và Khối lượng giao dịch**
  * Mô tả: Bảng số liệu thống kê định lượng sai số trước khi đối soát: Sai số giá Close = 0.00%, Sai số VWAP $\approx 0.0006\%$, Sai số Volume $\approx 9.09\%$ do dữ liệu trễ; và sau khi Batch chạy xong toàn bộ sai số triệt tiêu về $0.00\%$.
  * **[Các file code liên quan: results/logs/bench_reprocess.csv, results/logs/bench_reprocess_summary.csv, results/plots/benchmark2_reconciliation.png, SHORT_REPORT.md]**

* **4.3.3. Đo lường thời gian hội tụ tự lành ($T_{\text{conv}}$) và trả lời câu hỏi nghiên cứu RQ2**
  * Mô tả: Đo lường thời gian trôi qua từ khi cửa sổ đóng đến khi dữ liệu được chữa lành hoàn toàn thành Ground Truth ($T_{\text{conv}}$); xác thực câu trả lời cho RQ2.
  * **[Các file code liên quan: results/logs/bench_reprocess.csv, SHORT_REPORT.md, .agents/skills/crypto-lambda-lakehouse-reconciliation-audit/SKILL.md]**

### 4.4. Thực nghiệm 3: Đánh giá Hiệu quả Nén tệp nhỏ (Iceberg Compaction Efficiency Benchmark)
* **4.4.1. Thiết kế kịch bản sinh dữ liệu tệp nhỏ streaming và kích hoạt tiến trình nén**
  * Mô tả: Script `prepare_small_files.py` và `bench_compaction.py` tạo ra hàng loạt micro-batch ghi nhận file Parquet nhỏ (<2MB), sau đó kích hoạt tiến trình Iceberg Compaction gộp tệp.
  * **[Các file code liên quan: scripts/benchmarks/prepare_small_files.py, scripts/benchmarks/bench_compaction.py, src/batch_layer/compaction.py]**

* **4.4.2. Phân tích tỷ lệ giảm số lượng file, kích cỡ metadata và tốc độ quét dữ liệu**
  * Mô tả: Đánh giá kết quả thực nghiệm: Giảm số lượng file từ 20 xuống 11 file (giảm 45%), kích thước trung bình tăng từ 0.72MB lên 1.30MB, tốc độ quét truy vấn tăng tốc 2.10x (Execution Time giảm từ 185.9ms xuống 88.5ms).
  * **[Các file code liên quan: results/logs/bench_compaction.csv, results/plots/benchmark3_compaction.png, SHORT_REPORT.md]**

* **4.4.3. Đánh giá chi phí I/O đọc/ghi và trả lời câu hỏi nghiên cứu RQ3**
  * Mô tả: Phân tích đánh đổi giữa tài nguyên tính toán tiêu hao trong quá trình nén và lợi ích lâu dài về tốc độ đọc; đưa ra kết luận định lượng cho RQ3.
  * **[Các file code liên quan: src/batch_layer/compaction.py, src/batch_layer/maintenance.py, SHORT_REPORT.md]**

### 4.5. Thực nghiệm 4: Đánh giá Hệ thống Kiểm soát Chất lượng và Cách ly Lỗi (Data Quality & Fault Injection Benchmark)
* **4.5.1. Thiết lập ma trận tiêm lỗi 4 loại dị thường**
  * Mô tả: Kịch bản `bench_fault.py` tiêm tổng cộng 4 loại lỗi: Trùng lặp (Duplicate), Đến muộn (Late Arrival), Đảo thứ tự (Out-of-Order), Vi phạm cấu trúc (Schema Violation/Negative Price) với tỷ lệ kiểm soát.
  * **[Các file code liên quan: scripts/benchmarks/bench_fault.py, src/ingestion/fault_injector.py, src/data_quality/dq_checks.py]**

* **4.5.2. Đánh giá hiệu quả Data Quality Gate: Ma trận nhầm lẫn, Precision, Recall, F1-Score**
  * Mô tả: Phân tích bảng Confusion Matrix trên bảng cách ly `quarantine_trades`: Phát hiện chính xác tuyệt đối các bản ghi lỗi với Precision = 1.0, Recall = 1.0, F1-Score = 1.0 mà không có hiện tượng dương tính giả.
  * **[Các file code liên quan: results/logs/bench_fault_results.csv, results/logs/bench_fault_summary.csv, results/plots/benchmark_fault_matrix.png, SHORT_REPORT.md]**

* **4.5.3. Đo lường chi phí trễ bổ sung (Latency Overhead) của tầng kiểm tra chất lượng**
  * Mô tả: Đo lường thời gian thực thi của bộ lọc DataQualityChecker, chứng minh chi phí kiểm tra không đáng kể so với tổng thời gian thực thi của pipeline.
  * **[Các file code liên quan: src/data_quality/dq_metrics.py, results/logs/bench_fault_summary.csv, SHORT_REPORT.md]**

### 4.6. Kiểm toán 4 Kịch bản Chaos Smoke Tests và Phân tích Đánh đổi Kỹ thuật (Trade-offs)
* **4.6.1. Kết quả kiểm toán Chaos Smoke Test 1: Ngắt kết nối WebSocket và tự động hàn gắn chuỗi**
  * Mô tả: Thử nghiệm ngắt collector 20s; Speed Layer phát hiện Sequence Gap ($R_{\text{gap}} > 0$), Batch/REST Backfill kích hoạt bù đắp dữ liệu thiếu đưa $R_{\text{gap}}$ về $0\%$.
  * **[Các file code liên quan: scripts/benchmarks/run_chaos_smoke_tests.py, results/logs/chaos_smoke_tests.csv, src/ingestion/historical_backfill.py]**

* **4.6.2. Kết quả kiểm toán Chaos Smoke Test 2: Sự kiện đến trễ gấp đôi Watermark**
  * Mô tả: Tiêm dữ liệu trễ 30s sau khi nến 1 phút đã đóng; Speed Layer loại bỏ an toàn để giữ bounded memory, Batch Layer gom lại qua event_time và phục hồi Gold view chính xác.
  * **[Các file code liên quan: scripts/benchmarks/run_chaos_smoke_tests.py, src/speed_layer/window_aggregator.py, src/batch_layer/spark_batch_jobs.py]**

* **4.6.3. Kết quả kiểm toán Chaos Smoke Test 3: Trùng lặp sự kiện tại điểm nối giao thức (Protocol Seam)**
  * Mô tả: Phát lại 1,000 sự kiện Kafka trùng; kiểm chứng Silver Deduplication và ClickHouse ReplacingMergeTree ngăn chặn hiện tượng đếm nhân đôi volume.
  * **[Các file code liên quan: scripts/benchmarks/run_chaos_smoke_tests.py, src/data_quality/dq_checks.py, src/serving_layer/query_merger.py]**

* **4.6.4. Kết quả kiểm toán Chaos Smoke Test 4: Bùng nổ lưu lượng 10x Flash Crash và kiểm tra Backpressure**
  * Mô tả: Kích hoạt luồng tải đột biến 10x trong 3 phút; hệ thống không bị lỗi tràn bộ nhớ (OOM) nhờ cấu hình `maxOffsetsPerTrigger` và RocksDB state store.
  * **[Các file code liên quan: scripts/benchmarks/run_chaos_smoke_tests.py, src/speed_layer/spike_detector.py, docker-compose.yml]**

* **4.6.5. Thảo luận các đánh đổi kỹ thuật (Trade-offs): Chi phí tính toán kép, độ trễ và tính nhất quán**
  * Mô tả: Phân tích chuyên sâu về sự đánh đổi trong thực tế: Độ phức tạp khi duy trì hai mã nguồn (Stream và Batch) song hành, chi phí I/O lưu trữ, và độ trễ hội tụ cuối cùng.
  * **[Các file code liên quan: SHORT_REPORT.md, docs/md/caitan_lambda_architecture.md, docs/presentation/huong_dan_thuyet_trinh_va_phan_bien.md]**

---

# CHƯƠNG 5: KẾT LUẬN VÀ ĐỊNH HƯỚNG PHÁT TRIỂN

### 5.1. Tổng kết Kết quả Đạt được của Đề tài
* **5.1.1. Đánh giá mức độ hoàn thành so với mục tiêu và tiêu chí nghiệm thu đề tài**
  * Mô tả: Bảng đối chiếu 8 sản phẩm kỹ thuật cốt lõi (Ingestion, Bronze Iceberg, Speed Layer, Batch Layer, Query Merger, Data Quality Gate, Streamlit Dashboard, 4 bộ Benchmark) đạt mức hoàn thành 100%.
  * **[Các file code liên quan: report_materials/01_de_cuong_va_ke_hoach/ke_hoach_hoan_thanh_100_phan_tram.md, SHORT_REPORT.md]**

* **5.1.2. Tổng hợp câu trả lời định lượng cho 3 câu hỏi nghiên cứu (RQ1, RQ2, RQ3)**
  * Mô tả: Trình bày số liệu kết luận rõ ràng:
    * RQ1: Lambda Merge rút ngắn độ trễ từ hàng giây (Batch-only Spark) xuống dưới 50ms trên ClickHouse.
    * RQ2: Speed View có sai số thể tích tức thời ~9.09% khi có dữ liệu trễ, nhưng hội tụ về 0.00% tuyệt đối ngay sau chu kỳ Batch ($T_{\text{conv}}$).
    * RQ3: Iceberg Compaction giảm 45% số lượng file nhỏ và tăng tốc độ quét đọc dữ liệu lên 2.10 lần.
  * **[Các file code liên quan: SHORT_REPORT.md, results/logs/]**

* **5.1.3. Đánh giá tính ứng dụng của kiến trúc Auto-Correcting Lambda Lakehouse**
  * Mô tả: Tiềm năng ứng dụng thực tế trong các sàn giao dịch, quỹ đầu tư định lượng, và các hệ thống giám sát thanh khoản tài chính.
  * **[Các file code liên quan: dashboard/app.py, src/serving_layer/api_routes.py, docs/md/summary.md]**

### 5.2. Các Hạn chế và Thách thức Còn Tồn tại
* **5.2.1. Hạn chế về tài nguyên phần cứng đơn máy (Single-node Local Deployment)**
  * Mô tả: Phân tích các rào cản khi gom toàn bộ stack dữ liệu lớn vào một máy cá nhân 16GB RAM thay vì cụm máy chủ phân tán (Distributed Cluster).
  * **[Các file code liên quan: docker-compose.yml, development.md]**

* **5.2.2. Chi phí duy trì mã nguồn kép và độ phức tạp vận hành của kiến trúc Lambda**
  * Mô tả: Vấn đề "Dual Codebase Problem": Khi sửa đổi logic chỉ số tài chính, kỹ sư phải cập nhật đồng thời ở cả Streaming Job (`metrics_calculator.py`) và Batch Job (`spark_batch_jobs.py`).
  * **[Các file code liên quan: src/speed_layer/metrics_calculator.py, src/batch_layer/spark_batch_jobs.py]**

* **5.2.3. Hạn chế về phạm vi dữ liệu thử nghiệm**
  * Mô tả: Thử nghiệm hiện tại tập trung chủ yếu vào cặp giao dịch `BTCUSDT` từ sàn Binance; chưa phân tích sâu tác động của dữ liệu cross-exchange.
  * **[Các file code liên quan: src/ingestion/binance_ws.py, datasets/raw/]**

### 5.3. Định hướng Phát triển cho Khóa Luận Tốt Nghiệp (KLTN)
* **5.3.1. Mở rộng xử lý đa cặp giao dịch (Multi-symbol) và đa sàn (Multi-exchange)**
  * Mô tả: Mở rộng ingest luồng từ nhiều sàn (Binance, OKX, Bybit) và xử lý đồng thời 10+ cặp giao dịch tiền mã hóa hàng đầu.
  * **[Các file code liên quan: datasets/mock/aggtrades/, scripts/fetch_binance_data.py]**

* **5.3.2. Ứng dụng Học máy (Machine Learning) phát hiện bất thường thị trường nâng cao**
  * Mô tả: Tích hợp mô hình Isolation Forest hoặc AutoEncoder để phát hiện hành vi thao túng thị trường (Wash Trading, Spoofing, Pump & Dump) theo thời gian thực.
  * **[Các file code liên quan: src/speed_layer/spike_detector.py]**

* **5.3.3. Hoàn thiện kiến trúc Write-Audit-Publish (WAP) và triển khai trên hạ tầng Kubernetes (K8s)**
  * Mô tả: Nâng cấp mô hình kiểm toán WAP trên Iceberg Branches và đóng gói điều phối toàn bộ hệ thống bằng Helm Chart trên cụm Kubernetes phân tán.
  * **[Các file code liên quan: dagster_project/repository.py, dbt_project/]**

---

# TÀI LIỆU THAM KHẢO

* **[Tệp cấu hình BibTeX: thesis_workspace/references.bib, references/references.bib]**
1. Marz, N., & Warren, J. (2015). *Big Data: Principles and best practices of scalable realtime data systems*. Manning Publications.
2. Armbrust, M., Ghodsi, A., Xin, R., & Zaharia, M. (2021). *Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics*. Proceedings of CIDR 2021.
3. Akidau, T., et al. (2015). *The Dataflow Model: A practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing*. Proceedings of VLDB Endowment, 8(12), 1792-1803.
4. Kleppmann, M. (2017). *Designing Data-Intensive Applications: The big ideas behind reliable, scalable, and maintainable systems*. O'Reilly Media.
5. Kreps, J., Narkhede, N., & Rao, J. (2011). *Kafka: A distributed messaging system for log processing*. Proceedings of NetDB.
6. Zaharia, M., et al. (2016). *Apache Spark: A unified engine for big data processing*. Communications of the ACM, 59(11), 56-65.
7. Apache Iceberg Community. (2024). *Apache Iceberg Table Format Specification*. https://iceberg.apache.org/spec/
8. ClickHouse Inc. (2024). *ClickHouse: Fast Open-Source OLAP Database Management System*. https://clickhouse.com/docs/

---

# PHỤ LỤC

* **Phụ lục A: Cấu hình điều phối hạ tầng Docker Compose toàn diện**
  * **[Các file code liên quan: docker-compose.yml, Dockerfile.speed, Dockerfile.batch, Dockerfile.serving, Dockerfile.dagster, Dockerfile.dashboard]**

* **Phụ lục B: Kịch bản DDL khởi tạo cơ sở dữ liệu và bảng ClickHouse**
  * **[Các file code liên quan: configs/clickhouse/init.sql]**

* **Phụ lục C: Đặc tả Data Contract JSON Schema cho giao dịch và Dead-Letter-Queue**
  * **[Các file code liên quan: datasets/schemas/trade_event_schema.json, datasets/schemas/dlq_event_schema.json]**

* **Phụ lục D: Bảng nhật ký và số liệu thực nghiệm gốc (Raw Benchmark Logs)**
  * **[Các file code liên quan: results/logs/bench_latency.csv, results/logs/bench_reprocess.csv, results/logs/bench_compaction.csv, results/logs/bench_fault_results.csv, results/logs/chaos_smoke_tests.csv]**

* **Phụ lục E: Hướng dẫn cài đặt và Cấu trúc tổ chức đĩa CD nộp Khoa (theo quy chuẩn Mẫu 5)**
  * Mô tả: Hướng dẫn chi tiết các bước thiết lập môi trường 1 lệnh (`docker compose up -d`), yêu cầu phần cứng/phần mềm tối thiểu, và đặc tả cấu trúc cây thư mục đĩa CD lưu trữ nộp về Bộ môn Kỹ thuật Dữ liệu / Khoa CNTT:
    ```text
    CD_ROM:\
    ├── SETUP\          (Hướng dẫn cài đặt chi tiết và video/tài liệu demo)
    ├── SOURCE\         (Toàn bộ mã nguồn dự án: src, configs, scripts, docker-compose.yml)
    ├── THESIS\
    │   ├── DOC\        (Bản mềm báo cáo định dạng Microsoft Word .docx)
    │   └── PDF\        (Bản mềm báo cáo định dạng Adobe Acrobat .pdf)
    ├── REF\            (Các bài báo khoa học, sách tham khảo, tài liệu BibTeX)
    └── SOFT\           (Các phần mềm và công cụ cài đặt: Docker, Python, VS Code extensions)
    ```
  * **[Các file code liên quan: development.md, README.md, docker-compose.yml, requirements.txt]**
