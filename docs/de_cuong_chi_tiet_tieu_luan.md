# ĐỀ CƯƠNG CHI TIẾT TIỂU LUẬN CHUYÊN NGÀNH
## ĐỀ TÀI: THIẾT KẾ VÀ TRIỂN KHAI HỆ THỐNG DATA LAKEHOUSE THEO KIẾN TRÚC LAMBDA HỖ TRỢ ĐỐI SOÁT DỮ LIỆU THỜI GIAN THỰC CHO THỊ TRƯỜNG TIỀN MÃ HÓA
*(Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring)*

---

* **Trường:** Đại học Sư phạm Kỹ thuật TP. Hồ Chí Minh (HCMUTE)
* **Khoa:** Công Nghệ Thông Tin
* **Ngành:** Kỹ thuật Dữ liệu (Data Engineering)
* **Loại đề tài:** Tiểu luận chuyên ngành (Định hướng tiếp nối Khóa luận tốt nghiệp)
* **Sinh viên thực hiện:**
  1. Nguyễn Đặng Quốc Anh — MSSV: `23133004`
  2. Phạm Minh Quân — MSSV: `23133060`
* **Giảng viên hướng dẫn:** ThS. Đoàn Minh Trí
* **Thời gian thực hiện:** Học kỳ 1 — Năm học 2026-2027

---

# CẤU TRÚC TỔNG THỂ BÁO CÁO TIỂU LUẬN (5 CHƯƠNG)

```
┌────────────────────────────────────────────────────────────────────────┐
│                        CẤU TRÚC ĐỀ CƯƠNG 5 CHƯƠNG                       │
├────────────────────────────────────────────────────────────────────────┤
│ CHƯƠNG 1: TỔNG QUAN VÀ ĐẶT VẤN ĐỀ                                      │
│ CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ CÔNG NGHỆ CỐT LÕI                         │
│ CHƯƠNG 3: THIẾT KẾ KIẾN TRÚC HỆ THỐNG VÀ PIPELINE (SYSTEM DESIGN)      │
│ CHƯƠNG 4: TRIỂN KHAI THỰC NGHIỆM VÀ ĐÁNH GIÁ HIỆU NĂNG (BENCHMARKS)   │
│ CHƯƠNG 5: KẾT LUẬN VÀ ĐỊNH HƯỚNG PHÁT TRIỂN (TLCN → KLTN)             │
└────────────────────────────────────────────────────────────────────────┘
```

---

## TRANG TIÊU ĐỀ & CÁC MỤC ĐẦU
* **Trang bìa & Nhiệm vụ đề tài**
* **Lời cảm ơn & Cam đoan học thuật**
* **Tóm tắt đề tài (Abstract tiếng Việt & tiếng Anh)**
* **Mục lục, Danh mục Bảng biểu, Danh mục Hình ảnh**
* **Danh mục Từ viết tắt và Thuật ngữ chuyên ngành** (ACID, OLAP, VWAP, OHLCV, CDC, WAP, SLA, etc.)

---

# CHƯƠNG 1: TỔNG QUAN VÀ ĐẶT VẤN ĐỀ

### 1.1. Bối cảnh và Tính cấp thiết của đề tài
* Sự bùng nổ của thị trường giao dịch tài chính / tiền mã hóa (Cryptocurrency) với đặc tính giao dịch liên tục 24/7, tần suất cao (High-Frequency Trading) và độ biến động lớn.
* Nhu cầu phân tích dữ liệu đa dạng trong doanh nghiệp:
  * **Nhu cầu thời gian thực (Real-time Analytics):** Bắt kịp biến động giá, phát hiện đột biến khối lượng (Spike detection), đóng nến tức thời phục vụ giao dịch và cảnh báo rủi ro (SLA < 5 giây).
  * **Nhu cầu phân tích lịch sử & kiểm toán (Historical Analytics & Auditing):** Tổng hợp dữ liệu theo chu kỳ ngày/tháng/năm, backtesting thuật toán, tái lập dữ liệu và đảm bảo độ chính xác tuyệt đối (Ground Truth).

### 1.2. Vấn đề nghiên cứu & Khoảng trống kỹ thuật
* **Mâu thuẫn giữa Batch và Stream:** Sự đánh đổi cố hữu giữa tính chính xác hoàn hảo (nhưng độ trễ cao của Batch) và tốc độ tức thời (nhưng chấp nhận dữ liệu gần đúng của Stream).
* **Các hạn chế kinh điển của kiến trúc truyền thống:**
  1. *Hiện tượng tệp nhỏ (Small File Problem):* Streaming ghi liên tục tạo hàng triệu file Parquet kích thước nhỏ (<5MB), gây quá tải siêu dữ liệu (Metadata Overhead) và suy giảm nghiêm trọng tốc độ đọc.
  2. *Thiếu giao dịch ACID trên Data Lake thô:* Rủi ro đọc dữ liệu rác/xung đột khi có nhiều tiến trình ghi đồng thời.
  3. *Thiếu tầng kiểm soát chất lượng chủ động (Data Quality Gate):* Dữ liệu lỗi (Duplicate, Out-of-order, Late data) len lỏi vào tầng phục vụ phân tích.
  4. *Hiện tượng lệch pha (Divergence) giữa Batch View và Speed View:* Chưa có cơ chế đối soát và tự động hiệu chỉnh (Auto-Reconciliation) khi Batch hoàn thành.

### 1.3. Mục tiêu nghiên cứu
1. Thiết kế và hiện thực hóa kiến trúc **Data Lakehouse theo Lambda Architecture** 3 lớp hoàn chỉnh (Batch Layer, Speed Layer, Serving Layer).
2. Xây dựng thuật toán **Auto-Correcting Query Merger** tại Serving Layer tự động đối soát Batch–Speed dựa trên Batch Watermark.
3. Ứng dụng định dạng bảng mở **Apache Iceberg** trên MinIO S3 làm nguồn sự thật bất biến (*Single Source of Truth*).
4. Xây dựng module **Fault Injector** và hệ thống **Data Quality Gate** với cơ chế Quarantine tự động.
5. Thực thi **4 bài Benchmark định lượng** đo lường khoa học: Độ trễ, Độ chính xác đối soát, Khả năng chịu lỗi, và Hiệu quả Compaction.
6. Đóng gói toàn bộ hệ thống bằng **Docker Compose** vận hành tối ưu trên môi trường máy tính cá nhân 16GB RAM.

### 1.4. Đối tượng và Phạm vi nghiên cứu
* **Đối tượng:** Luồng dữ liệu giao dịch trực tiếp từ sàn Binance (Cặp giao dịch `BTCUSDT`).
* **Phạm vi kỹ thuật:** Môi trường cục bộ đóng gói Docker (Local Deployment), tập trung vào các chỉ số kỹ thuật của Data Engineering (Latency, Scanned volume, Error Delta, Compaction metrics), không bao gồm giao dịch tự động bằng tiền thật (Real Money Trading).

### 1.5. Phương pháp nghiên cứu
* Phương pháp nghiên cứu thiết kế hệ thống (Design Science Research).
* Phương pháp thực nghiệm đo đạc định lượng có kiểm soát (Controlled Quantitative Benchmarking).

### 1.6. Ý nghĩa khoa học và thực tiễn của đề tài
* Cung cấp một kiến trúc tham chiếu hoàn chỉnh, mã nguồn mở, có khả năng tái lập (reproducible) cho bài toán giám sát thời gian thực kết hợp kiểm toán lịch sử.
* Cung cấp số liệu đánh giá định lượng thực tế giữa các cách tiếp cận kiến trúc dữ liệu hiện đại.

---

# CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ NỀN TẢNG CÔNG NGHỆ CỐT LÕI

### 2.1. Tiến hóa của Kiến trúc Dữ liệu Doanh nghiệp
* Từ **Data Warehouse** (Cấu trúc cứng nhắc, chi phí cao) $\rightarrow$ **Data Lake** (Lưu trữ linh hoạt, giá rẻ nhưng thành "Data Swamp") $\rightarrow$ **Data Lakehouse** (Kết hợp ưu điểm ACID, Schema Enforcement/Evolution trên nền Object Storage giá rẻ).

### 2.2. Kiến trúc Lambda (Lambda Architecture)
* Nguyên lý thiết kế của Nathan Marz: Phân tách hệ thống thành 3 tầng độc lập:
  * **Batch Layer:** Xử lý Master Dataset bất biến, tạo ra Batch Views chính xác tuyệt đối.
  * **Speed Layer:** Xử lý luồng dữ liệu delta gần nhất, tạo ra Speed Views có độ trễ thấp (gần đúng).
  * **Serving Layer:** Hợp nhất kết quả từ Batch View và Speed View để trả về kết quả tổng thể.
* Phân tích ưu/nhược điểm và thách thức khi vận hành Lambda.

### 2.3. So sánh Đối sánh các Mô hình Kiến trúc Dữ liệu
* Phân tích so sánh toàn diện giữa 3 mô hình: **Lambda Architecture** vs. **Kappa Architecture** vs. **Medallion Architecture**.
* Đánh giá các trường hợp sử dụng (Use Cases) tối ưu cho từng kiến trúc.

### 2.4. Định dạng Bảng Mở (Open Table Formats) & Apache Iceberg
* So sánh chuyên sâu 3 định dạng phổ biến: **Apache Iceberg**, **Delta Lake**, **Apache Hudi** (Cơ chế metadata, Engine independence, Khả năng xử lý Batch/Streaming).
* Cấu trúc siêu dữ liệu phân tầng của Apache Iceberg: *Snapshot $\rightarrow$ Manifest List $\rightarrow$ Manifest File $\rightarrow$ Data Files (Parquet)*.
* Các tính năng cốt lõi: ACID Snapshot Isolation, Time Travel, Hidden Partitioning, và Kỹ thuật tối ưu hóa tệp nhỏ (Compaction/Bin-Packing).

### 2.5. Xử lý Luồng Dữ liệu (Stream Processing) với Spark Structured Streaming
* Mô hình Micro-batch và Continuous Processing.
* Xử lý thời gian sự kiện (*Event-Time Processing*), Kỹ thuật phân cửa sổ (*Tumbling/Sliding Windows*).
* Cơ chế quản lý dữ liệu đến muộn (*Watermarking*) và quản lý trạng thái (*Stateful Processing*).

### 2.6. Kho Phục vụ Phân tích Tốc độ Cao (OLAP) — ClickHouse
* Kiến trúc Column-Oriented Storage, Vectorized Query Execution.
* Dòng động cơ bảng `MergeTree`, `SummingMergeTree`, `ReplacingMergeTree`.
* Vai trò của ClickHouse trong Serving Layer làm cầu nối truy vấn tức thời.

### 2.7. Quản trị Chất lượng Dữ liệu Chủ động (Data Quality Engineering)
* Khái niệm Data Contracts và Data Quality Gates.
* Cơ chế cách ly dữ liệu lỗi (*Quarantine Pattern*) thay vì làm gián đoạn pipeline.
* Kỹ thuật tiêm lỗi chủ động (*Fault Injection*) trong đánh giá hệ thống phân tán.

### 2.8. Tổng quan Hệ sinh thái Công nghệ Triển khai
* Apache Kafka (KRaft Mode), Apache Spark (PySpark), MinIO Object Storage, FastAPI, Streamlit, Docker & Docker Compose.

---

# CHƯƠNG 3: THIẾT KẾ KIẾN TRÚC HỆ THỐNG VÀ PIPELINE (SYSTEM DESIGN)

### 3.1. Phân tích Bài toán Nghiệp vụ Giám sát Thị trường Tiền Mã Hóa
* Nguồn dữ liệu: **Binance WebSocket Live Stream** (Kênh `@trade` cho cặp `BTCUSDT`).
* Cấu trúc sự kiện giao dịch thô: `trade_id`, `price`, `quantity`, `trade_time`, `buyer_maker`.
* Các chỉ số nghiệp vụ cần tính toán:
  * **Mô hình nến OHLCV** (Open, High, Low, Close, Volume) ở khung 1 phút và 5 phút.
  * **Chỉ số VWAP** (Volume-Weighted Average Price): $\text{VWAP} = \frac{\sum (P_i \times V_i)}{\sum V_i}$.
  * **Tỷ suất sinh lợi (Return)** & **Tổng thanh khoản (Total Volume)**.
  * **Chỉ báo đột biến giá (Price Spike Detection):** Phát hiện chênh lệch giá vượt ngưỡng trong cửa sổ trượt.

### 3.2. Thiết kế Kiến trúc Tổng thể Hệ thống
* Sơ đồ phân tầng tổng thể 5 phân hệ: Ingestion $\rightarrow$ Storage $\rightarrow$ Processing (Speed/Batch) $\rightarrow$ Serving $\rightarrow$ Presentation.
* Luồng di chuyển của dữ liệu qua các trạng thái: *Raw Events $\rightarrow$ Provisional Speed View $\rightarrow$ Ground Truth Batch View $\rightarrow$ Reconciled Serving Output*.

### 3.3. Thiết kế Phân hệ Nạp Dữ liệu (Ingestion Layer) & Module Fault Injector
* **Python Collector:** Đọc bất đồng bộ (Asyncio/WebSockets) từ Binance, chuẩn hóa format trước khi đẩy vào Kafka topic `crypto.trades`.
* **Module Fault Injector (Chủ động kiểm thử):**
  * Tiêm lỗi trùng lặp dữ liệu (*Duplicate Events* - 10%).
  * Tiêm lỗi dữ liệu đến muộn (*Late Arrival 1-5m* - 10%).
  * Tiêm lỗi đảo lộn thứ tự thời gian (*Out-of-Order Events*).
  * Tiêm lỗi sai định dạng schema (Giá âm, thiếu `trade_id`).
  * Gắn cờ metadata `is_injected=true` để phục vụ đo lường Ground Truth.

### 3.4. Thiết kế Phân hệ Xử lý Nhanh (Speed Layer)
* **Spark Structured Streaming Job:**
  * Đọc luồng dữ liệu từ Kafka topic `crypto.trades`.
  * Áp dụng Tumbling Window (1m, 5m) dựa trên `trade_time`.
  * Cấu hình Watermark 1-2 phút để xử lý late data.
  * Tính toán nhanh OHLCV, VWAP, Volume.
  * Ghi kết quả trực tiếp vào bảng ClickHouse `speed_agg` với nhãn trạng thái `Provisional` (SLA < 5 giây).

### 3.5. Thiết kế Phân hệ Xử lý Lô & Lưu trữ (Batch Layer & Storage)
* **Tầng Bronze (Master Dataset):** Lưu trữ toàn bộ sự kiện gốc bất biến vào bảng Iceberg trên MinIO S3 (Snapshot Isolation).
* **Tầng Silver (Cleaned & Quarantined):**
  * Thực hiện Deduplication chính xác tuyệt đối theo `trade_id`.
  * Áp dụng Data Quality Gates: Các bản ghi lỗi bị tự động chuyển vào `quarantine_table` kèm lý do lỗi (`err_code`).
* **Tầng Gold (Business Aggregations):**
  * Spark Batch Job tổng hợp nến OHLCV và VWAP chuẩn xác 100%.
  * Đồng bộ kết quả vào bảng ClickHouse `batch_agg` với nhãn trạng thái `Reconciled` (Ground Truth).
* **Cập nhật Batch Watermark:** Ghi nhận mốc thời gian lớn nhất mà Batch Layer đã hoàn thành vào bảng `system_watermark`.

### 3.6. Thiết kế Tầng Phục vụ & Thuật toán Auto-Correcting Query Merger
* **Kiến trúc Serving API:** Xây dựng bằng FastAPI Async Gateway.
* **Thuật toán Query Merger:**
  $$\text{Query}(t_{start}, t_{end}) \xrightarrow{\text{So sánh với } T_{watermark}} \begin{cases} \text{Đọc } \texttt{batch\_agg} \rightarrow \text{Status: \textbf{Reconciled}} & \text{nếu } t_{end} < T_{watermark} \\ \text{Đọc } \texttt{speed\_agg} \rightarrow \text{Status: \textbf{Provisional}} & \text{nếu } t_{start} \ge T_{watermark} \\ \text{Hợp nhất } (\text{Batch} \cup \text{Speed}) \rightarrow \text{Status: \textbf{Partially Reconciled}} & \text{nếu } t_{start} < T_{watermark} \le t_{end} \end{cases}$$
* **Cơ chế tính toán sai lệch đối soát (`reconciliation_delta`):** Đo mức chênh lệch giữa giá/volume tính nhanh và giá/volume chính xác.

### 3.7. Thiết kế Giao diện Giám sát (Dashboard UI)
* Ứng dụng Streamlit hiển thị: Biểu đồ nến Real-time, Biểu đồ đường VWAP, Bảng cảnh báo Price Spike, Trạng thái Watermark và nhãn đối soát (`Provisional` vs `Reconciled`).

### 3.8. Phân bổ Tài nguyên Phần cứng & Ngân sách Bộ nhớ (Memory Budget 16GB RAM)
* Bảng phân bổ chi tiết giới hạn cgroups cho từng container Docker:
  * Kafka KRaft (768MB – 1GB), Spark Cluster (3.5 – 4GB), MinIO + Catalog (512 – 768MB), ClickHouse (1 – 1.5GB), FastAPI + Streamlit (512MB).
  * Tổng dịch vụ: ~7.5 – 8.5 GB RAM. Dành cho OS + WSL2: ~7.5 – 8.0 GB RAM.

---

# CHƯƠNG 4: TRIỂN KHAI THỰC NGHIỆM VÀ ĐÁNH GIÁ HIỆU NĂNG (BENCHMARKS)

### 4.1. Môi trường Thực nghiệm và Đóng gói Hệ thống
* Thông số phần cứng kiểm thử (CPU, RAM, SSD NVMe).
* Quy trình triển khai 1 lệnh: `docker compose up -d`.
* Tập dữ liệu thực nghiệm: Luồng giao dịch `BTCUSDT` từ Binance với hơn 5 triệu sự kiện.

### 4.2. Kịch bản Đánh giá 1: Benchmark Độ trễ Truy vấn (Query Latency Comparison)
* **Mục tiêu:** Đo lường thời gian đáp ứng truy vấn của Serving Layer.
* **Phương pháp:** Thực hiện 1,000 truy vấn ngẫu nhiên trên 3 phương án:
  1. *Batch-only Query* (Truy vấn thuần trên Iceberg/MinIO qua Spark SQL).
  2. *Speed-only Query* (Truy vấn trên dữ liệu stream tạm thời).
  3. *Lambda Merge Query* (Truy vấn qua FastAPI Query Merger với ClickHouse).
* **Chỉ số đo:** P50, P95, P99 Latency (ms), Throughput (RPS).

### 4.3. Kịch bản Đánh giá 2: Benchmark Độ chính xác và Đối soát Tự động (Reconciliation Correctness)
* **Mục tiêu:** Định lượng mức độ sai lệch của Speed View và kiểm chứng khả năng tự sửa sai của Batch Layer.
* **Phương pháp:** Đo sai số $\Delta = |\text{VWAP}_{speed} - \text{VWAP}_{batch}|$ tại các thời điểm có sự kiện trễ/trùng.
* **Chỉ số đo:** Tỷ lệ sai số (Error Percentage %), Thời gian hội tụ sai số về 0 sau khi Batch Job hoàn tất.

### 4.4. Kịch bản Đánh giá 3: Thử nghiệm Kiểm soát Chất lượng và Chịu lỗi (Data Quality & Fault Handling)
* **Mục tiêu:** Đánh giá độ nhạy và độ chính xác của Data Quality Gate tại tầng Silver.
* **Phương pháp:** Dùng Fault Injector tiêm 4 loại lỗi với tỷ lệ xác định trước.
* **Chỉ số đo:** 
  * Ma trận nhầm lẫn (Confusion Matrix).
  * Tỷ lệ phát hiện lỗi (**Precision, Recall, F1-Score** trên Quarantine Table).
  * Độ trễ phát sinh khi chạy kiểm tra ràng buộc (DQ Latency Overhead).

### 4.5. Kịch bản Đánh giá 4: Đánh giá Hiệu quả Gộp tệp nhỏ (Apache Iceberg Compaction Efficiency)
* **Mục tiêu:** Chứng minh giải pháp giải quyết Small File Problem của Iceberg.
* **Phương pháp:** Ghi streaming liên tục tạo ra hơn 2,000 tệp Parquet nhỏ (<2MB). Chạy tiến trình `OPTIMIZE` (Compaction).
* **Chỉ số đo:**
  * Số lượng tệp trước vs. sau Compaction.
  * Tốc độ quét dữ liệu khi truy vấn phân tích (Execution Time & MB Scanned).
  * Kích thước siêu dữ liệu (Metadata Overhead).

### 4.6. Phân tích Tổng hợp, Đánh giá Đánh đổi (Trade-offs) và Thảo luận Kết quả

---

# CHƯƠNG 5: KẾT LUẬN VÀ ĐỊNH HƯỚNG PHÁT TRIỂN

### 5.1. Tổng kết Kết quả Đạt được của Đề tài
* Bảng đối chiếu 8 sản phẩm nghiệm thu so với mục tiêu đề ra ban đầu:
  1. *Ingestion Pipeline* thu thập trực tiếp Binance WebSocket ổn định.
  2. *Bronze Master Dataset* lưu trữ bất biến trên Apache Iceberg/MinIO.
  3. *Speed Layer* xử lý nến OHLCV/VWAP với SLA < 5s.
  4. *Batch Layer* tính toán Ground Truth qua mô hình Bronze $\rightarrow$ Silver $\rightarrow$ Gold.
  5. *FastAPI Query Merger* đối soát tự động theo Batch Watermark.
  6. *Data Quality Gate & Fault Injector* cách ly lỗi tự động.
  7. *Streamlit Dashboard* trực quan hóa nến và cảnh báo biến động.
  8. *Báo cáo 4 bài Benchmark* định lượng với biểu đồ trực quan.

### 5.2. Các Hạn chế và Thách thức Kỹ thuật Còn Tồn tại
* Chi phí duy trì 2 luồng tính toán song song (Batch logic + Streaming logic).
* Giới hạn tài nguyên trên môi trường 1 node (Single-node Docker).

### 5.3. Định hướng Mở rộng cho Khóa Luận Tốt Nghiệp (KLTN — Giai đoạn 2)
* Mở rộng giám sát **Multi-symbol** (BTC, ETH, SOL, BNB) và **Multi-exchange** (Binance, OKX, Bybit).
* Tích hợp **Dagster Workflow Orchestrator** và **dbt-spark** quản lý vòng đời dữ liệu chuyên nghiệp.
* Bổ sung **Redis Hot Cache** cho Speed View để đạt throughput cực cao (<10ms).
* Tích hợp mô hình **Machine Learning** phát hiện bất thường giá/khối lượng tự động (Isolation Forest / AutoEncoder).
* Triển khai mô hình Write-Audit-Publish (WAP) và đóng gói cụm **Kubernetes (K8s)**.

---

# TÀI LIỆU THAM KHẢO (DỰ KIẾN)
1. Marz, N., & Warren, J. (2015). *Big Data: Principles and best practices of scalable realtime data systems*. Manning Publications.
2. Armbrust, M., et al. (2021). *Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics*. Proceedings of CIDR 2021.
3. Apache Iceberg Documentation (2024). *The Open Table Format for Analytic Datasets*. https://iceberg.apache.org/
4. ClickHouse Documentation (2024). *Fast Open-Source OLAP DBMS*. https://clickhouse.com/docs/
5. Chambers, B., & Zaharia, M. (2018). *Spark: The Definitive Guide: Big Data Processing Made Simple*. O'Reilly Media.
6. Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly Media.

---

# PHỤ LỤC
* **Phụ lục A:** File cấu hình `docker-compose.yml` hoàn chỉnh cho toàn bộ hệ thống.
* **Phụ lục B:** Schema DDL các bảng ClickHouse (`speed_agg`, `batch_agg`, `system_watermark`).
* **Phụ lục C:** Cấu hình Iceberg REST Catalog và Catalog Tables trên MinIO.
* **Phụ lục D:** Script Python tạo kịch bản tải và Fault Injector.
