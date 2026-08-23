# Hướng Dẫn Thuyết Trình Ý Tưởng & Bộ Hỏi - Đáp (Q&A) Đồ Án TLCN
## ĐỀ TÀI: Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa
*(Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring)*

> **Dành cho:** Nguyễn Đặng Quốc Anh (23133004) & Phạm Minh Quân (23133060)  
> **GVHD:** ThS. Đoàn Minh Trí — Khoa Công Nghệ Thông Tin · Ngành Kỹ thuật Dữ liệu (HCMUTE)  
> **Tài liệu tham chiếu:** [slides.html](slides.html), [proposal_tlcn_lambda.md](proposal_tlcn_lambda.md), [caitan_lambda_architecture.md](caitan_lambda_architecture.md)

---

# PHẦN 1: CHIẾN LƯỢC & ELEVATOR PITCH (2 PHÚT ĐẦU TIÊN)

Khi bắt đầu gặp Thầy, không nên đi ngay vào chi tiết code hay cài đặt mà hãy dùng **2 phút đầu** để gây ấn tượng bằng bức tranh tổng thể:

```
[Bối cảnh & Nhu cầu] ──> [Khoảng trống kỹ thuật] ──> [Giải pháp đề xuất & Điểm nhấn] ──> [Định hướng TLCN → KLTN]
```

### 🗣️ Đoạn Pitching mẫu (Thuộc lòng để mở đầu):
> *"Dạ thưa Thầy, hôm nay nhóm em xin phép trình bày đề xuất đề tài Tiểu luận chuyên ngành với định hướng phát triển nối tiếp lên Khóa luận tốt nghiệp.*  
> *Đề tài của nhóm là: **Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa**.*  
>  
> *Lý do nhóm chọn đề tài này là vì trong giám sát thị trường tài chính (crypto), doanh nghiệp luôn gặp **mâu thuẫn lớn giữa 2 nhu cầu**: vừa cần dữ liệu cập nhật tức thời (< 5 giây) để bắt kịp biến động nến/giá, vừa cần độ chính xác tuyệt đối (Ground Truth) để báo cáo và kiểm toán lịch sử.*  
>  
> *Điểm nhấn cốt lõi của đồ án không chỉ là dựng pipeline nạp dữ liệu đơn thuần, mà là hiện thực hóa thuật toán **Auto-Correcting Query Merger** tự động đối soát giữa dữ liệu tức thời (Provisional) và dữ liệu chính xác (Reconciled) dựa trên Batch Watermark, đi kèm **hệ thống kiểm soát chất lượng dữ liệu chủ động (Fault Injection)** và **4 bài Benchmark định lượng khoa học** trên hạ tầng local 16GB RAM.*  
>  
> *Sau đây, nhóm xin phép trình bày chi tiết về kiến trúc và kế hoạch thực nghiệm ạ."*

---

# PHẦN 2: KỊCH BẢN THUYẾT TRÌNH CHI TIẾT (10 – 15 PHÚT)
*(Dựa trực tiếp trên 12 Slides tương tác trong `docs/slides.html`)*

```
[Slide 0-2: Đặt vấn đề] ──> [Slide 3-4: Mục tiêu & Nghiệp vụ] ──> [Slide 5-9: Kiến trúc & Tech] ──> [Slide 10-12: Sản phẩm & Lộ trình]
```

---

## 1. Bối cảnh & Khoảng trống nghiên cứu (Slide 1, 2)
* **Ý cần nói:**
  * Xử lý dữ liệu lớn trong thực tế luôn có sự đánh đổi giữa **Batch** (chính xác, deduplication, ACID nhưng trễ hàng giờ) và **Streaming** (độ trễ < 5s nhưng dữ liệu gần đúng, dễ bị out-of-order/late data).
  * 4 vấn đề kinh điển của hệ thống cũ: *Small File Problem* khi stream liên tục, *Thiếu ACID Transactions*, *Thiếu Data Quality Gates*, và *Batch - Stream bị lệch pha*.
* **Giải pháp đề tài:**
  * Áp dụng **Lambda Architecture** kết hợp **Apache Iceberg (Data Lakehouse)** trên MinIO S3 làm Single Source of Truth.
  * Tích hợp **Auto-Correcting Query Merger** tại Serving Layer.

---

## 2. Mục tiêu nghiên cứu & Bài toán nghiệp vụ (Slide 3, 4)
* **Ý cần nói:**
  * **6 Mục tiêu cốt lõi:** Dựng 3 lớp Lambda, Thuật toán Query Merger, Pipeline chạy trên máy 16GB RAM, Data Quality Gate với Fault Injector, 4 bài Benchmark, và Báo cáo so sánh kiến trúc (Lambda vs Kappa vs Medallion).
  * **Bài toán thực tế:** Thu thập luồng giao dịch trực tiếp **BTCUSDT** từ **Binance WebSocket Live Stream** (`trade_id`, `price`, `qty`, `time`).
  * **Nghiệp vụ tính toán:**
    * Nến **OHLCV** (khung 1 phút và 5 phút).
    * Chỉ số trung bình giá theo khối lượng **VWAP** (Volume-Weighted Average Price).
    * Tổng khối lượng giao dịch và tỷ suất sinh lợi (Return).
    * Phát hiện đột biến giá (**Price Spike Detection**).
  * **5 Tiêu chí nghiệm thu API:** Trả về nến, cờ cảnh báo spike, nhãn trạng thái (`Provisional` vs `Reconciled`), vị trí `batch_watermark`, và độ lệch đối soát `reconciliation_delta`.

---

## 3. Kiến trúc hệ thống End-to-End & Phân tầng (Slide 5, 6, 7)
* **Ý cần nói:**
  * **Tầng Ingestion (Slide 6):** 
    * Python Async Collector kết nối Binance WebSocket $\rightarrow$ đi qua **Fault Injector** (chủ động bơm lỗi thử nghiệm) $\rightarrow$ đẩy vào **Apache Kafka KRaft** (Topic `crypto.trades`).
  * **Tầng Speed Layer (Slide 7 trái):** 
    * **Spark Structured Streaming** đọc Kafka theo Tumbling Window (1m, 5m) + Watermarking (cho phép trễ 1-2 phút) $\rightarrow$ tính nhanh OHLCV/VWAP $\rightarrow$ ghi vào bảng `speed_agg` trong ClickHouse với trạng thái `Provisional` (SLA < 5s).
  * **Tầng Batch Layer (Slide 7 phải):** 
    * Kafka $\rightarrow$ ghi bất biến vào **Bronze Table** (Apache Iceberg trên MinIO S3 - ACID Snapshot Isolation).
    * Định kỳ chạy **Spark Batch Job** (hoặc cron/trigger): Bronze $\rightarrow$ Silver (Deduplication theo `trade_id`, lọc lỗi vào Quarantine Table) $\rightarrow$ Gold (tính OHLCV/VWAP chuẩn xác) $\rightarrow$ ghi vào bảng `batch_agg` trong ClickHouse với trạng thái `Reconciled` (Ground Truth).

---

## 4. Trọng tâm: Thuật toán Auto-Correcting Query Merger (Slide 8)
* **Cơ chế hoạt động:**
  1. Client gửi request `GET /api/market?symbol=BTCUSDT&start=...&end=...` vào **FastAPI Gateway**.
  2. FastAPI truy vấn `batch_watermark` mới nhất từ hệ thống.
  3. **Phân luồng truy vấn thông minh:**
     * Nếu mốc thời gian truy vấn $< \text{batch\_watermark}$: Dữ liệu đã được Batch Layer xử lý xong $\rightarrow$ Đọc từ `batch_agg` $\rightarrow$ Trả về nhãn **`Reconciled`** (Chuẩn xác 100%, `error_delta = 0`).
     * Nếu mốc thời gian truy vấn $\ge \text{batch\_watermark}$: Dữ liệu Batch chưa chạm tới $\rightarrow$ Đọc từ `speed_agg` $\rightarrow$ Trả về nhãn **`Provisional`** (Tức thời, chấp nhận sai số nhỏ).
     * Nếu khoảng thời gian cắt ngang (giao thoa giữa Batch và Speed): Hợp nhất 2 tập dữ liệu $\rightarrow$ Trả về nhãn **`Partially Reconciled`** kèm chỉ số `reconciliation_delta`.

---

## 5. Stack Công nghệ, Ngân sách RAM 16GB & Sản phẩm (Slide 9, 10)
* **Ý cần nói:**
  * Toàn bộ hệ thống đóng gói qua **Docker Compose**, không tốn chi phí thuê Cloud, có thể tái lập (reproducible) 100%.
  * **Phân bổ ngân sách RAM nghiêm ngặt (16GB RAM):**
    * *Docker Services (7.5 - 8.5 GB):* Kafka KRaft (768MB-1GB), Spark Master/Worker (3.5-4GB), MinIO + Iceberg (512-768MB), ClickHouse (1-1.5GB), FastAPI (256MB), Streamlit (256MB).
    * *Dành cho OS, WSL2, Browser:* Còn dư 7.5 - 8.0 GB $\rightarrow$ Máy chạy cực kỳ mượt mà, không sợ crash.
  * **8 Sản phẩm nghiệm thu rõ ràng:** Ingestion Pipeline, Bronze Iceberg Table, Speed Layer, Batch Layer, Query Merger API, Data Quality Gate, Dashboard Streamlit, Báo cáo 4 Benchmark.

---

## 6. Kế hoạch Thực nghiệm 4 Kịch bản Benchmark
* **Kịch bản 1 (Latency):** Đo thời gian phản hồi giữa Batch-only (ClickHouse) vs Speed-only vs Lambda Merge (FastAPI).
* **Kịch bản 2 (Correctness & Reconciliation):** Đo độ lệch `error_delta` của Speed View so với Ground Truth khi có sự kiện trễ/trùng lặp; đo thời gian hệ thống tự đối soát về 0 khi Batch hoàn tất.
* **Kịch bản 3 (Data Quality & Fault Handling):** Đánh giá Precision/Recall của Quarantine Table khi chủ động bơm 4 loại lỗi (Duplicate, Late, Out-of-Order, Schema Error).
* **Kịch bản 4 (Compaction Efficiency):** Nạp streaming 2-3 tiếng tạo hàng nghìn file Parquet nhỏ $\rightarrow$ chạy `OPTIMIZE` của Iceberg $\rightarrow$ đo cải thiện tốc độ đọc (MB/s) và giảm metadata overhead.

---

## 7. Định hướng 2 Giai đoạn: TLCN $\rightarrow$ KLTN (Slide 11, 12)
* **Ý cần nói:**
  * **Giai đoạn 1 (TLCN - 13 tuần):** Tập trung xây dựng hoàn chỉnh nền tảng kiến trúc (MVP), luồng nạp Binance WebSocket, Query Merger v1.0, 4 bài Benchmark và Dashboard giám sát.
  * **Giai đoạn 2 (KLTN - Nâng cao):** Kế thừa trọn vẹn nền tảng TLCN để mở rộng: Multi-symbol (ETH, SOL), Multi-exchange (OKX, Bybit), tích hợp **Dagster & dbt-spark**, bổ sung **Redis Hot Cache**, ứng dụng **Machine Learning** phát hiện bất thường (Isolation Forest), và đóng gói Kubernetes.

---

# PHẦN 3: CHEAT SHEET KIẾN THỨC CỐT LÕI (BẮT BUỘC PHẢI THUỘC)

| Khái niệm | Định nghĩa ngắn gọn & Bản chất |
|:---|:---|
| **Lambda Architecture** | Kiến trúc xử lý Big Data chia làm 3 tầng: **Batch Layer** (tính toán toàn diện, bất biến, chính xác), **Speed Layer** (tính toán gia tăng, tức thời, gần đúng) và **Serving Layer** (hợp nhất kết quả từ 2 tầng). |
| **Kappa Architecture** | Kiến trúc chỉ dùng duy nhất 1 luồng Stream Processing (ví dụ Flink/Spark Streaming) cho cả thời gian thực và xử lý lại lịch sử. (Hạn chế: khó reprocess hàng tỷ bản ghi lịch sử phức tạp so với Batch). |
| **Medallion Architecture** | Mô hình tổ chức dữ liệu theo 3 tầng chất lượng: **Bronze** (Raw, bất biến) $\rightarrow$ **Silver** (Cleaned, Deduplicated, Filtered) $\rightarrow$ **Gold** (Aggregated, Fact/Dim tables, sẵn sàng phân tích). |
| **Apache Iceberg** | Open Table Format (định dạng bảng mở) cấp doanh nghiệp chạy trên nền Object Storage (MinIO/S3), hỗ trợ giao dịch **ACID**, **Time Travel**, **Partition Evolution** và giải quyết triệt để **Small File Problem** qua cơ chế Snapshot Metadata và Compaction. |
| **Watermark trong Streaming** | Mốc thời gian ngưỡng quy định hệ thống sẽ đợi dữ liệu đến muộn tối đa bao lâu trước khi đóng window tính toán. |
| **Batch Watermark trong Serving** | Mốc thời gian cao nhất mà Batch Layer đã hoàn thành việc tính toán Ground Truth. Là ranh giới phân định dữ liệu `Reconciled` và `Provisional`. |
| **VWAP** | Volume-Weighted Average Price: $\text{VWAP} = \frac{\sum (\text{Price} \times \text{Volume})}{\sum \text{Volume}}$, chỉ số phản ánh giá trị giao dịch thực tế chính xác hơn giá trung bình thông thường. |
| **Single Source of Truth** | Tầng Bronze trên MinIO/Iceberg: mọi luồng tính toán (Batch hay Stream Reprocess) đều đọc từ dữ liệu gốc bất biến này, không sợ mất mát. |

---

# PHẦN 4: BỘ CÂU HỎI PHẢN BIỆN DỰ PHÒNG (Q&A VỚI THẦY)

### ❓ Câu 1: Tại sao không dùng Kappa Architecture (chỉ dùng Spark Streaming) mà phải dùng Lambda cho phức tạp?
* **Trả lời chuẩn:**  
  > *"Dạ thưa Thầy, Kappa Architecture có ưu điểm là chỉ duy trì 1 codebase streaming. Tuy nhiên, trong bài toán tài chính và giám sát thị trường:*  
  > *1. Khi cần **tính toán lại lịch sử (Historical Reprocessing)** nhiều tháng hoặc thay đổi thuật toán nến/chỉ báo kỹ thuật, việc replay hàng trăm triệu bản ghi qua Stream Engine rất tốn tài nguyên và dễ nghẽn checkpoint.*  
  > *2. Spark Batch trên nền **Apache Iceberg** cho phép tận dụng tối đa Vectorized Reading, Metadata Pruning và Partitioning để quét hàng chục GB dữ liệu lịch sử nhanh gấp nhiều lần streaming.*  
  > *3. Lambda giúp phân định rõ ràng trách nhiệm: Speed Layer phục vụ SLA tức thời (<5s), còn Batch Layer đảm bảo tính toàn vẹn Ground Truth và kiểm toán.*  
  > *Trong báo cáo, nhóm cũng có phần so sánh thực nghiệm đánh đổi (Trade-offs) giữa Lambda và Kappa ạ."*

---

### ❓ Câu 2: Dữ liệu lấy từ đâu? Có đủ lớn để gọi là Big Data và làm Benchmark không?
* **Trả lời chuẩn:**  
  > *"Dạ, nhóm lấy dữ liệu thực tế từ **Binance WebSocket Trade Feed** của cặp **BTCUSDT**. Đây là cặp tiền có thanh khoản và tần suất giao dịch cao nhất thế giới (trung bình 50 - 200 giao dịch/giây, vào giờ cao điểm có thể lên tới hàng ngàn giao dịch/giây).*  
  > *Chỉ sau vài giờ thu thập, hệ thống đã ghi nhận hàng triệu sự kiện. Đồng thời, nhóm có xây dựng module **Historical Generator / Replay** từ dữ liệu quá khứ của Binance để giả lập các kịch bản tải cao (High Throughput) phục vụ đo đạc Benchmark ạ."*

---

### ❓ Câu 3: Tại sao lại cần "Fault Injector" (Bơm lỗi) vào dữ liệu?
* **Trả lời chuẩn:**  
  > *"Dạ, nếu chỉ lấy dữ liệu sạch từ Binance thì không thể đánh giá được độ tin cậy của tầng **Data Quality Gate** và cơ chế đối soát của **Query Merger**.*  
  > *Do đó, nhóm xây dựng module Fault Injector chủ động bơm 4 loại lỗi phổ biến trong mạng phân tán: **Dữ liệu trùng (Duplicate 10%)**, **Dữ liệu đến muộn (Late Arrival 10%)**, **Dữ liệu sai thứ tự (Out-of-Order)**, và **Dữ liệu sai schema**.*  
  > *Vì các lỗi này được gán nhãn `is_injected=true`, nhóm sẽ có **Ground Truth chuẩn** để tính toán chính xác các chỉ số khoa học như **Precision, Recall, F1-Score** của bộ lọc Quarantine Table và đo thời gian Speed View bị lệch trước khi được Batch View tự động sửa sai (Auto-Correct) ạ."*

---

### ❓ Câu 4: Máy tính cá nhân (16GB RAM) có chạy nổi toàn bộ cụm công nghệ này không?
* **Trả lời chuẩn:**  
  > *"Dạ thưa Thầy, nhóm đã tính toán và phân bổ ngân sách RAM rất chi tiết (thể hiện ở Slide 9):*  
  > *- Kafka chạy chế độ **KRaft** (bỏ Zookeeper) $\rightarrow$ chỉ tốn ~800MB RAM.*  
  > *- ClickHouse tối ưu cho Single-Node $\rightarrow$ chỉ tốn ~1.2GB RAM.*  
  > *- Spark Master/Worker được cấp cgroup giới hạn ~3.5GB RAM.*  
  > *- MinIO + FastAPI + Streamlit chiếm ~1.5GB.*  
  > *Tổng cộng toàn bộ Docker containers chiếm khoảng **7.5 – 8.5 GB RAM**, hệ thống vẫn còn dư **7.5 – 8 GB RAM** cho hệ điều hành và WSL2, đảm bảo vận hành ổn định không bị OOM ạ."*

---

### ❓ Câu 5: Đề tài này có điểm gì mới / đóng góp gì so với các đồ án trước đây?
* **Trả lời chuẩn:**  
  > *"Dạ thưa Thầy, có 3 điểm đóng góp kỹ thuật nổi bật:*  
  > *1. **Thuật toán Auto-Correcting Query Merger:** Tự động đối soát và hợp nhất 2 nguồn dữ liệu Batch và Speed dựa trên Batch Watermark, gắn nhãn trạng thái `Provisional` và `Reconciled` minh bạch.*  
  > *2. **Ứng dụng Apache Iceberg trên MinIO:** Giải quyết bài toán Small File Problem do streaming nạp liên tục bằng cơ chế Snapshot Isolation và Compaction.*  
  > *3. **Phương pháp Thực nghiệm Định lượng:** Đồ án không chỉ xây dựng hệ thống chạy được mà còn có 4 bài Benchmark đo lường định lượng với số liệu, biểu đồ rõ ràng ạ."*

---

# PHẦN 5: CHECKLIST TRƯỚC KHI GẶP THẦY

- [ ] **Mở sẵn Slide:** Mở file [docs/slides.html](slides.html) trên trình duyệt, nhấn `F11` chuyển sang chế độ Full Screen.
- [ ] **Kiểm tra phím tắt:** Thử bấm `→`, `←`, `Space` để lướt qua các hiệu ứng và biểu đồ SVG.
- [ ] **Tài liệu in / PDF:** Mang theo bản đề cương hoặc mở sẵn file Markdown [de_cuong_chi_tiet_tieu_luan.md](de_cuong_chi_tiet_tieu_luan.md).
- [ ] **Phân công trình bày:**
  * **Bạn A (Quốc Anh):** Trình bày Bối cảnh, Bài toán nghiệp vụ Binance, Ingestion Layer, Speed Layer & Serving Query Merger.
  * **Bạn B (Minh Quân):** Trình bày Batch Layer, Apache Iceberg, Data Quality/Fault Injection, Kế hoạch 4 Benchmark & Ngân sách RAM 16GB.
- [ ] **Tâm thế:** Tự tin, cầu thị, lắng nghe kỹ từng câu hỏi của Thầy và ghi chép lại các điểm Thầy góp ý chỉnh sửa.
