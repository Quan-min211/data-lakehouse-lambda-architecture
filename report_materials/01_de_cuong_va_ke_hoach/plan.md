# KẾ HOẠCH TIẾN ĐỘ THỰC HIỆN ĐỒ ÁN (13 TUẦN)
## ĐỀ TÀI: Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa
*(Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring)*

---

* **Loại đề tài:** Tiểu luận chuyên ngành (Định hướng nối tiếp Khóa luận tốt nghiệp)
* **Ngành:** Kỹ thuật Dữ liệu (Data Engineering) — Khoa Công Nghệ Thông Tin (HCMUTE)
* **Sinh viên thực hiện:** Nguyễn Đặng Quốc Anh (`23133004`) & Phạm Minh Quân (`23133060`)
* **Giảng viên hướng dẫn:** ThS. Đoàn Minh Trí
* **Thời gian thực hiện:** 13 Tuần (Giai đoạn Học kỳ 1 — Năm học 2026-2027)

---

## 📊 BẢNG TIẾN ĐỘ CHI TIẾT 13 TUẦN

| Tuần | Nhiệm vụ chính | Tiến độ | Ghi chú & Sản phẩm bàn giao |
|:---:|:---|:---:|:---|
| **Tuần 1** | **Chuẩn bị Dữ liệu & Thiết kế Data Structure**<br>• Phân tích cấu trúc dữ liệu thị trường từ sàn Binance.<br>• Định nghĩa Data Contracts và Data Structure cho sự kiện giao dịch (`crypto.trades`).<br>• Thiết lập môi trường Docker Compose nền tảng (Kafka KRaft, MinIO, ClickHouse). | 0% → 10% | • JSON Schema chuẩn hóa: `trade_id`, `symbol`, `price`, `qty`, `trade_time`, `is_buyer_maker`.<br>• Script kiểm thử kết nối Binance WebSocket Feed.<br>• Docker containers khởi chạy ổn định. |
| **Tuần 2** | **Xây dựng Nền tảng Apache Iceberg & Xử lý Vấn đề Small File**<br>• Cấu hình Apache Iceberg REST Catalog kết nối MinIO S3.<br>• Thiết kế bảng Bronze (`bronze.crypto_trades`) với cơ chế Snapshot Isolation.<br>• Thiết lập chiến lược phân vùng (Partitioning theo ngày) và thử nghiệm cơ chế Compaction (`OPTIMIZE` / Bin-packing). | 10% → 20% | • Bảng Iceberg trên MinIO S3 hoạt động với ACID guarantees.<br>• Kịch bản giả lập Small File Problem (hàng nghìn file Parquet < 2MB).<br>• Script thử nghiệm Compaction gộp file nhỏ. |
| **Tuần 3** | **Hiện thực Tầng Ingestion Pipeline & Module Fault Injector**<br>• Phát triển Python Async Collector kết nối Binance WebSocket Live Stream.<br>• Xây dựng module Fault Injector chủ động bơm 4 loại lỗi phân tán.<br>• Đẩy luồng dữ liệu vào Apache Kafka KRaft topic `crypto.trades`. | 20% → 30% | • Module `src/ingestion/kafka_producer.py` và `fault_injector.py`.<br>• 4 loại lỗi có kiểm soát: Duplicate (10%), Late Data 1-5m (10%), Out-of-Order, Schema Invalid.<br>• Cờ đánh dấu `is_injected=true` làm Ground Truth. |
| **Tuần 4** | **Hiện thực Tầng Xử lý Nhanh — Speed Layer (Spark Streaming)**<br>• Xây dựng Spark Structured Streaming Job đọc từ Kafka.<br>• Cấu hình Tumbling Window (1m, 5m) và Watermarking (1-2 phút).<br>• Tính toán tức thời các chỉ số: nến OHLCV, VWAP, Volume, Price Spike.<br>• Ghi dữ liệu gia tăng vào bảng `speed_agg` trong ClickHouse (Trạng thái `Provisional`). | 30% → 40% | • Script `src/speed_layer/spark_streaming.py`.<br>• Đạt độ trễ xử lý (SLA) < 5 giây.<br>• Dữ liệu nến tạm thời được cập nhật liên tục vào ClickHouse. |
| **Tuần 5** | **Thiết kế Data Quality Gate & Cơ chế Cách ly Lỗi (Quarantine Table)**<br>• Xây dựng bộ quy tắc kiểm tra chất lượng dữ liệu (Data Quality Constraints).<br>• Hiện thực cơ chế tự động chuyển bản ghi lỗi vào `quarantine_table` trên Iceberg.<br>• Tự động thu thập số liệu chất lượng dữ liệu (`sys_dq_metrics`). | 40% → 50% | • Module `src/data_quality/dq_checks.py`.<br>• Bảng `quarantine_table` ghi nhận bản ghi sai schema/giá âm kèm `error_code`.<br>• Báo cáo tỷ lệ dữ liệu sạch/lỗi tự động. |
| **Tuần 6** | **Hiện thực Tầng Xử lý Lô — Batch Layer (Bronze → Silver → Gold)**<br>• Xây dựng Spark Batch Processing Job định kỳ.<br>• Triển khai mô hình Medallion: Bronze (Raw) → Silver (Cleaned & Deduplicated theo `trade_id`) → Gold (Aggregated OHLCV/VWAP chuẩn xác).<br>• Đồng bộ dữ liệu Gold vào bảng `batch_agg` trong ClickHouse (Trạng thái `Reconciled` - Ground Truth). | 50% → 60% | • Script `src/batch_layer/spark_batch_jobs.py` & dbt models.<br>• Loại bỏ hoàn toàn bản ghi trùng lặp và dữ liệu trễ ngoài cửa sổ.<br>• Dữ liệu chuẩn xác 100% sẵn sàng cho đối soát. |
| **Tuần 7** | **Cơ chế Cập nhật Batch Watermark & Tự động Hóa Bảo trì Bảng**<br>• Thiết lập cơ chế ghi nhận mốc `batch_watermark` cao nhất sau mỗi chu kỳ Batch vào bảng `system_watermark`.<br>• Lên lịch trình bảo trì tự động: Iceberg Compaction, dọn dẹp Snapshot cũ (Expire Snapshots) và xóa tệp mồ côi (Remove Orphan Files). | 60% → 70% | • Bảng `system_watermark` cập nhật liên tục mốc hoàn tất của Batch Layer.<br>• Pipeline bảo trì tự động ngăn chặn phình to bộ nhớ và suy giảm tốc độ đọc. |
| **Tuần 8** | **Hiện thực Tầng Phục vụ — Auto-Correcting Query Merger API**<br>• Xây dựng FastAPI Gateway đóng vai trò Query Merger.<br>• Hiện thực thuật toán phân luồng dựa trên `batch_watermark`: `event_time < watermark` $\rightarrow$ đọc `batch_agg` (`Reconciled`); `event_time \ge watermark` $\rightarrow$ đọc `speed_agg` (`Provisional`); vùng giao thoa $\rightarrow$ hợp nhất và tính `reconciliation_delta`. | 70% → 75% | • Module `src/serving_layer/query_merger.py` và `api_routes.py`.<br>• REST API endpoints chuẩn hóa trả về JSON kèm nhãn trạng thái và sai số đối soát.<br>• Swagger UI tại `http://localhost:8000/docs`. |
| **Tuần 9** | **Xây dựng Giao diện Giám sát Streamlit Dashboard**<br>• Phát triển Dashboard trực quan hóa dữ liệu từ Serving API.<br>• Vẽ biểu đồ nến Real-time (Plotly Candlestick) kết hợp đường chỉ số VWAP.<br>• Bảng cảnh báo bất thường giá (Price Spike) và hiển thị vị trí Batch Watermark cùng nhãn đối soát thời gian thực. | 75% → 80% | • Ứng dụng `dashboard/app.py` chạy tại `http://localhost:8501`.<br>• Giao diện hiển thị trực quan biến động thị trường BTCUSDT và trạng thái tự sửa sai của hệ thống. |
| **Tuần 10** | **Thực thi Benchmark 1 (Latency) & Benchmark 2 (Reconciliation Correctness)**<br>• **Benchmark 1:** Đo lường và so sánh độ trễ truy vấn (P50, P95, P99 Latency, RPS) giữa 3 phương án: *Batch-only* vs *Speed-only* vs *Lambda Merge*.<br>• **Benchmark 2:** Đo sai số $\Delta$ giữa Speed View và Ground Truth khi có dữ liệu trễ/trùng; đo thời gian hội tụ sai số về 0 sau khi Batch hoàn tất. | 80% → 85% | • Scripts `scripts/benchmarks/bench_latency.py` và `bench_reprocess.py`.<br>• Xuất dữ liệu log thô dạng CSV vào `results/logs/`. |
| **Tuần 11** | **Thực thi Benchmark 3 (Data Quality/Fault) & Benchmark 4 (Compaction Efficiency)**<br>• **Benchmark 3:** Đánh giá ma trận nhầm lẫn (Confusion Matrix), độ chính xác (**Precision, Recall, F1-Score**) của Quarantine Gate khi tiêm lỗi.<br>• **Benchmark 4:** Đo lường cải thiện tốc độ quét dữ liệu (MB/s, Query Execution Time) và giảm metadata overhead trước vs sau khi chạy Iceberg Compaction. | 85% → 90% | • Scripts `scripts/benchmarks/bench_compaction.py` và `run_all_benchmarks.py`.<br>• Bảng số liệu thực nghiệm đầy đủ cho 4 bài toán kỹ thuật. |
| **Tuần 12** | **Phân tích Thực nghiệm, Xuất Biểu đồ Khoa học & Viết Báo cáo TLCN**<br>• Xử lý số liệu log, xuất biểu đồ trực quan hóa (Latency Distribution, Error Convergence, Compaction Speedup) vào `results/plots/`.<br>• Phân tích đánh đổi kiến trúc (Trade-offs: Lambda vs Kappa vs Medallion).<br>• Soạn thảo toàn diện 5 Chương Báo cáo Tiểu luận Chuyên ngành theo đề cương chuẩn. | 90% → 95% | • Tập hợp biểu đồ định lượng chất lượng cao trong `results/plots/`.<br>• Hoàn thiện bản thảo Báo cáo TLCN (Word/LaTeX/PDF). |
| **Tuần 13** | **Kiểm thử Toàn diện End-to-End, Hoàn thiện Slide & Chuẩn bị Bảo vệ**<br>• Smoke test toàn bộ pipeline từ Ingestion $\rightarrow$ Speed/Batch $\rightarrow$ Query Merger $\rightarrow$ Dashboard.<br>• Hoàn thiện bộ slide trình chiếu tương tác [slides.html].<br>• Quay video demo vận hành thực tế hệ thống.<br>• Rà soát câu hỏi phản biện và chuẩn bị bảo vệ đề tài trước Hội đồng. | 95% → 100% | • Hệ thống đóng gói Docker chạy mượt mà 100% trên máy 16GB RAM.<br>• Video demo và Slide thuyết trình hoàn chỉnh.<br>• Nộp báo cáo chính thức và sẵn sàng bảo vệ thành công! |

---

## 🎯 CÁC MỐC GIAI ĐOẠN CỐT LÕI (MILESTONES)

```
[Tuần 1–3] Nền tảng Hạ tầng, Data Structure, Iceberg & Ingestion
    │
    ▼
[Tuần 4–7] Xây dựng Luồng Xử lý Kép: Speed Layer & Batch Layer (DQ Gates)
    │
    ▼
[Tuần 8–9] Tầng Phục vụ Auto-Correcting Query Merger & Dashboard UI
    │
    ▼
[Tuần 10–11] Thực thi 4 Kịch bản Benchmark Định lượng Khoa học
    │
    ▼
[Tuần 12–13] Phân tích Số liệu, Hoàn tất Báo cáo 5 Chương & Bảo vệ Đề tài
```

---

## 📌 PHÂN BỔ NHIỆM VỤ GIỮA 2 THÀNH VIÊN

| Phân hệ / Nhiệm vụ | Phụ trách chính (Lead) | Phối hợp (Support) |
|:---|:---|:---|
| **Ingestion, Kafka & Fault Injector** | Nguyễn Đặng Quốc Anh | Phạm Minh Quân |
| **Speed Layer & Spark Streaming** | Nguyễn Đặng Quốc Anh | Phạm Minh Quân |
| **Serving Layer & Auto-Correcting Query Merger** | Nguyễn Đặng Quốc Anh | Phạm Minh Quân |
| **Apache Iceberg, MinIO S3 & Compaction** | Phạm Minh Quân | Nguyễn Đặng Quốc Anh |
| **Batch Layer, Data Quality Gates & Quarantine** | Phạm Minh Quân | Nguyễn Đặng Quốc Anh |
| **ClickHouse OLAP & Storage Schemas** | Phạm Minh Quân | Nguyễn Đặng Quốc Anh |
| **Benchmark Execution (4 Kịch bản) & Logging** | Cả hai cùng thực hiện | Cả hai cùng thực hiện |
| **Dashboard UI & Báo cáo Luận văn 5 Chương** | Cả hai cùng thực hiện | Cả hai cùng thực hiện |
