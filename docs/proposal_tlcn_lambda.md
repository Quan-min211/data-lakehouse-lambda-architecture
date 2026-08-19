# PROPOSAL — Thiết Kế và Triển Khai Hệ Thống Data Lakehouse theo Lambda Architecture
## (Bản chỉnh sửa Proposal TLCN — Cập nhật Lambda Architecture)

**Trường:** Đại học Sư phạm Kỹ thuật TP.HCM (HCMUTE)
**Chuyên ngành:** Kỹ thuật Máy tính / Khoa học Dữ liệu
**Loại đề tài:** Đồ án Tốt nghiệp (TLCN)

---

## 1. Đặt vấn đề

### 1.1. Bối cảnh

Trong thời đại Big Data, các doanh nghiệp phải đối mặt với luồng dữ liệu liên tục từ nhiều nguồn: giao dịch tài chính, sự kiện người dùng, IoT sensors, log hệ thống. Hai nhu cầu đối lập thường xuyên xung đột nhau:

- **Nhu cầu phân tích lịch sử (Batch Analytics):** Cần xử lý hàng tỷ bản ghi với độ chính xác tuyệt đối, nhưng chấp nhận độ trễ (vài giờ/ngày).
- **Nhu cầu phản hồi thời gian thực (Real-time):** Cần dữ liệu cập nhật trong vài giây, nhưng chấp nhận kết quả gần đúng.

Kiến trúc truyền thống (Data Warehouse hoặc Data Lake đơn thuần) không thể đáp ứng đồng thời cả hai nhu cầu này.

### 1.2. Khoảng trống nghiên cứu

| Vấn đề | Hệ thống truyền thống | Hướng giải quyết |
|:---|:---|:---|
| **Small File Problem** | Streaming liên tục tạo hàng nghìn tệp nhỏ, làm suy giảm hiệu năng đọc | Iceberg Compaction (OPTIMIZE) theo lịch |
| **Thiếu ACID Transactions** | Ghi đồng thời gây bất nhất dữ liệu | Apache Iceberg với ACID guarantees |
| **Không có Data Quality Gates** | Dữ liệu lỗi lọt vào tầng phân tích | DQ constraints + Quarantine Tables |
| **Batch vs Stream không tích hợp** | Hai hệ thống riêng biệt, không có Serving Layer thống nhất | **Lambda Architecture** — hợp nhất qua Query Merger |

---

## 2. Mục tiêu Đề tài

1. **Thiết kế** hệ thống Data Lakehouse theo mô hình Lambda Architecture gồm 3 lớp: Batch Layer, Speed Layer và Serving Layer.
2. **Triển khai** pipeline hoàn chỉnh từ ingestion (Kafka) → Batch/Speed processing (Spark/Flink) → Serving (ClickHouse + Redis + FastAPI).
3. **Thực nghiệm & Đánh giá** hiệu năng qua 3 kịch bản benchmark cụ thể: latency, reprocessing correctness, và compaction optimization.
4. **So sánh** Lambda Architecture với các mô hình kiến trúc thay thế (Kappa, Medallion thuần).

---

## 3. Lambda Architecture — Tổng quan Kiến trúc Đề xuất

```
 DATA SOURCES
 ┌─────────────────────────────────────────────────────────┐
 │  Kafka Streaming │ CSV/Parquet Batch │ CDC (Debezium)   │
 └──────┬───────────────────────────────────────┬──────────┘
        │                                       │
 ┌──────▼──────────────────┐    ┌───────────────▼──────────┐
 │       BATCH LAYER       │    │       SPEED LAYER         │
 │                         │    │                           │
 │  MinIO (Master Dataset) │    │  Kafka → Spark Streaming  │
 │  + Apache Iceberg       │    │  → Redis (Speed Views)    │
 │                         │    │                           │
 │  Spark Batch → dbt      │    │  Near-realtime, approx.   │
 │  → ClickHouse           │    │  delta from last batch    │
 │    (Batch Views)        │    │                           │
 └──────────────┬──────────┘    └───────────┬───────────────┘
                │                           │
 ┌──────────────▼───────────────────────────▼───────────────┐
 │                    SERVING LAYER                          │
 │                                                           │
 │  FastAPI Query Merger: Batch View + Speed View → Result   │
 │  ClickHouse (OLAP) + Redis (Hot Cache)                    │
 │  Metabase / Streamlit Dashboard                           │
 └───────────────────────────────────────────────────────────┘
```

### 3.1. Batch Layer
- **Master Dataset:** MinIO + Apache Iceberg (immutable, bất biến)
- **Processing:** Apache Spark Batch Jobs, điều phối bởi Dagster
- **Transformation:** dbt models (Bronze → Silver → Gold)
- **Output:** Batch Views trong ClickHouse (cập nhật định kỳ: hourly/daily)

### 3.2. Speed Layer
- **Input:** Kafka topics (streaming events)
- **Processing:** Spark Structured Streaming hoặc Apache Flink
- **Output:** Speed Views trong Redis (key-value aggregations, TTL-based expiry)
- **Đặc điểm:** Approximate results, at-least-once semantics, low latency (<5s)

### 3.3. Serving Layer
- **Query Merger (FastAPI):** Tự động chọn Batch View (nếu đã có) + Speed View (delta bổ sung)
- **OLAP Engine:** ClickHouse — phục vụ ad-hoc analytics queries
- **Hot Cache:** Redis — phục vụ dashboard real-time
- **Orchestration:** Dagster quản lý toàn bộ pipeline lineage

---

## 4. Các Bài toán Phù hợp với Khung Lambda Architecture

Dưới đây là 5 bài toán thực tế phù hợp với khung đề tài này. Mỗi bài toán đều khai thác được đặc trưng quan trọng nhất của Lambda (dual-layer processing + merged serving):

---

### 🚕 Bài toán 1: Phân tích Hành trình Taxi & Dự báo Cầu vận tải (Đề xuất cao nhất)

**Dataset:** NYC TLC Yellow Cab (~1.5 tỷ chuyến đi, 2009–2024, ~200GB Parquet)

**Bài toán:**
- **Batch Layer:** Tổng hợp thống kê chuyến đi theo giờ/ngày/khu vực (doanh thu, quãng đường, tip rate). Dùng Kimball Dimensional Model: Fact_Trip, Dim_Zone, Dim_Driver, Dim_Time.
- **Speed Layer:** Cập nhật real-time dashboard: "Số chuyến đang chạy", "Tổng doanh thu hôm nay tính đến hiện tại".
- **Query Merger:** Câu hỏi "Doanh thu tháng này?" = Batch (dữ liệu tuần trước) + Speed (delta 7 ngày gần nhất).

**Lý do phù hợp:**
- Dữ liệu đủ lớn để thể hiện Small File Problem và Compaction.
- Phân vùng tự nhiên theo thời gian và địa lý → benchmark Iceberg partitioning/Z-Ordering rõ ràng.
- Có cả batch history và streaming simulation (phát lại dữ liệu qua Kafka).

---

### 💳 Bài toán 2: Phát hiện Gian lận Giao dịch Tài chính (Fraud Detection)

**Dataset:** PaySim Synthetic Financial Dataset (~6.3 triệu giao dịch) hoặc Kaggle Credit Card Fraud

**Bài toán:**
- **Batch Layer:** Huấn luyện Feature Store: tính toán các đặc trưng lịch sử (average transaction per customer, variance, peer-group stats) định kỳ.
- **Speed Layer:** Khi có giao dịch mới → join với Feature Store trong Redis → chạy rule-based hoặc ML inference → flag suspect.
- **Query Merger:** Dashboard "Số vụ gian lận ngày hôm nay" = Batch (dữ liệu hôm qua) + Speed (delta trong ngày).

**Lý do phù hợp:**
- Khai thác mạnh Speed Layer — fraud detection yêu cầu phản hồi trong vài giây.
- Batch Layer tính Feature Store định kỳ, Speed Layer dùng features đó cho real-time scoring.
- Thể hiện rõ sự phân tách batch accuracy vs. speed approximation.

---

### 🛍️ Bài toán 3: Phân tích Hành vi Người dùng trên Nền tảng E-Commerce

**Dataset:** Clickstream data từ Kaggle (eCommerce behavior dataset từ Rees46 ~50M events, ~7GB)

**Bài toán:**
- **Batch Layer:** Tính toán Cohort Analysis, Funnel Analysis (view → add_to_cart → purchase), RFM Segmentation theo ngày.
- **Speed Layer:** Real-time session tracking: "Người dùng X đang xem gì?", "Số lượt view sản phẩm Y trong 5 phút qua".
- **Query Merger:** "Conversion rate hôm nay?" = Batch (dữ liệu qua đêm) + Speed (delta trong ngày làm việc).

**Lý do phù hợp:**
- Nested JSON schema phức tạp → thể hiện Schema Evolution của Iceberg.
- Sessionization trong Speed Layer (group events theo session_id) → challenging stream processing.
- Late-arriving data xử lý như thế nào (watermark trong Spark/Flink)?

---

### 🏭 Bài toán 4: Giám sát & Dự báo Bảo trì Thiết bị IoT (Predictive Maintenance)

**Dataset:** Giả lập hoặc dùng Kaggle dataset (Machine Failure Prediction, NASA CMAPSS Turbofan)

**Bài toán:**
- **Batch Layer:** Tính toán thống kê cảm biến theo chu kỳ (mean, std, trend per machine), huấn luyện mô hình dự báo hỏng hóc định kỳ.
- **Speed Layer:** Nhận sensor readings từ Kafka mỗi giây → so sánh với ngưỡng → cảnh báo real-time nếu anomaly.
- **Query Merger:** Dashboard "Tình trạng máy X hiện tại" = Batch stats (hôm qua) + Speed real-time readings (hôm nay).

**Lý do phù hợp:**
- Thể hiện high-throughput ingestion (IoT sensors gửi hàng nghìn records/giây).
- Speed Layer dùng stateful stream processing (so sánh với trung bình lịch sử).
- Mô hình Predictive Maintenance là use case phổ biến nhất của Lambda trong công nghiệp.

---

### 📦 Bài toán 5: Phân tích Chuỗi Cung ứng & Tối ưu Logistics

**Dataset:** Brazilian E-Commerce (Olist) + Synthetic Supply Chain Logistics

**Bài toán:**
- **Batch Layer:** Tính toán KPIs: On-time Delivery Rate, Supplier Performance Score, Inventory Turnover theo tháng/quý.
- **Speed Layer:** Real-time tracking: "Đơn hàng X đang ở đâu?", "Số đơn delay trong ngày hôm nay".
- **Query Merger:** "Tỷ lệ giao đúng hạn tháng này" = Batch (dữ liệu lịch sử tháng trước + batch hôm qua) + Speed (delta trong ngày).

**Lý do phù hợp:**
- Dữ liệu có cấu trúc rõ ràng (order, product, seller, customer) → Kimball model đẹp.
- Thể hiện SCD Type 2 (lịch sử thay đổi địa chỉ giao hàng, trạng thái đơn hàng).
- Ít tải hơn bài toán taxi → phù hợp nếu tài nguyên máy tính hạn chế.

---

## 5. Khung Đề cương 5 Chương (Cập nhật)

### Chương 1: Tổng quan
- Bối cảnh Big Data và nhu cầu xử lý dữ liệu hỗn hợp (Batch + Stream)
- Phân tích hạn chế của các kiến trúc hiện tại (Data Warehouse, Data Lake, Medallion thuần)
- Mục tiêu, phạm vi, phương pháp nghiên cứu

### Chương 2: Cơ sở Lý thuyết và Công nghệ
- **Lambda Architecture:** Lịch sử, 3 layer, ưu/nhược điểm
- **So sánh Lambda vs Kappa vs Medallion thuần**
- **Apache Iceberg:** Table Format, ACID, Time Travel, Partitioning
- **Kimball Dimensional Modeling:** Fact/Dimension, SCD Type 2
- **Data Quality:** Constraints, Quarantine, Metrics
- **Stack công nghệ:** Kafka, Spark, Flink, Redis, ClickHouse, Dagster, dbt

### Chương 3: Thiết kế Kiến trúc Hệ thống
- Lựa chọn và mô tả nguồn dữ liệu
- Thiết kế Batch Layer: MinIO schema, Iceberg partition strategy, dbt models
- Thiết kế Speed Layer: Kafka topics, Spark/Flink jobs, Redis data structures
- Thiết kế Serving Layer: Query Merger logic, ClickHouse schema, FastAPI endpoints
- Orchestration với Dagster: Asset graph, schedules, data quality checks

### Chương 4: Triển khai và Thực nghiệm
- Triển khai toàn bộ hệ thống bằng Docker Compose
- **Benchmark 1:** Latency comparison (Batch-only vs Speed-only vs Lambda Merge)
- **Benchmark 2:** Batch Reprocessing — Đo sai số Speed View vs Batch View sau khi batch hoàn tất
- **Benchmark 3:** Iceberg Compaction — Đo cải thiện hiệu năng sau khi gộp tệp nhỏ
- Đánh giá Data Quality: Tỷ lệ phát hiện lỗi, quarantine accuracy

### Chương 5: Kết luận và Hướng phát triển
- Tổng kết kết quả thực nghiệm
- So sánh với kỳ vọng và giả thuyết ban đầu
- Hạn chế (logic viết 2 lần, vận hành phức tạp)
- Hướng phát triển: Tích hợp Feature Store / MLOps, chuyển sang Kappa nếu cần simplify

---

## 6. Stack Công nghệ Tổng hợp

| Phân hệ | Công nghệ | Ghi chú |
|:---|:---|:---|
| Ingestion (Stream) | Apache Kafka + Debezium CDC | Real-time event bus |
| Ingestion (Batch) | Airbyte / Custom Spark Reader | Lịch sử + file dumps |
| Batch Processing | Apache Spark (PySpark) | Core compute engine |
| Stream Processing | Spark Structured Streaming | Speed Layer jobs |
| Transformation | dbt (Spark adapter) | Silver/Gold models |
| Storage | MinIO (S3-compatible) | Object store |
| Table Format | Apache Iceberg | ACID, Time Travel |
| Catalog | Iceberg REST Catalog | Schema registry |
| Batch Views | ClickHouse | OLAP engine |
| Speed Views | Redis | Hot cache, low-latency |
| Serving API | FastAPI | Query Merger + REST |
| Orchestration | Dagster | Asset lineage, schedules |
| Dashboard | Metabase / Streamlit | BI visualization |
| Containers | Docker Compose | Dev environment |

---

## 7. Điểm Khác biệt và Đóng góp Học thuật

1. **Triển khai Lambda Architecture end-to-end** với open-source stack trên môi trường local (Docker).
2. **Tích hợp Apache Iceberg** vào Master Dataset của Batch Layer — thay thế Parquet thô thông thường.
3. **Query Merger tự động** hợp nhất Batch và Speed Views mà không cần can thiệp thủ công.
4. **Benchmark định lượng** trên 3 chiều: latency, correctness, compaction efficiency.
5. **Có thể mở rộng** sang Feature Store / MLOps pipeline trong tương lai (đặt nền móng với Gold Layer và Redis Feature Cache).

