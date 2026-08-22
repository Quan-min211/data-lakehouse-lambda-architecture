# PROPOSAL — Thiết Kế và Triển Khai Hệ Thống Data Lakehouse theo Lambda Architecture
## (Bản chỉnh sửa Proposal TLCN — Cập nhật Lambda Architecture)

**Trường:** Đại học Sư phạm Kỹ thuật TP.HCM (HCMUTE)
**Chuyên ngành:** Kỹ thuật Dữ liệu / Kỹ thuật Máy tính
**Loại đề tài:** Tiểu luận chuyên ngành (TLCN)

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
3. **Thực nghiệm & Đánh giá** hiệu năng qua 4 kịch bản benchmark cụ thể: latency, reprocessing correctness, data quality fault handling, và compaction optimization.
4. **So sánh** Lambda Architecture với các mô hình kiến trúc thay thế (Kappa, Medallion thuần).

---

## 3. Lambda Architecture — Tổng quan Kiến trúc Đề xuất

```
 DATA SOURCES (Binance WebSocket Live Stream: BTCUSDT)
 ┌─────────────────────────────────────────────────────────┐
 │  Kafka Streaming │ Ingestion Collector │ Fault Injector │
 └──────┬───────────────────────────────────────┬──────────┘
        │                                       │
 ┌──────▼──────────────────┐    ┌───────────────▼──────────┐
 │       BATCH LAYER       │    │       SPEED LAYER         │
 │                         │    │                           │
 │  MinIO (Master Dataset) │    │  Kafka → Spark Streaming  │
 │  + Apache Iceberg       │    │  → ClickHouse (speed_agg) │
 │                         │    │                           │
 │  Spark Batch → dbt      │    │  Near-realtime, approx.   │
 │  → ClickHouse           │    │  delta from last batch    │
 │    (batch_agg)          │    │                           │
 └──────────────┬──────────┘    └───────────┬───────────────┘
                │                           │
 ┌──────────────▼───────────────────────────▼───────────────┐
 │                    SERVING LAYER                          │
 │                                                           │
 │  FastAPI Query Merger: Batch View + Speed View → Result   │
 │  ClickHouse (OLAP) + Redis (Hot Cache)                    │
 │  Streamlit Dashboard                                      │
 └───────────────────────────────────────────────────────────┘
```

---

## 4. Stack Công nghệ Tổng hợp

| Phân hệ | Công nghệ | Ghi chú |
|:---|:---|:---|
| Ingestion (Stream) | Python Collector + Kafka KRaft | Real-time event bus |
| Batch Processing | Apache Spark (PySpark) | Core compute engine |
| Stream Processing | Spark Structured Streaming | Speed Layer jobs |
| Storage | MinIO (S3-compatible) | Object store |
| Table Format | Apache Iceberg | ACID, Time Travel |
| Catalog | Iceberg REST Catalog | Schema registry |
| Batch Views & Speed Views | ClickHouse | OLAP engine |
| Hot Cache | Redis | Fast lookups |
| Serving API | FastAPI | Query Merger + REST |
| Orchestration | Dagster | Asset lineage, schedules |
| Dashboard | Streamlit | BI visualization |
| Containers | Docker Compose | Dev environment |
