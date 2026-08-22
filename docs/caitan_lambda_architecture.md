# Cải Tiến Tiểu Luận: Data Lakehouse Nâng Cấp với Lambda Architecture
## (Phiên bản chỉnh sửa — Hướng tới TLCN)

---

## 1. Lý do Chuyển sang Lambda Architecture

### 1.1. Hạn chế của kiến trúc cũ (Pure Medallion / Kappa-Style)
| Hạn chế | Mô tả |
|:---|:---|
| **Không phân tách rõ luồng Batch / Stream** | Toàn bộ xử lý dồn vào Spark Structured Streaming, gây áp lực tài nguyên và khó tối ưu từng luồng độc lập |
| **Không có tầng Historical Reprocessing** | Khi thay đổi logic nghiệp vụ, không có cơ chế tái xử lý dữ liệu lịch sử quy mô lớn một cách có hệ thống |
| **Serving Layer thiếu tính thống nhất** | Batch views và real-time views nằm rải rác ở nhiều nơi, không có Query Merger tập trung |
| **Khó kiểm soát Data Lineage cho hai luồng** | Dagster Assets khó mô hình hóa quan hệ phụ thuộc giữa Batch và Speed Layer |

### 1.2. Tại sao Lambda Architecture phù hợp hơn?
Lambda Architecture giải quyết triệt để 3 bài toán cốt lõi:
- **Tính chính xác (Accuracy):** Batch Layer xử lý toàn bộ lịch sử, luôn cho kết quả đúng.
- **Tính kịp thời (Low Latency):** Speed Layer phục vụ dữ liệu gần thực (near-realtime) trong khi Batch chưa xong.
- **Khả năng tái xử lý (Reprocessability):** Khi logic thay đổi, chỉ cần chạy lại Batch Layer trên Master Dataset.

---

## 2. Kiến trúc Hệ thống Chi Tiết (Lambda Architecture + Data Lakehouse)

### Layer 1: Batch Layer
- Master Dataset Store: MinIO + Apache Iceberg (Bronze Tables).
- Batch Processor: Apache Spark Batch + dbt models.
- Output: Batch Views trong ClickHouse (`batch_agg`).

### Layer 2: Speed Layer
- Message Broker: Apache Kafka KRaft.
- Stream Processor: Spark Structured Streaming (Tumbling Window + Watermarking).
- Output: Speed Views trong ClickHouse (`speed_agg`) / Redis.

### Layer 3: Serving Layer
- Query Merger: FastAPI Gateway (Tự động hợp nhất dựa trên Batch Watermark).
- UI Dashboard: Streamlit.
