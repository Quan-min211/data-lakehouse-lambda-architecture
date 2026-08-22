# Cải Tiến Tiểu Luận: Data Lakehouse Nâng Cấp với Lambda Architecture
## (Phiên bản chỉnh sửa — Hướng tới TLCN)

---

## 1. Lý do Chuyển sang Lambda Architecture

### 1.1. Hạn chế của kiến trúc cũ (Pure Medallion / Kappa-Style)

Bản đề cương trước đây xây dựng pipeline theo mô hình **Medallion thuần** (Bronze → Silver → Gold) kết hợp Kafka Streaming đơn luồng. Sau khi đánh giá, mô hình này bộc lộ các hạn chế sau:

| Hạn chế | Mô tả |
|:---|:---|
| **Không phân tách rõ luồng Batch / Stream** | Toàn bộ xử lý dồn vào Spark Structured Streaming, gây áp lực tài nguyên và khó tối ưu từng luồng độc lập |
| **Không có tầng Historical Reprocessing** | Khi thay đổi logic nghiệp vụ, không có cơ chế tái xử lý dữ liệu lịch sử quy mô lớn một cách có hệ thống |
| **Serving Layer thiếu tính thống nhất** | Batch views và real-time views nằm rải rác ở nhiều nơi, không có Query Merger tập trung |
| **Khó kiểm soát Data Lineage cho hai luồng** | Dagster Assets khó mô hình hóa quan hệ phụ thuộc giữa Batch và Speed Layer |

### 1.2. Tại sao Lambda Architecture phù hợp hơn?

**Lambda Architecture** do Nathan Marz đề xuất là mô hình kiến trúc dữ liệu phân tách rõ ràng thành **3 lớp độc lập**:

```
                  ┌──────────────────────────────────────────────────┐
                  │                  DATA SOURCES                    │
                  │   (Kafka Topics / CDC / File Upload / API Events) │
                  └──────────────┬──────────────────┬───────────────┘
                                 │                  │
                      ┌──────────▼──────────┐  ┌───▼─────────────────┐
                      │   BATCH LAYER       │  │   SPEED LAYER        │
                      │  (Immutable Master) │  │  (Real-time approx.) │
                      │  Apache Spark       │  │  Kafka + Flink/Spark │
                      │  + Apache Iceberg   │  │  Structured Streaming│
                      │  (Historical Store) │  │  (Hot Store: Redis / │
                      │                     │  │   ClickHouse Stream) │
                      └──────────┬──────────┘  └──────┬──────────────┘
                                 │                    │
                      ┌──────────▼────────────────────▼──────────────┐
                      │              SERVING LAYER                    │
                      │  ClickHouse (Batch Views) + Redis (Speed Views│
                      │  → Query Merger API (FastAPI)                 │
                      │  → Metabase / Streamlit Dashboard             │
                      └───────────────────────────────────────────────┘
```

Lambda Architecture giải quyết triệt để 3 bài toán cốt lõi:
- **Tính chính xác (Accuracy):** Batch Layer xử lý toàn bộ lịch sử, luôn cho kết quả đúng.
- **Tính kịp thời (Low Latency):** Speed Layer phục vụ dữ liệu gần thực (near-realtime) trong khi Batch chưa xong.
- **Khả năng tái xử lý (Reprocessability):** Khi logic thay đổi, chỉ cần chạy lại Batch Layer trên Master Dataset.

---

## 2. Kiến trúc Hệ thống Chi Tiết (Lambda Architecture + Data Lakehouse)

### 2.1. Tổng quan 3 Layer

#### Layer 1: Batch Layer (Tầng Xử lý Lô)

**Vai trò:** Xử lý toàn bộ dataset lịch sử, sản sinh ra **Batch Views** chính xác, tin cậy.

| Thành phần | Công nghệ | Chức năng |
|:---|:---|:---|
| Master Dataset Store | MinIO + Apache Iceberg | Lưu raw data bất biến (Bronze Iceberg Tables) |
| Batch Processor | Apache Spark (Batch Mode) | Tính toán Batch Views định kỳ (daily/hourly) |
| Transformation | dbt + Spark Executor | Làm sạch Silver → Tính toán Gold Aggregations |
| Batch Views Storage | ClickHouse (Cold Tables) | Lưu kết quả batch đã tổng hợp, sẵn sàng query |
| Orchestration | Dagster (Scheduled Jobs) | Lên lịch và điều phối toàn bộ Batch Pipeline |

**Đặc tính thiết kế:**
- Raw data trong MinIO/Iceberg là **immutable** — không bao giờ bị xóa hay sửa đổi.
- Batch Views được **ghi đè hoàn toàn (overwrite)** sau mỗi lần chạy, đảm bảo consistency.
- Hỗ trợ **Iceberg Time Travel** để truy vấn dữ liệu tại bất kỳ thời điểm nào trong quá khứ.

#### Layer 2: Speed Layer (Tầng Xử lý Nhanh)

**Vai trò:** Bù đắp độ trễ của Batch Layer bằng cách xử lý **dữ liệu mới nhất (delta)** gần thực.

| Thành phần | Công nghệ | Chức năng |
|:---|:---|:---|
| Message Broker | Apache Kafka | Nhận event stream từ nguồn |
| Stream Processor | Spark Structured Streaming / Apache Flink | Tính toán incremental aggregations |
| Speed Views Storage | Redis (Sorted Sets/Hashes) | Lưu kết quả near-realtime |
| State Management | Kafka Compacted Topics / Flink RocksDB | Quản lý trạng thái stream |

**Đặc tính thiết kế:**
- Speed Layer chỉ xử lý dữ liệu **từ thời điểm batch cuối cùng đến hiện tại** (delta window).
- Kết quả Speed Layer là **gần đúng (approximate)** — sẽ bị thay thế bởi Batch Views sau khi batch cycle hoàn tất.
- **Không cần đảm bảo exactly-once** ở Speed Layer (at-least-once là đủ), giúp giảm chi phí tính toán.

#### Layer 3: Serving Layer (Tầng Phục vụ)

**Vai trò:** Hợp nhất Batch Views và Speed Views, cung cấp giao diện query thống nhất.

| Thành phần | Công nghệ | Chức năng |
|:---|:---|:---|
| Query Merger | FastAPI + Python | Merge kết quả Batch (ClickHouse) + Speed (Redis) |
| OLAP Engine | ClickHouse | Phục vụ ad-hoc query trên Batch Views |
| Cache / Hot Store | Redis | Phục vụ Speed Views độ trễ thấp (<10ms) |
| BI Dashboard | Metabase / Streamlit | Giao diện phân tích và visualization |

**Chiến lược Query Merger (Pseudo-code):**
```python
def query_total_revenue(start_time, end_time):
    batch_cutoff = get_last_batch_timestamp()

    # Batch View: Dữ liệu đã được tổng hợp chính xác (lịch sử)
    batch_result = clickhouse.query(
        f"SELECT SUM(revenue) FROM gold_revenue WHERE ts < '{batch_cutoff}'"
    )

    # Speed View: Delta dữ liệu từ batch cutoff đến hiện tại
    speed_result = redis.get(f"speed:revenue:{batch_cutoff}:{end_time}")

    # Merge: Batch (accurate) + Speed (approximate) = Near-realtime answer
    return batch_result + speed_result
```

---

### 2.2. Tích hợp Mô hình Medallion vào Lambda

Mô hình Medallion vẫn được giữ lại, nhưng được **tích hợp vào Batch Layer** như sau:

| Tầng Medallion | Vai trò trong Lambda | Công nghệ |
|:---|:---|:---|
| **Bronze (Raw)** | Master Dataset — bất biến, nguồn sự thật duy nhất | MinIO + Iceberg |
| **Silver (Cleaned)** | Output của Batch Processor — đã làm sạch, deduplicate | Spark + dbt + Iceberg |
| **Gold (Business)** | Batch Views — aggregations, Fact/Dim tables | ClickHouse + Iceberg |
| **Speed (Delta)** | Không theo Medallion — chỉ có Speed Views ngắn hạn | Redis |

### Cơ chế Data Quality Gate trong Lambda

- **Bronze → Silver (Batch):** Kiểm tra đầy đủ ràng buộc, tiêm lỗi vào `quarantine_table`, sinh `sys_dq_metrics`.
- **Speed Layer:** Chỉ kiểm tra nhẹ (schema validation, null check) — ưu tiên tốc độ hơn độ chính xác.
- **Serving Layer:** Nếu Speed View lỗi, fallback về Batch View gần nhất (graceful degradation).

---

## 3. So Sánh: Kiến trúc Cũ vs. Lambda Architecture

| Tiêu chí | Medallion Thuần (Cũ) | Lambda Architecture (Mới) |
|:---|:---|:---|
| **Phân tách Batch/Stream** | Không — chung một pipeline | ✅ Rõ ràng: 2 pipeline độc lập |
| **Độ trễ phục vụ** | Phụ thuộc Spark Streaming | ✅ <10ms (Speed/Redis) + Batch chính xác |
| **Khả năng tái xử lý** | Thủ công, phức tạp | ✅ Chỉ cần chạy lại Batch Layer |
| **Tính nhất quán** | Eventual Consistency | ✅ Strong (Batch) + Approximate (Speed) |
| **Độ phức tạp hệ thống** | Thấp - Trung bình | Trung bình - Cao (2 pipeline) |
| **Phù hợp đồ án TLCN** | Khó thể hiện kỹ năng phân tích sâu | ✅ Nhiều chiều benchmark, so sánh rõ ràng |

---

## 4. Ba Kịch bản Thực nghiệm Nâng cấp

### Benchmark 1 — Latency Comparison (Serving Layer)
**Mục tiêu:** So sánh độ trễ truy vấn giữa các chiến lược serving.

| Chiến lược | Phương thức | Metric đo |
|:---|:---|:---|
| Batch-only | ClickHouse Cold Query | Query Time (ms) |
| Speed-only | Redis Lookup | Query Time (ms) |
| Lambda Merge | FastAPI Merge (Batch + Speed) | Query Time (ms), Data Freshness (s) |

### Benchmark 2 — Batch Reprocessing Correctness
**Mục tiêu:** Thay đổi logic nghiệp vụ, chạy lại Batch Layer và kiểm chứng kết quả đúng so với Speed Views.

- **Metric:** Sai số (%) giữa Speed View và Batch View sau khi Batch hoàn tất.
- **Kỳ vọng:** Batch View luôn overwrite và chính xác hơn Speed View.

### Benchmark 3 — Small File Compaction & Iceberg Optimization
**Mục tiêu:** Giả lập nạp streaming 2 giờ → hàng nghìn tệp nhỏ → chạy Iceberg `OPTIMIZE` → đo cải thiện.

- **Metric:** Số tệp trước/sau compaction, Query scan time, Metadata overhead.

---

## 5. Hạn chế & Hướng Khắc phục

| Hạn chế của Lambda | Hướng khắc phục trong đồ án |
|:---|:---|
| **Logic xử lý phải viết 2 lần** (batch + stream) | Sử dụng dbt models cho Batch, chia sẻ transformation logic qua Python modules |
| **Khó đồng bộ schema** giữa 2 pipeline | Dùng Iceberg Schema Evolution + Kafka Schema Registry (Avro) |
| **Phức tạp vận hành** | Docker Compose tổng thể, Dagster monitoring tập trung |
| **Kappa Architecture là lựa chọn thay thế** | Đồ án nên đề cập so sánh Lambda vs Kappa trong Chương 2 |

---

## 6. Cập nhật Stack Công nghệ

| Phân hệ | Công nghệ |
|:---|:---|
| Ingestion | Apache Kafka, Debezium CDC, Airbyte |
| Batch Processing | Apache Spark (Batch), dbt, Polars |
| Stream Processing | Spark Structured Streaming / Apache Flink |
| Storage | MinIO (S3-compatible), Apache Iceberg |
| Catalog | Iceberg REST Catalog / Apache Polaris |
| Batch Views | ClickHouse OLAP Engine |
| Speed Views | Redis (Sorted Sets, Hashes) |
| Serving API | FastAPI (Query Merger + REST Gateway) |
| Orchestration | Dagster (Software-Defined Assets) |
| Dashboard | Metabase / Streamlit |
| Containerization | Docker Compose (Dev) |

---

> **Ghi chú:** Tài liệu này là bản chỉnh sửa của "Cải Tiến Tiểu Luận Data Lakehouse" gốc,
> cập nhật kiến trúc sang Lambda Architecture. Nội dung phần Dataset và thực nghiệm chi tiết
> sẽ được bổ sung sau khi thống nhất lựa chọn nguồn dữ liệu.
