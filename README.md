# Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa
## (Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring)

[![Course](https://img.shields.io/badge/Course-TLCN-blue)](https://github.com/)
[![Architecture](https://img.shields.io/badge/Architecture-Lambda-green)](https://github.com/)
[![Status](https://img.shields.io/badge/Status-End--to--End%20Prototype-green)](https://github.com/)

Thiết kế và triển khai hệ thống **Data Lakehouse** theo mô hình **Lambda Architecture** (Batch Layer + Speed Layer + Serving Layer) với cơ chế tự động đối soát dựa trên Batch Watermark cho thị trường tiền mã hóa (BTCUSDT Binance), sử dụng **Apache Iceberg** làm Open Table Format và **Docker Compose** để đóng gói toàn bộ hạ tầng.

Đề tài Tiểu luận Chuyên ngành (TLCN) — Khoa Công Nghệ Thông Tin · Đại học Sư phạm Kỹ thuật TP.HCM (HCMUTE).
Niên khóa: Học kỳ 1 — Năm học 2026-2027.

---

> [!IMPORTANT]
> **Developer Quick Start:** Đọc **[AGENTS.md](AGENTS.md)** trước khi bắt đầu code.
> Đọc **[development.md](development.md)** để nắm quy trình phát triển, Git workflow, và coding standards.

## Table of Contents

- [Project Overview](#project-overview)
- [Research Questions](#research-questions)
- [System Architecture](#system-architecture)
- [Benchmark Scope](#benchmark-scope)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Installation & Environment Setup](#installation--environment-setup)
- [Quick Start](#quick-start)
- [Contributors](#contributors)
- [References](#references)

---

## Project Overview

Trong thời đại Big Data, các doanh nghiệp phải đối mặt đồng thời với hai nhu cầu xung đột:
- **Phân tích lịch sử (Batch Analytics):** Cần xử lý hàng tỷ bản ghi với độ chính xác tuyệt đối, chấp nhận độ trễ vài giờ/ngày.
- **Phản hồi thời gian thực (Real-time):** Cần dữ liệu cập nhật trong vài giây, chấp nhận kết quả gần đúng.

Đề tài giải quyết 4 khoảng trống nghiên cứu chính:

| Vấn đề | Kiến trúc truyền thống | Giải pháp đề xuất |
|:---|:---|:---|
| **Small File Problem** | Streaming tạo hàng nghìn tệp nhỏ | Iceberg Compaction (OPTIMIZE) |
| **Thiếu ACID Transactions** | Ghi đồng thời gây bất nhất dữ liệu | Apache Iceberg ACID |
| **Thiếu Data Quality Gates** | Dữ liệu lỗi lọt vào tầng phân tích | DQ constraints + Quarantine Tables |
| **Batch vs Stream không tích hợp** | Hai hệ thống riêng biệt | Lambda Architecture + Query Merger |

---

## Research Questions

1. **RQ1:** Liệu Lambda Architecture có giúp giảm đáng kể độ trễ truy vấn so với kiến trúc Batch-only truyền thống?
2. **RQ2:** Độ chính xác của Speed View (gần đúng) sai lệch bao nhiêu so với Batch View (chính xác) sau khi Batch Layer hoàn tất tái xử lý?
3. **RQ3:** Apache Iceberg Compaction (OPTIMIZE) cải thiện hiệu năng đọc bao nhiêu phần trăm sau khi streaming tạo hàng nghìn tệp nhỏ?

---

## System Architecture

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
 │  Spark/Python Batch     │    │  → ClickHouse speed_agg   │
 │  → ClickHouse batch_agg │    │  + Redis latest hot cache │
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

- **Batch Layer:** MinIO + Apache Iceberg Bronze (immutable master dataset) → Spark/Python DQ + dedup + OHLCV → ClickHouse `batch_agg` (reconciled Batch View).
- **Speed Layer:** Kafka → Spark Structured Streaming → ClickHouse `speed_agg` (provisional range view) + Redis (latest-candle hot cache).
- **Serving Layer:** FastAPI Query Merger routes by watermark, reads the newest ClickHouse revision, and uses Redis only for the latest-candle endpoint.
- **dbt:** SQL models are retained as experimental/reference artefacts; the Docker runtime path is `spark_batch_jobs.py`.

---

## Benchmark Scope

### Benchmark 1 — Query Latency Comparison
So sánh độ trễ truy vấn Batch-only, Speed-only và Lambda Merge trên ClickHouse views/FastAPI. Redis được đo riêng như hot cache nếu mở rộng benchmark.

### Benchmark 2 — Batch Reprocessing Correctness
Thay đổi logic nghiệp vụ, chạy lại Batch Layer, đo sai số (%) giữa Speed View và Batch View.

### Benchmark 3 — Small File Compaction Efficiency
Tạo append nhỏ có kiểm soát vào Iceberg Bronze, chạy bin-packing compaction và đo cải thiện hiệu năng đọc. Chỉ row `mode=real` được dùng cho kết luận.

---

## Project Structure

```
lakehouse-lambda-benchmark/
│
├── AGENTS.md                    # Machine-readable spec cho AI agents & coding standards
├── README.md                    # Tài liệu dự án chính (file này)
├── development.md               # Hướng dẫn phát triển, Git workflow, coding conventions
├── docker-compose.yml           # Docker Compose: Kafka, Spark, MinIO, ClickHouse, Redis, Dagster
├── .gitignore                   # Git ignore rules
├── .dockerignore                # Docker ignore rules
├── .env.example                 # Template biến môi trường
│
├── configs/                     # Cấu hình dịch vụ & pipeline
│   ├── README.md
│   ├── kafka/                   # Kafka broker & topic configs
│   ├── spark/                   # Spark cluster & session configs
│   ├── iceberg/                 # Iceberg catalog & table configs
│   ├── clickhouse/              # ClickHouse server & schema configs
│   ├── redis/                   # Redis configs
│   └── dagster/                 # Dagster workspace & repository configs
│
├── src/                         # Source code chính
│   ├── __init__.py
│   ├── ingestion/               # Data ingestion layer
│   │   ├── __init__.py
│   │   ├── kafka_producer.py    # Kafka producer: phát dữ liệu streaming
│   │   ├── batch_loader.py      # Batch loader: nạp CSV/Parquet vào Bronze
│   │   └── README.md
│   ├── batch_layer/             # Batch Layer processing
│   │   ├── __init__.py
│   │   ├── spark_batch_jobs.py  # Spark Batch: Bronze → Silver → Gold
│   │   ├── iceberg_utils.py     # Iceberg table operations (create, compact, time-travel)
│   │   ├── clickhouse_sync.py   # Sync Gold data → ClickHouse Batch Views
│   │   └── README.md
│   ├── speed_layer/             # Speed Layer processing
│   │   ├── __init__.py
│   │   ├── spark_streaming.py   # Spark Structured Streaming → Redis
│   │   ├── redis_writer.py      # Write Speed Views to Redis
│   │   └── README.md
│   ├── serving_layer/           # Serving Layer (Query Merger + API)
│   │   ├── __init__.py
│   │   ├── query_merger.py      # FastAPI Query Merger: Batch + Speed → Result
│   │   ├── api_routes.py        # REST API endpoints
│   │   └── README.md
│   ├── data_quality/            # Data Quality Gates
│   │   ├── __init__.py
│   │   ├── dq_checks.py         # Constraint checks, quarantine logic
│   │   ├── dq_metrics.py        # sys_dq_metrics generation
│   │   └── README.md
│   └── utils/                   # Shared utilities
│       ├── __init__.py
│       ├── config_loader.py     # Load YAML/ENV configs
│       ├── logging_utils.py     # Centralized logging
│       └── README.md
│
├── dbt_project/                 # dbt transformation models
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── bronze/              # Raw ingestion models
│   │   ├── silver/              # Cleaned & deduplicated models
│   │   └── gold/                # Business aggregations & Fact/Dim tables
│   └── README.md
│
├── dagster_project/             # Dagster orchestration
│   ├── workspace.yaml
│   ├── repository.py            # Asset definitions & schedules
│   └── README.md
│
├── scripts/                     # Helper scripts & benchmark runners
│   ├── __init__.py
│   ├── README.md
│   ├── setup_infra.py           # Kiểm tra & khởi tạo hạ tầng (MinIO buckets, Kafka topics, etc.)
│   ├── seed_data.py             # Nạp dữ liệu mẫu ban đầu
│   └── benchmarks/              # Benchmark execution scripts
│       ├── __init__.py
│       ├── bench_latency.py     # Benchmark 1: Query Latency Comparison
│       ├── bench_reprocess.py   # Benchmark 2: Batch Reprocessing Correctness
│       ├── bench_compaction.py  # Benchmark 3: Small File Compaction Efficiency
│       └── run_all_benchmarks.py # Chạy toàn bộ 3 kịch bản benchmark
│
├── results/                     # Kết quả benchmark & visualization
│   ├── README.md
│   ├── plots/                   # Biểu đồ kết quả (latency, compaction, etc.)
│   └── logs/                    # Raw benchmark logs (CSV)
│
├── datasets/                    # Dataset configs & sample data
│   ├── README.md
│   └── sample/                  # Dữ liệu mẫu nhỏ để smoke test
│
├── docs/                        # Tài liệu dự án
│   ├── README.md
│   ├── proposal.pdf             # Bản Proposal TLCN đã duyệt
│   ├── caitan_lambda_architecture.md
│   ├── proposal_tlcn_lambda.md
│   ├── sys-arch.md              # System Architecture chi tiết
│   ├── plans/                   # Kế hoạch sprint & task tracking
│   └── process/                 # Tài liệu tiến trình thực hiện
│
├── tests/                       # Unit tests & integration tests
│   ├── __init__.py
│   ├── test_ingestion.py
│   ├── test_batch_layer.py
│   ├── test_speed_layer.py
│   ├── test_serving_layer.py
│   └── test_data_quality.py
│
├── dashboard/                   # Dashboard UI (Streamlit)
│   ├── app.py                   # Streamlit main app
│   └── README.md
│
└── references/                  # Tài liệu tham khảo & papers
    ├── README.md
    └── references.bib           # BibTeX references
```

---

## Tech Stack

| Phân hệ | Công nghệ | Ghi chú |
|:---|:---|:---|
| Ingestion (Stream) | Apache Kafka + Binance replay | Real-time event bus |
| Ingestion (Batch) | Binance REST + mock generator | Lịch sử mẫu và controlled faults |
| Batch Processing | Apache Spark (PySpark) | Core compute engine |
| Stream Processing | Spark Structured Streaming | Speed Layer jobs |
| Transformation | Spark/Python | DQ, deduplication và OHLCV Gold runtime |
| dbt | dbt-spark models | Artefact tham khảo, không nằm trên đường demo runtime |
| Storage | MinIO (S3-compatible) | Object store |
| Table Format | Apache Iceberg | ACID, Time Travel |
| Catalog | Iceberg REST Catalog | Schema registry |
| Batch Views | ClickHouse | OLAP engine |
| Speed Views | ClickHouse `speed_agg` + Redis | Range query provisional + latest hot cache |
| Serving API | FastAPI | Query Merger + REST |
| Orchestration | Dagster | Asset lineage, schedules |
| Dashboard | Streamlit / Metabase | BI visualization |
| Containers | Docker Compose | Dev environment |

---

## Installation & Environment Setup

### 1. Clone the Repository
```bash
git clone <repo-url>
cd lakehouse-lambda-benchmark
```

### 2. Set Up Environment Variables
```bash
cp .env.example .env
# Chỉnh sửa .env với các giá trị phù hợp
```

### 3. Start Infrastructure (Docker Compose)
```bash
docker compose up -d
```

Lệnh trên sẽ khởi chạy toàn bộ: Kafka, Spark, MinIO, ClickHouse, Redis, Dagster.

### 4. Verify Services
```bash
docker compose ps
# Kiểm tra tất cả services đang running
```

---

## Quick Start

### 1. Run One-command Local Pipeline
```bash
python scripts/run_lambda_pipeline.py --top-n 5 --mock-days 1 --replay-limit 5000
```

Lệnh trên sẽ bật các Docker services cần thiết, lấy dữ liệu Binance REST mới nhất, sinh mock data, replay vào Kafka, chạy Batch Layer, để Speed Layer xử lý streaming và smoke check FastAPI.

### 2. Full Smoke Test
```bash
python scripts/e2e_full_pipeline.py --top-n 2 --mock-days 1 --replay-limit 1000
```

### 3. Seed Sample Data Manually
```bash
python scripts/seed_data.py --sample
```

### 4. Run Batch Pipeline Manually
```bash
# Khuyến nghị chạy qua Docker để ghi Iceberg thật
docker compose run --rm batch-pipeline

# Hoặc chạy trực tiếp trên host nếu chỉ cần ClickHouse sync
python src/batch_layer/spark_batch_jobs.py --mode full
```

### 5. Run Speed Layer
```bash
docker compose up -d speed-layer
docker compose logs -f speed-layer
```

### 6. Start Serving API Manually
```bash
uvicorn src.serving_layer.api_routes:app --host 0.0.0.0 --port 8000
```

### 7. Run Benchmarks
```bash
# Chạy toàn bộ 3 kịch bản benchmark
python scripts/benchmarks/run_all_benchmarks.py

# Hoặc chạy từng benchmark riêng lẻ
python scripts/benchmarks/bench_latency.py
python scripts/benchmarks/bench_reprocess.py
python scripts/benchmarks/bench_compaction.py
```

---

## Contributors

| Họ và Tên | MSSV | Vai trò |
|:---|:---|:---|
| **Nguyễn Đặng Quốc Anh** | `23133004` | Sinh viên thực hiện (Ingestion, Speed Layer, Serving Query Merger) |
| **Phạm Minh Quân** | `23133060` | Sinh viên thực hiện (Apache Iceberg, Batch Layer, Data Quality) |
| **ThS. Đoàn Minh Trí** | — | Giảng viên hướng dẫn |

---

## References

1. Marz, N. & Warren, J. (2015). *Big Data: Principles and Best Practices of Scalable Real-Time Data Systems*. Manning Publications.
2. Armbrust, M. et al. (2021). *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR 2021.
3. Apache Iceberg Documentation. https://iceberg.apache.org/docs/latest/
4. Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit*, 3rd Edition. Wiley.
5. Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly.

---

> **Trạng thái:** End-to-end local implementation đã được kiểm chứng. Benchmark real và biểu đồ mới nhất nằm trong `results/logs/` và `results/plots/`; giới hạn thực nghiệm cùng checklist còn lại nằm ở `docs/md/s/ke_hoach_hoan_thanh_100_phan_tram.md`.
