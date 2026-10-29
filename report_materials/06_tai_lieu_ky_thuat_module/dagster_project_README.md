#  orchestration | Dagster Project

Thư mục điều phối quy trình xử lý dữ liệu lô (Batch Layer Orchestration) sử dụng **Dagster** theo mô hình **Software-Defined Assets (SDA)** cho kiến trúc Data Lakehouse Lambda.

---

## 🏗 Kiến Trúc Pipeline & Asset Lineage DAG

Pipeline mô hình hóa toàn bộ vòng đời dữ liệu theo chuẩn Medallion Architecture (Bronze -> Silver -> Gold) và đồng bộ sang Tầng Phục vụ (Serving Layer):

```text
bronze_crypto_trades (Bronze)
       │
       ▼
silver_cleaned_trades (Silver - DQ Gate)
       │
       ▼
gold_market_aggregates (Gold - Ground Truth OHLCV)
       │
       ▼
clickhouse_batch_sync (Nạp nến vào ClickHouse batch_agg)
       │
       ▼
system_watermark_sync (Cập nhật lakehouse.system_watermark)

[Nhánh bảo trì độc lập]
iceberg_small_files_compaction (Tối ưu hóa Small Files trên MinIO)
```

---

## 📦 Danh Sách Software-Defined Assets

| Asset Key | Tầng (Layer) | Mô tả & Chức năng | Metadata chính |
| :--- | :--- | :--- | :--- |
| `bronze_crypto_trades` | **Bronze** | Ghi nhận dữ liệu giao dịch thô (TradeEvent) vào Apache Iceberg Bronze Table | Partition: `days(trade_time)`, Snapshot Isolation |
| `silver_cleaned_trades` | **Silver** | Tích hợp **Data Quality Gate**: Khử trùng lặp `(trade_id, symbol)`, lọc lỗi schema, cách ly vào Quarantine | `valid_records`, `rejected_quarantined`, `quality_pass_rate_pct` |
| `gold_market_aggregates`| **Gold** | Tổng hợp nến Tumbling Window 1m/5m/1h (OHLCV, VWAP) chuẩn mực (**Ground Truth**) | `total_candles`, `status: Reconciled` |
| `clickhouse_batch_sync` | **Serving** | Đồng bộ nến Gold trực tiếp vào ClickHouse OLAP table `lakehouse.batch_agg` | `rows_inserted`, latency `< 15ms` |
| `system_watermark_sync` | **Serving** | Chốt mốc `watermark_time` vào ClickHouse để **Query Merger** phân luồng Case 1/2/3 | `new_watermark_utc`, `reconciliation_boundary` |
| `iceberg_small_files_compaction` | **Maintenance** | Tối ưu hóa Small Files (Bin-Packing Compaction) trên MinIO S3 | `files_before`, `files_after`, `file_reduction_pct` |

---

## ⏱ Jobs & Lập Lịch Tự Động (Schedules)

* **`batch_lakehouse_pipeline_job`**: Điều phối 5 Assets từ Bronze -> Silver -> Gold -> ClickHouse -> Watermark.
  * **Lịch trình (`batch_pipeline_schedule`)**: Chạy tự động **mỗi 15 phút** (`*/15 * * * *`).
* **`iceberg_compaction_job`**: Tác vụ bảo trì gộp các file Parquet nhỏ thành file tối ưu (~128MB).
  * **Lịch trình (`compaction_schedule`)**: Chạy tự động **mỗi 2 giờ** (`0 */2 * * *`).

---

## 🚀 Hướng Dẫn Vận Hành

### 1. Khởi động Dagster Webserver & Daemon:
```powershell
python -m dagster dev -f dagster_project/repository.py
```
Truy cập UI tại: **`http://localhost:3000`**

### 2. Chạy Unit Tests:
```powershell
python -m unittest tests/test_dagster_pipeline.py -v
```
