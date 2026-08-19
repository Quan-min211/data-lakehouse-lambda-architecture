# Batch Layer

Thư mục chứa source code cho Batch Layer trong Lambda Architecture.

| File | Mô tả |
|:---|:---|
| `spark_batch_jobs.py` | Spark Batch jobs — xử lý Bronze → Silver → Gold |
| `iceberg_utils.py` | Iceberg table operations (create, compact, time-travel) |
| `clickhouse_sync.py` | Đồng bộ dữ liệu Gold vào ClickHouse (Batch Views) |
