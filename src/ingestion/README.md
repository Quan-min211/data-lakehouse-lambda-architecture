# Ingestion Layer

Thư mục chứa source code cho tầng nạp dữ liệu (Ingestion Layer).

| File | Mô tả |
|:---|:---|
| `kafka_producer.py` | Kafka producer — phát dữ liệu streaming vào Kafka topics |
| `batch_loader.py` | Batch loader — nạp CSV/Parquet files vào Bronze (MinIO/Iceberg) |
