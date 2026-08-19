# Speed Layer

Thư mục chứa source code cho Speed Layer trong Lambda Architecture.

| File | Mô tả |
|:---|:---|
| `spark_streaming.py` | Spark Structured Streaming — đọc từ Kafka, tính toán incremental |
| `redis_writer.py` | Ghi Speed Views vào Redis (key-value aggregations, TTL) |
