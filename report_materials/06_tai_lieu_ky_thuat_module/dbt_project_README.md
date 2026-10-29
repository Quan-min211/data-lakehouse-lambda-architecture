# dbt Project

Thư mục chứa dbt transformation models tham khảo cho mô hình Medallion (Bronze → Silver → Gold).

> Runtime chính của đồ án là `src/batch_layer/spark_batch_jobs.py`: Spark/Python thực hiện DQ, deduplication, OHLCV và đồng bộ ClickHouse. dbt không được gọi bởi Docker demo hoặc Dagster job hiện tại; các model ở đây được giữ để đối chiếu/thử nghiệm SQL, tránh có hai source of truth cho transform production-local.

| Thư mục | Mô tả |
|:---|:---|
| `models/bronze/` | Raw ingestion models — dữ liệu thô từ Master Dataset |
| `models/silver/` | Cleaned & deduplicated models — đã qua Data Quality Gates |
| `models/gold/` | Business aggregations — Fact/Dim tables, KPIs |
