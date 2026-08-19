# dbt Project

Thư mục chứa dbt transformation models cho mô hình Medallion (Bronze → Silver → Gold).

| Thư mục | Mô tả |
|:---|:---|
| `models/bronze/` | Raw ingestion models — dữ liệu thô từ Master Dataset |
| `models/silver/` | Cleaned & deduplicated models — đã qua Data Quality Gates |
| `models/gold/` | Business aggregations — Fact/Dim tables, KPIs |
