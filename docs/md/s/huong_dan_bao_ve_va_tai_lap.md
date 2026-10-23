# Hướng Dẫn Demo, Tái Lập Và Bảo Vệ Đồ Án

Ngày cập nhật: 2026-10-23

## Kiến trúc chốt để trình bày

```text
Binance REST -> mock có kiểm soát -> Kafka
                                  |-> Spark Streaming -> ClickHouse speed_agg -> Redis latest cache
                                  |-> Spark/Python Batch -> Iceberg Bronze -> ClickHouse batch_agg -> watermark

FastAPI Query Merger: watermark + batch_agg + speed_agg
Streamlit: market view, DQ quarantine, system readiness
```

- Iceberg Bronze là immutable master dataset.
- `batch_agg` là Ground Truth đã DQ/deduplicate/reprocess.
- `speed_agg` là provisional range view.
- Redis chỉ là hot cache cho candle mới nhất, không phải source cho range query.
- Nếu hai view có cùng `(symbol, window_start)`, Batch được ưu tiên.
- `spark_batch_jobs.py` là transform runtime chính; dbt chỉ là artefact SQL tham khảo.

## Kịch bản demo 8 phút

1. Khởi động hạ tầng và kiểm tra container.

```powershell
docker compose up -d --build
docker compose ps
```

2. Chạy một luồng cô lập hoàn chỉnh. Lệnh này lấy mẫu mới từ Binance, sinh mock theo mẫu vừa lấy, replay Kafka, chạy batch Iceberg/ClickHouse và smoke-check Serving.

```powershell
python scripts/run_lambda_pipeline.py --top-n 1 --klines-limit 100 --aggtrades-limit 50 --mock-days 1 --ticks-per-min 1 --fault-mode light --replay-limit 300 --smoke-symbol BTCUSDT
```

3. Trình bày ba trạng thái query qua API/Dashboard:

```powershell
Invoke-RestMethod http://localhost:8000/api/system/status
Invoke-RestMethod "http://localhost:8000/api/speed/latest?symbol=BTCUSDT"
Start-Process http://localhost:8501
```

Giải thích `Reconciled`, `Provisional`, `Partially Reconciled`, watermark và DQ quarantine. Dashboard phải cho thấy Redis cache/latest candle và System Readiness.

4. Chạy full E2E độc lập. Script fail với exit code khác 0 nếu batch/speed/watermark/Redis/API không tiến triển.

```powershell
python scripts/e2e_full_pipeline.py --top-n 1 --klines-limit 100 --aggtrades-limit 50 --mock-days 1 --ticks-per-min 1 --replay-limit 300 --symbol BTCUSDT
```

5. Mở `results/plots/benchmark_summary_dashboard.png`; chỉ dùng CSV/plot mới nhất khi trả lời ba Research Question.

## Số liệu minh chứng đã có

| Mục | Kết quả thực tế |
|---|---|
| E2E mới nhất | PASS trong 31,03 s; Batch +1.440, Speed +20, Redis hit, watermark tăng 07:20 -> 07:38 UTC |
| RQ1 latency | 3 lượt x 5 mẫu/strategy/query; median P50 xấp xỉ 49,3-50,7 ms trên host local |
| RQ2 correctness | 3 lượt identical; 60/60 candle, coverage 100%, MAE 77,576542, MAPE 0,0937%, RMSE 97,388508 |
| RQ3 compaction | 3 lượt real; speedup 1,89x / 2,10x / 2,17x; median 2,10x, median file count 20 -> 11 |
| Unit + integration tests | 67/67 pass: Dagster 5, Dashboard 5, DQ/Ingestion 18, Serving/Speed 21, Docker integration 18 |

Raw data nằm tại `results/logs/`; biểu đồ nằm tại `results/plots/`. Benchmark 3 chỉ công bố các row `mode=real`, không dùng simulation để rút kết luận.

## Câu trả lời phản biện ngắn

**Vì sao Lambda thay vì chỉ stream?** Batch tạo ground truth có thể reprocess; stream cung cấp số liệu provisional mới. Watermark biến khác biệt đó thành trạng thái query rõ ràng, không che giấu tính gần đúng.

**Vì sao Iceberg và ClickHouse cùng tồn tại?** Iceberg/MinIO giữ Bronze immutable, snapshot và maintenance; ClickHouse là serving OLAP cho candle range. Chúng có trách nhiệm khác nhau.

**Small files xử lý thế nào?** Tạo controlled small appends, đo metadata/file count và read latency trước/sau, sau đó dùng Iceberg bin-packing compaction. Workaround Docker hiện tại là Spark `local[2]`, partition thấp, tắt codegen/vectorized Parquet để tránh JVM `SIGSEGV` trên Docker Desktop.

**Làm sao tránh double count?** Cả ClickHouse query và Query Merger đọc revision mới nhất từ `ReplacingMergeTree`; merger deduplicate theo `(symbol, window_start)` và ưu tiên Batch ở boundary.

**dbt có đang chạy không?** Không nằm trên runtime path. Transform chính là Spark/Python đã được E2E kiểm chứng; dbt được giữ làm artefact nghiên cứu/tham khảo. Đây là quyết định minh bạch để chỉ có một source of truth.

## Giới hạn cần nêu trung thực

- Số liệu là local Docker Desktop, workload nhỏ; không suy rộng thành benchmark cluster production.
- `FINAL` đảm bảo revision nhất quán nhưng có overhead; với dữ liệu lớn cần materialized view/merge schedule hoặc bảng serving đã compact.
- RQ3 tạo small file từ controlled batch append; Speed Layer hiện không ghi thẳng Iceberg micro-batch.
- Redis hiện tối ưu latest candle, chưa được đưa vào range-query path vì ClickHouse là nguồn range query thống nhất.

## Kiểm tra cuối trước bảo vệ

```powershell
python -m pytest tests/test_dagster_pipeline.py tests/test_dashboard_components.py -q
python -m pytest tests/test_data_quality.py tests/test_ingestion.py -q
python -m pytest tests/test_query_merger.py tests/test_speed_layer.py -q
python -m pytest tests/test_integration.py -q
python scripts/benchmarks/plot_benchmarks.py
```

Khi thay đổi data volume, image version hoặc cấu hình Spark, phải chạy lại benchmark thay vì tái sử dụng số liệu ở trên.
