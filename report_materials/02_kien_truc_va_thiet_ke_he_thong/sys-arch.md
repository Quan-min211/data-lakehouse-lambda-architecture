# System Architecture

Tài liệu này mô tả kiến trúc hiện thực của hệ thống Data Lakehouse theo Lambda Architecture trong repository.

## 1. Tổng quan

Hệ thống xử lý dữ liệu giao dịch crypto theo hai đường song song:

- Batch Layer tạo dữ liệu chuẩn xác, đã qua Data Quality, deduplication và đối soát.
- Speed Layer tạo dữ liệu gần thời gian thực, có trạng thái provisional.
- Serving Layer hợp nhất Batch View và Speed View bằng Batch Watermark để tránh double-counting.

```text
Binance REST / WebSocket / Mock Data
        |
        v
Kafka topic: crypto_trades_raw
        |
        +---------------------------+
        |                           |
        v                           v
Batch Layer                     Speed Layer
Spark Batch / Python DQ         Spark Structured Streaming
Iceberg Bronze on MinIO         1m window OHLCV/VWAP
ClickHouse batch_agg            ClickHouse speed_agg
ClickHouse dq_quarantine        Redis Speed hot cache
system_watermark                checkpoint volume
        |                           |
        +-------------+-------------+
                      |
                      v
Serving Layer: FastAPI Query Merger
ClickHouse batch_agg + speed_agg + watermark
                      |
                      v
Streamlit Dashboard / API Consumers
```

## 2. Data Contract

Mọi nguồn dữ liệu được chuẩn hóa về `TradeEvent`:

| Field | Type | Ý nghĩa |
|:---|:---|:---|
| `trade_id` | int | ID giao dịch/aggregate trade |
| `symbol` | string | Cặp giao dịch, ví dụ `BTCUSDT` |
| `price` | float | Giá khớp |
| `quantity` | float | Khối lượng |
| `trade_time` | int | Event-time epoch milliseconds |
| `is_buyer_maker` | bool | Side từ Binance |
| `ingestion_time` | int | Thời điểm hệ thống nhận |
| `is_injected` | bool | Đánh dấu dữ liệu lỗi giả lập |
| `fault_type` | string/null | Loại lỗi giả lập |

Data contract được dùng xuyên suốt ingestion, batch, speed, DQ và benchmark.

## 3. Ingestion Layer

Nguồn dữ liệu:

- `scripts/fetch_binance_data.py`: lấy dữ liệu Binance REST mới nhất vào `datasets/raw`.
- `scripts/generate_mock_data.py`: sinh mock data từ thống kê dữ liệu thật và fault injection vào `datasets/mock`.
- `scripts/stream_binance_live.py`: stream Binance WebSocket thật vào Kafka.
- `scripts/stream_continuous.py`: stream dữ liệu giả lập liên tục vào Kafka.
- `scripts/run_lambda_pipeline.py`: điều phối một lệnh cho flow Binance REST -> mock -> Kafka replay -> Batch/Speed -> Serving smoke check.

Kafka topics chính:

- `crypto_trades_raw`: raw normalized trade events.
- `crypto_trades_dlq`: dead-letter queue cho payload lỗi.

## 4. Batch Layer

Mục tiêu: tạo ground truth đã đối soát.

Module chính:

- `src/batch_layer/spark_batch_jobs.py`
- `src/batch_layer/iceberg_utils.py`
- `src/batch_layer/clickhouse_sync.py`
- `src/data_quality/dq_checks.py`
- `src/data_quality/quarantine.py`

Luồng xử lý:

```text
datasets/mock/aggtrades hoặc datasets/raw/aggtrades
-> load JSON/CSV
-> normalize TradeEvent
-> validate DQ rules
-> deduplicate by (symbol, trade_id)
-> write rejected records to lakehouse.dq_quarantine
-> aggregate one-minute OHLCV/VWAP candles
-> optional/production write Bronze Iceberg table
-> sync Gold candles to lakehouse.batch_agg
-> update lakehouse.system_watermark
-> append batch run CSV log
```

Iceberg:

- Catalog: `iceberg_catalog`
- Namespace: `bronze`
- Table: `bronze.crypto_trades`
- Storage: MinIO bucket `warehouse`
- Partition: `days(trade_time_ts)`
- Target file size: 128 MB

ClickHouse Batch tables:

- `lakehouse.batch_agg`: reconciled OHLCV/VWAP candles.
- `lakehouse.system_watermark`: batch watermark used by Query Merger.
- `lakehouse.dq_quarantine`: rejected DQ records for audit.

Docker:

- `batch-pipeline` runs `spark_batch_jobs.py --write-iceberg --iceberg-strict`.
- Dagster sets `BATCH_WRITE_ICEBERG=true` and `BATCH_ICEBERG_STRICT=true`.

## 5. Speed Layer

Mục tiêu: phục vụ dữ liệu gần thời gian thực.

Module chính:

- `src/speed_layer/spark_streaming.py`
- `src/speed_layer/window_aggregator.py`
- `src/speed_layer/clickhouse_writer.py`
- `src/speed_layer/redis_writer.py`
- `src/speed_layer/metrics_calculator.py`
- `src/speed_layer/spike_detector.py`

Luồng xử lý:

```text
Kafka crypto_trades_raw
-> Spark Structured Streaming
-> parse JSON theo TradeEvent schema
-> event-time column từ trade_time
-> withWatermark(event_time, 1 minute)
-> tumbling window 1 minute by symbol
-> OHLCV + VWAP + is_spike
-> ClickHouse lakehouse.speed_agg
-> Redis speed:candle:<SYMBOL>:<window_start_ms>
-> Redis speed:candle:<SYMBOL>:latest
```

ClickHouse `speed_agg` là store truy vấn lịch sử provisional. Redis là hot cache cho candle mới nhất và dashboard/API mở rộng.

Redis key convention:

```text
speed:candle:BTCUSDT:1788523200000
speed:candle:BTCUSDT:latest
```

Speed Layer được khuyến nghị chạy bằng Docker:

```bash
docker compose up -d speed-layer
docker compose logs -f speed-layer
```

## 6. Serving Layer

Mục tiêu: cung cấp API hợp nhất dữ liệu Batch + Speed.

Module chính:

- `src/serving_layer/api_routes.py`
- `src/serving_layer/query_merger.py`
- `src/serving_layer/clickhouse_client.py`
- `src/serving_layer/watermark_reader.py`
- `src/serving_layer/schemas.py`

Endpoints:

- `GET /health`
- `GET /api/system/status`
- `GET /api/watermark`
- `GET /api/market`
- `GET /api/speed/latest`
- `GET /api/reconciliation`

Query Merger dùng Batch Watermark `W`:

```text
Case 1: end_time <= W
-> 100% Batch View

Case 2: start_time >= W
-> 100% Speed View

Case 3: start_time < W < end_time
-> Batch range [start_time, W)
-> Speed range [W, end_time)
-> merge, dedupe by (symbol, window_start), prefer Batch
```

Response `/api/market` gồm:

- candles đã gắn trạng thái `Reconciled` hoặc `Provisional`.
- `source_breakdown`: số candle từ batch/speed và số duplicate bị loại.
- `freshness_lag_seconds`: độ trễ watermark so với query `end_time`.
- `reconciliation_delta`: sai số VWAP tại vùng đối soát khi có dữ liệu song song.

Endpoint `/api/system/status` trả trạng thái nhẹ cho demo/dashboard:

- dependency status cho ClickHouse, Redis, Kafka.
- batch watermark hiện tại.
- `freshness_lag_seconds` của watermark.
- trạng thái tổng thể `ok` hoặc `degraded`.

Endpoint `/api/speed/latest?symbol=BTCUSDT` đọc Redis hot cache:

- key `speed:candle:<SYMBOL>:latest`.
- `cache_status`: `hit`, `miss` hoặc `unavailable`.
- `candle`: payload nến provisional mới nhất nếu có.

## 7. Small File Compaction

Mục tiêu: xử lý small files trong Iceberg Bronze.

Module:

- `src/batch_layer/compaction.py`
- `scripts/benchmarks/bench_compaction.py`

Compaction job hỗ trợ:

- diagnose small files qua Iceberg metadata table `.files`.
- bin-packing compaction bằng `CALL iceberg_catalog.system.rewrite_data_files`.
- sort compaction tùy chọn.
- manifest rewrite.
- expire snapshots.

Dagster:

- asset `iceberg_small_files_compaction`
- job `iceberg_compaction_job`
- schedule mỗi 2 giờ.

Benchmark:

```bash
python scripts/benchmarks/bench_compaction.py
python scripts/benchmarks/bench_compaction.py --simulate
```

Kết quả benchmark được ghi vào `results/logs/bench_compaction.csv`.

## 8. Orchestration

Dagster asset graph:

```text
bronze_crypto_trades
-> silver_cleaned_trades
-> clickhouse_quarantine_sync
-> gold_market_aggregates
-> clickhouse_batch_sync
-> system_watermark_sync
```

Maintenance:

```text
iceberg_small_files_compaction
```

One-command local pipeline:

```bash
python scripts/run_lambda_pipeline.py --top-n 5 --mock-days 1 --replay-limit 5000
```

Full smoke test:

```bash
python scripts/e2e_full_pipeline.py --top-n 2 --mock-days 1 --replay-limit 1000
```

## 9. Observability

CSV logs:

- `results/logs/batch_pipeline_runs.csv`
- `results/logs/lambda_pipeline_runs.csv`
- `results/logs/e2e_full_pipeline.csv`
- `results/logs/e2e_stream_test.csv`
- `results/logs/bench_*.csv`

Health checks:

- Docker healthcheck for Kafka, MinIO, ClickHouse, Redis.
- FastAPI `/health`.
- `scripts/check_system.py` for local readiness checks.

## 10. Current Design Notes

1. ClickHouse is used for both `batch_agg` and `speed_agg` so the Serving Layer can query time ranges consistently.
2. Redis is used as a Speed hot cache, not as the only Speed View store.
3. Iceberg is the Bronze source of truth for batch runs in Docker/Dagster strict mode.
4. Query Merger uses half-open windows `[start, end)` to avoid boundary duplication.
5. Batch records always win when the same `(symbol, window_start)` exists in both Batch and Speed Views.
