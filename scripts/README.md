# Scripts

Thư mục chứa helper scripts và benchmark runners.

| File | Mô tả |
|:---|:---|
| `setup_infra.py` | Kiểm tra & khởi tạo hạ tầng (MinIO buckets, Kafka topics, etc.) |
| `seed_data.py` | Nạp dữ liệu mẫu ban đầu |
| `auto_pipeline_daemon.py` | **Autonomous Pipeline Daemon**: Tự động chạy nền Ingestion -> Kafka -> Speed + Auto-Batch -> Reconciliation Audit -> ClickHouse Optimize |
| `run_lambda_pipeline.py` | Chạy một lệnh cho flow Binance REST → mock data → Kafka replay → Batch/Speed → Serving smoke check |
| `e2e_full_pipeline.py` | Smoke test full pipeline dựa trên `run_lambda_pipeline.py`, kiểm tra batch/speed/watermark/API/Redis |

## Autonomous Zero-Touch Pipeline (Tự động hóa 100%)

Chạy toàn bộ hệ thống tự động khép kín (Ingestion đẩy vào Kafka + Speed Layer xử lý tức thời + Batch Layer tự động đọc Kafka định kỳ, lọc DQ/Quarantine, cập nhật Watermark và đối soát `reconciliation_audit_metrics`):

```bash
# Chạy tự động với luồng giả lập liên tục + tiêm lỗi & đối soát mỗi 20 giây:
python scripts/auto_pipeline_daemon.py --mode simulated --batch-interval 20

# Chạy tự động với luồng Binance WebSocket thật (Top 5 coin) mỗi 30 giây:
python scripts/auto_pipeline_daemon.py --mode live --top-n 5 --batch-interval 30

# Chạy riêng Batch Layer ở chế độ Auto-Daemon đọc trực tiếp từ Kafka:
python src/batch_layer/spark_batch_jobs.py --from-kafka --daemon --interval 60
```

## One-command pipeline

Chạy demo pipeline hữu hạn từ dữ liệu Binance mới nhất qua toàn bộ kiến trúc local:

```bash
python scripts/run_lambda_pipeline.py --top-n 5 --mock-days 1 --replay-limit 5000
```

Lệnh này sẽ bật Docker services cần thiết, fetch Binance REST data, generate mock data, replay mock events vào Kafka, chạy Batch Layer container và kiểm tra nhanh FastAPI. Nếu chỉ muốn kiểm tra CLI mà không chạy hạ tầng:

```bash
python scripts/run_lambda_pipeline.py --no-docker --skip-fetch --skip-mock --skip-replay --skip-batch --skip-smoke
```

Smoke test full pipeline nhỏ:

```bash
python scripts/e2e_full_pipeline.py --top-n 2 --mock-days 1 --replay-limit 1000
```

## Benchmarks (`benchmarks/`)

| File | Mô tả |
|:---|:---|
| `bench_latency.py` | Benchmark 1: Query Latency Comparison (Batch vs Speed vs Lambda) |
| `bench_reprocess.py` | Benchmark 2: Batch Reprocessing Correctness |
| `bench_compaction.py` | Benchmark 3: Small File Compaction Efficiency |
| `prepare_small_files.py` | Tạo nhiều append nhỏ vào Iceberg Bronze để Benchmark 3 có dữ liệu real cho small-file problem |
| `run_all_benchmarks.py` | Chạy toàn bộ 3 kịch bản benchmark |

Chuẩn bị dữ liệu real cho Benchmark 3:

```bash
python scripts/benchmarks/prepare_small_files.py --chunk-size 25 --max-records 1000 --repeat 2
python scripts/benchmarks/bench_compaction.py --runs 20
```

Benchmark 1 ghi cả raw latency và summary P50/P95/P99:

```bash
python scripts/benchmarks/bench_latency.py --runs 50 --symbols BTCUSDT ETHUSDT
```

Kết quả:

- `results/logs/bench_latency.csv`
- `results/logs/bench_latency_summary.csv`
