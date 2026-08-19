# Scripts

Thư mục chứa helper scripts và benchmark runners.

| File | Mô tả |
|:---|:---|
| `setup_infra.py` | Kiểm tra & khởi tạo hạ tầng (MinIO buckets, Kafka topics, etc.) |
| `seed_data.py` | Nạp dữ liệu mẫu ban đầu |

## Benchmarks (`benchmarks/`)

| File | Mô tả |
|:---|:---|
| `bench_latency.py` | Benchmark 1: Query Latency Comparison (Batch vs Speed vs Lambda) |
| `bench_reprocess.py` | Benchmark 2: Batch Reprocessing Correctness |
| `bench_compaction.py` | Benchmark 3: Small File Compaction Efficiency |
| `run_all_benchmarks.py` | Chạy toàn bộ 3 kịch bản benchmark |
