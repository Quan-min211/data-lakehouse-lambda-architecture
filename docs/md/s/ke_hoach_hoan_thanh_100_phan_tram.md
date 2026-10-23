# Kế Hoạch Hoàn Thành 100% Đồ Án Lambda Lakehouse

Ngày lập: 2026-10-23

## Mục tiêu hoàn thành

Đồ án được xem là hoàn thành khi có thể chứng minh bằng source code, Docker demo và số liệu thực nghiệm rằng:

1. Lambda Architecture chạy end-to-end: Binance/mock -> Kafka -> Speed Layer và Batch Layer -> Serving API/dashboard.
2. Batch View là ground truth, Speed View là provisional, và Query Merger xử lý đúng watermark/deduplication.
3. Ba câu hỏi nghiên cứu có số liệu real, lặp lại được và biểu đồ/CSV tương ứng.
4. Iceberg được dùng thật cho Bronze; vấn đề small files được tạo, đo và compact bằng kết quả thật.
5. Tài liệu, source code, demo và slide không mâu thuẫn nhau.

## Hiện trạng đã xác nhận

- Demo một lệnh đã chạy thành công: Binance REST -> mock -> Kafka replay -> Batch/Iceberg -> ClickHouse -> Redis -> FastAPI.
- Batch, Speed, Redis cache, DQ quarantine, watermark, Query Merger và dashboard readiness đã hoạt động.
- Đã tạo được small files thật bằng append nhỏ vào Bronze Iceberg.
- Real Benchmark 3 đã chạy thành công ba lượt sau khi giới hạn tài nguyên Spark Docker và tắt codegen/vectorized Parquet path gây `SIGSEGV`. Median: 20 -> 11 file, 5.340 ms -> 2.549 ms, speedup 2,10x.
- Benchmark 1 đã chạy ba lượt độc lập (`BTCUSDT`, 5 mẫu mỗi strategy/query) sau khi Serving đọc revision cuối cùng của ClickHouse. Median P50: Q1 Batch/Lambda/Speed = 49,839/50,344/49,528 ms; Q2 = 49,299/49,836/50,559 ms; Q3 = 50,724/50,238/50,738 ms.
- Benchmark 2 đã chạy ba lượt độc lập với seed xác định, `FINAL` và force-seed Speed View. Cả ba lượt trùng nhau: 60/60 candle, coverage 100%, MAE 77,576542, MAPE 0,0937%, RMSE 97,388508.
- E2E sau rebuild FastAPI đã PASS: 1.440 batch candle mới, 20 speed row mới, watermark tăng `07:20 -> 07:38 UTC`, Redis hit và API hợp lệ trong 31,03 giây.

## Tiến độ triển khai ngày 2026-10-23

- [x] Giới hạn Batch Spark Docker về `local[2]`, 2 partition và memory 768 MB để tránh tranh chấp tài nguyên.
- [x] Tắt Spark whole-stage codegen/vectorized Parquet path cho Iceberg benchmark; đây là workaround cho JVM `SIGSEGV` trong môi trường Docker Desktop hiện tại.
- [x] Làm parser kết quả `rewrite_data_files` tương thích với tên cột Iceberg 1.5.x.
- [x] Tạo controlled small-file append và chạy Benchmark 3 real mode ba lượt độc lập.
- [x] Chạy ba lượt độc lập Benchmark 1 và 2 với `BTCUSDT`, có CSV raw/summary và median.
- [x] Sinh biểu đồ benchmark từ CSV mới nhất; compaction plot chỉ dùng row `mode=real`.
- [x] Xử lý revision cũ của `ReplacingMergeTree`: Serving/benchmark đọc `FINAL`; seed Benchmark 2 có deterministic seed thay vì Python `hash()` ngẫu nhiên theo process.
- [x] Rebuild FastAPI và chạy E2E sau thay đổi serving.

## P0 - Bắt buộc trước khi kết luận đồ án hoàn thành

### 1. Khắc phục và xác thực real Benchmark 3

Việc cần làm:

1. Đã tái tạo lỗi và xác định đường Spark SQL codegen/vectorized Parquet là không ổn định trong Docker Desktop hiện tại.
2. Đã giới hạn `local[2]`, shuffle partitions 2 và memory 768 MB cho batch/benchmark container.
3. Đã chạy lại theo thứ tự: tạo small files -> đo before -> bin-pack compaction -> đo after.
4. Đã chạy ba lượt độc lập; median speedup là 2,10x. Cần ghi cấu hình/machine/data volume vào báo cáo chính thức.

Điều kiện đạt:

- `bench_compaction.py` chạy real mode không fallback simulation, exit code 0.
- Có file count, latency trung bình/P50/P95 trước-sau, thời gian compaction và speedup thật.
- Ít nhất 3 lần chạy độc lập, kết luận dựa trên median thay vì một lần chạy.

### 2. Chạy đủ ba benchmark thực nghiệm

Benchmark 1 - Query latency:

- So sánh Batch-only, Speed-only và Lambda Merge trên cùng symbol/time range.
- Ghi P50/P95/P99, RPS và số request lỗi.
- Chạy warm-up trước khi đo; tối thiểu 3 lượt và biểu đồ box/bar.

Benchmark 2 - Reprocessing correctness:

- Dùng seed/mock cố định, có duplicate, late data và invalid data đã biết ground truth.
- Đo MAE/MAPE/RMSE cho close/VWAP, volume và trade count; đo thời gian hội tụ sau batch reprocess.
- Lưu event seed/config cùng CSV để tái lập.

Benchmark 3 - Compaction:

- Chỉ công bố kết quả `mode=real`, không dùng row `simulated=True` cho kết luận nghiên cứu.
- Ghi file count, byte size, snapshot count, query latency và compaction duration.

Điều kiện đạt:

- Có CSV raw, summary, biểu đồ và một bảng tổng hợp trong báo cáo cho cả ba benchmark.
- Mỗi kết luận phải trỏ được về một log/biểu đồ cụ thể.

### 3. Chốt kiến trúc thực tế và đồng bộ tài liệu

Quyết định cần chốt trong tài liệu:

- Iceberg Bronze là master/immutable event store.
- ClickHouse `batch_agg` là Batch View phục vụ query lịch sử.
- ClickHouse `speed_agg` phục vụ speed range query; Redis chỉ là hot cache cho latest/provisional candle.
- Batch được ưu tiên nếu cả hai view có cùng `(symbol, window_start)`.

Việc cần làm:

1. Đồng bộ `README.md`, `docs/md/sys-arch.md`, proposal, report và slide với kiến trúc trên.
2. Sửa các tài liệu cũ còn nói Redis là Speed View duy nhất, dbt đã chạy trong pipeline chính, hoặc benchmark đã hoàn tất khi chưa có số liệu real.
3. Cập nhật `docs/md/README.md` để bỏ nhãn TODO sai của `sys-arch.md`.

Điều kiện đạt:

- Không còn mâu thuẫn giữa sơ đồ, bảng công nghệ, mô tả luồng dữ liệu và code.

## P1 - Hoàn thiện kỹ thuật và độ tin cậy

### 4. Chốt vai trò dbt

Chọn một trong hai hướng:

- Tích hợp dbt-spark thành bước Dagster/Batch chính cho Silver/Gold.
- Hoặc giữ transform Python/Spark là đường chính và sửa tài liệu để dbt là artefact thử nghiệm, không phải thành phần bắt buộc.

Quyết định đã chốt: chọn hướng hai. `spark_batch_jobs.py` là đường transform chính và đã được E2E kiểm chứng; dbt giữ vai trò artefact thử nghiệm/tham khảo, không phải dependency runtime của demo.

Điều kiện đạt:

- Có một source of truth duy nhất cho transform Silver/Gold, không có hai mô tả mâu thuẫn.

### 5. Hoàn thiện E2E test tự động

Mở rộng `scripts/e2e_full_pipeline.py` để kiểm tra:

- Kafka có nhận đúng số event demo.
- Speed Layer có record mới trong ClickHouse và Redis.
- Batch ghi Iceberg snapshot, `batch_agg`, watermark và `dq_quarantine`.
- API `/api/system/status`, `/api/market`, `/api/speed/latest` trả dữ liệu hợp lệ.
- Query hybrid không duplicate candle tại watermark boundary.

Điều kiện đạt:

- Một command trả exit code khác 0 khi bất kỳ thành phần nào sai.
- CSV log gồm run id, record count, snapshot id, watermark, speed/cache result và trạng thái cuối.

### 6. Tăng tính tái lập và vận hành

Việc cần làm:

- Lưu config benchmark theo run: seed, symbols, thời gian, số event, fault rate, version image.
- Thêm script dọn dữ liệu demo theo `datasets/runs/<run_id>` thay vì xóa dữ liệu chung.
- Đặt version cố định cho image/JAR quan trọng khi đã xác nhận benchmark ổn định.
- Thêm hướng dẫn recovery khi Kafka checkpoint, Iceberg catalog hoặc Docker volume lỗi.

Điều kiện đạt:

- Một người khác clone repo, dùng `.env.example`, chạy Docker và lặp lại demo theo README.

## P2 - Nâng chất lượng trình bày và bảo vệ

### 7. Hoàn thiện dashboard cho demo

- Hiển thị Batch watermark, Speed latest candle, Redis cache hit, DQ pass rate, số quarantine và compaction file count.
- Thêm trạng thái rõ `provisional` và `reconciled` để giải thích Lambda Architecture trực quan.
- Dùng dữ liệu từ run demo mới nhất, tránh dashboard chỉ hiện dữ liệu cũ.

### 8. Chuẩn bị gói minh chứng bảo vệ

Tạo một thư mục `docs/defense/` hoặc tài liệu tương đương có:

- Sơ đồ kiến trúc cuối cùng.
- Kịch bản demo 5-10 phút và command theo thứ tự.
- Bảng kết quả 3 benchmark, cách đọc biểu đồ và giới hạn nghiên cứu.
- Danh sách câu hỏi phản biện: vì sao Lambda, vì sao Iceberg, vì sao ClickHouse/Redis, cách xử lý duplicate/late data/small files.
- Phụ lục môi trường thực nghiệm: CPU/RAM, Docker Desktop, phiên bản service và dataset size.

Trạng thái: đã tạo `docs/md/s/huong_dan_bao_ve_va_tai_lap.md` gồm kịch bản demo, lệnh tái lập, số liệu RQ1-RQ3, giới hạn nghiên cứu và câu trả lời phản biện. Trước buổi bảo vệ chỉ cần bổ sung CPU/RAM thực tế của máy trình bày nếu khác máy benchmark.

## Thứ tự thực hiện đề xuất

1. Sửa Spark/Iceberg `SIGSEGV` và hoàn thành Benchmark 3 real mode.
2. Chạy lại Benchmark 1, 2, 3 theo cùng một profile dữ liệu; sinh CSV/plots cuối.
3. Chốt dbt là integrated hoặc experimental, sau đó đồng bộ toàn bộ tài liệu.
4. Mở rộng E2E test và chạy demo sạch từ Docker đang tắt.
5. Cập nhật dashboard, README, slide và tạo gói bảo vệ.
6. Chỉ sau khi các điều kiện P0 đạt, kết luận đồ án hoàn thành 100%.

## Lệnh kiểm chứng cuối

```powershell
docker compose up -d --build
python scripts/run_lambda_pipeline.py --top-n 1 --klines-limit 100 --aggtrades-limit 50 --mock-days 1 --ticks-per-min 1 --fault-mode light --replay-limit 300 --smoke-symbol BTCUSDT
python scripts/e2e_full_pipeline.py
python scripts/benchmarks/run_all_benchmarks.py
python -m pytest tests -q
```

Không được ký nhận hoàn thành nếu Benchmark 3 vẫn ở simulation hoặc các tài liệu còn khẳng định kết quả chưa được chạy thật.
