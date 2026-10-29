# Báo cáo tổng kết hiện trạng hệ thống Lambda Lakehouse

Ngày rà soát và demo thực tế gần nhất: 2026-10-23
Phạm vi rà soát: toàn bộ repository tại `C:\HCMUTE\nam4ki1\tlcn`, gồm hạ tầng Docker Compose, ingestion, batch layer, speed layer, serving layer, data quality, benchmark, Dagster, dashboard và tài liệu.

## 0. Cập nhật sau khi triển khai

Các mục ưu tiên cao đã được xử lý sau bản rà soát ban đầu:

- Đã sửa benchmark compaction gọi đúng `run_bin_packing_compaction()`.
- Đã thêm `scripts/run_lambda_pipeline.py` để chạy một lệnh cho flow Binance REST -> mock data -> Kafka replay -> Batch/Speed -> Serving smoke check.
- Đã thêm `scripts/e2e_full_pipeline.py` để smoke test batch/speed/watermark/API/Redis.
- Speed Layer hiện ghi song song ClickHouse `speed_agg` và Redis hot cache `speed:candle:<SYMBOL>:latest`.
- Docker/Dagster batch path đã bật ghi Iceberg strict mode.
- Batch pipeline đã persist rejected DQ records vào ClickHouse `lakehouse.dq_quarantine`.
- Query Merger đã có dedupe theo `(symbol, window_start)` ưu tiên Batch, `source_breakdown` và `freshness_lag_seconds`.
- `docs/md/sys-arch.md` đã được hoàn thiện thành tài liệu kiến trúc component-level.
- FastAPI đã có endpoint `/api/system/status` cho readiness/freshness.
- Dashboard đã có tab System Readiness đọc `/api/system/status`.
- FastAPI và dashboard đã có `/api/speed/latest` để đọc Redis latest candle.
- Đã thêm `scripts/benchmarks/prepare_small_files.py` để tạo nhiều append nhỏ vào Iceberg Bronze trước khi chạy Benchmark 3 real mode.
- Benchmark latency ghi thêm `results/logs/bench_latency_summary.csv` với P50/P95/P99, không thay đổi raw CSV cũ.
- `run_all_benchmarks.py` đã ghi cả latency raw CSV và latency summary CSV.

### Kết quả demo Docker ngày 2026-10-23

Đã chạy thành công luồng cô lập `pipeline_20261023_064253` bằng một lệnh:

```bash
python scripts/run_lambda_pipeline.py --top-n 1 --klines-limit 100 --aggtrades-limit 50 --mock-days 1 --ticks-per-min 1 --fault-mode light --replay-limit 300 --smoke-symbol BTCUSDT
```

- Binance REST lấy được 100 klines và 50 aggTrades của `BTCUSDT`.
- Mock generator tạo 1.579 event, gồm 165 fault có kiểm soát.
- Batch xử lý 1.579 record: 1.440 valid, 18 quarantine, 121 duplicate loại bỏ, DQ pass rate 91,2%.
- Bronze Iceberg ghi thành công 1.440 record, snapshot `6667383451293328729`; ClickHouse ghi 1.440 batch candle và watermark được cập nhật.
- Speed Layer nhận replay, ghi 275 candle vào `lakehouse.speed_agg` và Redis; `/api/speed/latest?symbol=BTCUSDT` trả cache `hit`.
- `/api/system/status` báo Kafka, ClickHouse, Redis đều `ok`; `/api/market` trả 59 candle từ batch trong cửa sổ mặc định.
- Đã tạo thử 4 small-file append thật (92 valid record, chunk 25) vào Bronze Iceberg, tất cả thành công; log ở `results/logs/small_file_preparation.csv`.
- Real Benchmark 3 đã chạy được sau khi dùng Spark local `local[2]`, giới hạn partition/memory và tắt codegen/vectorized Parquet path không ổn định trong Docker Desktop. Ba lượt real cho speedup 1,89x, 2,10x và 2,17x; median 2,10x. Lượt median: 20 -> 11 file, latency trung bình 5.340,0 ms -> 2.549,1 ms. Simulation vẫn chỉ chạy khi truyền `--simulate`.
- Benchmark 1 đã lặp lại ba lượt sau khi chuẩn hoá ClickHouse `ReplacingMergeTree` bằng `FINAL`; median P50 nằm trong khoảng 49,3-50,7 ms cho từng strategy/query trên máy local. Đây là kết quả latency thực tế, không suy diễn rằng một layer luôn nhanh hơn layer khác ở workload nhỏ.
- Benchmark 2 đã lặp lại ba lượt với deterministic seed và force-seed cửa sổ đo; cả ba lượt đều 60/60 candle, coverage 100%, MAE 77,576542, MAPE 0,0937%, RMSE 97,388508.
- E2E mới nhất (`2026-10-23T07:38:35Z`) PASS sau rebuild FastAPI: batch +1.440, speed +20, watermark `07:20 -> 07:38 UTC`, Redis latest hit, API hợp lệ, 31,03 giây.
- Test suite đã pass theo các nhóm độc lập: Dagster 5/5, Dashboard 5/5, Data Quality/Ingestion 18/18, Serving/Speed 21/21, Docker integration 18/18; tổng 67/67. Test Dagster tách compaction Spark ra khỏi materialization in-memory, vì compaction đã có real benchmark/integration riêng. Test integration dùng Kafka host listener `localhost:9094` thay vì listener nội bộ Docker `9092`.
- Đã xử lý thêm failure mode stream liên tục Kafka retention/checkpoint loss: Speed Layer mặc định `failOnDataLoss=true` để fail-closed, thay vì silent skip offset. Operator chỉ có thể dùng checkpoint mới với `KAFKA_STARTING_OFFSETS=earliest` hoặc `--allow-data-loss` khi recovery được ghi nhận. Unit Speed 12/12 và Docker integration 18/18 pass sau thay đổi; runbook/report tại `docs/md/s/bao_cao_stream_lien_tuc_kafka_offset_checkpoint.md`.

### Việc còn mở sau demo

1. Đưa median của ba benchmark vào báo cáo/slide cuối, kèm cấu hình máy, Docker image và dataset size.
2. Ghi rõ trong README/report: `spark_batch_jobs.py` là transform chính; dbt là artefact tham khảo, không phải dependency runtime.
3. Chỉ cân nhắc cho Query Merger dùng Redis latest cache trong truy vấn realtime cực ngắn khi có benchmark riêng chứng minh lợi ích; hiện ClickHouse vẫn là nguồn range query thống nhất.

## 1. Kết luận nhanh

Hệ thống hiện đã có khung triển khai khá đầy đủ cho kiến trúc Lambda Lakehouse: Kafka, Spark, MinIO, Iceberg REST Catalog, ClickHouse, Redis, FastAPI, Streamlit, Dagster, batch pipeline, speed streaming, query merger, data quality và benchmark scripts đều đã xuất hiện trong repo.

Tuy nhiên, nếu đối chiếu với kiến trúc ban đầu và mục tiêu nghiên cứu, chưa nên xem là hoàn tất 100%. Trạng thái đúng hơn là: hệ thống đã có prototype end-to-end và nhiều module có thể chạy, nhưng vẫn còn một số khoảng hở về tích hợp, vận hành một lệnh, kiểm chứng benchmark thật, Redis Speed View, tài liệu kiến trúc chi tiết và một vài lỗi/khác biệt implementation.

Các điểm chính:

- Batch Layer đã có pipeline Bronze/Silver/Gold logic, ghi ClickHouse batch view, cập nhật watermark và có tùy chọn ghi Iceberg Bronze.
- Speed Layer đã có Spark Structured Streaming đọc Kafka, aggregate OHLCV/VWAP theo cửa sổ 1 phút, phát hiện spike và ghi `lakehouse.speed_agg` vào ClickHouse.
- Serving Layer đã có FastAPI Query Merger dựa trên batch watermark, chia 3 case history/realtime/hybrid.
- Small File Problem đã có hướng xử lý bằng Iceberg compaction, Dagster schedule và benchmark script; lỗi gọi sai tên hàm đã được sửa, còn cần chạy benchmark thật trên cụm Docker có dữ liệu đủ lớn.
- Luồng Binance/mock đã có một command orchestration qua `scripts/run_lambda_pipeline.py`.
- Speed View hiện có hai store: ClickHouse `speed_agg` để query range và Redis hot cache để phục vụ dữ liệu realtime mới nhất.

## 2. Hệ thống đã xong 100% chưa?

Chưa xong 100% theo kiến trúc ban đầu.

### Những phần đã làm tốt

1. Hạ tầng Docker Compose đã khá đầy đủ:
   - Kafka KRaft
   - MinIO
   - Iceberg REST Catalog
   - Spark master/worker/thrift
   - ClickHouse
   - Redis
   - FastAPI
   - Streamlit
   - Dagster
   - service riêng cho `speed-layer`
   - service riêng cho `batch-pipeline`

2. Data contract cho trade event đã có:
   - `trade_id`
   - `symbol`
   - `price`
   - `quantity`
   - `trade_time`
   - `is_buyer_maker`
   - `ingestion_time`
   - `is_injected`
   - `fault_type`

3. Batch Layer đã có pipeline chạy bằng Python:
   - load records từ `datasets/mock/aggtrades`, `datasets/raw/aggtrades`, `datasets/sample`
   - data quality + deduplicate
   - aggregate Gold candles 1 phút
   - ghi ClickHouse `lakehouse.batch_agg`
   - cập nhật `lakehouse.system_watermark`
   - log CSV vào `results/logs/batch_pipeline_runs.csv`
   - có tùy chọn ghi Bronze vào Iceberg bằng `--write-iceberg`

4. Speed Layer đã có Spark Structured Streaming:
   - đọc Kafka topic `crypto_trades_raw`
   - parse schema
   - watermark 1 phút
   - tumbling window 1 phút
   - tính OHLCV/VWAP/spike
   - ghi ClickHouse `lakehouse.speed_agg`
   - checkpoint vào volume Docker

5. Serving Layer đã có Query Merger:
   - Case 1: toàn bộ query nằm trước hoặc bằng watermark -> dùng Batch View
   - Case 2: query sau watermark -> dùng Speed View
   - Case 3: cắt query tại watermark -> Batch + Speed
   - API `/api/market`, `/api/watermark`, `/api/reconciliation`

6. Data Quality đã có hướng xử lý:
   - kiểm tra required fields
   - kiểm tra price/quantity/time hợp lệ
   - deduplicate theo `(symbol, trade_id)`
   - rejected records và DQ report

7. Benchmark đã có script cho 3 kịch bản:
   - latency
   - reprocess correctness
   - compaction efficiency

### Những phần chưa thật sự hoàn tất

1. Tài liệu kiến trúc và README đã được cập nhật; vẫn cần đồng bộ chúng mỗi khi thay đổi quyết định về Speed View.

2. Speed View hiện ghi cả ClickHouse `speed_agg` và Redis hot cache; cần chốt trong báo cáo rằng ClickHouse phục vụ range query, còn Redis phục vụ latest/provisional candle.

3. dbt tồn tại nhưng batch pipeline chính không thực sự gọi dbt model Bronze -> Silver -> Gold:
   - Logic hiện đang nằm trong Python `spark_batch_jobs.py`.
   - dbt project có model, nhưng chưa thấy được nối vào pipeline chính như một bước bắt buộc.

4. Benchmark compaction ở chế độ thật đã sửa lỗi gọi sai method, nhưng vẫn cần chạy real benchmark trên Docker với dữ liệu Iceberg đủ lớn để có số liệu cuối.

5. Đã có orchestrator duy nhất cho flow:
   - fetch Binance
   - generate mock
   - start infra
   - stream vào Kafka
   - chạy batch
   - chạy speed
   - kiểm tra serving/dashboard
   qua `scripts/run_lambda_pipeline.py`.

6. Chưa thấy bằng chứng kết quả benchmark thật được commit trong `results/logs` hoặc `results/plots`.

7. Batch ghi Iceberg đang là optional:
   - chạy bình thường có thể chỉ sync ClickHouse.
   - muốn đúng Lakehouse/Iceberg làm Master Dataset thì nên bắt buộc ghi Bronze Iceberg trong pipeline Docker hoặc Dagster production mode.

## 3. Có thể gộp mock và dữ liệu trực tiếp vào một công đoạn một lệnh không?

Có thể làm được, và repo hiện đã có đủ phần nền để gộp.

Hiện có các script rời:

- `scripts/fetch_binance_data.py`: lấy dữ liệu thật từ Binance REST, lưu vào `datasets/raw`.
- `scripts/generate_mock_data.py`: đọc dữ liệu thật trong `datasets/raw/klines`, fit phân phối thống kê và sinh mock data vào `datasets/mock`.
- `scripts/stream_binance_live.py`: stream dữ liệu thật từ Binance WebSocket vào Kafka.
- `scripts/stream_continuous.py`: stream dữ liệu giả lập/random walk vào Kafka, có lấy giá hiện tại từ Binance REST làm seed.
- `src/batch_layer/spark_batch_jobs.py`: chạy batch pipeline.
- `docker compose up -d speed-layer`: chạy speed layer.

Nên tạo một script orchestration mới, ví dụ:

```bash
python scripts/run_lambda_pipeline.py --source binance --top-n 10 --mock-days 7 --fault-mode full
```

Flow đề xuất:

1. Kiểm tra Docker services:
   - Kafka
   - MinIO
   - Iceberg REST
   - ClickHouse
   - Redis nếu vẫn giữ
   - FastAPI
   - Speed Layer

2. Fetch dữ liệu Binance mới nhất:
   - gọi lại logic từ `fetch_binance_data.py`
   - lưu `datasets/raw/klines`, `datasets/raw/aggtrades`, `datasets/raw/tickers`

3. Generate mock theo dữ liệu mới nhất:
   - gọi logic từ `generate_mock_data.py`
   - sinh `datasets/mock/klines`, `datasets/mock/aggtrades`
   - fault injection phục vụ DQ/benchmark

4. Đưa dữ liệu qua Kafka:
   - với mode live: dùng WebSocket Binance -> Kafka
   - với mode mock: replay `datasets/mock/aggtrades/*.json` -> Kafka
   - với mode hybrid: fetch Binance để cập nhật distribution, generate mock, rồi replay mock vào Kafka

5. Chạy Batch Layer:
   - `docker compose run --rm batch-pipeline`
   - hoặc gọi `spark_batch_jobs.py --write-iceberg --iceberg-strict`

6. Speed Layer chạy song song:
   - `docker compose up -d speed-layer`
   - kiểm tra ClickHouse `speed_agg` có dữ liệu mới

7. Kiểm tra Serving Layer:
   - gọi `/health`
   - gọi `/api/watermark`
   - gọi `/api/market`
   - ghi smoke test log vào `results/logs`

Nên có 3 mode:

- `--mode mock`: lấy dữ liệu Binance mới nhất, generate mock, replay mock.
- `--mode live`: stream Binance WebSocket thật vào Kafka.
- `--mode hybrid`: vừa fetch lịch sử để batch/mock, vừa stream live cho speed.

Điểm cần lưu ý: nếu một lệnh vừa stream live vô hạn vừa chạy batch thì phải định nghĩa rõ thời điểm dừng. Nên có tham số như `--duration-minutes 10` hoặc `--events 100000`.

## 4. Batch Layer và Speed Layer đã xây và chạy được hết chưa?

### Batch Layer

Batch Layer đã xây tương đối đầy đủ và có thể chạy ở mức prototype/production-local.

Các thành phần đã có:

- `src/batch_layer/spark_batch_jobs.py`
- `src/batch_layer/clickhouse_sync.py`
- `src/batch_layer/iceberg_utils.py`
- `src/batch_layer/compaction.py`
- Dagster assets trong `dagster_project/repository.py`
- Docker service `batch-pipeline`

Luồng batch hiện tại:

```text
datasets/raw hoặc datasets/mock
-> load TradeEvent
-> clean_and_deduplicate
-> aggregate_gold_candles
-> optional write Bronze Iceberg
-> insert ClickHouse batch_agg
-> update system_watermark
-> write CSV run log
```

Đánh giá:

- Chạy được nếu ClickHouse sẵn sàng.
- Ghi Iceberg có pre-flight check MinIO/Iceberg REST và strict mode.
- Có Dagster orchestration cho asset graph.
- Nhưng dbt chưa được tích hợp trực tiếp vào batch pipeline chính.
- Iceberg mới chắc chắn ở Bronze; Silver/Gold Iceberg/dbt chưa phải đường chạy chính.

### Speed Layer

Speed Layer đã xây và có service Docker để chạy.

Các thành phần đã có:

- `src/speed_layer/spark_streaming.py`
- `src/speed_layer/window_aggregator.py`
- `src/speed_layer/clickhouse_writer.py`
- `src/speed_layer/metrics_calculator.py`
- `src/speed_layer/spike_detector.py`
- Docker service `speed-layer`

Luồng speed hiện tại:

```text
Kafka crypto_trades_raw
-> Spark Structured Streaming
-> parse TradeEvent
-> watermark + tumbling window 1 phút
-> OHLCV/VWAP/spike
-> ClickHouse speed_agg
```

Đánh giá:

- Về code, đã có thể chạy.
- Khuyến nghị chạy qua Docker để tránh lỗi Hadoop/winutils trên Windows.
- Có checkpoint.
- Có idempotency mức ClickHouse bằng `ReplacingMergeTree(created_at)`.
- Nhưng chưa đúng hoàn toàn với mô tả ban đầu nếu yêu cầu Speed View phải nằm ở Redis.

## 5. Small File Problem ở Iceberg xử lý sao rồi?

Small File Problem đã được thiết kế hướng xử lý khá rõ, nhưng cần xác nhận bằng benchmark thật.

Đã có:

1. Iceberg table config:
   - `write.target-file-size-bytes = 134217728` tương đương 128 MB.
   - partition theo `days(trade_time_ts)`.
   - format version 2.

2. Compaction job:
   - `src/batch_layer/compaction.py`
   - `run_bin_packing_compaction()`
   - `run_sort_compaction()`
   - `rewrite_manifests()`
   - `expire_old_snapshots()`
   - `run_full_compaction()`

3. Diagnosis:
   - đếm file nhỏ qua metadata table `.files`
   - thống kê `file_count`, `avg_size_mb`, `small_file_pct`
   - đưa recommendation theo threshold

4. Dagster maintenance asset:
   - `iceberg_small_files_compaction`
   - schedule mỗi 2 giờ

5. Benchmark:
   - `scripts/benchmarks/bench_compaction.py`
   - có real mode và simulation mode
   - ghi CSV `results/logs/bench_compaction.csv`

Vấn đề còn lại:

- Benchmark real đã sửa lỗi tên hàm compaction.
- Cần chạy benchmark thật sau khi có dữ liệu Iceberg đủ nhiều file nhỏ.
- Nếu speed layer không ghi trực tiếp vào Iceberg mà chỉ ghi ClickHouse, small file trong Iceberg chủ yếu phát sinh từ batch append nhiều lần, không phải từ streaming ingestion như giả thuyết ban đầu. Muốn benchmark đúng RQ3, cần có luồng ghi micro-batch hoặc nhiều batch nhỏ vào Bronze Iceberg để tạo small files có kiểm soát.

Khuyến nghị:

1. Chạy `scripts/benchmarks/prepare_small_files.py` trên Docker để tạo small files thật, sau đó chạy `bench_compaction.py` real mode và lưu kết quả.
3. Ghi rõ trong báo cáo thực nghiệm dòng nào là real, dòng nào là simulation.
4. Nếu muốn đúng với luận điểm "streaming ingestion tạo small files", cần thêm sink Iceberg cho raw stream hoặc micro-batch write vào Bronze Iceberg.

## 6. Logic query hiện tại và hướng cải thiện

### Logic hiện tại

Query Merger hiện dựa trên batch watermark `W`:

```text
Case 1: end_time <= W
-> lấy 100% từ batch_agg

Case 2: start_time >= W
-> lấy 100% từ speed_agg

Case 3: start_time < W < end_time
-> lấy batch phần trước watermark
-> lấy speed phần sau watermark
-> merge và sort theo window_start
```

Đây là logic đúng với Lambda Architecture cơ bản và đã có test unit cho 3 case.

### Vấn đề cần cải thiện

1. Ranh giới watermark đã được chuẩn hóa và có dedupe sau merge:
   - comment nói Speed lấy `(W, T_end]`
   - code query speed lại truyền `start_time=watermark`
   - nếu ClickHouse client filter dùng `>= start_time`, window đúng tại watermark có thể bị lấy cả batch và speed.
   - Key dedupe là `(symbol, window_start)`, Batch được ưu tiên khi có trùng.

2. Case 2 dùng `elif start_time >= watermark`, nhưng comment nói `T_start > W`.
   - Cần thống nhất `>` hay `>=`.
   - Với candle window, nên quy ước rõ watermark là `window_end` đã batch hoàn tất.

3. Reconciliation delta hiện chỉ lấy candle cuối vùng boundary:
   - chưa phản ánh sai số toàn khoảng.
   - nên tính thêm MAE/MAPE cho VWAP, volume, trade_count.

4. Không có cơ chế fallback khi một view thiếu dữ liệu:
   - Nếu batch query rỗng nhưng speed có dữ liệu, API hiện trả rỗng ở Case 1.
   - Nếu ClickHouse lỗi, chưa có graceful degradation rõ ở tầng API.

5. Không có cache query result:
   - Dashboard gọi lặp có thể gây tải ClickHouse.
   - Có thể dùng Redis cho response cache hoặc speed hot keys.

6. Query chưa expose metadata đủ rõ:
   - nên trả thêm `source_breakdown`: số candle từ batch, số candle từ speed.
   - nên trả `freshness_lag_seconds`: `now - watermark`.
   - nên trả `data_quality_summary` nếu có.

7. Chưa có logic xử lý late data trong Serving:
   - Speed có watermark Spark 1 phút, Batch có watermark riêng.
   - Cần quy tắc đối với late trades: khi nào provisional bị thay thế, khi nào hiển thị cảnh báo.

### Đề xuất cải thiện query

1. Chuẩn hóa boundary:
   - Batch range: `[start_time, watermark)`
   - Speed range: `[watermark, end_time)`
   hoặc:
   - Batch range: `[start_time, watermark]`
   - Speed range: `(watermark, end_time]`
   Nhưng phải được implement nhất quán ở ClickHouse SQL.

2. Thêm dedupe sau merge:
   - key: `(symbol, window_start)`
   - nếu cùng key tồn tại cả batch và speed, ưu tiên batch.

3. Thêm `source` vào từng candle:
   - `batch`
   - `speed`
   - `merged`

4. Thêm reconciliation metrics:
   - `vwap_mae`
   - `vwap_mape`
   - `volume_delta_pct`
   - `trade_count_delta_pct`
   - số window mismatch

5. Thêm fallback policy:
   - nếu batch unavailable, trả speed với warning.
   - nếu speed unavailable, trả batch với warning.
   - nếu cả hai unavailable, trả lỗi rõ ràng.

6. Tối ưu ClickHouse query:
   - dùng partition pruning theo `window_start`
   - dùng `FINAL` cẩn thận với `ReplacingMergeTree`; chỉ dùng khi cần chống duplicate tức thì vì `FINAL` có thể chậm.
   - cân nhắc materialized view hoặc AggregatingMergeTree nếu volume lớn.

## 7. Logic xây dựng hệ thống nên cải thiện thế nào?

### Ưu tiên cao

1. One-command pipeline đã có:

```bash
python scripts/run_lambda_pipeline.py --mode hybrid --top-n 10 --mock-days 7 --duration-minutes 10
```

Script này nên điều phối:

- service check
- fetch Binance
- generate mock
- replay/mock stream
- start speed layer
- run batch
- check API
- write summary log

2. Chốt tài liệu cho Speed View:

Có 2 hướng:

- Hướng A: giữ ClickHouse `speed_agg` là Speed View chính, cập nhật tài liệu và proposal.
- Hướng B: làm đúng proposal ban đầu, thêm Redis writer và để Query Merger đọc Redis cho vùng realtime.

Nếu mục tiêu là benchmark latency, Redis sẽ giúp thể hiện rõ Speed-only latency thấp. Nếu mục tiêu là đơn giản và dễ query candle history, ClickHouse `speed_agg` thực dụng hơn.

3. Làm Iceberg thành source of truth thật:

- Batch pipeline Docker nên mặc định bật `--write-iceberg --iceberg-strict`.
- Dagster `BATCH_WRITE_ICEBERG` nên bật trong môi trường demo/benchmark.
- Silver/Gold nên hoặc nằm trong Iceberg, hoặc tài liệu phải nói rõ Gold serving nằm ở ClickHouse.

4. Hoàn thiện benchmark compaction:

- lỗi tên method đã được sửa thành `run_bin_packing_compaction()`.
- real-mode không còn fallback ngầm sang simulation.
- cần xử lý JVM `SIGSEGV` trước khi công bố số liệu thật.

5. Tạo test end-to-end:

Một script `scripts/e2e_full_pipeline.py` nên kiểm tra:

- Kafka nhận message
- Speed ghi `speed_agg`
- Batch ghi `batch_agg`
- Watermark tăng
- API `/api/market` trả candle
- CSV log được ghi

### Ưu tiên trung bình

1. Tích hợp dbt thật sự:
   - nếu giữ dbt trong kiến trúc, nên có step chạy dbt hoặc Dagster asset gọi dbt.
   - nếu không dùng dbt, nên giảm vai trò trong tài liệu để tránh bị hỏi khi bảo vệ.

2. Cải thiện Data Quality:
   - lưu rejected records/quarantine vào file hoặc table riêng.
   - log DQ metrics theo run vào ClickHouse hoặc CSV.
   - dashboard hiển thị số lỗi theo `fault_type`.

3. Cải thiện observability:
   - thêm healthcheck API cho Kafka/ClickHouse/Redis/MinIO.
   - thêm endpoint `/api/system/status`.
   - thêm lag: Kafka lag, speed freshness, batch freshness.

4. Cải thiện benchmark:
   - benchmark latency nên phân biệt p50/p95/p99.
   - benchmark reprocess nên có expected truth và error metrics rõ.
   - benchmark compaction nên tách real/simulated.

5. Cập nhật tài liệu:
   - hoàn thiện `docs/md/sys-arch.md`.
   - cập nhật README status.
   - thêm sơ đồ thực tế: Speed -> ClickHouse hay Speed -> Redis.

### Ưu tiên thấp nhưng tốt cho bảo vệ

1. Thêm `Makefile` hoặc PowerShell task:

```bash
make up
make fetch
make mock
make stream
make batch
make bench
make demo
```

2. Thêm architecture decision record:
   - vì sao dùng ClickHouse cho speed view thay Redis, nếu giữ như hiện tại.
   - vì sao watermark theo `window_end`.
   - vì sao batch là ground truth.

3. Thêm dashboard trang "System Readiness":
   - Kafka OK
   - ClickHouse OK
   - batch watermark
   - latest speed window
   - DQ pass rate
   - compaction file count

## 8. Danh sách việc nên làm tiếp theo

1. Chạy benchmark thật và lưu CSV/plot vào `results`.
2. Chạy script tạo small files thật có kiểm soát cho RQ3 trên cụm Docker demo.
3. Tích hợp dbt vào batch pipeline chính hoặc cập nhật tài liệu để giảm vai trò dbt.
4. Bổ sung dashboard chart/metric sâu hơn cho Redis cache nếu muốn theo dõi nhiều symbol cùng lúc.
5. Chạy full Docker smoke test `scripts/e2e_full_pipeline.py` trên máy demo.
6. Bổ sung expected truth rõ hơn cho benchmark reprocess correctness.
7. Cân nhắc cho Query Merger dùng Redis latest cache trong Case 2 ngắn hạn, thay vì chỉ cung cấp endpoint Redis riêng.

## 9. Kết luận cuối

Hệ thống đã đạt mức prototype end-to-end tốt và có đủ nền tảng để trình bày kiến trúc Lambda Lakehouse. Tuy nhiên, chưa nên tuyên bố hoàn thành 100% nếu xét đúng mục tiêu ban đầu. Các phần cần hoàn thiện nhất là tích hợp một-lệnh, xác thực benchmark thật, thống nhất Speed View Redis/ClickHouse, sửa compaction benchmark, và làm Iceberg trở thành master dataset bắt buộc hơn trong đường chạy chính.

Nếu hoàn thành các mục ưu tiên cao ở trên, hệ thống có thể được xem là gần hoàn chỉnh cho mục tiêu TLCN: có kiến trúc rõ, chạy được demo, có benchmark định lượng, có xử lý small file, có batch/speed/serving, và có câu chuyện đối soát dữ liệu thời gian thực tương đối thuyết phục.
