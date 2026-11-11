# BẢNG HẰNG SỐ BẤT BIẾN & SỐ LIỆU ĐẶC TẢ KỸ THUẬT TOÀN HỆ THỐNG
# (GLOBAL CONSTANTS & SINGLE SOURCE OF TRUTH)

> **Mục đích tài liệu:** Tài liệu này là **"Single Source of Truth" (Nguồn sự thật duy nhất)** về toàn bộ định danh, cấu trúc bảng, thông số cấu hình và số liệu thực nghiệm đo đạc từ hệ thống.  
> **Quy tắc bắt buộc:** Mọi bản thảo chương (Chương 1 đến Chương 5) và các bản đặc tả kỹ thuật con (`specs/*.md`) **bắt buộc phải tham chiếu chính xác 100%** các thông số và giá trị trong tài liệu này; tuyệt đối không tự ý làm tròn sai lệch hay suy đoán số liệu khác.  
> **Nguồn trích xuất:** `docker-compose.yml`, `configs/clickhouse/init.sql`, `configs/iceberg/tables.yaml`, `configs/kafka/topics.yaml`, `configs/redis/redis.yaml`, `SHORT_REPORT.md`, `AGENTS.md`, và toàn bộ tệp nhật ký `results/logs/*.csv`.

---

## PHẦN 1: HẠ TẦNG CONTAINER VÀ PHÂN BỔ BỘ NHỚ (DOCKER COMPOSE)

Toàn bộ hệ thống gồm **14 dịch vụ** được định nghĩa trong [docker-compose.yml](file:///docker-compose.yml) trên dải mạng ảo bridge `lakehouse-net` (Subnet: `172.20.0.0/16`). Hệ thống được thiết kế tối ưu trên ngân sách máy cá nhân **16 GB RAM**, trong đó tổng dung lượng RAM cấp phát cho các tiến trình chạy đồng thời 24/7 dao động trong khoảng **7.5 – 8.5 GB RAM**, dành ra ~7.5 – 8.0 GB RAM cho Hệ điều hành (Windows Host) và WSL2.

### 1.1. Bảng Chi Tiết Cấu Hình 14 Dịch Vụ Container

| STT | Tên Service | Tên Container (`container_name`) | Base Image / Dockerfile | Cổng Mạng (Host:Container) | Giới Hạn RAM (`mem_limit`) | Vai Trò & Trạng Thái Hoạt Động |
| :---: | :--- | :--- | :--- | :--- | :---: | :--- |
| **1** | `kafka` | `lakehouse-kafka` | `apache/kafka:3.7.0` | `9092:9092`<br>`9094:9094` (Host) | **1.0 GB** (`1g`) | Message Broker chế độ KRaft (không ZooKeeper). Chạy thường trực 24/7. |
| **2** | `minio` | `lakehouse-minio` | `quay.io/minio/minio:latest` | `9000:9000` (S3 API)<br>`9001:9001` (Console) | **768 MB** (`768m`) | S3-Compatible Object Storage lưu trữ Master Dataset & Parquet. Chạy 24/7. |
| **3** | `minio-setup` | `lakehouse-minio-setup` | `amazon/aws-cli:latest` | *Không mở port* | *Không giới hạn* (thoát ngay) | Khởi tạo 4 bucket S3: `warehouse`, `bronze`, `silver`, `gold` rồi dừng. |
| **4** | `iceberg-rest` | `lakehouse-iceberg-rest` | `tabulario/iceberg-rest:latest` | `8181:8181` | **512 MB** (`512m`) | REST Catalog quản lý metadata bảng Iceberg trên S3. Chạy 24/7. |
| **5** | `clickhouse` | `lakehouse-clickhouse` | `clickhouse/clickhouse-server:latest` | `8123:8123` (HTTP)<br>`9009:9000` (Native TCP) | **1.5 GB** (`1500m`) | OLAP Database Engine lưu Speed View, Batch View, Watermark, Audit. Chạy 24/7. |
| **6** | `redis` | `lakehouse-redis` | `redis:7.2-alpine` | `6379:6379` | **384 MB** (`384m`) | In-Memory Hot Cache cho nến tức thời của Speed Layer. Chạy 24/7. |
| **7** | `speed-layer` | `lakehouse-speed-layer` | `Dockerfile.speed` | *Không mở port ngoài* | **2.0 GB** (`2g`) | Spark Structured Streaming (Micro-batch 5s) Kafka $\rightarrow$ ClickHouse/Redis. Chạy 24/7. |
| **8** | `fastapi` | `lakehouse-fastapi` | `Dockerfile.serving` | `8000:8000` | **512 MB** (`512m`) | Serving Layer Gateway & Auto-Correcting Query Merger API. Chạy 24/7. |
| **9** | `streamlit` | `lakehouse-streamlit` | `Dockerfile.dashboard` | `8501:8501` | **512 MB** (`512m`) | Giao diện Dashboard thời gian thực, trực quan hóa nến & đối soát. Chạy 24/7. |
| **10** | `dagster` | `lakehouse-dagster` | `Dockerfile.dagster` | `3000:3000` | **1.0 GB** (`1g`) | Workflow Orchestration Webserver & Daemon điều phối Batch Pipeline. Chạy 24/7. |
| **11** | `batch-pipeline` | `lakehouse-batch-pipeline` | `Dockerfile.batch` | *Không mở port ngoài* | **3.0 GB** (`3g`) | Spark Batch Job chạy định kỳ hoặc theo yêu cầu (`restart: "no"`). Thoát khi xong. |
| **12** | `spark-master` | `lakehouse-spark-master` | `Dockerfile.spark-thrift` | `7077:7077`<br>`8080:8080` (Web UI) | **768 MB** (`768m`) | Master node cho cụm Spark độc lập (Dùng khi chạy dbt-spark/Thrift). |
| **13** | `spark-worker` | `lakehouse-spark-worker` | `Dockerfile.spark-thrift` | `8081:8081` (Worker UI) | **4.0 GB** (`4g`) | Worker node (2 Cores, 3GB Spark Memory) cho cụm Spark độc lập. |
| **14** | `spark-thrift` | `lakehouse-spark-thrift` | `Dockerfile.spark-thrift` | `10000:10000` (Thrift)<br>`4040:4040` (App UI) | **6.0 GB** (`6g`) | Spark Thrift Server phục vụ kết nối JDBC/ODBC cho `dbt-spark`. |

> [!NOTE]
> **Phân nhóm tài nguyên thực tế:**
> * **Nhóm dịch vụ thường trực 24/7 (Services 1, 2, 4, 5, 6, 7, 8, 9, 10):** Tổng `mem_limit` = $1.0 + 0.75 + 0.5 + 1.5 + 0.375 + 2.0 + 0.5 + 0.5 + 1.0 \approx \mathbf{8.125\text{ GB}}$, hoàn toàn tương thích và an toàn trên phần cứng 16 GB RAM.
> * **Nhóm dịch vụ thực thi theo phiên (Batch/Thrift):** Tiến trình `batch-pipeline` (3GB) hoặc cụm `spark-thrift` được kích hoạt theo lịch biểu của Dagster hoặc lệnh kiểm thử riêng biệt khi cần tái xử lý hoặc chạy mô hình dbt.

---

## PHẦN 2: ĐỊNH DANH CÁC THỰC THỂ DỮ LIỆU & LƯU TRỮ

### 2.1. Apache Kafka Topics & Producer ([configs/kafka/topics.yaml](file:///configs/kafka/topics.yaml))
* **`crypto_trades_raw`**: Topic nạp dòng sự kiện giao dịch trực tiếp từ WebSocket Binance và Fault Injector.
  * Số phân vùng (*Partitions*): `3`
  * Hệ số nhân bản (*Replication Factor*): `1`
  * Thời gian lưu giữ (*Retention*): `86,400,000 ms` (24 giờ)
  * Chính sách dọn dẹp (*Cleanup Policy*): `delete`
  * Phân đoạn log (*Segment Bytes*): `104,857,600 bytes` (100 MB)
* **`crypto_trades_dlq`**: Dead Letter Queue lưu trữ các bản ghi vi phạm cấu trúc nghiêm trọng từ Ingestion Layer.
  * Số phân vùng (*Partitions*): `3` (giá trị vận hành thực tế do broker tự tạo topic; đặc tả logic ban đầu: `1`)
  * Hệ số nhân bản (*Replication Factor*): `1`
  * Thời gian lưu giữ (*Retention*): `86,400,000 ms` (24 giờ, mặc định broker; đặc tả logic ban đầu: `604,800,000 ms` / 7 ngày)
  * Chính sách dọn dẹp (*Cleanup Policy*): `delete`
  * Định danh luồng lỗi (*source_stream*): Giá trị truyền thực tế khi chạy luồng trong [src/ingestion/binance_ws.py](file:///src/ingestion/binance_ws.py) là `"binance_ws"`, trùng với giá trị mặc định của thuộc tính `source_stream` trong lớp `DLQEvent` tại [src/ingestion/models.py](file:///src/ingestion/models.py) (trước bản sửa, mặc định là `"binance_websocket"`).
  * Tạo topic: `SpeedLayerStreamingJob._ensure_topic_exists()` đọc `configs/kafka/topics.yaml` qua `get_kafka_topic_specs()` và tạo mọi topic chưa tồn tại với đúng `partitions`, `replication_factor`, `retention.ms`, `cleanup.policy`; topic đã có không bị sửa.
* **Cấu hình Kafka Producer ([src/ingestion/kafka_producer.py](file:///src/ingestion/kafka_producer.py)):**
  * Cơ chế xác nhận: Sử dụng xác nhận toàn phần **`acks="all"`** (đảm bảo bản ghi được ghi thành công vào toàn bộ các in-sync replicas, tuyệt đối không dùng `acks=1`).
  * Tham số chống mất tin & nén: `retries = 3`, `max_in_flight_requests_per_connection = 1` (bảo toàn thứ tự sự kiện), `compression_type = "gzip"`, `batch_size = 16384 bytes`, `linger_ms = 10 ms`.

### 2.2. Hợp Đồng Dữ Liệu Cấu Trúc (JSON Schema) vs. Kiểm Tra Ngữ Nghĩa (Data Quality Gate)
* **Phạm vi của `datasets/schemas/trade_event_schema.json`:**
  * Chỉ kiểm tra định dạng cấu trúc cú pháp và kiểu dữ liệu JSON cơ bản (`type: object`, `type: integer`, `type: string`, `type: number`).
  * Ràng buộc ngữ nghĩa khớp với Data Quality Gate: `trade_id` `minimum: 0`; `symbol` `minLength: 1`, `pattern: "^[A-Z0-9]+$"`; `price`, `quantity` `minimum: 1e-8`; `trade_time` `exclusiveMinimum: 0`; `ingestion_time` `minimum: 0`; `fault_type` `enum: [duplicate, late_data, out_of_order, schema_invalid, negative_price, null]`.
  * Lược đồ là hợp đồng công bố; không có đoạn mã nào chạy trình kiểm tra JSON Schema lúc vận hành. Việc thực thi ràng buộc do `dq_checks.py` (tầng lô) và bộ lọc Spark (tầng tốc độ) đảm nhận.
* **Cơ chế phân tích cú pháp không chặn (Fail-safe Parsing) trong `from_binance_raw()` ([src/ingestion/models.py](file:///src/ingestion/models.py)):**
  * Không kiểm tra điều kiện `price > 0` tại thời điểm phân tích gói tin WebSocket. Khi thiếu khóa, hàm gán **giá trị canh gác luôn bị DQ Gate chặn**: `symbol = ""` (`symbol_empty`), `trade_id = -1` (`trade_id_negative`), `price = 0.0` / `quantity = 0.0` (`*_non_positive`), `trade_time = 0` khi thiếu cả `T` lẫn `E` (`trade_time_non_positive`). Hàm không bao giờ lấy đồng hồ hệ thống thay cho thời gian sự kiện.
  * **Tách bạch trách nhiệm:** Việc thẩm định giá trị nghiệp vụ (lọc giá âm, khối lượng bằng 0, mã rỗng) được nhường hoàn toàn cho module kiểm tra chất lượng [src/data_quality/dq_checks.py](file:///src/data_quality/dq_checks.py) tại tầng Silver (`price_non_positive`, `trade_id_negative`, `symbol_empty`) để chuyển vào bảng `lakehouse.dq_quarantine`.

### 2.3. Apache Iceberg Tables & Object Storage ([configs/iceberg/tables.yaml](file:///configs/iceberg/tables.yaml))
* **Catalog Type:** `rest` (Endpoint: `http://iceberg-rest:8181`)
* **S3 Warehouse Path:** `s3://warehouse/` (MinIO Endpoint: `http://minio:9000`)
* **Tên Catalog trong Spark:** `iceberg_catalog`
* **Định danh các bảng dữ liệu Medallion:**
  1. **Bronze Table:** `iceberg_catalog.bronze.crypto_trades`
     * Định dạng lưu trữ: Apache Parquet (Snappy compression, Format-Version: 2).
     * Phân vùng (*Partitioning*): `days(trade_time_ts)` (Partition theo ngày UTC).
     * Kích thước tệp mục tiêu (*Target file size*): `134,217,728 bytes` (128 MB).
     * Thuộc tính bảng: `write.distribution-mode = 'hash'`, `history.expire.max-snapshot-age-ms = '604800000'` (7 ngày).
  2. **Silver View / Table (dbt):** `silver_cleaned_trades` (Dữ liệu đã chuẩn hóa, lọc sạch, khử trùng theo `trade_id`, phân vùng theo `trade_date`).
  3. **Gold View / Table (dbt):** `gold_market_aggregates` (Tổng hợp nến 1 phút OHLCV & VWAP phục vụ đối soát).

### 2.4. ClickHouse OLAP Database & Tables ([configs/clickhouse/init.sql](file:///configs/clickhouse/init.sql))
* **Tên Database:** `lakehouse`
* **Danh sách các bảng vật lý:**
  1. **`lakehouse.speed_agg` (Speed View):**
     * Engine: `ReplacingMergeTree(created_at)`
     * Partition key: `toYYYYMMDD(window_start)`
     * Order by (Primary Key): `(symbol, window_start)`
     * Trạng thái dữ liệu: `Provisional` (Gần đúng, SLA < 5 giây).
     * Các cột: `symbol`, `window_start`, `window_end`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `trade_count`, `vwap`, `is_spike`, `created_at`.
  2. **`lakehouse.batch_agg` (Batch View / Ground Truth):**
     * Engine: `ReplacingMergeTree(created_at)`
     * Partition key: `toYYYYMMDD(window_start)`
     * Order by (Primary Key): `(symbol, window_start)`
     * Trạng thái dữ liệu: `Reconciled` (Chuẩn xác 100%, đã qua Deduplication & Data Quality Gate).
     * Các cột: Tương tự `speed_agg`, bổ sung `batch_run_id` (String).
  3. **`lakehouse.system_watermark`:**
     * Engine: `ReplacingMergeTree(updated_at)`
     * Order by: `(layer)`
     * Bản ghi khởi tạo: `layer = 'batch_layer'`, `watermark_time = '1970-01-01 00:00:00.000 UTC'`.
  4. **`lakehouse.dq_quarantine` (Quarantine Table):**
     * Engine: `MergeTree()`
     * Partition key: `toYYYYMM(quarantined_at)`
     * Order by: `(batch_run_id, quarantined_at)`
     * Các cột: `batch_run_id`, `trade_id`, `symbol`, `price`, `quantity`, `trade_time`, `dq_error`, `raw_record`, `quarantined_at`.
  5. **`lakehouse.reconciliation_audit_metrics` (Audit & Telemetry Table):**
     * Engine: `ReplacingMergeTree(created_at)`
     * Partition key: `toYYYYMMDD(window_start)`
     * Order by: `(symbol, window_start)`
     * Các chỉ số lưu trữ: `run_id`, `symbol`, `window_start`, `window_end`, `speed_count`, `batch_count`, `speed_volume`, `batch_volume`, `speed_vwap`, `batch_vwap`, `volume_drift_pct`, `vwap_error`, `sequence_gap_count`, `sequence_gap_rate`, `duplicate_count`, `duplication_ratio`, `convergence_latency_seconds`, `reconciliation_status`, `created_at`.

### 2.5. Redis In-Memory Hot Cache
* **Cổng dịch vụ:** `6379`
* **Bộ nhớ tối đa & Chính sách đẩy rác:** `--maxmemory 256mb --maxmemory-policy allkeys-lru --save ""`
* **Cấu trúc Khóa Redis trong mã nguồn:**
  * Tiền tố mặc định trong code ([src/speed_layer/redis_writer.py](file:///src/speed_layer/redis_writer.py)): `speed:candle` (Thời gian sống `TTL = 3600` giây / 1 giờ, Tiền tố và TTL được `RedisConfig` trong `src/utils/config.py` đọc theo thứ tự ưu tiên: biến môi trường (`REDIS_TTL_SECONDS`, `REDIS_KEY_PREFIX`) > `configs/redis/redis.yaml` (`ttl_seconds: 3600`, `key_prefix: "speed:candle"`) > giá trị mặc định. Thư mục `configs/` được gắn chỉ đọc vào container `speed-layer` và `fastapi`; API `/api/speed/latest` dùng cùng tiền tố cấu hình).
  * Khóa theo dấu thời gian cửa sổ: `speed:candle:{symbol}:{epoch_ms}` (ví dụ: `speed:candle:BTCUSDT:1760688000000`).
  * Khóa con trỏ `:latest`: `speed:candle:{symbol}:latest` (chỉ ghi một lần cho mỗi `symbol` trong mỗi micro-batch, trỏ tới nến có `window_start` lớn nhất của `symbol` đó trong lô; tất cả khóa được gửi trong cùng một pipeline Redis).
* **Payload lưu trữ:** Chuỗi JSON chứa đầy đủ các trường OHLCV, VWAP, trade_count, cờ `is_spike`, nhãn `"status": "provisional"`, và timestamp `"cached_at"`.
* **Hành vi chịu lỗi kết nối:** Khi kết nối hoặc lệnh ghi pipeline thất bại, `RedisSpeedWriter` đặt `_available = False`, lưu `_last_error_time` và hủy client hiện tại. Trong khoảng `reconnect_cooldown_seconds = 30.0` giây sau lỗi, mọi micro-batch bỏ qua Redis; hết khoảng này, lần gọi `get_client()` kế tiếp tự thử kết nối lại (`ping()`), thành công thì đặt lại `_available = True`. Ghi ClickHouse luôn diễn ra trước và độc lập với Redis.

---

## PHẦN 3: CÁC THAM SỐ CẤU HÌNH HỆ THỐNG MẶC ĐỊNH

| Thành Phần Hệ Thống | Tên Tham Số | Giá Trị Mặc Định | Đơn Vị / Ý Nghĩa Kỹ Thuật |
| :--- | :--- | :---: | :--- |
| **Speed Layer** | `window_duration` | `1 minute` | Độ dài cửa sổ Tumbling Window tính OHLCV & VWAP (hỗ trợ `5 minutes`). |
| **Speed Layer** | `watermark_duration` | `1 minute` | Ngưỡng Watermark trễ tối đa (`event_time - 1 minute`) trước khi loại bỏ record. |
| **Speed Layer** | `SPEED_TRIGGER_INTERVAL` | `5 seconds` | Chu kỳ kích hoạt micro-batch Spark Streaming (Bảo đảm SLA < 5s). |
| **Speed Layer** | `KAFKA_MAX_OFFSETS_PER_TRIGGER` | `50000` | Giới hạn tối đa số bản ghi đọc mỗi micro-batch (Chống sập OOM khi Flash Crash). |
| **Speed Layer** | `stateStore.providerClass` | `RocksDBStateStoreProvider` | Nhà cung cấp lưu trữ trạng thái trên ổ đĩa thay vì Heap JVM. **Lưu ý vận hành:** giá trị bị khóa theo checkpoint; volume `speed-checkpoints` hiện tại được tạo với `HDFSBackedStateStoreProvider` nên container vẫn dùng HDFS-backed (nhật ký 2026-11-11 18:55 xác nhận). Chỉ checkpoint mới mới dùng RocksDB. |
| **Speed Layer** | `stateStore.maintenanceInterval` | `60s` | Chu kỳ giải phóng bộ nhớ và dọn rác trạng thái RocksDB. |
| **Speed Layer** | `spike_threshold` | `0.02` (2.0%) | Ngưỡng biến động giá tức thời kích hoạt cảnh báo đột biến (`is_spike = 1`). |
| **Speed Layer** | `volatility_threshold` | `0.03` (3.0%) | Ngưỡng biên độ giá High - Low trong cửa sổ xác định biến động cực đoan. |
| **Serving Layer** | `Watermark Reader Access` | *Truy vấn trực tiếp* | Truy vấn SQL trực tiếp ClickHouse, không dùng In-memory Cache TTL 2s. |
| **Speed Layer** | `REDIS_TTL_SECONDS` | `3600` (1 giờ) | Thời gian tồn tại của nến tạm thời trong Redis (`redis.yaml` đã đồng bộ `3600`, nhưng không được mã nạp). |
| **Speed Layer** | `reconnect_cooldown_seconds` | `30.0` giây | Thời gian chờ trước khi `RedisSpeedWriter` tự thử kết nối lại sau lỗi Redis. |
| **Alerting** | `PRICE_SPIKE` severity | `CRITICAL` nếu Đóng–Mở $\ge 5\%$ hoặc Đỉnh–Đáy $\ge 6\%$; ngược lại `WARNING` | Quy tắc trong `AlertManager.check_price_spike()` khi `is_spike == 1`; được `AutoCorrectingQueryMerger.merge_query()` gọi cho mọi nến trả về, mỗi `(symbol, window_start)` chỉ phát một lần (ghi nhớ tối đa `MAX_SPIKE_ALERT_KEYS = 5000` nến). |
| **Alerting** | `ALERT_DRIFT_THRESHOLD_PCT` / `ALERT_QUARANTINE_THRESHOLD` / `ALERT_LAG_THRESHOLD_SECONDS` | `5.0` / `100` / `15.0` | Ngưỡng cảnh báo độ lệch khối lượng (%), số bản ghi cách ly và độ trễ dữ liệu (giây). |
| **Batch Layer** | `write.target-file-size-bytes` | `134217728` | Kích thước file Parquet chuẩn khi ghi và khi chạy Compaction (128 MB). |
| **Batch Layer** | `history.expire.max-snapshot-age-ms`| `604800000` | Tuổi thọ tối đa của snapshot cũ trước khi dọn rác (7 ngày). |
| **Fault Injector** | `late_min_seconds` / `late_max_seconds` | `60` / `300` | Khoảng thời gian làm lùi `trade_time` khi tiêm lỗi đến muộn (1 - 5 phút). |
| **Fault Injector** | Ngưỡng cộng dồn $t_1..t_4$ | $t_1 = s$, $t_2 = s + l$, $t_3 = s + l + o$, $t_4 = s + l + o + d$ | Một số ngẫu nhiên $u$ cho mỗi sự kiện; xác suất thực tế của mỗi loại lỗi bằng đúng tham số cấu hình khi $s + l + o + d \le 1$. |
| **Fault Injector** | Tỷ lệ trong `bench_fault.py` (`fault_rate = 0.20`) | `schema_invalid` 4%, `late_data` 5%, `out_of_order` 4%, `duplicate` 7% | Cộng thêm `negative_price` 2% sinh trước khi qua `FaultInjector`. |
| **Speed Layer** | Kiểu `trade_id` trong Spark schema | `LongType()` | Khớp với hợp đồng dữ liệu (`integer`); so sánh tie-break theo số nguyên. |
| **Batch Layer** | `BATCH_WATERMARK_LATENESS_SECONDS` | `60` giây | Chỉ áp dụng khi Batch đọc Kafka theo từng đợt: $W = \lfloor \max(\text{trade\_time}) - 60\text{s} \rfloor_{\text{phút}}$. Đọc file (tập đầy đủ) thì $W = \max(\text{window\_end})$. |
| **Batch Layer** | Quy tắc ghi Watermark | Đơn điệu tăng | `ClickHouseBatchSync.update_watermark()` chỉ ghi khi ứng viên lớn hơn mốc hiện tại (`get_current_watermark()`). |
| **Batch Layer** | `BATCH_SILVER_STORE_DIR` | `datasets/processed/silver_store` | Kho Silver tích lũy (JSONL, phân vùng `symbol/ngày/giờ`) dùng để tính lại cửa sổ bị ảnh hưởng ở chế độ Kafka. |
| **Batch Layer** | Thời điểm commit offset Kafka | Sau khi ghi ClickHouse | Ngữ nghĩa *at-least-once*; offset lưu ở `results/logs/batch_kafka_offsets.json` (ghi nguyên tử bằng `os.replace`). |
| **Batch Layer** | `BATCH_FROM_KAFKA` / `BATCH_CONSUME_KAFKA` | `false` | Hai tên được chấp nhận như nhau ở CLI và Dagster. Không có bản ghi Kafka mới thì chu kỳ bỏ qua, **không** fallback sang `datasets/mock`. |
| **Đối soát** | `VWAP_MATCH_TOLERANCE` / `VOLUME_MATCH_TOLERANCE_PCT` | `1e-4` (đơn vị giá) / `1e-4` (%) | Tiêu chí `MATCHED` dùng chung trong [src/utils/reconciliation.py](file:///src/utils/reconciliation.py) cho bảng audit, `/api/reconciliation` và `bench_reprocess.py`. |

> [!NOTE]
> **Cơ chế đọc Watermark tại Serving Layer ([src/serving_layer/watermark_reader.py](file:///src/serving_layer/watermark_reader.py)):**  
> Lớp `WatermarkReader` thực thi truy vấn trực tiếp bảng `lakehouse.system_watermark` trên ClickHouse qua câu lệnh SQL có tham số:  
> `SELECT max(watermark_time), count() FROM lakehouse.system_watermark WHERE layer = {layer:String}` (trước bản sửa 2026-11-12: `ORDER BY updated_at DESC LIMIT 1`, có thể đọc phải mốc nhỏ hơn được ghi sau).  
> Vì bảng là `ReplacingMergeTree(updated_at) ORDER BY layer`, khi gộp phần dữ liệu ClickHouse chỉ giữ dòng có `updated_at` mới nhất; do đó tính đơn điệu phải được bảo đảm ở phía **ghi**, không chỉ ở phía đọc.  
> Khối mã đi kèm xử lý ngoại lệ `try...except` và trả về giá trị dự phòng (*fallback*) an toàn `1970-01-01 00:00:00 UTC` khi chưa có dữ liệu hoặc mất kết nối. **Mã nguồn thực tế không sử dụng bộ đệm (In-memory Cache TTL 2s)** để đảm bảo Serving API luôn phản ánh chính xác 100% mốc Watermark mới nhất mà không bị trễ cache.

### 3.1. Danh Mục Mã Lỗi Phân Loại Dữ Liệu Rác (Quarantine Error Codes)
Được chuẩn hóa trong [src/data_quality/dq_checks.py](file:///src/data_quality/dq_checks.py) và [src/data_quality/quarantine.py](file:///src/data_quality/quarantine.py):
1. **`price_non_positive`**: Bản ghi có mức giá khớp lệnh $\le 0$ (Ví dụ: giá âm do Fault Injector tiêm hoặc lỗi hệ thống sàn).
2. **`quantity_non_positive`**: Khối lượng giao dịch $\le 0$.
3. **`trade_id_negative`**: Mã định danh giao dịch `trade_id` $< 0$.
4. **`symbol_empty`**: Tên cặp tiền giao dịch bị rỗng hoặc không xác định.
5. **`trade_time_non_positive`**: Dấu thời gian giao dịch $\le 0$.
6. **`missing required fields: <danh_sách>`**: Bản ghi thiếu một trong các trường bắt buộc (`trade_id`, `symbol`, `price`, `quantity`, `trade_time`).
7. **`price_not_finite`** / **`quantity_not_finite`** *(bổ sung 2026-11-12)*: Giá hoặc khối lượng là `NaN` / `±inf`. Được kiểm tra trước các luật dấu vì `NaN <= 0` cho kết quả `False` và `json.loads` của Python chấp nhận `NaN`, `Infinity`.
8. **`ERR_DUPLICATE_TRADE_ID`**: Bản ghi bị trùng lặp khóa tự nhiên `(symbol, trade_id)` (Bản ghi đến sau có cùng hoặc nhỏ hơn `ingestion_time` sẽ bị loại bỏ). **Lưu ý thượng tôn mã nguồn:** mã này *không* xuất hiện trong code; bản trùng không vào `dq_quarantine` mà chỉ được đếm vào `duplicates_removed`. Ở chế độ Kafka, bản trùng với lần chạy trước được đếm riêng (`cross_run_duplicates`).

### 3.2. Ghi chú Thực tế Triển khai Mã nguồn (Code-Verified Ground Truth)
1. **Cấu hình Kafka Topic & Producer (`src/ingestion/kafka_producer.py`):**
   - Tệp `configs/kafka/topics.yaml` đã được đồng bộ với giá trị vận hành thực tế (`crypto_trades_dlq`: 3 partitions, retention 24 giờ; ghi chú đặc tả logic ban đầu 1 partition / 7 ngày). Tệp YAML được Speed Layer nạp lúc khởi động để tạo các topic còn thiếu; nếu một producer ghi trước khi Speed Layer chạy, broker tự tạo topic theo mặc định trong `docker-compose.yml` (3 partitions, retention 24 giờ, `replication.factor = 1`) — hai nguồn cho cùng giá trị.
   - Producer cấu hình `acks="all"`, nhưng trên cụm đơn nút (`replication.factor = 1`), tập bản sao đồng bộ (ISR) chỉ có 1 nút chính nên tương đương `acks=1`. Hàm `send_trade()` hoạt động bất đồng bộ: trả về `True` ngay khi đẩy bản ghi vào bộ đệm cục bộ (local buffer) mà không chặn luồng để đợi phản hồi từ broker; lỗi broker phát sinh sau đó được errback ghi nhật ký và đếm vào `async_send_failures`.
2. **Cơ chế Kết nối lại WebSocket (`src/ingestion/binance_ws.py`):**
   - Thuật toán chờ theo hàm mũ (exponential backoff) chỉ kích hoạt khi phát sinh ngoại lệ (`Exception`). Khi kết nối đóng bình thường, vòng lặp kết nối lại tức thời và bộ đếm số lần thử lại được đặt về 0 ngay sau khi `run_forever()` kết thúc.
3. **Phân phối Xác suất của `FaultInjector` sau khi sửa (`src/ingestion/fault_injector.py`):**
   - Hàm vẫn rút một số ngẫu nhiên duy nhất $u \in [0, 1)$ cho mỗi sự kiện, nhưng so sánh với **ngưỡng cộng dồn** $t_1 = s$, $t_2 = s + l$, $t_3 = s + l + o$, $t_4 = s + l + o + d$ (với $s, l, o, d$ lần lượt là `schema_invalid_rate`, `late_data_rate`, `out_of_order_rate`, `duplicate_rate`). Mỗi loại lỗi chiếm một đoạn rời nhau $[t_{k-1}, t_k)$ nên xác suất thực tế bằng đúng tham số cấu hình (điều kiện $s + l + o + d \le 1$).
   - Với `fault_rate = 0.20` trong `bench_fault.py`: `schema_invalid` **4%**, `late_data` **5%**, `out_of_order` **4%**, `duplicate` **7%** (tổng 20% số sự kiện đi qua `FaultInjector`), cộng thêm `negative_price` **2%** được sinh trước bước tiêm lỗi.
   - **Lịch sử:** Trước bản sửa, các ngưỡng không cộng dồn khiến tỷ lệ thực tế chỉ còn 4% / 1% / 0% / 2%; các đợt chạy `bm_fault_20261027_*` được đo với phiên bản lỗi này và không còn dùng làm số liệu chính thức.

---

## PHẦN 4: TOÀN BỘ SỐ LIỆU THỰC NGHIỆM ĐO ĐẠC BẤT BIẾN (RESULTS/LOGS)

### 4.1. Benchmark 1: Độ Trễ Truy Vấn Phân Tích (Query Latency — RQ1)
* **Tệp log gốc:** [results/logs/bench_latency.csv](file:///results/logs/bench_latency.csv) (Tổng cộng **2,205 lượt truy vấn** qua 4 đợt; đợt chính thức 2026-11-11 có 540 lượt = 2 symbol × 3 kịch bản × 3 chiến lược × 30 lần) và [results/logs/bench_latency_summary.csv](file:///results/logs/bench_latency_summary.csv).
* **Mốc thời gian:** $T_{\text{now}}$ là `MAX(window_start)` của `batch_agg` (đợt 2026-11-11: `2026-10-17 09:59 UTC`), không phải giờ hệ thống; watermark giả lập = $T_{\text{now}} - 24\text{h}$.
* **Phương pháp phân vị:** nội suy tuyến tính (mặc định numpy). Lưu ý `bench_latency_summary.csv` dùng phương pháp hạng dưới của `_percentile()` nên lệch vài phần trăm mili-giây so với các bảng dưới đây.
* **3 Kịch bản đo lường:**
  * **Q1 (Historical Query):** Khoảng thời gian $[T_{\text{now}} - 48\text{h}, T_{\text{now}} - 24\text{h}]$ (100% nằm trong Batch View).
  * **Q2 (Realtime Query):** Khoảng thời gian $[T_{\text{now}} - 5\text{m}, T_{\text{now}}]$ (100% nằm trong Speed View).
  * **Q3 (Hybrid Query):** Khoảng thời gian $[T_{\text{now}} - 30\text{m}, T_{\text{now}}]$ (Cắt ngang qua System Watermark).
* **3 Chiến lược đối sánh:**
  * **`batch_only`**: Chỉ quét bảng `lakehouse.batch_agg` trên ClickHouse.
  * **`speed_only`**: Chỉ quét bảng `lakehouse.speed_agg` trên ClickHouse.
  * **`lambda`**: Sử dụng thuật toán `AutoCorrectingQueryMerger` ghép nối tự động.

#### Bảng Phân Phối Độ Trễ Chi Tiết (Đợt Thực Nghiệm Chính Thức Gần Nhất — 2026-11-11, `BTCUSDT`):
*(Mỗi kịch bản 30 lần đo; số nến trả về trung bình: Q1 = 105, Q2 = 5, Q3 = 30 cho cả 3 chiến lược)*

| Loại Truy Vấn (`query_type`) | Chiến Lược Truy Vấn (`strategy`) | Số Lần Chạy (`count`) | Mean Latency (ms) | **P50 Latency (ms)** | **P95 Latency (ms)** | **P99 Latency (ms)** | Min Latency (ms) | Max Latency (ms) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Q1 (Historical)** | `batch_only` | 30 | 60.91 | **63.66** | 73.67 | 109.65 | 16.24 | 124.12 |
| **Q1 (Historical)** | `speed_only` | 30 | 62.08 | **60.09** | 75.80 | 112.84 | 11.17 | 127.68 |
| **Q1 (Historical)** | `lambda` | 30 | 67.54 | **65.42** | 100.05 | 122.02 | 50.01 | 129.18 |
| **Q2 (Realtime)** | `batch_only` | 30 | 59.07 | **55.88** | 96.17 | 121.85 | 18.64 | 125.26 |
| **Q2 (Realtime)** | `speed_only` | 30 | 59.69 | **58.74** | 96.70 | 102.78 | 14.71 | 103.49 |
| **Q2 (Realtime)** | `lambda` | 30 | 59.76 | **59.44** | 99.79 | 135.07 | 12.15 | 141.63 |
| **Q3 (Hybrid)** | `batch_only` | 30 | 56.64 | **60.28** | 79.51 | 96.30 | 10.55 | 100.89 |
| **Q3 (Hybrid)** | `speed_only` | 30 | 62.91 | **58.73** | 96.21 | 109.58 | 48.82 | 110.26 |
| **Q3 (Hybrid)** | `lambda` | 30 | 67.63 | **64.51** | 117.19 | 128.15 | 11.17 | 130.05 |

#### Bảng Tổng Hợp Theo Chiến Lược (Overall Strategy Latency, `BTCUSDT`):

| Chiến Lược Truy Vấn | Đợt | Tổng Số Mẫu | Mean Latency (ms) | **Median P50 (ms)** | **P95 Latency (ms)** | **P99 Latency (ms)** | Phạm Vi Biến Thiên (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`batch_only`** | 2026-11-11 | 90 | 58.87 | **60.12** | 80.54 | 124.24 | 10.55 – 125.26 |
| **`speed_only`** | 2026-11-11 | 90 | 61.56 | **58.94** | 96.70 | 112.18 | 11.17 – 127.68 |
| **`lambda` (Query Merger)** | 2026-11-11 | 90 | 64.98 | **62.84** | 114.71 | 131.32 | 11.17 – 141.63 |
| `batch_only` | 2026-10-23 (lịch sử) | 90 | 57.15 | 58.35 | 66.26 | 91.03 | 48.79 – 131.46 |
| `speed_only` | 2026-10-23 (lịch sử) | 90 | 54.70 | 51.58 | 62.10 | 71.72 | 39.96 – 76.26 |
| `lambda` (Query Merger) | 2026-10-23 (lịch sử) | 90 | 54.61 | 50.92 | 61.90 | 68.70 | 16.23 – 71.78 |

*Tính cả `ETHUSDT` (180 mẫu/chiến lược, đợt 2026-11-11): P50 `batch_only` 60.26 ms, `speed_only` 58.08 ms, `lambda` 62.35 ms; P99 lần lượt 124.36 / 108.42 / 129.36 ms.*

> [!IMPORTANT]
> **Kết luận trả lời RQ1 (cập nhật sau đợt đo 2026-11-11):**
> * Đợt đo lại **không tái lập** được ưu thế độ trễ của `lambda` ghi nhận ở đợt 2026-10-23. Ở đợt mới, `lambda` có P50 cao nhất (**62.84 ms**, so với 60.12 ms của `batch_only` và 58.94 ms của `speed_only`) và P99 cao nhất (**131.32 ms**).
> * Chênh lệch P50 giữa ba chiến lược chỉ khoảng 2–4 ms, nhỏ hơn mức dao động của chính từng chiến lược giữa hai đợt (ví dụ P50 `speed_only` 51.58 → 58.94 ms). Với kích thước dữ liệu hiện tại (5–105 nến mỗi truy vấn), độ trễ bị chi phối bởi chi phí khứ hồi tới ClickHouse, không phải bởi lượng dữ liệu quét.
> * Kết luận có thể bảo vệ: Query Merger **không làm tăng đáng kể** độ trễ so với truy vấn trực tiếp một bảng (≤ ~5 ms ở P50), trong khi trả về kết quả đã ghép đúng theo watermark. Không đủ bằng chứng để khẳng định `lambda` nhanh hơn `batch_only`. Ở Q3, `lambda` phải gửi ba truy vấn (Batch, Speed, biên watermark) thay vì một, nên chậm hơn là hợp lý về cơ chế.

---

### 4.2. Benchmark 2: Độ Chính Xác Tái Xử Lý & Sai Số Đối Soát (Reprocessing Correctness — RQ2)
* **Tệp log gốc:** [results/logs/bench_reprocess_summary.csv](file:///results/logs/bench_reprocess_summary.csv) và [results/logs/bench_reprocess.csv](file:///results/logs/bench_reprocess.csv).
* **Số lượng bản ghi kiểm toán:** đợt chính thức 2026-11-11 (`run_id = bm2_20261111_190017`) ghi **112 bản ghi** (57 BTCUSDT + 55 ETHUSDT) vào `lakehouse.reconciliation_audit_metrics`; đợt 2026-10-23 trước đó ghi 116 bản ghi.
* **Cửa sổ đo:** $[T_{\text{ref}} - 2\text{h}, T_{\text{ref}} - 1\text{h}]$ với $T_{\text{ref}}$ = `MAX(window_start)` của `batch_agg` (`2026-10-17 09:59 UTC`). Chỉ những phút có nến trong `batch_agg` mới được so khớp (57 và 55 trên 60 phút).

> [!WARNING]
> **Giới hạn phương pháp của Benchmark 2:** Ở chế độ mặc định (`force_seed = True`), `bench_reprocess.py` gọi `seed_speed_view.py` để **tạo lại Speed View từ chính Batch View cộng nhiễu ngẫu nhiên có seed cố định**: khối lượng nhân hệ số $U(0.95, 0.985)$, VWAP lệch theo $\mathcal{N}(0.0002, 0.0012)$. Kỳ vọng sai số tuyệt đối của nhiễu này là $\approx 0.0012\sqrt{2/\pi} \approx 0.096\%$, khớp với MAPE đo được (0.0937% – 0.1020%). Như vậy MAPE trong bảng dưới **phản ánh tham số nhiễu mô phỏng**, không phải sai số thực của Spark Structured Streaming so với Batch. Sai số thực của luồng (do sự kiện trễ bị loại) được đo ở các kịch bản hỗn loạn của Phần 4.5 ($\Delta V_{\%} = 19.70\% - 25.65\%$ trước đối soát).

#### Bảng Sai Số Giữa Speed View (Gần đúng) và Batch View (Ground Truth):

| Cặp Giao Dịch (`symbol`) | Khung Giờ Đo Đạc (UTC) | Số Nến Khớp Lệnh (`matched_candles`) | Tỷ Lệ Bao Phủ (`coverage_pct`) | MAE (Mean Absolute Error) | **MAPE (%) (Mean Abs Pct Error)** | RMSE (Root Mean Squared Error) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **BTCUSDT** (Run 2026-11-11, chính thức) | 2026-10-17 `07:59` $\rightarrow$ `08:59` | 57 | 100.0% | 61.27 | **0.0937%** | 77.19 |
| **ETHUSDT** (Run 2026-11-11, chính thức) | 2026-10-17 `07:59` $\rightarrow$ `08:59` | 55 | 100.0% | 3.21 | **0.1020%** | 3.96 |
| **BTCUSDT** (Run 2026-10-23) | `05:42` $\rightarrow$ `06:42` | 60 | 100.0% | 101.68 | **0.1181%** | 129.15 |
| **BTCUSDT** (Run 2026-10-23) | `06:19` $\rightarrow$ `07:19` | 60 | 100.0% | 77.58 | **0.0937%** | 97.39 |
| **BTCUSDT** (Run 2026-09-29) | `06:08` $\rightarrow$ `07:08` | 120 | 100.0% | 65.30 | **0.0821%** | 81.47 |
| **ETHUSDT** (Run 2026-09-29) | `06:08` $\rightarrow$ `07:08` | 120 | 100.0% | 2.31 | **0.0948%** | 2.87 |
| **SOLUSDT** (Run 2026-09-29) | `14:23` $\rightarrow$ `16:10` | 120 | 100.0% | 0.0819 | **0.0584%** | 0.0901 |
| **BNBUSDT** (Run 2026-09-29) | `14:23` $\rightarrow$ `16:10` | 120 | 100.0% | 0.3639 | **0.0626%** | 0.4028 |
| **XRPUSDT** (Run 2026-09-29) | `14:23` $\rightarrow$ `16:10` | 120 | 100.0% | 0.00033 | **0.0574%** | 0.00037 |

#### So Sánh Trước và Sau Khi Tiến Trình Batch Tái Xử Lý (Reconciliation Impact):

| Chỉ Số Tài Chính Đối Soát | Trước Khi Đối Soát (Speed View tức thời) | Sau Khi Đối Soát (Batch Reconciled Ground Truth) | Trạng Thái Hội Tụ |
| :--- | :---: | :---: | :---: |
| **Sai số giá đóng cửa (Close Price Error)** | $\approx 0.00\text{ USD}$ ($0.000\%$) | **$0.00\text{ USD}$ ($0.000\%$)** | Khớp tuyệt đối |
| **Sai số giá gia quyền (VWAP Error $\varepsilon_{\text{VWAP}}$)** | $0.057\% - 0.118\%$ (Lệch do late data) | **$0.000\text{ USD}$ ($0.000\%$)** | Triệt tiêu hoàn toàn về 0 |
| **Độ lệch khối lượng ($\Delta V_{\%}$)** | $9.09\% - 24.20\%$ (Do sự kiện trễ bị drop) | **$0.000\%$** | Triệt tiêu hoàn toàn về 0 |
| **Tỷ lệ mất chuỗi ($R_{\text{gap}}$)** | $25.00\%$ (Khi rớt mạng WebSocket) | **$0.000\%$** (Sau REST Backfill) | Hàn gắn 100% |
| **Thời gian tự phục hồi ($T_{\text{conv}}$)** | — | **$0.0035\text{s} - 0.252\text{s}$** | Hội tụ tức thời sau Batch Run |

---

### 4.3. Benchmark 3: Hiệu Quả Gộp Tệp Nhỏ Apache Iceberg (Compaction Efficiency — RQ3)
* **Tệp log gốc:** [results/logs/bench_compaction.csv](file:///results/logs/bench_compaction.csv).
* **Chế độ kiểm thử:** `mode = real` (Đo đạc trực tiếp trên bảng vật lý Iceberg qua REST Catalog và MinIO S3, loại trừ các dòng giả lập `simulated = True`).

* **Đợt chính thức:** 2026-11-11 19:02 UTC, chạy trong container `batch-pipeline`: `prepare_small_files.py --chunk-size 25 --max-records 1000 --repeat 2` rồi `bench_compaction.py --runs 20` (20 lần đọc trước và 20 lần đọc sau compaction, lọc `symbol = BTCUSDT`). Chiến lược `binpack`, `target = 128 MB`, `min_size = 64 MB`.

#### Bảng Kết Quả Đợt Chính Thức (2026-11-11, `runs = 20`):

| Chỉ Số | Trước Compaction (`before`) | Sau Compaction (`after`) | Thay Đổi |
| :--- | :---: | :---: | :---: |
| **Số file Parquet (`file_count`)** | 118 | **10** | **Giảm 91.5%** |
| **Tổng dung lượng dữ liệu** | 114.37 MB | 105.97 MB | −7.3% |
| **Kích thước file trung bình** | 0.97 MB (min 0.00, max 3.57) | **10.60 MB** (min 1.18, max 14.75) | ×10.9 |
| **Độ trễ đọc trung bình (20 lần, gồm lần đầu)** | 2,220.0 ms | 1,226.1 ms | 1.81× (`speedup_ratio` trong CSV) |
| **Lần đọc đầu tiên (cold read)** | 8,740.5 ms | 1,245.1 ms | — |
| **Độ trễ đọc trung bình (bỏ lần đầu, 19 lần)** | 1,876.8 ms | 1,225.1 ms | **1.53×** |
| **Độ trễ đọc trung vị (bỏ lần đầu)** | 1,821.8 ms | 1,206.4 ms | **1.51×** |
| **Độ lệch chuẩn (20 lần)** | 1,553.1 ms | 55.5 ms | — |
| **Thời gian compaction (`compaction_duration_s`)** | — | **17.54 giây** (Spark rewrite 17.18 s) | 118 file viết lại → 10 file |

> [!IMPORTANT]
> **Cách đọc hệ số tăng tốc:** `bench_compaction.py` tính `speedup_ratio` bằng tỉ số hai **giá trị trung bình** gồm cả lần đọc đầu tiên. Lần đọc đầu của pha `before` là lần đọc lạnh (khởi tạo JVM, nạp metadata, tải 118 file từ MinIO) và chậm gấp ~4.8 lần các lần còn lại, nên đẩy hệ số lên 1.81×. Pha `after` chạy sau khi bộ nhớ đệm đã nóng nên không có lần đọc lạnh tương ứng. Con số có thể bảo vệ là **≈ 1.5× (trung vị 1.51×, trung bình bỏ lần đầu 1.53×)**. Lợi ích rõ nhất của compaction là **giảm số file 91.5%** và **làm độ trễ ổn định hơn** (độ lệch chuẩn 1,553 → 55.5 ms).

#### Bảng Lịch Sử Các Đợt Cũ (2026-10-23, `runs = 2` — chỉ tham khảo):

> [!WARNING]
> Các đợt dưới đây chỉ đo 2 lần đọc mỗi pha, và lần đọc `before` đầu tiên là lần đọc lạnh (~8.18 s so với ~2.5 s các lần sau). Vì vậy hệ số ~2.1× **bị thổi phồng bởi chi phí khởi động lạnh** và không dùng làm kết luận chính.

| Đợt Chạy Thực Nghiệm | Trạng Thái Nén (`phase`) | Số Lượng File Parquet (`file_count`) | Tỷ Lệ Giảm Số File (%) | Thời Gian Đọc Truy Vấn (ms) | Thời Gian Nén (`compaction_duration_s`) | **Hệ Số Tăng Tốc (Speedup Ratio)** |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Run 1 (2026-10-23 07:24)** | Trước Compaction (`before`) | **20 files** | — | 8,180.69 ms | — | — |
| | Sau Compaction (`after`) | **11 files** | **Giảm 45.0%** | 2,437.92 ms | 1.76 giây | **2.095x** |
| **Run 2 (2026-10-23 07:25)** | Trước Compaction (`before`) | **19 files** | — | 7,895.30 ms | — | — |
| | Sau Compaction (`after`) | **11 files** | **Giảm 42.1%** | 2,455.21 ms | 1.63 giây | **2.165x** |
| **Run 3 (2026-10-23 06:59)** | Trước Compaction (`before`) | **18 files** | — | 8,832.35 ms | — | — |
| | Sau Compaction (`after`) | **10 files** | **Giảm 44.4%** | 3,188.79 ms | 1.68 giây | **1.893x** |

#### Thông Số Kỹ Thuật Tổng Hợp Định Lượng (Đợt Chính Thức 2026-11-11):
* **Số lượng tệp tin:** Giảm từ **118 tệp xuống còn 10 tệp** (**$-91.5\%$**). Các đợt cũ (18–20 → 10–11 tệp, $-42\%$ đến $-45\%$) có ít tệp nhỏ hơn vì chỉ chuẩn bị một lượt dữ liệu.
* **Kích thước trung bình mỗi tệp:** Tăng từ **$0.97\text{ MB}$ lên $10.60\text{ MB}$**. Vẫn thấp hơn nhiều so với đích 128 MB vì tổng dữ liệu bảng chỉ ~106 MB và được chia theo phân vùng.
* **Thời gian nén thực tế:** **$17.54\text{ giây}$** để viết lại 118 tệp (các đợt cũ 1.63–1.76 giây cho 18–20 tệp).
* **Tốc độ đọc phân tích:** Tăng tốc **$\mathbf{\approx 1.5\times}$** ở trạng thái nóng (trung vị 1,821.8 → 1,206.4 ms). Con số 1.81× trong CSV gồm cả lần đọc lạnh đầu tiên.

---

### 4.4. Benchmark 4: Kiểm Soát Chất Lượng & Phân Loại Lỗi (Data Quality Gate)
* **Tệp log gốc:** [results/logs/bench_fault_summary.csv](file:///results/logs/bench_fault_summary.csv) và [results/logs/bench_fault_results.csv](file:///results/logs/bench_fault_results.csv).
* **Kịch bản thực thi chính thức:** `Run ID = bm_fault_20261111_185324` (Thực hiện 3 lần lặp, `count = 5000` sự kiện gốc mỗi lượt, `fault_rate = 0.20`, seed `42 + iteration`, chạy sau khi sửa `FaultInjector` sang ngưỡng cộng dồn và đồng bộ tiêu đề CSV). Đợt `bm_fault_20261111_181102` cho số đếm giống hệt (cùng seed), chỉ khác thời gian xử lý. Các đợt `bm_fault_20261027_072857` và `bm_fault_20261027_073031` đo trên phiên bản `FaultInjector` cũ và chỉ giữ làm lịch sử.

#### Ma Trận Nhầm Lẫn (Confusion Matrix) & Chỉ Số Phân Loại:

| Chỉ Số Đo Lường (`Metric`) | Giá Trị Thực Nghiệm | Diễn Giải Ý Nghĩa Kỹ Thuật |
| :--- | :---: | :--- |
| **Tổng số bản ghi xử lý (`total_records_sum`)** | **16,018** | Tổng bản ghi qua 3 vòng lặp (5,318 + 5,354 + 5,346), gồm cả bản sao `duplicate`. |
| **True Positives (TP)** | **874** | Bản ghi lỗi (`schema_invalid`, `negative_price`) được Gate phát hiện chính xác và đưa vào Quarantine (309 + 274 + 291). |
| **False Positives (FP)** | **0** | Bản ghi hợp lệ bị Gate chặn nhầm (Lỗi loại I — không xảy ra). |
| **False Negatives (FN)** | **0** | Bản ghi lỗi lọt qua Gate vào tầng Silver (Lỗi loại II — không xảy ra). |
| **True Negatives (TN)** | **14,126** | Bản ghi hợp lệ được cho đi tiếp vào Silver (4,691 + 4,726 + 4,709). |
| **Bản ghi trùng bị khử (`duplicates_removed`)** | **1,018** | 318 + 354 + 346; nằm ngoài ma trận nhầm lẫn vì không thuộc `clean_records` lẫn `rejected_records`. |
| **Precision (Độ chuẩn xác)** | **1.0000 (100.0%)** | $TP / (TP + FP) = 874 / (874 + 0) = 1.0$. |
| **Recall (Độ phủ phát hiện)** | **1.0000 (100.0%)** | $TP / (TP + FN) = 874 / (874 + 0) = 1.0$. |
| **F1-Score (Cân bằng điều hòa)** | **1.0000 (100.0%)** | $2 \times (Precision \times Recall) / (Precision + Recall) = 1.0$. |
| **Accuracy (`mean_accuracy`)** | **0.9365 (93.65%)** | Trung bình 3 lượt (0.9402, 0.9339, 0.9353) của $(TP + TN) / \text{Total}$; giá trị nhỏ hơn 1 chỉ vì 1,018 bản ghi trùng đã khử nằm trong mẫu số: $(874 + 14126) / 16018 = 0.9364$. |
| **Tỷ lệ cho qua hợp lệ (`pass_rate_pct`)** | **88.19%** | Trung bình 3 lượt (88.21%, 88.27%, 88.08%); $14126 / 16018$. |
| **Tỷ lệ từ chối cách ly (`rejection_rate_pct`)**| **5.46%** | Trung bình 3 lượt (5.81%, 5.12%, 5.44%); $874 / 16018$. |
| **Thời gian xử lý trung vị (`median_duration_ms`)**| **14.26 ms** | Trung vị 3 lượt (12.51, 30.27, 14.26 ms) cho khoảng 5,300 bản ghi mỗi lượt. |
| **Thông lượng kiểm tra (`median_throughput_rps`)** | **374,895 bản ghi/giây** | Trung vị 3 lượt (425,100; 176,875; 374,895). Thời gian và thông lượng dao động giữa các đợt do tải máy; các chỉ số phân loại thì tất định. |

> [!NOTE]
> Tệp `results/logs/bench_fault_results.csv` đã được đồng bộ tiêu đề 21 cột (thêm `dedup_elimination_rate_pct` sau `duplicates_removed`). Các dòng của `bm_fault_20261027_072857` để trống cột này vì được sinh bởi phiên bản script cũ. `bench_fault.py` gọi `align_csv_header()` trước mỗi lần ghi, nên tiêu đề không còn lệch khi danh sách cột thay đổi.

---

### 4.5. Kết Quả Kiểm Toán Giao Thức Hỗn Loạn (Chaos Smoke Test Protocol)
* **Tệp log gốc:** [results/logs/chaos_smoke_tests.csv](file:///results/logs/chaos_smoke_tests.csv).
* **Kết quả tổng quát:** Đạt trạng thái **PASS 100% (4/4 Test Cases)**, được cấp quyền sẵn sàng vận hành liên tục 24/7 (*Readiness Gate Decision: GO*).

#### Bảng Kiểm Toán Chi Tiết 4 Kịch Bản Lỗi Phân Tán:

| Mã Kịch Bản | Tên Kịch Bản Kiểm Toán Phân Tán | Dị Thường Tiêm Vào Hệ Thống (`Injected Anomaly`) | Độ Lệch Trước Đối Soát (`Pre-Reconciliation Drift`) | Trạng Thái Sau Đối Soát (`Post-Reconciliation Drift`) | Thời Gian Tự Phục Hồi ($T_{\text{conv}}$) | Kết Quả Nghiệm Thu |
| :---: | :--- | :--- | :--- | :--- | :---: | :---: |
| **TC1** | **WebSocket Drop & Gap Healing** | Ngắt collector, cố tình cắt bỏ 50 mã `trade_id` liên tiếp trong luồng stream. | $R_{\text{gap}} = 25.0\%$<br>($\Delta V_{\%} = 24.20\% - 25.65\%$) | **$R_{\text{gap}} = 0.0\%$**<br>(**$\Delta V_{\%} = 0.0\%$**) | **0.120 giây** | **PASS** |
| **TC2** | **Late-Arriving Events (2x Watermark)** | Bơm 40 bản ghi bị trễ 120 giây (Vượt quá ngưỡng Watermark 60 giây của Speed Layer). | $\Delta V_{\%} = 19.70\% - 20.67\%$ (Speed Layer drop an toàn giữ RAM). | **$\Delta V_{\%} = 0.0\%$**<br>(Batch Layer cứu dữ liệu theo `event_time`). | **0.251 giây** | **PASS** |
| **TC3** | **Protocol Seam Duplicate Flood** | Phát lặp lại 1,000 bản ghi Kafka 2 lần (Tổng 2,000 sự kiện) mô phỏng ranh giới reconnect. | $R_{\text{dup}} = 50.0\%$ (Khối lượng giao dịch bị đếm nhân đôi ảo). | **$R_{\text{dup}} = 0.0\%$**<br>(ReplacingMergeTree & DQ Gate khử trùng). | **0.185 giây** | **PASS** |
| **TC4** | **10x Flash Crash Replay Burst** | Đột biến tải gấp 10 lần tốc độ bình thường (Bơm 5,000 bản ghi trong vài giây). | Tải bùng nổ 5,000 events/burst (Nguy cơ nghẽn I/O và tràn RAM). | **Xử lý trọn vẹn 5,000 events**<br>(**0 lỗi OOM**, RAM duy trì ổn định). | **0.080 giây** | **PASS** |

---

## PHẦN 5: BẢNG TRA CỨU ÁNH XẠ CÂU HỎI NGHIÊN CỨU (RQ MAPPING)

| Câu Hỏi Nghiên Cứu | Nội Dung Khẳng Định Học Thuật | Số Liệu Minh Chứng Cốt Lõi | Tệp Dữ Liệu Nguồn |
| :--- | :--- | :--- | :--- |
| **RQ1: Latency Overhead** | Query Merger của kiến trúc Lambda trả về kết quả hợp nhất Batch + Speed theo watermark mà **không làm tăng đáng kể** độ trễ so với truy vấn trực tiếp một bảng ClickHouse. (Không khẳng định Lambda nhanh hơn Batch-only.) | • Đợt 2026-11-11 (BTCUSDT, 90 mẫu/chiến lược): P50 `lambda` **62.84 ms**, `batch_only` 60.12 ms, `speed_only` 58.94 ms; chênh lệch 2–4 ms, nằm trong biên dao động giữa các đợt.<br>• P99 `lambda` **131.32 ms**; Q3 `lambda` gửi 3 truy vấn thay vì 1.<br>• Ưu thế ở đợt 2026-10-23 (P50 50.92 ms) **không tái lập**. | [bench_latency.csv](file:///results/logs/bench_latency.csv) |
| **RQ2: Reprocessing Accuracy** | Speed View tức thời chấp nhận sai số để đổi lấy độ trễ thấp, và chu kỳ Batch đưa các cửa sổ về giá trị đối soát (sai số 0). | • Sai số thực của luồng (kịch bản hỗn loạn TC1–TC2): $\Delta V_{\%} \approx 19.70\% - 25.65\%$ trước đối soát, $0.00\%$ sau đối soát, $T_{\text{conv}} \le 0.252\text{s}$.<br>• Benchmark 2 (2026-11-11): MAPE VWAP 0.0937% (BTC) / 0.1020% (ETH). **Lưu ý:** Speed View ở benchmark này được sinh từ Batch View cộng nhiễu mô phỏng, nên MAPE phản ánh tham số nhiễu (kỳ vọng ≈ 0.096%), không phải sai số thực của Spark. | [bench_reprocess_summary.csv](file:///results/logs/bench_reprocess_summary.csv)<br>[chaos_smoke_tests.csv](file:///results/logs/chaos_smoke_tests.csv) |
| **RQ3: Compaction Efficiency** | Compaction bin-pack trên Apache Iceberg giảm mạnh số tệp nhỏ và làm độ trễ đọc phân tích nhanh hơn, ổn định hơn. | • Đợt 2026-11-11 (`runs = 20`): số file Parquet **118 → 10 (−91.5%)**, kích thước trung bình **0.97 → 10.60 MB**, thời gian nén 17.54 s.<br>• Độ trễ đọc ở trạng thái nóng: trung vị **1,821.8 → 1,206.4 ms (≈ 1.51×)**; độ lệch chuẩn 1,553 → 55.5 ms.<br>• Hệ số 1.81× (CSV) và ~2.1× (các đợt cũ `runs = 2`) bao gồm lần đọc lạnh nên bị thổi phồng. | [bench_compaction.csv](file:///results/logs/bench_compaction.csv) |
---

## PHẦN 6: DANH MỤC TÀI LIỆU THAM KHẢO CHUẨN HÓA (XẾP THEO THỨ TỰ TỪ ĐIỂN A-Z)

> **Quy tắc bắt buộc (Mẫu 5 - HCMUTE):** Đánh số thứ tự theo thứ tự từ điển (A-Z) của tên tác giả. Mọi trích dẫn trong Chương 1 đến Chương 5 phải dùng đúng mã số `[1]` đến `[8]` dưới đây:

1. `[1]` Akidau T., Bradshaw R., Chambers C., Chernyak S., Fernández-Moctezuma R. J., Lax R., McVeety S., Mills D., Perry F., Schmidt E., Whittle S., *The Dataflow Model: A practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing*, Proceedings of the VLDB Endowment, Vol. 8 (12), 2015, pp. 1792 - 1803.
2. `[2]` Apache Iceberg Community, *Apache Iceberg Table Format Specification v2*, Apache Software Foundation, 2024.
3. `[3]` Armbrust M., Ghodsi A., Xin R., Zaharia M., *Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics*, Proceedings of the 11th Annual Conference on Innovative Data Systems Research (CIDR 2021), Virtual Event, Jan. 11-15, 2021, pp. 1 - 8.
4. `[4]` ClickHouse Inc., *ClickHouse: Fast Open-Source OLAP Database Management System Documentation*, ClickHouse Official Technical Reference, 2024.
5. `[5]` Kleppmann M., *Designing Data-Intensive Applications: The big ideas behind reliable, scalable, and maintainable systems*, 1st Edition, O'Reilly Media, Sebastopol, CA, 2017.
6. `[6]` Kreps J., Narkhede N., Rao J., *Kafka: A distributed messaging system for log processing*, Proceedings of the 6th International Workshop on Networking Meets Databases (NetDB), Athens, Greece, Jun. 12, 2011, pp. 1 - 7.
7. `[7]` Marz N., Warren J., *Big Data: Principles and best practices of scalable realtime data systems*, 1st Edition, Manning Publications, Shelter Island, NY, 2015.
8. `[8]` Zaharia M., Xin R. S., Wendell P., Das T., Armbrust M., Dave A., Meng X., Rosen J., Venkataraman S., Franklin M. J., Ghodsi A., Gonzalez J., Shenker S., Stoica I., *Apache Spark: A unified engine for big data processing*, Communications of the ACM, Vol. 59 (11), 2016, pp. 56 - 65.

---

## PHẦN 7: HẰNG SỐ ĐỊNH DẠNG VĂN BẢN & ĐÓNG GÓI BÁO CÁO (MẪU 5 - HCMUTE)

### 7.1. Thông Số Định Dạng Trang & Phông Chữ
- **Khổ giấy:** A4 đứng (`210mm x 297mm`), độ dài toàn văn: **50 – 100 trang** (Mục tiêu thiết kế: **75 – 85 trang**).
- **Căn lề trang:** Lề trên `2 cm`, Lề dưới `2 cm`, Lề trái `3 cm`, Lề phải `2 cm`. Số trang đặt ở **giữa, bên dưới**.
- **Phông chữ & Dãn dòng:** `Times New Roman` (Unicode), dãn dòng `1.5 lines`.
- **Quy chuẩn cỡ chữ & Kiểu chữ:**
  * Tên chương (`CHƯƠNG 1: ...`): Cỡ **14**, IN HOA, Đậm, Căn giữa.
  * Tiểu mục mức 1 (`1.1. TÊN MỤC`): Cỡ **13**, IN HOA, Đậm, Căn trái.
  * Tiểu mục mức 2 (`1.1.1. Tên tiểu mục`): Cỡ **13**, Chữ thường, Đậm, Căn trái.
  * Tiểu mục mức 3 (`1.1.1.1. Tên ý nhỏ` - nếu có): Cỡ **13**, Chữ thường, Nghiêng, Căn trái.
  * Nội dung văn bản: Cỡ **13**, Normal, Căn đều 2 bên (Justify).
  * Nội dung trong Bảng (Table): Cỡ **12**, Normal, Căn giữa.
  * Tên bảng: Cỡ **11**, Đậm, Căn giữa, đặt **TRÊN bảng**.
  * Chú thích bảng: Cỡ **10**, Nghiêng, Căn giữa, đặt **DƯỚI bảng**.
  * Tên hình: Cỡ **11**, Đậm, Căn giữa, đặt **DƯỚI hình**.
  * Tài liệu tham khảo: Cỡ **11**, Thường, Căn trái.

### 7.2. Danh Mục Chữ Viết Tắt Được Phép Sử Dụng (Xếp theo A-Z)
Chỉ sử dụng các chữ viết tắt sau đây trong báo cáo (không tự ý viết tắt các từ tiếng Anh thông dụng khác):
- **ACID**: Atomicity, Consistency, Isolation, Durability (Tính nguyên tử, Nhất quán, Cô lập, Bền vững)
- **API**: Application Programming Interface (Giao diện lập trình ứng dụng)
- **DLQ**: Dead-Letter Queue (Hàng đợi chứa bản ghi lỗi)
- **DSR**: Design Science Research (Nghiên cứu khoa học thiết kế)
- **DWH**: Data Warehouse (Kho dữ liệu)
- **HFT**: High-Frequency Trading (Giao dịch tần suất cao)
- **JVM**: Java Virtual Machine (Máy ảo Java)
- **MAE / MAPE**: Mean Absolute Error / Mean Absolute Percentage Error (Sai số tuyệt đối trung bình / Sai số phần trăm tuyệt đối trung bình)
- **OCC**: Optimistic Concurrency Control (Kiểm soát đồng thời lạc quan)
- **OHLCV**: Open, High, Low, Close, Volume (Giá mở cửa, cao nhất, thấp nhất, đóng cửa và khối lượng)
- **OLAP**: Online Analytical Processing (Xử lý phân tích trực tuyến)
- **OOM**: Out-Of-Memory (Tràn bộ nhớ)
- **RMSE**: Root Mean Squared Error (Sai số bình phương trung bình căn)
- **SDA**: Software-Defined Assets (Tài sản dữ liệu định nghĩa bằng phần mềm)
- **SLA**: Service Level Agreement (Cam kết chất lượng dịch vụ)
- **TTL**: Time-To-Live (Thời gian tồn tại của dữ liệu đệm)
- **UML**: Unified Modeling Language (Ngôn ngữ mô hình hóa thống nhất)
- **VWAP**: Volume-Weighted Average Price (Giá trung bình gia quyền theo khối lượng)
