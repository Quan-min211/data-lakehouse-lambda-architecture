# BÁO CÁO TIẾN ĐỘ ĐỒ ÁN TLCN

## Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa

> **Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring**

| Thông tin | Chi tiết |
|:---|:---|
| **Sinh viên** | Nguyễn Đặng Quốc Anh (23133004) — Phạm Minh Quân (23133060) |
| **GVHD** | ThS. Đoàn Minh Trí |
| **Khoa** | Công nghệ Thông tin — ĐH Sư phạm Kỹ thuật TP.HCM (HCMUTE) |
| **Học kỳ** | HK1 — Năm học 2026–2027 |
| **Ngày báo cáo** | 10/09/2026 |

---

## MỤC LỤC

1. [Câu hỏi nghiên cứu & Mục tiêu đề tài](#1-câu-hỏi-nghiên-cứu--mục-tiêu-đề-tài)
2. [Dữ liệu — Thu thập & Mock](#2-dữ-liệu--thu-thập--mock)
3. [Kiến trúc hệ thống đã xây dựng](#3-kiến-trúc-hệ-thống-đã-xây-dựng)
4. [Chi tiết từng tầng đã triển khai](#4-chi-tiết-từng-tầng-đã-triển-khai)
5. [So sánh kiến trúc đề ra vs thực tế](#5-so-sánh-kiến-trúc-đề-ra-vs-thực-tế)
6. [Phân chia công việc](#6-phân-chia-công-việc)
7. [Kết quả kiểm thử tự động](#7-kết-quả-kiểm-thử-tự-động)
8. [Những phần còn lại cần hoàn thiện](#8-những-phần-còn-lại-cần-hoàn-thiện)

---

## 1. Câu hỏi nghiên cứu & Mục tiêu đề tài

Đề tài giải quyết bài toán: **Làm thế nào để một hệ thống vừa cung cấp dữ liệu thời gian thực (gần đúng) vừa đảm bảo tính chính xác tuyệt đối của dữ liệu lịch sử?**

### Ba câu hỏi nghiên cứu cốt lõi (Research Questions)

| Mã | Câu hỏi | Benchmark |
|:--|:---|:---|
| **RQ1** | Lambda Architecture có giúp giảm độ trễ truy vấn so với Batch-only không? | Benchmark 1: Query Latency |
| **RQ2** | Speed View (gần đúng) sai lệch bao nhiêu % so với Batch View (chính xác)? | Benchmark 2: Reconciliation Accuracy |
| **RQ3** | Iceberg Compaction cải thiện hiệu năng đọc bao nhiêu % sau streaming? | Benchmark 3: Compaction Efficiency |

---

## 2. Dữ liệu — Thu thập & Mock

### 2.1 Nguồn dữ liệu thực — Binance REST API

**Script:** `scripts/fetch_binance_data.py`

#### Cách lấy dữ liệu

- **API:** Binance Public REST API (không cần API key)
  - `GET /api/v3/klines` — Lấy nến 1-phút OHLCV
  - `GET /api/v3/aggTrades` — Lấy lịch sử giao dịch tổng hợp (aggTrades)
  - `GET /api/v3/ticker/24hr` — Snapshot 24h toàn thị trường
- **Lọc thông minh:** Tự động lọc Top-N cặp USDT theo khối lượng 24h lớn nhất, loại bỏ stablecoin-stablecoin (USDC, BUSD, FDUSD...)
- **Lưu trữ:** CSV (klines), JSON (aggTrades), phân cấp theo symbol vào `datasets/raw/`

#### Danh sách 13 cặp tiền đã lấy

| # | Symbol | Loại |
|:--|:---|:---|
| 1 | BTCUSDT | Bitcoin |
| 2 | ETHUSDT | Ethereum |
| 3 | SOLUSDT | Solana |
| 4 | XRPUSDT | Ripple |
| 5 | DOGEUSDT | Dogecoin |
| 6 | LINKUSDT | Chainlink |
| 7 | UNIUSDT | Uniswap |
| 8 | SUIUSDT | Sui |
| 9 | ZECUSDT | Zcash |
| 10 | ENSOUSDT | Ens |
| 11 | USDCUSDT | USD Coin |
| 12 | USD1USDT | USD1 |
| 13 | RLUSDUSDT | RLUSD |

#### Khối lượng dữ liệu thực đã lấy

| Loại dữ liệu | Số bản ghi | Số file | Ghi chú |
|:---|:---:|:---:|:---|
| **Klines 1m** (OHLCV nến 1 phút) | ~10,000 nến | 13 CSV | Mỗi symbol ~1,000 nến gần nhất |
| **AggTrades** (lịch sử giao dịch) | 5,000 giao dịch | 13 JSON | 500 giao dịch/symbol |
| **Ticker 24h** (snapshot thị trường) | 1 snapshot | 1 JSON | Toàn bộ thị trường |
| **Tổng** | **~15,001 bản ghi** | 27 files | |

#### Cấu trúc thuộc tính (Schema) của TradeEvent

| Thuộc tính | Kiểu dữ liệu | Mô tả |
|:---|:---|:---|
| `trade_id` | `int` (BIGINT) | Mã giao dịch duy nhất từ Binance aggTrade ID |
| `symbol` | `string` | Cặp giao dịch (BTCUSDT, ETHUSDT...) |
| `price` | `float` (DOUBLE) | Giá khớp lệnh (USDT) |
| `quantity` | `float` (DOUBLE) | Khối lượng coin khớp lệnh |
| `trade_time` | `int` (BIGINT) | Thời điểm khớp lệnh — **Event-time (epoch ms UTC)** |
| `is_buyer_maker` | `bool` | True: Taker bán / False: Taker mua |
| `ingestion_time` | `int` (BIGINT) | Thời điểm Producer nhận — **Processing-time (ms)** |
| `is_injected` | `bool` | True: bản ghi được tiêm lỗi thực nghiệm |
| `fault_type` | `string` (nullable) | `duplicate` / `late_data` / `out_of_order` / `schema_invalid` |

#### Cấu trúc Kline (OHLCV 1 phút)

| Thuộc tính | Mô tả |
|:---|:---|
| `open_time` | Epoch ms mở cửa nến |
| `open` / `high` / `low` / `close` | Giá mở, cao, thấp, đóng |
| `volume` | Tổng khối lượng coin trong 1 phút |
| `quote_volume` | Tổng giá trị USDT trong 1 phút |
| `trade_count` | Số giao dịch trong 1 phút |

---

### 2.2 Mock Data — Dữ liệu tổng hợp mở rộng

**Script:** `scripts/generate_mock_data.py`

#### Lý do cần Mock Data

Dữ liệu thực từ Binance chỉ đủ để kiểm tra tính đúng đắn (smoke test), **không đủ thể tích** để:
- Benchmark so sánh hiệu năng có ý nghĩa thống kê
- Mô phỏng Small File Problem cần hàng nghìn micro-batch
- Test các kịch bản lỗi với tỷ lệ kiểm soát (10% duplicate, 3% schema invalid...)

#### Phương pháp sinh dữ liệu

Dữ liệu được sinh bằng **Geometric Brownian Motion (GBM)** — mô hình giá tài chính chuẩn mực trong học thuật (Black-Scholes):

```
DeltaS = mu * S * Dt + sigma * S * sqrt(Dt) * epsilon
epsilon ~ N(0,1)
```

Trong đó:
- `mu` (drift), `sigma` (volatility): **calibrate từ dữ liệu lịch sử thực** của từng symbol
- `Dt = 1 phút`: step time của mô hình
- Kết quả: dữ liệu mock có **phân phối thống kê khớp với thị trường thực**

Ví dụ tham số calibrate cho BTCUSDT:
- `last_price`: 78,170.94 USDT
- `sigma_annual`: 0.294 (độ biến động 29.4%/năm)
- `volume_mean`: 8.50 BTC/phút

#### Fault Injection — Bơm lỗi có kiểm soát

| Loại lỗi | Tỷ lệ | Mô tả | Mục đích kiểm thử |
|:---|:---:|:---|:---|
| **Duplicate** | 10% | Gửi lại cùng `trade_id` với `ingestion_time` khác | Kiểm thử Deduplication tại Batch Layer |
| **Late Data** | 10% | Giảm `trade_time` lùi 1–5 phút | Kiểm thử Watermark của Spark Streaming |
| **Out-of-Order** | 5% | Đảo thứ tự phát sự kiện | Kiểm thử xử lý sự kiện không thứ tự |
| **Schema Invalid** | 3% | Giá âm, khối lượng = 0 | Kiểm thử Data Quality Gate |

#### Khối lượng Mock Data đã sinh

| Thông số | Giá trị |
|:---|:---|
| **Thời gian mô phỏng** | 7 ngày (26/08/2026 – 02/09/2026) |
| **Số cặp tiền** | 13 symbols |
| **Tần suất** | 8 ticks/phút/symbol |
| **Tổng nến (Klines)** | **131,040 nến 1m** |
| **Tổng aggTrades** | **1,262,456 giao dịch** |
| **Tổng bản ghi lỗi** | **253,682 bản ghi** |
| **Tổng tất cả** | **~1,516,000 bản ghi** |
| **Kích thước ổ đĩa** | ~360 MB (13 file JSON x ~27 MB) |

#### Chi tiết lỗi đã tiêm

| Loại lỗi | Số bản ghi |
|:---|:---:|
| Duplicate | 92,612 |
| Late Data | 83,486 |
| Out-of-Order | 50,713 |
| Schema Invalid | 26,871 |
| **Tổng** | **253,682** |

#### Cấu trúc lưu trữ

```
datasets/
├── raw/               # Dữ liệu thực từ Binance (~15,001 bản ghi)
│   ├── klines/        # 13 CSV — OHLCV 1 phút
│   ├── aggtrades/     # 13 JSON — lịch sử giao dịch
│   └── tickers/       # 1 JSON — snapshot 24h toàn thị trường
└── mock/              # Dữ liệu tổng hợp GBM (~1.26M records)
    ├── aggtrades/     # 13 JSON (~27 MB/file)
    ├── klines/        # 13 CSV — nến 1m synthetic
    └── stats/         # 13 JSON — tham số calibration GBM
```

> **Lưu ý:** Toàn bộ `datasets/raw/` và `datasets/mock/` được thêm vào `.gitignore`
> — không commit lên Git để tránh vi phạm giới hạn dung lượng repository.

---

## 3. Kiến trúc hệ thống đã xây dựng

### 3.1 Kiến trúc Lambda tổng thể

```
+----------------------------------------------------------------+
|                      NGUON DU LIEU                             |
|  Binance API (REST/WebSocket)  /  Mock Data Generator (GBM)   |
+---------------------------+------------------------------------+
                            |
                            v
+----------------------------------------------------------------+
|                    TANG THU THAP (INGESTION)                   |
|  ResilientKafkaProducer + FaultInjector (4 loai loi)          |
|  -> Kafka Topic: crypto_trades_raw  | DLQ: crypto_trades_dlq  |
+----------+-----------------------------------+-----------------+
           |                                   |
           v                                   v
+---------------------+          +-----------------------------+
|  TANG LO (BATCH)    |          |  TANG TOC DO (SPEED)        |
|                     |          |                             |
| IcebergTableManager |          | SparkStructuredStreaming     |
|  -> Bronze Table    |          |  + WindowAggregator         |
|    (Iceberg v2)     |          |    (Watermark 1m, Win 1m)   |
|                     |          |  + MetricsCalculator        |
| CompactionJob       |          |    (OHLCV, VWAP)            |
|  -> Bin-Packing     |          |  + SpikeDetector            |
|  -> Sort Compact    |          |  + ClickHouseSpeedWriter    |
|  -> Manifest Rw     |          |                             |
|  -> Expire Snap     |          | Trigger: 5 giay (SLA < 5s)  |
|                     |          |                             |
| Dagster 6 SDA       |          | -> ClickHouse speed_agg     |
|  bronze->silver     |          |   (ReplacingMergeTree)      |
|  ->gold->CH sync    |          |                             |
|  ->watermark sync   |          |                             |
+----------+----------+          +-------------+---------------+
           |  batch_agg                        |  speed_agg
           +-----------------+-----------------+
                             |
                   system_watermark (W = Batch Cutoff)
                             |
                             v
+----------------------------------------------------------------+
|                  TANG PHUC VU (SERVING)                        |
|                                                                |
|  Auto-Correcting Query Merger (3 Cases)                       |
|  +----------------------------------------------------------+  |
|  | CASE 1: T_end <= W   -> 100% batch_agg  (Reconciled)    |  |
|  | CASE 2: T_start >= W -> 100% speed_agg (Provisional)    |  |
|  | CASE 3: Hybrid -> Cat tai W, ghep 2 nguon               |  |
|  |                  ZERO Double-Counting                   |  |
|  |                  Delta = |VWAP_speed - VWAP_batch|      |  |
|  +----------------------------------------------------------+  |
|                                                                |
|  FastAPI REST API -- Port 8000                                 |
|  GET /health | /api/watermark | /api/market | /api/reconcile  |
+---------------------------+------------------------------------+
                            |
                            v
+----------------------------------------------------------------+
|                   TANG TRUC QUAN (DASHBOARD)                   |
|  Streamlit + Plotly -- Port 8501                               |
|  - Bieu do nen Candlestick OHLCV + Duong VWAP                 |
|  - Canh bao Price Spike (tam giac cam)                        |
|  - Badge Reconciled (xanh) vs Provisional (tim)               |
|  - 5 the KPI: Gia, VWAP, Query Case, Watermark, Delta        |
|  - Bang doi soat Batch vs Speed (Benchmark 2)                 |
|  - Auto-refresh 3s/5s/10s + Fallback data khi API offline    |
+----------------------------------------------------------------+
```

### 3.2 Ha tang Docker Compose

| Service | Image | Port | Mo ta |
|:---|:---|:---|:---|
| **Kafka** (KRaft) | `apache/kafka:3.7.0` | 9092, 9094 | Message broker — khong ZooKeeper |
| **MinIO** | `minio/minio:latest` | 9000, 9001 | S3-compatible object storage |
| **Iceberg REST** | `tabulario/iceberg-rest` | 8181 | Catalog quan ly schema Iceberg |
| **ClickHouse** | `clickhouse/clickhouse-server` | 8123, 9009 | OLAP engine cho Batch + Speed Views |
| **Redis** | `redis:7.2-alpine` | 6379 | Hot cache (du phong) |
| **FastAPI** | Custom Dockerfile | 8000 | Serving Layer API |
| **Streamlit** | Custom Dockerfile | 8501 | Dashboard UI |

**Tong tai nguyen:** ~7.5–8.5 GB RAM (phu hop may 16 GB)

---

## 4. Chi tiết từng tầng đã triển khai

### 4.1 Tầng Thu Thập — Ingestion Layer (HOAN THANH)

**Người thực hiện:** Nguyễn Đặng Quốc Anh
**Tài liệu:** `docs/process/01_ingestion_data_contract.md`

| File | Chức năng |
|:---|:---|
| `src/ingestion/models.py` | `TradeEvent` & `DLQEvent` — Data Contract chuẩn hóa |
| `src/ingestion/kafka_producer.py` | `ResilientKafkaProducer` — retry exponential backoff, DLQ routing |
| `src/ingestion/fault_injector.py` | `FaultInjector` — 4 loại lỗi có kiểm soát |
| `src/ingestion/binance_ws.py` | WebSocket streaming Top-10 coin, auto-reconnect |
| `src/ingestion/historical_backfill.py` | Backfill dữ liệu lịch sử |
| `datasets/schemas/trade_event_schema.json` | JSON Schema chuẩn hóa — Data Contract |

**Đặc điểm kỹ thuật:**
- Kafka Producer: `acks=all`, `retries=3`, key partition theo `symbol`
- Compression: gzip, batch_size: 16 KB, linger: 10ms
- Dead-Letter Queue: tự động route message lỗi vào `crypto_trades_dlq`
- **8 Unit Tests — 100% passed**

---

### 4.2 Tầng Lô — Batch Layer (HOAN THANH)

**Người thực hiện:** Phạm Minh Quân

#### Apache Iceberg Table Management

**File:** `src/batch_layer/iceberg_utils.py` (517 dòng)

| Chức năng | Chi tiết |
|:---|:---|
| **SparkSession** | Iceberg REST Catalog + MinIO S3A + Iceberg Extensions |
| **Bronze Table** | `iceberg_catalog.bronze.crypto_trades`, Format v2 |
| **Phân vùng** | Hidden Partition `days(trade_time_ts)` theo ngày UTC |
| **Ghi dữ liệu** | Append-only, ACID, mỗi lần ghi tạo 1 Snapshot mới |
| **Đọc dữ liệu** | Filter `symbol`, `date range`, hỗ trợ Time Travel (snapshot_id) |
| **Thống kê** | `get_file_stats()`, `get_partition_stats()`, `get_snapshot_history()` |

**Schema Bronze Table:**

```sql
CREATE TABLE iceberg_catalog.bronze.crypto_trades (
    trade_id          BIGINT    NOT NULL,  -- Khoa dedup
    symbol            STRING    NOT NULL,
    price             DOUBLE    NOT NULL,
    quantity          DOUBLE    NOT NULL,
    trade_time        BIGINT    NOT NULL,  -- epoch ms (Event-time)
    trade_time_ts     TIMESTAMP NOT NULL,  -- Partition column
    is_buyer_maker    BOOLEAN,
    ingestion_time    BIGINT,
    is_injected       BOOLEAN,
    fault_type        STRING,
    batch_run_id      STRING,
    bronze_written_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(trade_time_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'  -- 128 MB
)
```

#### Iceberg Compaction Job

**File:** `src/batch_layer/compaction.py` (500 dòng)

| Chiến lược | Lệnh Iceberg | Mục đích |
|:---|:---|:---|
| **Bin-Packing** | `CALL system.rewrite_data_files(strategy='binpack')` | Gộp file nhỏ -> 128 MB **(Benchmark 3)** |
| **Sort Compaction** | `CALL system.rewrite_data_files(strategy='sort')` | Sắp xếp `symbol, trade_time_ts` |
| **Manifest Rewrite** | `CALL system.rewrite_manifests()` | Gộp metadata manifests nhỏ |
| **Expire Snapshots** | `CALL system.expire_snapshots()` | Xóa snapshot > 7 ngày |
| **Full Pipeline** | `run_full_compaction()` | Chạy tuần tự 4 bước trên |

#### Dagster Software-Defined Assets (6 Assets)

**File:** `dagster_project/repository.py` (450 dòng)

```
bronze_crypto_trades
        |
        v
silver_cleaned_trades   <- DQ Gate: khu trung, quarantine schema loi
        |
        v
gold_market_aggregates  <- Tong hop OHLCV + VWAP (Ground Truth)
        |
        v
clickhouse_batch_sync   <- Nap vao lakehouse.batch_agg
        |
        v
system_watermark_sync   <- Chot moc W vao lakehouse.system_watermark

iceberg_small_files_compaction  <- Job bao tri doc lap (moi 2 gio)
```

**Lịch chạy tự động:**
- `batch_lakehouse_pipeline_job`: mỗi **15 phút** (`*/15 * * * *`)
- `iceberg_compaction_job`: mỗi **2 giờ** (`0 */2 * * *`)

**Kết quả thực tế đã ghi vào ClickHouse:**
- `lakehouse.batch_agg`: **75 nến** (5 symbols x 15 phút)
- `lakehouse.system_watermark`: 1 mốc watermark đã cập nhật thành công

---

### 4.3 Tầng Tốc Độ — Speed Layer (HOAN THANH)

**Người thực hiện:** Nguyễn Đặng Quốc Anh
**Tài liệu:** `docs/process/02_speed_layer_spark_streaming.md`

| File | Chức năng |
|:---|:---|
| `src/speed_layer/spark_streaming.py` | Entry point: SparkSession `local[2]`, Trigger 5s, checkpoint |
| `src/speed_layer/window_aggregator.py` | Parse Kafka JSON -> Watermark 1m -> Tumbling Window 1m |
| `src/speed_layer/metrics_calculator.py` | OHLCV + VWAP = Sum(P*Q)/Sum(Q) |
| `src/speed_layer/spike_detector.py` | is_spike: (Close-Open)/Open >= 2% hoac (High-Low)/Low >= 3% |
| `src/speed_layer/clickhouse_writer.py` | `foreachBatch` -> ClickHouse `lakehouse.speed_agg` |

**Đặc điểm kỹ thuật:**
- Event-time processing: dùng `trade_time` (Binance Event-time ms)
- Watermark 1 phút: tiếp nhận late data <= 1 phút, loại bỏ data trễ hơn
- **SLA: latency end-to-end < 5 giây**
- `ReplacingMergeTree(created_at)`: Idempotent khi retry ghi
- **17/17 Unit Tests passed**

---

### 4.4 Tầng Phục Vụ — Serving Layer (HOAN THANH)

**Người thực hiện:** Nguyễn Đặng Quốc Anh
**Tài liệu:** `docs/process/03_serving_layer_query_merger.md`

#### Auto-Correcting Query Merger — Thuật toán cốt lõi

**File:** `src/serving_layer/query_merger.py` (238 dòng)

```
Đầu vào: symbol, T_start, T_end
Mốc Watermark: W = lakehouse.system_watermark

CASE 1: T_end <= W
  -> 100% Batch View (Reconciled, Ground Truth)
  -> Delta = 0.0

CASE 2: T_start >= W
  -> 100% Speed View (Provisional, tuc thi)
  -> Delta = None

CASE 3: T_start < W < T_end  [Giao thoa]
  -> [T_start, W]  : Batch View (Reconciled)
  -> (W, T_end]    : Speed View (Provisional)
  -> ZERO Double-Counting (khong nhan doi nen tai bien W)
  -> Delta = |VWAP_speed - VWAP_batch| (phuc vu Benchmark 2)
```

#### REST API Endpoints

| Endpoint | Mô tả |
|:---|:---|
| `GET /health` | Kiểm tra trạng thái hệ thống |
| `GET /api/watermark` | Mốc Watermark hiện tại của Batch Layer |
| `GET /api/market?symbol=BTCUSDT&start_time=...&end_time=...` | Dữ liệu nến qua Query Merger |
| `GET /api/reconciliation?symbol=...` | Báo cáo đối soát Batch vs Speed |

**24/24 Unit Tests passed**

---

### 4.5 Tầng Giao Diện — Dashboard Layer (HOAN THANH)

**Người thực hiện:** Nguyễn Đặng Quốc Anh
**Tài liệu:** `docs/process/04_dashboard_streamlit_ui.md`

| Component | File | Chức năng |
|:---|:---|:---|
| **Main App** | `dashboard/app.py` | Streamlit, Dark theme, Auto-refresh, Fallback data |
| **Candlestick** | `dashboard/components/candlestick.py` | Plotly: Nến + VWAP + Spike + Volume |
| **Metrics Cards** | `dashboard/components/metrics_cards.py` | 5 thẻ KPI: Giá, VWAP, Query Case, Watermark, Delta |
| **Reconciliation** | `dashboard/components/reconciliation_view.py` | Bảng đối soát MAE Batch vs Speed |

**Tính năng nổi bật:**
- Nến `Reconciled`: màu xanh/đỏ truyền thống
- Nến `Provisional`: màu tím cảnh báo (từ Speed Layer)
- Tam giác đánh dấu Price Spike trên đỉnh nến
- Fallback data khi API offline — dashboard không crash trắng
- **27/27 Unit Tests passed**

---

### 4.6 Co so ha tang — ClickHouse Schema (HOAN THANH)

**File:** `configs/clickhouse/init.sql`

| Bảng | Engine | Mục đích |
|:---|:---|:---|
| `lakehouse.speed_agg` | `ReplacingMergeTree(created_at)` | Nến tức thời từ Speed Layer (Provisional) |
| `lakehouse.batch_agg` | `ReplacingMergeTree(created_at)` | Nến chính xác từ Batch Layer (Reconciled) |
| `lakehouse.system_watermark` | `ReplacingMergeTree(updated_at)` | Mốc ranh giới phân chia Case 1/2/3 |

Cả 3 bảng dùng `PARTITION BY toYYYYMMDD(window_start)` và `ORDER BY (symbol, window_start)`.

---

## 5. So sánh kiến trúc đề ra vs thực tế

### 5.1 Sơ đồ đề ra ban đầu (Proposal)

```
DATA SOURCES
├── Kafka Streaming
├── CSV/Parquet Batch
└── CDC (Debezium)
       |
       ├── BATCH LAYER
       |   ├── MinIO (Master Dataset) + Apache Iceberg
       |   ├── Spark Batch -> dbt (Bronze->Silver->Gold)
       |   └── -> ClickHouse (Batch Views)
       |
       └── SPEED LAYER
           ├── Kafka -> Spark Streaming
           └── -> Redis (Speed Views)
                      |
                  SERVING LAYER
                  ├── FastAPI Query Merger
                  ├── ClickHouse + Redis (Hot Cache)
                  └── Metabase / Streamlit Dashboard
```

### 5.2 Sơ đồ thực tế đã xây dựng

```
DATA SOURCES
├── [OK] Binance REST API (13 symbols, 15,001 ban ghi thuc)
├── [OK] Mock Data Generator GBM (1,262,456 aggTrades + 131,040 klines)
└── [OK] Kafka Producer + Fault Injector (4 loai loi kiem soat)
       |
       ├── BATCH LAYER                          [HOAN THANH]
       |   ├── [OK] MinIO + Iceberg REST Catalog (Docker)
       |   ├── [OK] iceberg_utils.py (Bronze Table v2, partition days)
       |   ├── [OK] compaction.py (Bin-Pack, Sort, Manifest, Expire)
       |   ├── [OK] Dagster 6 SDA: bronze->silver->gold->CH->watermark
       |   ├── [--] dbt Models: CON THIEU (chua trien khai)
       |   └── [OK] ClickHouse batch_agg (75 nen da ghi thuc te)
       |
       └── SPEED LAYER                          [HOAN THANH]
           ├── [OK] spark_streaming.py (Trigger 5s, SLA < 5s)
           ├── [OK] window_aggregator.py (Watermark 1m, Window 1m)
           ├── [OK] metrics_calculator.py (OHLCV, VWAP)
           ├── [OK] spike_detector.py (Price Spike Detection)
           ├── [OK] clickhouse_writer.py -> speed_agg
           └── [THAY DOI] Redis Speed Views -> dung ClickHouse
                      |
               SERVING LAYER                    [HOAN THANH]
               ├── [OK] system_watermark (ClickHouse)
               ├── [OK] query_merger.py (Case 1/2/3, ZERO Double-Count)
               ├── [OK] api_routes.py (FastAPI)
               └── [THAY DOI] Redis Hot Cache -> chua trien khai
                      |
               DASHBOARD LAYER                  [HOAN THANH]
               └── [OK] Streamlit + Plotly (Candlestick, Reconciliation)
                      |
               ORCHESTRATION                    [HOAN THANH]
               └── [OK] Dagster (6 Assets, 2 Jobs, 2 Schedules)
```

### 5.3 Bảng so sánh chi tiết

| Hạng mục | Đề ra | Thực tế | Trạng thái |
|:---|:---|:---|:---:|
| Data Collection | Kafka + Batch load | REST API + Mock GBM | OK |
| Kafka Broker | Apache Kafka (KRaft) | Apache Kafka 3.7.0 (KRaft) | OK |
| Object Storage | MinIO | MinIO (Docker) | OK |
| Table Format | Apache Iceberg | Iceberg v2 (REST Catalog) | OK |
| Batch Processing | Spark Batch | PySpark 3.5.1 | OK |
| Bronze Table | MinIO + Iceberg | `bronze.crypto_trades` partition days | OK |
| Silver/Gold Layer | dbt transformation | Dagster SDA simulate | dbt chua co models |
| Batch Views | ClickHouse | `lakehouse.batch_agg` | OK |
| Stream Processing | Spark Structured Streaming | Spark Micro-batch 5s | OK |
| Speed Views | **Redis** | **ClickHouse** `speed_agg` | Doi sang ClickHouse |
| Serving API | FastAPI Query Merger | FastAPI + 3-Case Merger | OK (nang cap) |
| Dashboard | Metabase / Streamlit | Streamlit + Plotly custom | OK |
| Orchestration | Dagster | Dagster 6 SDA + 2 Schedules | OK |
| Compaction | Iceberg OPTIMIZE | Bin-Pack + Sort + Manifest + Expire | OK (day du hon) |
| Benchmark Scripts | 3 bo benchmark | Chua co code thuc | CHUA LAAM |
| Data Quality Module | `src/data_quality/` | Logic simulate trong Dagster | Chua day du |
| Unit Tests | pytest | 81/81 PASS | OK |

### 5.4 Điểm thích nghi so với đề ra (Adaptive Changes)

**1. Speed Views: Redis -> ClickHouse**

Thay vì dùng Redis làm Speed View cache, nhóm sử dụng trực tiếp ClickHouse `speed_agg` với `ReplacingMergeTree`. Lý do: đơn giản hóa kiến trúc, giảm độ phức tạp vận hành, đảm bảo SLA < 5s.

**2. Auto-Correcting Merger (nâng cấp đáng kể)**

Đề ra chỉ nêu "Query Merger" đơn giản. Thực tế đã thiết kế thuật toán phân luồng **3 Case** với cơ chế tính sai số `Delta_reconciliation` và chuẩn `CandleStatus` (Reconciled/Provisional/Partially Reconciled) phục vụ trực tiếp Benchmark 2.

**3. Dagster SDA thay vì Spark Batch Job thuần**

Đề ra nêu `spark_batch_jobs.py`. Thực tế triển khai bằng Dagster Software-Defined Assets với lịch chạy tự động, lineage DAG, metadata tracking.

---

## 6. Phân chia công việc

| Hạng mục | Nguyễn Đặng Quốc Anh (23133004) | Phạm Minh Quân (23133060) |
|:---|:---:|:---:|
| Data Contract & JSON Schema | OK | |
| Kafka Producer & Fault Injector | OK | |
| Binance WebSocket & Backfill | OK | |
| **Fetch Binance Data (REST API)** | | OK |
| **Mock Data Generator (GBM)** | | OK |
| **Iceberg Utils (Bronze DDL, Write, Read, Time Travel)** | | OK |
| **Compaction Job (4 chien luoc)** | | OK |
| Speed Layer (5 modules) | OK | |
| Serving Layer (Query Merger + FastAPI) | OK | |
| Dashboard (Streamlit + Plotly) | OK | |
| Dagster Orchestration (6 SDA) | OK | |
| Tài liệu data (`data.md`, `reportmock.md`) | | OK |

---

## 7. Kết quả kiểm thử tự động

| Test File | So Test | Ket qua | Noi dung |
|:---|:---:|:---:|:---|
| `test_ingestion.py` | 8 | 8/8 PASS | TradeEvent, DLQEvent, FaultInjector 4 loai loi |
| `test_speed_layer.py` | 17 | 17/17 PASS | OHLCV, VWAP, Out-of-Order, SpikeDetector |
| `test_query_merger.py` | 24 | 24/24 PASS | Case 1/2/3, Zero Double-Counting, FastAPI |
| `test_dashboard_components.py` | 27 | 27/27 PASS | Candlestick render, fallback data |
| `test_dagster_pipeline.py` | 5 | 5/5 PASS | Assets, Jobs, Schedules, Lineage |
| **TONG** | **81** | **81/81 PASS** | |

---

## 8. Những phần còn lại cần hoàn thiện

| Hạng mục | Uu tien | Ghi chu |
|:---|:---:|:---|
| **Benchmark 1** — Query Latency (`bench_latency.py`) | CAO | So sanh Batch-only vs Lambda |
| **Benchmark 2** — Reconciliation Accuracy (`bench_reprocess.py`) | CAO | Do sai so VWAP Speed vs Batch |
| **Benchmark 3** — Compaction Efficiency (`bench_compaction.py`) | CAO | Do cai thien read latency sau Bin-Pack |
| **Ghi ket qua CSV vao `results/logs/`** | CAO | Lam bang du lieu cho luan van |
| **Data Quality Module** (`src/data_quality/`) | TRUNG BINH | Tach logic DQ ra module doc lap |
| **dbt Models** (`dbt_project/models/`) | TRUNG BINH | Bronze/Silver/Gold SQL models |
| **Bao cao luan van** | CAO | Viet noi dung tung chuong |

---

*Ngay cap nhat: 10/09/2026 — Pham Minh Quan*
