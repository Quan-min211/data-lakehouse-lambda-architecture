# 📊 Tài Liệu Dữ Liệu — Lambda Architecture Crypto Lakehouse

> **Mục đích tài liệu:** Mô tả toàn diện nguồn gốc, quy trình thu thập, làm sạch, xử lý và đặc tính dữ liệu của hệ thống **Data Lakehouse theo kiến trúc Lambda** phục vụ đối soát dữ liệu thời gian thực thị trường tiền mã hóa (TLCN 2026-2027).

---

## 1. 📍 Nguồn Dữ Liệu — Lấy Từ Đâu?

### 1.1. Nhà cung cấp chính

**Binance** — Sàn giao dịch tiền điện tử lớn nhất thế giới theo khối lượng giao dịch.

| Thông tin | Chi tiết |
|-----------|---------|
| **Nhà cung cấp** | Binance Global |
| **Website chính thức** | https://www.binance.com |
| **API Documentation** | https://binance-docs.github.io/apidocs/spot/en/ |
| **Loại dữ liệu** | Cryptocurrency Market Data (Aggregate Trades & OHLCV Candles) |
| **Loại tài sản** | Top-10 cặp tiền tệ USDT có thanh khoản cao nhất (động, cập nhật mỗi lần khởi động) |
| **Chi phí** | **Miễn phí** — Binance Public API không yêu cầu API Key cho dữ liệu public |
| **Rate Limit** | 1.200 request weight/phút (REST API) |

### 1.2. Địa chỉ API cụ thể

#### 🔴 Real-Time WebSocket — Aggregate Trade Stream (Speed Layer)

```
wss://stream.binance.com:9443/stream?streams={symbol1}@aggTrade/{symbol2}@aggTrade/...
```

- **Giao thức:** WebSocket (WSS)
- **Endpoint:** `wss://stream.binance.com:9443/stream`
- **Kiểu stream:** Combined Multi-Stream (`{symbol}@aggTrade`) — **Aggregate Trades** (gộp các lệnh khớp cùng giá, cùng chiều, cùng thời điểm)
- **Ví dụ đầy đủ:**

```
wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/bnbusdt@aggTrade/...
```

- **Tần suất:** Gửi message mỗi khi có aggregate trade mới (thường vài ms đến vài giây)
- **Giữ kết nối:** ping mỗi 30 giây, pong timeout 10 giây (cấu hình trong `binance_ws.py`)
- **Auto-reconnect:** Exponential backoff (2s → 60s, tối đa 20 lần) qua module `tenacity`

> **Tại sao dùng `@aggTrade` thay vì `@trade`?**
> `@aggTrade` gộp nhiều lệnh khớp cùng giá, cùng chiều mua/bán trong cùng millisecond thành 1 event, giảm số lượng message cần xử lý và phản ánh đúng hơn hành vi thị trường tổng thể.

#### 📦 REST API — Lấy Danh Sách Top Coins (24hr Ticker)

```
GET https://api.binance.com/api/v3/ticker/24hr
```

- **Mục đích:** Lấy danh sách tất cả cặp giao dịch, sắp xếp theo `quoteVolume` (khối lượng USDT giao dịch 24h) để chọn Top-N coins động
- **Không cần API Key**
- **Lọc thêm:** Loại trừ các leveraged token: `UPUSDT`, `DOWNUSDT`, `BEARUSDT`, `BULLUSDT`

#### 📦 REST API — Lấy Lịch Sử Nến OHLCV (Klines / Candlestick)

```
GET https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval=1m&limit=1000
```

| Tham số | Giá trị | Mô tả |
|---------|---------|-------|
| `symbol` | `BTCUSDT`, `ETHUSDT`,... | Cặp giao dịch |
| `interval` | `1m` | Khung thời gian nến 1 phút |
| `limit` | `1000` | Số nến tối đa mỗi request |

- **Request weight:** 2 (giới hạn 1.200/phút → budget an toàn: 1.100)
- **1 request = 1.000 nến ≈ ~16.7 giờ dữ liệu lịch sử**

---

## 2. 🔄 Quy Trình Thu Thập Dữ Liệu (Step-by-Step)

### 2.1. Bước 0 — Chọn Top-N Coins (Chạy trước mỗi phiên)

**Module:** `src/ingestion/binance_ws.py` — `fetch_top_symbols()`

```
[Binance /api/v3/ticker/24hr]
        ↓
Lọc cặp kết thúc bằng "USDT"
        ↓
Loại bỏ leveraged tokens (UP, DOWN, BEAR, BULL)
        ↓
Sắp xếp giảm dần theo quoteVolume (Khối lượng USDT 24h)
        ↓
Chọn Top-N (mặc định TOP_N_COINS=10 từ .env)
        ↓
[Danh sách ký hiệu: BTCUSDT, ETHUSDT, BNBUSDT, ...]
```

**Kết quả mẫu (Top 10 ngày điển hình):**
`BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, DOGEUSDT, ADAUSDT, AVAXUSDT, SHIBUSDT, DOTUSDT`

---

### 2.2. Bước 1A — Real-Time WebSocket Stream (Speed Layer Input)

**Module:** `src/ingestion/binance_ws.py` — class `BinanceWebSocketProducer`
**Công cụ:** `websocket-client`, `kafka-python`, `tenacity`

```
Bước 1: Gọi /api/v3/ticker/24hr → Lấy Top-N USDT pairs (loại leveraged tokens)
Bước 2: Xây dựng Combined Stream URL (N symbols @aggTrade)
Bước 3: Kết nối wss://stream.binance.com:9443/stream
Bước 4: Với mỗi message nhận được:
         │
         ├── Validate cấu trúc: {"stream": "...", "data": {...}}
         │         OK  → parse data["data"] → chuẩn hóa TradeEvent
         │         ERR → DLQEvent → Kafka topic: crypto_trades_dlq
         │
         ├── [Nếu bật Fault Injector] → FaultInjector.process_event()
         │         → Có thể inject: Duplicate / Late / Out-of-Order / Schema Invalid
         │         → Đánh dấu is_injected=true, fault_type=<loại lỗi>
         │
         └── TradeEvent → ResilientKafkaProducer.send_trade()
                  → Kafka topic: crypto.trades (key=symbol)
Bước 5: ping/pong mỗi 30s để duy trì kết nối (timeout 10s)
Bước 6: Mất kết nối → exponential backoff retry (2s → 60s tối đa 20 lần)
```

**Tham số vận hành:**
- `KAFKA_BOOTSTRAP_SERVERS`: `kafka:9092` (trong Docker) hoặc `localhost:9092` (host)
- `KAFKA_TOPIC_RAW`: `crypto.trades`
- `KAFKA_TOPIC_DLQ`: `crypto_trades_dlq`
- `TOP_N_COINS`: `10` (mặc định)

---

### 2.3. Bước 1B — Batch Historical OHLCV (Batch Layer Input)

**Module:** `ingestion/producer_batch.py`
**Công cụ:** `requests`, `kafka-python`

```
Bước 1: Gọi /api/v3/ticker/24hr → Lấy Top-N USDT pairs
Bước 2: Vòng lặp qua từng symbol:
         │
         ├── GET /api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000
         │         → 1.000 nến 1-phút gần nhất (~16.7 giờ lịch sử)
         │
         ├── Chuyển đổi mỗi kline row → Trade-compatible tick (kline_to_tick())
         │         event_type="kline_batch" để phân biệt nguồn với WebSocket
         │         dùng close_price làm price, volume làm quantity
         │
         ├── Publish từng tick → Kafka topic: crypto.trades
         │
         └── Quản lý Rate Limit:
               - Sleep 0.5s giữa các symbol
               - Nếu tổng weight >= 1.100 → Sleep 60s rồi reset counter
Bước 3: Hoàn thành: ~10 symbols × 1.000 candles = ~10.000 ticks/lần chạy
```

---

### 2.4. Bước 1C — Fault Injector (Benchmark Ground Truth)

**Module:** `src/ingestion/fault_injector.py` — class `FaultInjector`

> **Mục đích:** Chủ động tiêm lỗi có kiểm soát vào luồng dữ liệu để tạo Ground Truth cho Benchmark 3 (Data Quality & Fault Handling).

```
TradeEvent (clean)
        ↓
FaultInjector.process_event()
        │
        ├── [schema_invalid_rate] → price < 0, quantity = 0 → is_injected=True, fault_type="schema_invalid"
        ├── [late_data_rate]      → trade_time lùi 1-5 phút   → is_injected=True, fault_type="late_data"
        ├── [out_of_order_rate]   → giữ lại, phát sau         → is_injected=True, fault_type="out_of_order"
        └── [duplicate_rate]      → phát thêm 1 bản ghi trùng → is_injected=True, fault_type="duplicate"
        ↓
List[TradeEvent] → Kafka topic: crypto.trades
```

**Tỷ lệ mặc định (cấu hình qua .env):**

| Loại lỗi | Biến môi trường | Tỷ lệ gợi ý |
|----------|----------------|------------|
| `duplicate` | `FAULT_DUPLICATE_RATE` | 10% |
| `late_data` | `FAULT_LATE_RATE` | 10% |
| `out_of_order` | `FAULT_OOO_RATE` | 5% |
| `schema_invalid` | `FAULT_SCHEMA_RATE` | 3% |

---

### 2.5. Bước 2 — Kafka → Bronze (Batch Layer — Spark Batch Job)

**Module:** `src/batch_layer/bronze_writer.py`
**Công cụ:** Apache Spark 3.5, Apache Iceberg, MinIO S3

```
Kafka topic: crypto.trades (JSON — TradeEvent format)
        ↓
Spark Batch Job (trigger: theo lịch Dagster hoặc thủ công)
        ↓
Đọc từ Kafka consumer, parse TradeEvent schema
        ↓
Thêm cột metadata: batch_run_id, bronze_written_at
        ↓
Ghi Append → Apache Iceberg Bronze Table
  Path: s3://bronze/ (MinIO)
  Catalog: Iceberg REST (http://iceberg-rest:8181)
  Table: bronze.crypto_trades
  Partition: days(trade_time)    ← Hidden Partition để tránh small files
  Mode: Append-only (immutable raw truth)
```

---

### 2.6. Bước 3 — Kafka → Speed View (Speed Layer — Spark Streaming)

**Module:** `src/speed_layer/spark_streaming.py`
**Công cụ:** Apache Spark Structured Streaming, ClickHouse

```
Kafka topic: crypto.trades (JSON)
        ↓
Spark Structured Streaming (continuous)
        ↓
Watermark: 2 phút (xử lý late events trong 2 phút)
        ↓
Tumbling Window Aggregation: 1 phút + 5 phút
        ↓
Tính OHLCV + VWAP + Spike Detection (SLA < 5 giây)
        ↓
Ghi → ClickHouse: lakehouse.speed_agg
  status = 'Provisional'
  Engine: ReplacingMergeTree(created_at)
```

---

### 2.7. Bước 4 — Bronze → Silver (Batch Layer — DQ Gate & Dedup)

**Module:** `src/batch_layer/silver_processor.py`
**Công cụ:** Apache Spark, Apache Iceberg

```
Bronze Table (Iceberg — s3://bronze/)
        ↓
Data Quality Gate (src/data_quality/dq_checks.py):
  - 6 rules kiểm tra tính hợp lệ
        │
        ├── PASS → Silver processor tiếp tục
        └── FAIL → Quarantine Table (s3://silver/quarantine/)
                   ghi kèm error_code + rejected_at
        ↓
Deduplication: ROW_NUMBER() OVER (PARTITION BY symbol, trade_id ORDER BY ingestion_time DESC) = 1
        ↓
Ghi MERGE → Apache Iceberg Silver Table
  Table: silver.crypto_trades_clean
  Partition: days(trade_time)
  Mode: MERGE INTO (idempotent upsert theo trade_id)
```

---

### 2.8. Bước 5 — Silver → Gold (Batch Layer — OHLCV Aggregation)

**Module:** `src/batch_layer/gold_aggregator.py`
**Công cụ:** Apache Spark Window Functions

```
Silver Table (Iceberg — s3://silver/)
        ↓
Tổng hợp OHLCV (1m + 5m windows)
  - open  = price tại min(trade_time)
  - high  = MAX(price)
  - low   = MIN(price)
  - close = price tại max(trade_time)
  - volume = SUM(quantity)
  - vwap   = SUM(price × quantity) / SUM(quantity)
  - trade_count = COUNT(*)
        ↓
Ghi → Apache Iceberg Gold Table
  Table: gold.ohlcv_aggregated
  Partition: days(trade_time)
        ↓
Sync → ClickHouse: lakehouse.batch_agg
  status = 'Reconciled'
  Cập nhật lakehouse.system_watermark (layer='batch_layer')
```

---

## 3. 🧹 Làm Sạch Dữ Liệu (Data Quality Gate — Bronze → Silver)

### 3.1. Kiểm Tra Chất Lượng Dữ Liệu (DQ Rules)

**Module:** `src/data_quality/dq_checks.py`

| # | Rule | Điều kiện lỗi | Hành động |
|---|------|---------------|-----------|
| 1 | **trade_id không null** | `trade_id IS NULL` | Quarantine |
| 2 | **symbol không null** | `symbol IS NULL OR symbol = ''` | Quarantine |
| 3 | **Giá hợp lệ** | `price IS NULL OR price <= 0` | Quarantine |
| 4 | **Khối lượng hợp lệ** | `quantity IS NULL OR quantity <= 0` | Quarantine |
| 5 | **trade_time hợp lệ** | `trade_time IS NULL OR trade_time <= 0` | Quarantine |
| 6 | **Phát hiện lỗi inject** | `is_injected = True AND fault_type = 'schema_invalid'` | Quarantine + DQ Metric |

**Nguyên tắc:** Dữ liệu lỗi **không bao giờ bị xóa im lặng** — luôn ghi vào Quarantine Table để audit và đo Benchmark 3. Pipeline không dừng vì dữ liệu xấu.

### 3.2. Khử Trùng Lặp (Deduplication)

Chiến lược dedup theo `(symbol, trade_id)`:

```sql
-- Giữ lại 1 bản ghi mới nhất theo ingestion_time cho mỗi (symbol, trade_id)
ROW_NUMBER() OVER (PARTITION BY symbol, trade_id ORDER BY ingestion_time DESC) = 1
```

**Lý do cần dedup:** Fault Injector có thể tạo duplicate events (10%) để benchmark khả năng phát hiện. Dual ingestion (WebSocket + REST klines) cũng có thể trùng `trade_id`.

### 3.3. Chiến Lược Ghi Silver (MERGE INTO)

```
Nếu bảng Silver đã tồn tại:
    MERGE INTO silver.crypto_trades_clean AS target
    USING new_clean_data AS source
    ON (target.trade_id = source.trade_id AND target.symbol = source.symbol)
    WHEN NOT MATCHED THEN INSERT ALL  ← Chỉ chèn bản ghi mới (trades là immutable)

Nếu chưa tồn tại:
    CREATE TABLE với Partition: days(trade_time)
```

---

## 4. ⚙️ Xử Lý Dữ Liệu (Silver → Gold)

### 4.1. Tổng Hợp OHLCV (Batch Layer — Chính xác tuyệt đối)

**Module:** `src/batch_layer/gold_aggregator.py`

```python
# Tumbling Window 1 phút
windowed = silver_df.groupBy("symbol", F.window("trade_time_ts", "1 minute")).agg(
    F.min(F.struct("trade_time", "price")).getField("price").alias("open"),   # Giá đầu tiên
    F.max("price").alias("high"),
    F.min("price").alias("low"),
    F.max(F.struct("trade_time", "price")).getField("price").alias("close"),  # Giá cuối cùng
    F.sum("quantity").alias("volume"),
    (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
    F.count("*").alias("trade_count"),
)
```

> **Kỹ thuật `F.min(struct(trade_time, price))`:** Đảm bảo `open` LUÔN là giá tại `trade_time` nhỏ nhất, `close` tại `trade_time` lớn nhất — bất kể thứ tự partition trong Spark.

### 4.2. Tổng Hợp OHLCV (Speed Layer — Tức thời, gần đúng)

**Module:** `src/speed_layer/window_aggregator.py`

```python
# Tumbling Window 1 phút với Watermark 2 phút
windowed = kafka_df \
    .withWatermark("trade_time_ts", "2 minutes") \
    .groupBy("symbol", F.window("trade_time_ts", "1 minute")) \
    .agg(
        F.first("price").alias("open"),      # Gần đúng (không đảm bảo chính xác như Batch)
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("quantity").alias("volume"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.count("*").alias("trade_count"),
    )
```

> **SLA mục tiêu:** Latency từ Binance event → `speed_agg` < 5 giây (đo trong Benchmark 1).

### 4.3. VWAP — Volume-Weighted Average Price

**Công thức:** `VWAP = SUM(price × quantity) / SUM(quantity)`

| Layer | Độ chính xác | Nguồn dữ liệu |
|-------|-------------|---------------|
| `speed_agg` | **Gần đúng** (late/duplicate events chưa loại hết) | Spark Streaming, watermark 2 phút |
| `batch_agg` | **Chính xác tuyệt đối** (đã dedup + DQ Gate) | Spark Batch từ Silver Iceberg |

### 4.4. Price Spike Detection (Speed Layer)

**Module:** `src/speed_layer/spike_detector.py`

```python
# Sliding window 5 phút để phát hiện đột biến giá
# Flag is_spike = True khi giá thay đổi > SPIKE_THRESHOLD (mặc định 2%)
price_change_pct = abs(current_close - prev_close) / prev_close * 100
is_spike = price_change_pct > config.speed_layer.spike_threshold
```

---

## 5. 📋 Schema Từng Tầng

### 5.1. TradeEvent — Data Contract Chuẩn Hóa (Kafka Message)

**Module:** `src/ingestion/models.py` — `@dataclass TradeEvent`

| Trường | Kiểu Python | Nullable | Nguồn | Mô tả |
|--------|------------|----------|-------|-------|
| `trade_id` | `int` | NO | `a` (aggTrade ID từ Binance) | ID aggregate trade duy nhất theo symbol |
| `symbol` | `str` | NO | `s` (uppercase) | Cặp giao dịch: `BTCUSDT`, `ETHUSDT`,... |
| `price` | `float` | NO | `p` (parse từ string) | Giá khớp lệnh |
| `quantity` | `float` | NO | `q` (parse từ string) | Khối lượng coin trong aggregate trade |
| `trade_time` | `int` | NO | `T` (epoch ms) | Thời điểm lệnh khớp (event-time ms UTC) |
| `is_buyer_maker` | `bool` | NO | `m` | True = Maker bán (Taker mua market) |
| `ingestion_time` | `int` | YES | `int(time.time() * 1000)` | Thời điểm hệ thống nhận message (ms) |
| `is_injected` | `bool` | YES | FaultInjector | True nếu đây là bản ghi tiêm lỗi thực nghiệm |
| `fault_type` | `str\|None` | YES | FaultInjector | `duplicate`, `late_data`, `out_of_order`, `schema_invalid` |

> **Ánh xạ từ Binance aggTrade payload:** `a→trade_id`, `s→symbol`, `p→price`, `q→quantity`, `T→trade_time`, `m→is_buyer_maker`.

---

### 5.2. Schema Lớp Bronze (`s3://bronze/` — Apache Iceberg)

**Table:** `bronze.crypto_trades` | **Catalog:** Iceberg REST (`http://iceberg-rest:8181`)

| Tên Cột | Kiểu Iceberg | Nullable | Mô Tả |
|---------|-------------|----------|-------|
| `trade_id` | `LongType` | NO | ID aggregate trade từ Binance |
| `symbol` | `StringType` | NO | Cặp giao dịch uppercase (`BTCUSDT`) |
| `price` | `DoubleType` | NO | Giá khớp lệnh |
| `quantity` | `DoubleType` | NO | Khối lượng coin |
| `trade_time` | `LongType` | NO | Epoch milliseconds UTC — **Partition field** |
| `is_buyer_maker` | `BooleanType` | NO | Market maker hay taker |
| `ingestion_time` | `LongType` | YES | Epoch ms khi producer nhận |
| `is_injected` | `BooleanType` | YES | Cờ lỗi inject |
| `fault_type` | `StringType` | YES | Loại lỗi inject |
| `batch_run_id` | `StringType` | YES | ID của Batch Job lần ghi |
| `bronze_written_at` | `TimestampType` | YES | Thời điểm ghi vào Bronze |

**Partition:** `days(trade_time)` — Hidden Partition (Iceberg tự quản lý tên folder)
**Format:** Apache Parquet + Iceberg metadata
**Ghi mode:** Append-only (immutable raw truth)

---

### 5.3. Schema Lớp Silver (`s3://silver/` — Apache Iceberg)

**Table:** `silver.crypto_trades_clean`

| Tên Cột | Kiểu Iceberg | Nullable | Mô Tả |
|---------|-------------|----------|-------|
| `trade_id` | `LongType` | NO | **Khóa dedup** — đã lọc null |
| `symbol` | `StringType` | NO | Cặp giao dịch uppercase |
| `price` | `DoubleType` | NO | Giá hợp lệ (đã qua DQ Gate) |
| `quantity` | `DoubleType` | NO | Khối lượng hợp lệ |
| `trade_time` | `LongType` | NO | Epoch ms UTC — **Partition field** |
| `is_buyer_maker` | `BooleanType` | NO | Market maker hay taker |
| `ingestion_time` | `LongType` | YES | Thời điểm ingestion |
| `is_injected` | `BooleanType` | YES | Cờ lỗi inject (để trace ground truth) |
| `fault_type` | `StringType` | YES | Loại lỗi (chỉ non-null với schema_invalid đã qua) |
| `silver_processed_at` | `TimestampType` | YES | Thời điểm Silver Job xử lý |
| `batch_run_id` | `StringType` | YES | ID Batch Job |

**Partition:** `days(trade_time)`
**Ghi mode:** MERGE INTO (Upsert idempotent theo `(symbol, trade_id)`)

---

### 5.4. Schema Lớp Quarantine (`s3://silver/quarantine/` — Apache Iceberg)

| Tên Cột | Mô Tả |
|---------|-------|
| *(Tất cả cột Bronze)* | Giữ nguyên dữ liệu gốc không thay đổi |
| `error_code` | Mã lỗi DQ: `NULL_TRADE_ID`, `INVALID_PRICE`, `INVALID_QUANTITY`, `INJECTED_SCHEMA_ERROR`,... |
| `error_message` | Mô tả chi tiết vi phạm rule |
| `rejected_at` | `TimestampType` — thời điểm bị cách ly |

---

### 5.5. Schema Lớp Gold (`s3://gold/` — Apache Iceberg)

**Table:** `gold.ohlcv_aggregated`

| Tên Cột | Kiểu Iceberg | Nullable | Mô Tả |
|---------|-------------|----------|-------|
| `symbol` | `StringType` | NO | Cặp giao dịch |
| `window_start` | `TimestampType` | NO | Thời điểm mở nến — **Partition field** |
| `window_end` | `TimestampType` | NO | Thời điểm đóng nến |
| `window_duration` | `StringType` | NO | `1m` hoặc `5m` |
| `open_price` | `DoubleType` | NO | Giá mở cửa (giá tại trade_time nhỏ nhất) |
| `high_price` | `DoubleType` | NO | Giá cao nhất trong nến |
| `low_price` | `DoubleType` | NO | Giá thấp nhất trong nến |
| `close_price` | `DoubleType` | NO | Giá đóng cửa (giá tại trade_time lớn nhất) |
| `volume` | `DoubleType` | NO | Tổng khối lượng coin trong nến |
| `vwap` | `DoubleType` | NO | Volume-Weighted Average Price |
| `trade_count` | `LongType` | YES | Số aggregate trades trong nến |
| `batch_run_id` | `StringType` | YES | ID Batch Job |
| `gold_written_at` | `TimestampType` | YES | Thời điểm ghi |

**Partition:** `days(window_start)`
**Ghi mode:** Overwrite per batch run (full recompute từ Silver — idempotent)

---

### 5.6. ClickHouse Schema (OLAP — Serving Layer)

**Database:** `lakehouse` | **Config:** `configs/clickhouse/init.sql`

#### Bảng `speed_agg` — Speed View (Provisional)

| Tên Cột | Kiểu ClickHouse | Mô Tả |
|---------|----------------|-------|
| `symbol` | `LowCardinality(String)` | Cặp giao dịch |
| `window_start` | `DateTime64(3, 'UTC')` | Mở nến |
| `window_end` | `DateTime64(3, 'UTC')` | Đóng nến |
| `open_price` | `Float64` | Giá mở |
| `high_price` | `Float64` | Giá cao |
| `low_price` | `Float64` | Giá thấp |
| `close_price` | `Float64` | Giá đóng |
| `volume` | `Float64` | Khối lượng |
| `trade_count` | `UInt64` | Số trades |
| `vwap` | `Float64` | VWAP gần đúng |
| `is_spike` | `UInt8` | Flag đột biến giá |
| `created_at` | `DateTime64(3, 'UTC')` | Thời điểm ghi |

**Engine:** `ReplacingMergeTree(created_at)` — tự động dedup khi merge
**Partition:** `toYYYYMMDD(window_start)`

#### Bảng `batch_agg` — Batch View (Reconciled)

> Cùng cấu trúc với `speed_agg`, thêm cột `batch_run_id String` để truy vết.

#### Bảng `system_watermark` — Mốc Batch Watermark

| Tên Cột | Kiểu | Mô Tả |
|---------|------|-------|
| `layer` | `String` | `'batch_layer'` |
| `watermark_time` | `DateTime64(3, 'UTC')` | Mốc thời gian Batch đã xử lý đến |
| `updated_at` | `DateTime64(3, 'UTC')` | Thời điểm cập nhật |

**Engine:** `ReplacingMergeTree(updated_at)` ORDER BY `(layer)`

---

## 6. 🌐 Tổng Quan Về Dữ Liệu

### 6.1. Phạm Vi & Quy Mô

| Thông số | Giá trị |
|----------|---------|
| **Số cặp giao dịch** | Top 10 USDT pairs (dynamic, cập nhật mỗi lần khởi động) |
| **Khung thời gian** | Từ ngày chạy pipeline trở đi (liên tục) |
| **Lịch sử khởi tạo** | ~1.000 nến × 10 symbols = ~10.000 records/lần batch đầu |
| **Tần suất streaming** | Liên tục (aggTrade events ~vài ms đến vài giây/event) |
| **Speed Layer SLA** | < 5 giây latency từ Binance → `speed_agg` |
| **Batch Trigger** | Theo lịch Dagster (cấu hình trong `dagster_project/`) |
| **Kích thước ước tính** | Bronze: ~100MB-500MB/ngày; Silver: tương đương sau dedup; Gold: ~MB/ngày |
| **Retention Kafka** | 24 giờ (`log.retention.hours=24`) |
| **Iceberg Snapshot Retention** | 7 ngày (trước khi `expire_snapshots()` chạy) |

### 6.2. Đặc Điểm Dữ Liệu

- **Dữ liệu tài chính thời gian thực:** Mỗi record = 1 aggregate trade trên sàn Binance
- **Append-heavy:** Bronze nhận hàng nghìn-triệu records/ngày (thị trường mở 24/7)
- **Biến động cao:** Giá thay đổi từng giây, đặc biệt trong sự kiện thị trường lớn
- **Dual ingestion:** Cùng `trade_id` có thể đến từ WebSocket lẫn batch REST → cần dedup
- **Fault-injected data:** Intentional errors với `is_injected=True` để benchmark DQ Gate
- **UTC timezone:** Tất cả timestamp đều là UTC

### 6.3. Top Coins Điển Hình (USDT Pairs — Top Volume)

```
BTCUSDT   — Bitcoin / USDT         (Khối lượng lớn nhất)
ETHUSDT   — Ethereum / USDT
BNBUSDT   — BNB / USDT
SOLUSDT   — Solana / USDT
XRPUSDT   — Ripple / USDT
DOGEUSDT  — Dogecoin / USDT
ADAUSDT   — Cardano / USDT
AVAXUSDT  — Avalanche / USDT
SHIBUSDT  — Shiba Inu / USDT       (Meme coin, giá nhỏ)
DOTUSDT   — Polkadot / USDT
... (top 10 thay đổi theo thị trường, cập nhật mỗi lần khởi động producer)
```

---

## 7. 📝 Mô Tả Chi Tiết Ánh Xạ Dữ Liệu

### 7.1. Ánh Xạ Binance aggTrade → TradeEvent

| Trường gốc (Binance) | Trường sau chuẩn hóa | Ý nghĩa chi tiết |
|---------------------|---------------------|-----------------|
| `e` = `"aggTrade"` | *(không lưu)* | Event type — aggTrade là aggregate |
| `E` | *(không lưu trực tiếp)* | Event time từ Binance server |
| `s` | `symbol` (uppercase) | Cặp giao dịch, VD: `"BTCUSDT"` |
| `a` | `trade_id` | Aggregate Trade ID — **Khóa dedup** |
| `p` | `price` (float) | Giá khớp lệnh (parse từ string) |
| `q` | `quantity` (float) | Khối lượng aggregate (parse từ string) |
| `T` | `trade_time` (epoch ms) | Thời điểm khớp lệnh thực tế |
| `m` | `is_buyer_maker` (bool) | `True` = Taker bán (short), `False` = Taker mua |

### 7.2. Phân Biệt Speed vs Batch Output

| Chỉ số | `speed_agg` (Provisional) | `batch_agg` (Reconciled) |
|--------|--------------------------|--------------------------|
| **Độ chính xác OHLCV** | Gần đúng (open/close dùng first/last) | Chính xác (dùng min/max trade_time) |
| **VWAP** | Gần đúng (có thể thiếu late events) | Chính xác (full data sau DQ + dedup) |
| **Duplicate handling** | Watermark 2 phút (một phần) | Hoàn toàn dedup theo (symbol, trade_id) |
| **Latency** | < 5 giây | Theo lịch Batch (phút - giờ) |
| **Status label** | `'Provisional'` | `'Reconciled'` |
| **Serving Layer** | Dùng khi `t >= system_watermark` | Dùng khi `t < system_watermark` |

### 7.3. Query Merger Logic (Auto-Correcting Serving Layer)

**Module:** `src/serving_layer/query_merger.py`

```
Nhận request: GET /api/market?symbol=BTCUSDT&from=T1&to=T2
        ↓
Đọc system_watermark (WM) từ ClickHouse
        ↓
Xác định vùng truy vấn:
  ├── [T1, T2] ⊂ [0, WM)   → 100% Reconciled   → trả batch_agg
  ├── [T1, T2] ⊂ [WM, ∞)   → 100% Provisional  → trả speed_agg
  └── Giao thoa [T1, WM) + [WM, T2] → Partially Reconciled
        │                               → UNION batch_agg + speed_agg
        │                               → tính reconciliation_delta = |VWAP_speed - VWAP_batch|
        ↓
Trả Response:
  {
    "status": "Reconciled" | "Provisional" | "Partially Reconciled",
    "data": [...],
    "batch_watermark": "2026-09-01T10:00:00Z",
    "reconciliation_delta": 0.0023  (chỉ có trong Partially Reconciled)
  }
```

---

## 8. 📂 Vị Trí File & Tài Nguyên Tham Khảo

| Module / Tài nguyên | Đường dẫn trong repo |
|--------------------|---------------------|
| Binance WebSocket Producer | `src/ingestion/binance_ws.py` |
| Kafka Producer (Resilient) | `src/ingestion/kafka_producer.py` |
| TradeEvent & DLQEvent Models | `src/ingestion/models.py` |
| Fault Injector | `src/ingestion/fault_injector.py` |
| Historical Backfill (REST → Kafka) | `src/ingestion/historical_backfill.py` |
| Batch Producer (root, alternative) | `ingestion/producer_batch.py` |
| Bronze Writer (Kafka → Iceberg) | `src/batch_layer/bronze_writer.py` |
| Silver Processor (DQ + Dedup) | `src/batch_layer/silver_processor.py` |
| Gold Aggregator (OHLCV + VWAP) | `src/batch_layer/gold_aggregator.py` |
| ClickHouse Sync (Gold → batch_agg) | `src/batch_layer/clickhouse_sync.py` |
| Watermark Manager | `src/batch_layer/watermark_manager.py` |
| Iceberg Utilities (table init) | `src/batch_layer/iceberg_utils.py` |
| Compaction Job | `src/batch_layer/compaction.py` |
| DQ Checks | `src/data_quality/dq_checks.py` |
| Quarantine Table | `src/data_quality/quarantine.py` |
| DQ Metrics Collector | `src/data_quality/dq_metrics.py` |
| Spark Streaming Job | `src/speed_layer/spark_streaming.py` |
| Window Aggregator | `src/speed_layer/window_aggregator.py` |
| Price Spike Detector | `src/speed_layer/spike_detector.py` |
| ClickHouse Writer (Speed) | `src/speed_layer/clickhouse_writer.py` |
| FastAPI Serving Layer | `src/serving_layer/api_routes.py` |
| Query Merger | `src/serving_layer/query_merger.py` |
| Watermark Reader | `src/serving_layer/watermark_reader.py` |
| ClickHouse Schema DDL | `configs/clickhouse/init.sql` |
| Kafka Topic Config | `configs/kafka/topics.yaml` |
| Iceberg Table Config | `configs/iceberg/tables.yaml` |
| Spark Config | `configs/spark/spark-defaults.conf` |
| Docker Compose (toàn bộ infra) | `docker-compose.yml` |
| Unit Tests Ingestion | `tests/test_ingestion.py` |
| Benchmark Scripts | `scripts/benchmarks/` |

---

## 9. 📊 Tóm Tắt Toàn Bộ Data Flow — Lambda Architecture

```
[BINANCE]
    │
    ├── WebSocket (wss://stream.binance.com:9443/stream)
    │       @aggTrade stream — Top 10 USDT pairs (dynamic)
    │       ↓ FaultInjector (optional — Benchmark mode)
    │   [KAFKA] topic: crypto.trades (key=symbol)
    │       │
    │       ├── ══ SPEED LAYER ══════════════════════════════════
    │       │   Spark Structured Streaming
    │       │   Watermark 2 phút + Tumbling Window 1m/5m
    │       │   OHLCV + VWAP + Spike Detection
    │       │   ↓ SLA < 5 giây
    │       │   [CLICKHOUSE] lakehouse.speed_agg (Provisional)
    │       │
    │       └── ══ BATCH LAYER ══════════════════════════════════
    │           Spark Batch Job (scheduled by Dagster)
    │           ↓
    │           [ICEBERG BRONZE] s3://bronze/ (MinIO)
    │             Table: bronze.crypto_trades
    │             Partition: days(trade_time), Append-only
    │           ↓ DQ Gate (6 rules) + Dedup (trade_id)
    │           ├── [ICEBERG SILVER] s3://silver/
    │           │     Table: silver.crypto_trades_clean
    │           │     MERGE INTO (idempotent)
    │           └── [ICEBERG QUARANTINE] s3://silver/quarantine/
    │                 Bad records + error_code
    │           ↓ OHLCV + VWAP Aggregation (1m + 5m)
    │           [ICEBERG GOLD] s3://gold/
    │             Table: gold.ohlcv_aggregated
    │           ↓ ClickHouse Sync
    │           [CLICKHOUSE] lakehouse.batch_agg (Reconciled)
    │           [CLICKHOUSE] lakehouse.system_watermark → UPDATE
    │
    └── REST API (/api/v3/klines) — Historical Backfill
            1.000 nến × 10 symbols → Kafka crypto.trades
            (xử lý bởi cùng Batch pipeline)

[SERVING LAYER]
    FastAPI Auto-Correcting Query Merger
    ↓ Đọc system_watermark
    ├── t < WM  → batch_agg (Reconciled)
    ├── t >= WM → speed_agg (Provisional)
    └── giao thoa → UNION + reconciliation_delta
    ↓
    REST API: http://localhost:8000/docs

[DASHBOARD]
    Streamlit → http://localhost:8501
    ├── Candlestick chart (Plotly, real-time)
    ├── Price Spike alerts
    ├── Watermark status panel
    └── DQ Metrics panel
```

---

*Tài liệu được cập nhật: 2026-09-01*
*Phiên bản: Lambda Architecture v1.0*
*Nhóm: Phạm Minh Quân (23133060) & Nguyễn Đặng Quốc Anh (23133004)*
*GVHD: ThS. Đoàn Minh Trí — HCMUTE, Khoa CNTT, 2026-2027*
