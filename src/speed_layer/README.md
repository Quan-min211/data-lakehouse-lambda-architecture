# Speed Layer — Spark Structured Streaming

Tầng Tốc Độ (Speed Layer) trong kiến trúc **Lambda Lakehouse**, chịu trách nhiệm xử lý luồng sự kiện giao dịch thời gian thực (`crypto_trades_raw`) từ Kafka với độ trễ thấp (**SLA < 5 giây**) và nạp vào ClickHouse table `lakehouse.speed_agg` (Trạng thái: `Provisional`).

---

## 🏛️ Kiến Trúc & Luồng Dữ Liệu

```
                       KAFKA TOPIC: crypto_trades_raw
                                     │
                                     ▼
                    [SPARK STRUCTURED STREAMING]
             (Micro-batch trigger: 5s, Event-time trade_time)
                                     │
         ┌───────────────────────────┴───────────────────────────┐
         ▼                                                       ▼
 [WATERMARKING & WINDOW]                                  [ANALYTICS ENGINE]
 • Watermark: 1-2 phút                                    • MetricsCalculator:
 • Tumbling Window: 1m & 5m                                 - OHLCV (Open, High, Low, Close, Volume)
                                                            - VWAP = Σ(Price × Qty) / Σ(Qty)
                                                          • SpikeDetector:
                                                            - Price Spike (|Close - Open| / Open ≥ 2%)
                                                            - Volatility Range ((High - Low) / Low ≥ 3%)
                                     │
                                     ▼
                      [CLICKHOUSE SINK WRITER]
                 (Using foreachBatch + clickhouse-connect)
                                     │
                                     ▼
                      TABLE: lakehouse.speed_agg
                   (Engine: ReplacingMergeTree, UTC)
```

---

## 📁 Cấu Trúc Thành Phần

| File | Chức năng |
| :--- | :--- |
| [`spark_streaming.py`](./spark_streaming.py) | **Entry Point** chính, khởi tạo SparkSession, quản lý vòng đời streaming query, checkpointing. |
| [`window_aggregator.py`](./window_aggregator.py) | Parse JSON Kafka theo Data Contract, cấu hình Watermark và Tumbling Window 1m/5m. |
| [`metrics_calculator.py`](./metrics_calculator.py) | Tính toán chính xác nến OHLCV và công thức chuẩn VWAP $\\frac{\\sum P \\cdot Q}{\\sum Q}$. |
| [`spike_detector.py`](./spike_detector.py) | Thuật toán phát hiện biến động giá bất thường (Price Spike Detection) theo thời gian thực. |
| [`clickhouse_writer.py`](./clickhouse_writer.py) | Sink Writer ghi dữ liệu micro-batch vào ClickHouse, đảm bảo tính Idempotent với ReplacingMergeTree. |

---

## 🚀 Hướng Dẫn Chạy

### 1. Khởi động hạ tầng Docker
```bash
docker compose up -d kafka clickhouse
```

### 2. Chạy Speed Layer Job
```bash
# Chạy mặc định (Kafka localhost:9094, Window 1m, Watermark 1m, Trigger 5s)
python src/speed_layer/spark_streaming.py

# Tuỳ biến tham số:
python src/speed_layer/spark_streaming.py --trigger "3 seconds" --watermark "2 minutes"
```

---

## 🧪 Chạy Kiểm Thử (Unit Tests)

```bash
python -m unittest discover -s tests -v
```
