# KẾ HOẠCH CHI TIẾT 13 TUẦN — NGUYỄN ĐẶNG QUỐC ANH
## MSSV: `23133004` | TLCN 2026-2027
### Đề tài: Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa

> **Phụ trách chính:** Ingestion Pipeline · Fault Injector · Speed Layer (Spark Streaming) · Serving Layer (Auto-Correcting Query Merger) · Benchmark 1 & 2
> **Phối hợp:** Xác nhận Bronze Table schema, hỗ trợ Batch Layer ghi đúng ClickHouse

---

## 📅 TUẦN 1 — Chuẩn bị dữ liệu & hoàn thiện Ingestion Pipeline

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | ⚠️ **Thống nhất 1 thư mục ingestion duy nhất** | Xóa `ingestion/` root hoặc `src/ingestion/` | Chỉ còn 1 nguồn code, không có 2 nơi song song |
| 2 | Finalize `TradeEvent` model | `src/ingestion/models.py` | Fields: `trade_id`, `symbol`, `price`, `quantity`, `trade_time`, `is_buyer_maker`, `is_injected`, `fault_type`, `ingestion_time` — **thông báo cho Quân** |
| 3 | Test WebSocket Binance thực tế | `src/ingestion/binance_ws.py` | Nhận được data live từ `BTCUSDT@aggTrade`, log ra terminal |
| 4 | Test Kafka Producer ghi data | `src/ingestion/kafka_producer.py` | Data được ghi vào topic `crypto_trades_raw`, verify bằng Kafka console consumer |
| 5 | Test Dead-Letter Queue (DLQ) | `src/ingestion/kafka_producer.py` | Message lỗi được chuyển đúng vào `crypto_trades_dlq` |
| 6 | Cấu hình Kafka topics | `configs/kafka/topics.yaml` | Topics `crypto_trades_raw` (3 partitions), `crypto_trades_dlq` (1 partition) tồn tại |
| 7 | Thống nhất ClickHouse schema với Quân | `configs/clickhouse/init.sql` | Draft DDL 3 bảng: `speed_agg`, `batch_agg`, `system_watermark` |

**🎯 Milestone Tuần 1:** Ingestion pipeline chạy được end-to-end (WS → Kafka). `TradeEvent` model finalized, gửi cho Quân.

---

## 📅 TUẦN 2 — Chuẩn bị nền tảng & chốt Interface

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | ⚠️ **Chốt ClickHouse schema với Quân** | `configs/clickhouse/init.sql` | DDL finalized — không đổi nữa |
| 2 | Test Kafka Consumer cơ bản | `tests/test_kafka_consumer.py` | Đọc được messages từ topic `crypto_trades_raw` |
| 3 | Viết Historical Backfill từ REST API | `src/ingestion/historical_backfill.py` | Fetch lịch sử BTCUSDT từ Binance REST, publish vào Kafka |
| 4 | Test toàn bộ Ingestion với 10,000 events | — | Hệ thống ổn định, không mất message, DLQ hoạt động |
| 5 | Setup `src/utils/` config loader & logger | `src/utils/config.py`, `src/utils/logger.py` | Tất cả module đọc cấu hình từ `.env`, logging chuẩn hóa |

**🎯 Milestone Tuần 2:** Ingestion ổn định, có thể generate data liên tục. ClickHouse schema đã chốt.

---

## 📅 TUẦN 3 — Tích hợp Fault Injector & kiểm thử dữ liệu lỗi

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tích hợp Fault Injector vào Ingestion pipeline | `src/ingestion/binance_ws.py` + `src/ingestion/fault_injector.py` | FaultInjector được gọi trong `on_message()`, lỗi được inject trước khi gửi Kafka |
| 2 | Test 4 loại lỗi độc lập | `tests/test_fault_injector.py` | Duplicate (10%), Late (10%), Out-of-order, Schema invalid — đều inject đúng |
| 3 | Cấu hình tỷ lệ lỗi qua env vars | `.env.example` | `FAULT_DUPLICATE_RATE=0.10`, `FAULT_LATE_RATE=0.10` config được |
| 4 | Verify nhãn `is_injected=true` trong Kafka | — | Consumer đọc messages, kiểm tra `is_injected` và `fault_type` field đúng |
| 5 | Generate dataset lỗi cho Quân test DQ Gate | — | Export 50,000 events có mix 4 loại lỗi vào file CSV để Quân dùng |

**🎯 Milestone Tuần 3:** Fault Injector hoàn chỉnh, data có nhãn `is_injected=true` đang chảy vào Kafka.

---

## 📅 TUẦN 4 — Xây dựng Speed Layer (Spark Structured Streaming)

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tạo Spark Streaming Job entry point | `src/speed_layer/spark_streaming.py` | `python spark_streaming.py` khởi động Spark Streaming job |
| 2 | Đọc từ Kafka topic | `src/speed_layer/spark_streaming.py` | Spark đọc được `crypto_trades_raw` từ Kafka |
| 3 | Cấu hình Tumbling Window 1m + 5m | `src/speed_layer/window_aggregator.py` | Window aggregation hoạt động theo `trade_time` (event-time) |
| 4 | Cấu hình Watermark 1-2 phút | `src/speed_layer/window_aggregator.py` | Late events trong 1-2 phút được xử lý, events trễ hơn bị drop |
| 5 | Tính OHLCV + VWAP nhanh | `src/speed_layer/metrics_calculator.py` | OHLCV (Open/High/Low/Close/Volume) + VWAP đúng công thức |
| 6 | Price Spike Detection | `src/speed_layer/spike_detector.py` | Flag `is_spike=true` khi giá thay đổi > ngưỡng trong sliding window |
| 7 | Ghi vào `speed_agg` ClickHouse | `src/speed_layer/clickhouse_writer.py` | Data ghi được với nhãn `status='Provisional'` — **dùng schema Quân đã chốt** |

**🎯 Milestone Tuần 4:** Speed Layer ghi được vào `speed_agg` với latency < 5 giây.

---

## 📅 TUẦN 5 — Tối ưu Speed Layer & kiểm thử hiệu năng

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Đo latency end-to-end Speed Layer | `tests/test_speed_latency.py` | Đo P50/P95/P99 từ Binance event → `speed_agg` — mục tiêu < 5s |
| 2 | Test xử lý late data đúng | — | Event trễ 1 phút được xử lý; event trễ 3 phút bị drop đúng |
| 3 | Test duplicate events | — | Duplicate events không làm sai VWAP (do Watermark) |
| 4 | Tối ưu checkpoint location | `src/speed_layer/spark_streaming.py` | Checkpoint lưu vào đúng thư mục, restart không mất state |
| 5 | Hỗ trợ Quân xác nhận Silver Processor schema | — | Xác nhận fields của `TradeEvent` sau dedup vẫn đúng |

**🎯 Milestone Tuần 5:** Speed Layer đạt SLA < 5s, xử lý late/duplicate đúng.

---

## 📅 TUẦN 6 — Chuẩn bị Serving Layer & nhận mock data từ Quân

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Thiết kế Response Schema | `src/serving_layer/schemas.py` | Định nghĩa Pydantic models: `MarketDataResponse` với `status`, `reconciliation_delta`, `batch_watermark` |
| 2 | Tạo ClickHouse Query Client | `src/serving_layer/clickhouse_client.py` | Đọc được từ cả `speed_agg` và `batch_agg` |
| 3 | Nhận mock `batch_agg` data từ Quân | — | Verify schema đúng, query được từ ClickHouse |
| 4 | Phác thảo Query Merger logic | `src/serving_layer/query_merger.py` | Draft logic phân luồng theo watermark (chưa cần test đầy đủ) |
| 5 | Thiết lập FastAPI skeleton đầy đủ | `src/serving_layer/api_routes.py` | Các endpoint được định nghĩa: `GET /api/market`, `GET /api/health`, `GET /api/watermark` |

**🎯 Milestone Tuần 6:** Serving Layer có skeleton hoàn chỉnh, đọc được ClickHouse.

---

## 📅 TUẦN 7 — Hoàn thiện Query Merger & nhận Watermark từ Quân

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tạo Watermark Reader | `src/serving_layer/watermark_reader.py` | Đọc `max(processed_until)` từ bảng `system_watermark` trong ClickHouse |
| 2 | Hoàn thiện Query Merger algorithm | `src/serving_layer/query_merger.py` | Logic 3 trường hợp: `t_end < wm` → Reconciled; `t_start >= wm` → Provisional; giao thoa → Partially Reconciled |
| 3 | Tính `reconciliation_delta` | `src/serving_layer/query_merger.py` | `delta = abs(VWAP_speed - VWAP_batch)` cho vùng giao thoa |
| 4 | Implement `GET /api/market` endpoint | `src/serving_layer/api_routes.py` | API trả về JSON đầy đủ với `status` và `reconciliation_delta` |
| 5 | Test Query Merger với watermark mock | `tests/test_query_merger.py` | 3 test cases cho 3 trường hợp phân luồng |

**🎯 Milestone Tuần 7:** Query Merger logic hoàn chỉnh, test pass.

---

## 📅 TUẦN 8 — Tích hợp hệ thống & test E2E lần đầu

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Chạy toàn bộ hệ thống lần đầu | — | `docker compose up -d` → tất cả services green |
| 2 | Chạy Ingestion → Speed Layer song song | — | Data chảy từ Binance → Kafka → `speed_agg` liên tục |
| 3 | Gọi `GET /api/market` với data thực | — | API trả kết quả Provisional đúng |
| 4 | Kích hoạt Batch Layer (phối hợp Quân) | — | Sau khi Batch chạy: `system_watermark` cập nhật, API chuyển sang Reconciled |
| 5 | Ghi nhận và fix bugs tích hợp | — | List bugs E2E đã fix |
| 6 | Swagger UI hoạt động | `http://localhost:8000/docs` | Tất cả endpoints có docs đầy đủ |

**🎯 Milestone Tuần 8:** Hệ thống chạy E2E thành công, Query Merger phân luồng đúng.

---

## 📅 TUẦN 9 — Dashboard Streamlit (phần của Quốc Anh)

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tạo Streamlit app skeleton | `dashboard/app.py` | `streamlit run app.py` mở được tại `http://localhost:8501` |
| 2 | Component: Biểu đồ nến Real-time | `dashboard/components/candlestick.py` | Plotly Candlestick chart tự refresh hiển thị BTCUSDT |
| 3 | Component: Bảng cảnh báo Price Spike | `dashboard/components/spike_table.py` | Bảng liệt kê spike events với timestamp và mức thay đổi giá |
| 4 | Tích hợp các component của Quân | `dashboard/app.py` | App hiển thị đầy đủ Candlestick + Spike + Watermark + DQ panel |

**🎯 Milestone Tuần 9:** Dashboard đầy đủ tính năng, kết nối API hoạt động.

---

## 📅 TUẦN 10 — Thực thi Benchmark 1 & 2

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Viết script Benchmark 1 (Latency) | `scripts/benchmarks/bench_latency.py` | Script thực hiện 1,000 queries ngẫu nhiên, đo P50/P95/P99/RPS |
| 2 | **Chạy Benchmark 1** | — | Kết quả: Batch-only vs Speed-only vs Lambda Merge latency |
| 3 | Xuất kết quả CSV | `results/logs/bench_latency_results.csv` | Dữ liệu thô đầy đủ |
| 4 | Viết script Benchmark 2 (Reconciliation) | `scripts/benchmarks/bench_reprocess.py` | Script đo `VWAP_delta` theo thời gian, đo thời gian hội tụ về 0 |
| 5 | **Chạy Benchmark 2** | — | Kết quả: Error % theo loại lỗi, Convergence time sau Batch |
| 6 | Xuất kết quả CSV | `results/logs/bench_reprocess_results.csv` | Dữ liệu thô đầy đủ |

**🎯 Milestone Tuần 10:** Benchmark 1 & 2 hoàn thành, có số liệu raw CSV.

---

## 📅 TUẦN 11 — Tổng hợp Benchmark & hỗ trợ Quân

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Hỗ trợ Quân chạy Benchmark 3 & 4 | — | Đảm bảo Fault Injector data và hệ thống ổn định |
| 2 | Tổng hợp script chạy all benchmarks | `scripts/benchmarks/run_all_benchmarks.py` | 1 lệnh chạy cả 4 Benchmark |
| 3 | Vẽ biểu đồ Benchmark 1 | `results/plots/benchmark1_latency.png` | Biểu đồ Box plot / Bar chart so sánh 3 phương án latency |
| 4 | Vẽ biểu đồ Benchmark 2 | `results/plots/benchmark2_convergence.png` | Biểu đồ đường Error Delta hội tụ về 0 theo thời gian |

**🎯 Milestone Tuần 11:** Cả 4 Benchmark có kết quả, biểu đồ Benchmark 1 & 2 hoàn chỉnh.

---

## 📅 TUẦN 12 — Phân tích số liệu & Viết báo cáo

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Phân tích kết quả Benchmark 1 & 2 | — | Viết đoạn phân tích Trade-offs Lambda vs Batch-only vs Speed-only |
| 2 | Viết Chương 3 — Thiết kế kiến trúc | `docs/bao_cao/chuong3.md` | Hoàn chỉnh mục 3.1 → 3.8 (Ingestion, Speed, Serving, Query Merger) |
| 3 | Viết Chương 4 — Thực nghiệm Benchmark | `docs/bao_cao/chuong4.md` | Phần 4.1, 4.2, 4.3 (phần Quốc Anh) + 4.6 (Trade-offs tổng hợp) |
| 4 | Viết Abstract tiếng Anh | `docs/bao_cao/abstract.md` | 200-250 từ tóm tắt đề tài bằng tiếng Anh |

**🎯 Milestone Tuần 12:** Chương 3 & 4 (phần mình) hoàn thảo.

---

## 📅 TUẦN 13 — Hoàn thiện & Chuẩn bị bảo vệ

| STT | Nhiệm vụ | Kết quả cần đạt |
|:---:|:---|:---|
| 1 | Smoke test Speed Layer + Serving Layer + Dashboard | Chạy ổn định, không crash |
| 2 | Rà soát báo cáo (Chương 3, 4 — phần Quốc Anh) | Không còn lỗi, đủ nội dung |
| 3 | Hoàn thiện slide (phần Quốc Anh: Slide Ingestion + Speed + Serving) | Slide rõ ràng, có số liệu Benchmark 1 & 2 |
| 4 | Chuẩn bị câu trả lời phản biện về Query Merger, Watermark | Nắm vững cơ chế phân luồng và công thức reconciliation_delta |
| 5 | Nộp báo cáo & bảo vệ đề tài | 🎓 Hoàn thành! |

---

## 📌 FILES QUỐC ANH CHỊU TRÁCH NHIỆM

```
src/ingestion/
├── models.py                 ← Tuần 1  ⚠️ Phải finalize sớm cho Quân
├── binance_ws.py             ← Tuần 1
├── kafka_producer.py         ← Tuần 1
├── fault_injector.py         ← Tuần 3
├── historical_backfill.py    ← Tuần 2

src/speed_layer/
├── spark_streaming.py        ← Tuần 4
├── window_aggregator.py      ← Tuần 4
├── metrics_calculator.py     ← Tuần 4
├── spike_detector.py         ← Tuần 4
└── clickhouse_writer.py      ← Tuần 4

src/serving_layer/
├── api_routes.py             ← Tuần 6-8
├── query_merger.py           ← Tuần 7  ⚠️ Core algorithm
├── clickhouse_client.py      ← Tuần 6
├── watermark_reader.py       ← Tuần 7
└── schemas.py                ← Tuần 6

src/utils/
├── config.py                 ← Tuần 2
└── logger.py                 ← Tuần 2

configs/kafka/topics.yaml     ← Tuần 1

scripts/benchmarks/
├── bench_latency.py          ← Tuần 10
└── bench_reprocess.py        ← Tuần 10

dashboard/
├── app.py                    ← Tuần 9
└── components/
    ├── candlestick.py        ← Tuần 9
    └── spike_table.py        ← Tuần 9

results/logs/
├── bench_latency_results.csv
└── bench_reprocess_results.csv

results/plots/
├── benchmark1_latency.png
└── benchmark2_convergence.png
```
