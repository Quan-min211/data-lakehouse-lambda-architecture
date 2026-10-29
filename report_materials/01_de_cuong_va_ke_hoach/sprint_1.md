# SPRINT 1 (TUẦN 1): THIẾT KẾ DATA CONTRACTS & INGESTION PIPELINE

* **Người thực hiện:** Nguyễn Đặng Quốc Anh (`23133004`)
* **Thời gian:** Tuần 1
* **Trạng thái:** Hoàn thành (100%)

---

## Danh Sách Nhiệm Vụ & Tiến Độ

- [x] **Task 1.1: Định nghĩa Data Contracts & JSON Schemas**
  - [x] Tạo `datasets/schemas/trade_event_schema.json` (Chuẩn hóa sự kiện Binance aggTrade).
  - [x] Tạo `datasets/schemas/dlq_event_schema.json` (Schema cho Dead-Letter Queue).
  - [x] Tạo `src/utils/config.py` (Load biến môi trường từ .env).
  - [x] Tạo `src/utils/logger.py` (Logging chuẩn UTF-8).

- [x] **Task 1.2: Xây dựng Module Ingestion & Fault Injector**
  - [x] Tạo `src/ingestion/models.py` (`TradeEvent`, `DLQEvent`).
  - [x] Tạo `src/ingestion/fault_injector.py` (Bơm 4 loại lỗi: duplicate, late, out-of-order, schema invalid).
  - [x] Tạo `src/ingestion/kafka_producer.py` (`ResilientKafkaProducer` có retry backoff).
  - [x] Tạo `src/ingestion/binance_ws.py` (Top-10 WebSocket stream có auto-reconnect).
  - [x] Tạo `src/ingestion/historical_backfill.py` (Backfill dữ liệu lịch sử).

- [x] **Task 1.3: Tạo Seed Data & Unit Tests**
  - [x] Tạo `scripts/seed_data.py` (Sinh 1,000–5,000 trade events phục vụ test offline).
  - [x] Tạo `tests/test_ingestion.py` (8 unit tests chạy xanh 100%).
  - [x] Cập nhật `src/ingestion/README.md`.
