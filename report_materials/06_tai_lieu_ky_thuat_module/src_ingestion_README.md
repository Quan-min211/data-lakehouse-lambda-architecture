# Tầng Thu Thập & Chuẩn Hóa Dữ Liệu (Ingestion Layer)

Thư mục này chứa mã nguồn thu thập dữ liệu giao dịch tiền mã hóa thời gian thực (Live WebSocket) và lịch sử (Historical Backfill) từ sàn giao dịch Binance, cùng module chủ động tiêm lỗi có kiểm soát (**Fault Injector**) phục vụ đánh giá thực nghiệm.

---

## 🏛️ Kiến Trúc Thành Phần

```
                                  ┌────────────────────────┐
                                  │ Binance WebSocket Feed │
                                  │ (Top 10 USDT Pairs)    │
                                  └───────────┬────────────┘
                                              │
                                       ┌──────▼──────┐
                                       │ binance_ws  │
                                       └──────┬──────┘
                                              │
                                       ┌──────▼──────┐
                                       │fault_injector│ (Duplicate, Late Data, OOO, Schema Invalid)
                                       └──────┬──────┘
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     ▼                                                 ▼
        ┌─────────────────────────┐                       ┌─────────────────────────┐
        │   crypto_trades_raw     │                       │    crypto_trades_dlq    │
        │   (Valid Trade Events)  │                       │  (Dead-Letter Queue)    │
        └─────────────────────────┘                       └─────────────────────────┘
```

---

## 📂 Danh Sách Module

* `models.py`: Định nghĩa Data Models (`TradeEvent`, `DLQEvent`) với đầy đủ phương thức chuyển đổi JSON / dictionary và validate kiểu dữ liệu.
* `fault_injector.py`: Module chủ động bơm 4 loại lỗi phân tán:
  1. **Duplicate ($10%$):** Nhân bản giao dịch cùng `trade_id`.
  2. **Late Data ($10%$):** Lùi thời gian giao dịch $1 - 5$ phút trong quá khứ.
  3. **Out-of-Order:** Đảo trật tự phát sự kiện.
  4. **Schema Invalid:** Chèn giá âm hoặc thiếu trường.
  * Tự động đánh dấu `is_injected=True` và gắn `fault_type` làm Ground Truth đánh giá.
* `kafka_producer.py`: `ResilientKafkaProducer` hỗ trợ nén GZIP, phân vùng theo `symbol`, retry exponential backoff và chuyển tiếp message hỏng vào DLQ.
* `binance_ws.py`: `BinanceWebSocketProducer` tự động gọi REST API lấy Top N coin thanh khoản cao nhất, kết nối combined stream và auto-reconnect khi rớt mạng.
* `historical_backfill.py`: `HistoricalBackfillProducer` nạp dữ liệu lịch sử phục vụ đối soát và chạy Spark Batch.

---

## 🚀 Hướng Dẫn Sử Dụng

### 1. Chạy Sinh Dữ Liệu Mẫu Offline
```bash
# Sinh 1000 giao dịch mẫu BTCUSDT có tiêm lỗi lưu vào datasets/sample/
python scripts/seed_data.py --sample --count 1000 --inject-faults
```

### 2. Chạy Thu Thập Live WebSocket Binance Vào Kafka
```bash
python -c "from src.ingestion.binance_ws import BinanceWebSocketProducer; producer = BinanceWebSocketProducer(top_n=10); producer.start_streaming()"
```

### 3. Chạy Unit Tests
```bash
python tests/test_ingestion.py
```
