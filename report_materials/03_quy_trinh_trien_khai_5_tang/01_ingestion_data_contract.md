# TIẾN TRÌNH 01: THIẾT KẾ DATA CONTRACT VÀ HIỆN THỰC TẦNG INGESTION

* **Tác giả:** Nguyễn Đặng Quốc Anh
* **Ngày hoàn thành:** 23/08/2026

---

## 1. Mục Tiêu Kỹ Thuật Đạt Được
Đã hoàn thành thiết kế và hiện thực toàn diện tầng Thu thập dữ liệu (Ingestion Layer) cho hệ thống Data Lakehouse Lambda Architecture:
1. **Data Contracts:** Chuẩn hóa cấu trúc JSON sự kiện giao dịch tiền mã hóa (`TradeEvent`) với các trường cốt lõi: `trade_id`, `symbol`, `price`, `quantity`, `trade_time` (Event-time ms), `is_buyer_maker`, `ingestion_time` và metadata đánh dấu lỗi kiểm thử (`is_injected`, `fault_type`).
2. **Fault Injection Engine:** Xây dựng module tiêm lỗi có kiểm soát giúp giả lập 4 hiện tượng mạng phân tán:
   * **Duplicate ($10%$):** Gửi lại bản ghi cùng ID để kiểm thử cơ chế Deduplication tại Batch Layer.
   * **Late Data ($10%$):** Lùi thời gian sự kiện $1 - 5$ phút để kiểm thử cơ chế Watermarking và xử lý trễ của Spark Streaming.
   * **Out-of-Order:** Đảo trật tự phát tin.
   * **Schema Invalid:** Giá âm hoặc thiếu trường để kiểm thử Data Quality Gate & Quarantine Table.
3. **Resilient Ingestion:** Kafka Producer đóng gói cơ chế retry exponential backoff và tự động chuyển các gói tin lỗi parse vào `crypto_trades_dlq`.

---

## 2. Kết Quả Kiểm Thử (Verification)
* Đã chạy bộ kiểm thử tự động `tests/test_ingestion.py`:
  ```
  Ran 8 tests in 0.001s
  OK
  ```
* Đã sinh tập dữ liệu mẫu `datasets/sample/btcusdt_trades_sample.json` (1.000 bản ghi) phục vụ chạy offline.
