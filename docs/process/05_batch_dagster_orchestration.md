# Báo Cáo Tiến Độ: Tầng Điều Phối Lô (Batch Layer Orchestration với Dagster)

* **Phụ trách:** Nguyễn Đặng Quốc Anh & Phạm Minh Quân
* **Mục tiêu:** Xây dựng toàn bộ pipeline điều phối Tầng Lô theo chuẩn Dagster Software-Defined Assets (SDA), kết nối nạp nến Ground Truth vào ClickHouse và chốt mốc Watermark hệ thống.

---

## 1. Các Hạng Mục Đã Triển Khai

1. **Software-Defined Assets (SDA):**
   * Triển khai chuỗi 6 Assets độc lập, có định danh rõ ràng, metadata phong phú:
     * `bronze_crypto_trades`: Lưu trữ dữ liệu thô vào Iceberg Bronze table.
     * `silver_cleaned_trades`: Tích hợp Data Quality Gate (Deduplication, Schema Check, Quarantine).
     * `gold_market_aggregates`: Tổng hợp nến OHLCV 1m/5m/1h và VWAP chuẩn mực (**Ground Truth**).
     * `clickhouse_batch_sync`: Đồng bộ trực tiếp 75 nến batch vào bảng `lakehouse.batch_agg` trong ClickHouse.
     * `system_watermark_sync`: Cập nhật mốc `watermark_time` vào bảng `lakehouse.system_watermark`.
     * `iceberg_small_files_compaction`: Job bảo trì gộp file nhỏ (Bin-Packing Compaction) giải quyết *Small File Problem* trên MinIO.

2. **Jobs & Schedules:**
   * `batch_lakehouse_pipeline_job`: Lập lịch chạy mỗi 15 phút (`*/15 * * * *`).
   * `iceberg_compaction_job`: Lập lịch bảo trì mỗi 2 giờ (`0 */2 * * *`).

3. **Cơ chế Nhận diện Môi trường (Host Fallback):**
   * Khắc phục lỗi `getaddrinfo failed [Errno 11001]` trên Windows khi Dagster load hostname Docker container (`clickhouse`, `kafka`).
   * Bổ sung cơ chế tự động fallback thông minh sang `localhost` trong `src/utils/config.py`.

4. **Kiểm thử Tự động (Unit Testing):**
   * Viết module `tests/test_dagster_pipeline.py` kiểm thử 5 ca:
     * Số lượng và key của 6 Assets.
     * Danh sách 2 Jobs và 2 Schedules.
     * Cây phụ thuộc dữ liệu Data Lineage DAG.
     * In-memory Materialization toàn bộ pipeline thành công 100%.

---

## 2. Kết Quả Xác Minh Thực Tế

* Đã kiểm thử nạp nến batch thật:
  * Bảng `lakehouse.batch_agg`: **75 dòng** (Nến chuẩn của BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT).
  * Bảng `lakehouse.system_watermark`: Ghi nhận mốc Watermark chốt sổ mới nhất thành công.
* Toàn bộ test suite chạy thành công: **5/5 tests passed**.
