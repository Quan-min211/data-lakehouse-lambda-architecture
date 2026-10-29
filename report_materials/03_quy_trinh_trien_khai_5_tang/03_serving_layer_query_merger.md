# TIẾN TRÌNH 03: XÂY DỰNG TẦNG PHỤC VỤ (SERVING LAYER) & AUTO-CORRECTING QUERY MERGER

* **Tác giả:** Nguyễn Đặng Quốc Anh (MSSV: 23133004)
* **Ngày hoàn thành:** 04/09/2026

---

## 1. Mục Tiêu Kỹ Thuật Đạt Được
Đã hoàn thành thiết kế và hiện thực hóa thành công Tầng Phục Vụ (Serving Layer) cùng thuật toán cốt lõi **Auto-Correcting Query Merger**:
1. **Thuật toán Phân luồng 3 Kịch bản (3 Query Cases):**
   * **Case 1 (History):** $T_{\text{end}} \le W \implies$ Lấy $100\%$ từ `lakehouse.batch_agg` (Trạng thái: `Reconciled` - Chuẩn xác tuyệt đối).
   * **Case 2 (Realtime):** $T_{\text{start}} \ge W \implies$ Lấy $100\%$ từ `lakehouse.speed_agg` (Trạng thái: `Provisional` - Tức thời từ Spark Streaming).
   * **Case 3 (Hybrid - Giao thoa):** $T_{\text{start}} < W < T_{\text{end}} \implies$ Tự động phân tách tại Watermark $W$:
     - $[T_{\text{start}}, W] \implies$ Lấy từ Batch View (`Reconciled`)
     - $(W, T_{\text{end}}] \implies$ Lấy từ Speed View (`Provisional`)
     - Ghép nối dữ liệu đảm bảo **ZERO DOUBLE-COUNTING** (không bị trùng lặp nến tại mốc biên).
     - Tính sai số hiệu chỉnh: $\Delta_{\text{reconciliation}} = |\text{VWAP}_{\text{speed}} - \text{VWAP}_{\text{batch}}|$.
2. **Khởi Tạo Hệ Thống REST API (FastAPI):**
   * `GET /health`: Trạng thái hệ thống.
   * `GET /api/watermark`: Lấy mốc System Watermark thời gian thực.
   * `GET /api/market`: Phục vụ dữ liệu nến cho Dashboard qua Query Merger.
   * `GET /api/reconciliation`: Báo cáo đối soát chi tiết phục vụ Benchmark 2.
3. **Pydantic Data Contracts:**
   * Chuẩn hóa mô hình dữ liệu phản hồi (`MarketDataResponse`, `CandleData`, `ReconciliationDetail`).
   * Hỗ trợ CORS cho phép Streamlit Dashboard truy cập an toàn.

---

## 2. Kết Quả Kiểm Thử (Verification)
* **Toàn bộ 24/24 Unit Tests đạt chuẩn tuyệt đối (thời gian chạy: 0.02s):**
  ```text
  test_duplicate_injection (test_ingestion.TestFaultInjector) ... ok
  test_late_data_injection (test_ingestion.TestFaultInjector) ... ok
  test_normal_pass_through (test_ingestion.TestFaultInjector) ... ok
  test_schema_invalid_injection (test_ingestion.TestFaultInjector) ... ok
  test_dlq_event (test_ingestion.TestTradeEventModels) ... ok
  test_from_binance_raw (test_ingestion.TestTradeEventModels) ... ok
  test_to_dict_and_to_json (test_ingestion.TestTradeEventModels) ... ok
  test_trade_event_creation (test_ingestion.TestTradeEventModels) ... ok
  test_case_1_history_query (test_query_merger.TestAutoCorrectingQueryMerger) ... ok
  test_case_2_realtime_query (test_query_merger.TestAutoCorrectingQueryMerger) ... ok
  test_case_3_hybrid_query_and_zero_double_counting (test_query_merger.TestAutoCorrectingQueryMerger) ... ok
  test_reconciliation_report (test_query_merger.TestAutoCorrectingQueryMerger) ... ok
  test_health_check (test_query_merger.TestFastAPIRoutes) ... ok
  test_market_endpoint (test_query_merger.TestFastAPIRoutes) ... ok
  test_watermark_endpoint (test_query_merger.TestFastAPIRoutes) ... ok
  test_ohlcv_calculation (test_speed_layer.TestMetricsCalculator) ... ok
  test_ohlcv_out_of_order_input (test_speed_layer.TestMetricsCalculator) ... ok
  test_vwap_calculation (test_speed_layer.TestMetricsCalculator) ... ok
  test_vwap_empty_trades (test_speed_layer.TestMetricsCalculator) ... ok
  test_spark_schema_field_names (test_speed_layer.TestSchemaAndFormatting) ... ok
  test_normal_candle (test_speed_layer.TestSpikeDetector) ... ok
  test_price_dump_spike (test_speed_layer.TestSpikeDetector) ... ok
  test_price_jump_spike (test_speed_layer.TestSpikeDetector) ... ok
  test_range_volatility_spike (test_speed_layer.TestSpikeDetector) ... ok

  Ran 24 tests in 0.020s
  OK
  ```
