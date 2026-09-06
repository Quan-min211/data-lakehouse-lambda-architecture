# TIẾN TRÌNH 04: XÂY DỰNG GIAO DIỆN GIÁM SÁT THỊ TRƯỜNG VỚI STREAMLIT & PLOTLY

* **Tác giả:** Nguyễn Đặng Quốc Anh (MSSV: 23133004)
* **Ngày hoàn thành:** 06/09/2026

---

## 1. Mục Tiêu Kỹ Thuật Đạt Được
Đã hoàn thành thiết kế và hiện thực hóa toàn diện Tầng Trực Quan Hóa (Dashboard Layer) cho đề tài Khóa Luận Tốt Nghiệp:
1. **Biểu Đồ Nến Plotly Candlestick Tương Tác (`dashboard/components/candlestick.py`):**
   * Vẽ biểu đồ Subplots gồm 2 tầng: Nến OHLCV (75% chiều cao) và Khối lượng Volume (25% chiều cao).
   * Đường kẻ chỉ số **VWAP** (Volume-Weighted Average Price) trực quan hóa xu hướng giá theo trọng số khối lượng.
   * **Tô màu phân biệt trạng thái học thuật:**
     * Nến xanh/đỏ nhãn **`Reconciled`** (Dữ liệu đã chốt từ Batch Layer qua DQ Gate).
     * Nến nhãn **`Provisional`** (Dữ liệu vi xử lý tức thời từ Speed Layer).
   * Đánh dấu tam giác cảnh báo **Price Spike** (Sốc giá) nhấp nháy trên đỉnh cây nến.
2. **Thanh Trạng Thái Query Merger & System Watermark (`dashboard/components/metrics_cards.py`):**
   * Hiển thị mốc ranh giới **System Watermark** giữa 2 tầng.
   * Hiển thị kịch bản phân luồng: **Case 1 (History)**, **Case 2 (Realtime)**, **Case 3 (Hybrid)**.
   * Đo lường sai số hiệu chỉnh: $\Delta_{\text{reconciliation}} = |\text{VWAP}_{\text{speed}} - \text{VWAP}_{\text{batch}}|$.
3. **Bảng Đối Soát Dữ Liệu Hai Tầng Phục Vụ Benchmark 2 (`dashboard/components/reconciliation_view.py`):**
   * So sánh song song từng cây nến giữa Batch View và Speed View.
   * Thống kê sai số tuyệt đối trung bình (MAE) phục vụ đánh giá luận văn.
4. **Cơ Chế Khả Dụng Cao & Dự Phòng (High Availability & Fallback):**
   * Kết nối tới REST API của Serving Layer (`GET /api/market`, `GET /api/watermark`, `GET /api/reconciliation`).
   * Tích hợp bộ sinh dữ liệu mô phỏng dự phòng tự động khi API Server chưa kích hoạt, bảo đảm Dashboard không bao giờ bị crash trắng trang.
5. **Cơ Chế Tự Động Làm Mới (Auto-Refresh):**
   * Cho phép thiết lập tần suất tự động tải lại dữ liệu (mỗi 3s, 5s hoặc 10s).

---

## 2. Kết Quả Kiểm Thử (Verification)
* **Toàn bộ 27/27 Unit Tests đạt chuẩn tuyệt đối (0.840s):**
  ```text
  test_generate_fallback_data (test_dashboard_components.TestDashboardComponents) ... ok
  test_render_candlestick_empty (test_dashboard_components.TestDashboardComponents) ... ok
  test_render_candlestick_with_data (test_dashboard_components.TestDashboardComponents) ... ok
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

  Ran 27 tests in 0.840s
  OK
  ```
