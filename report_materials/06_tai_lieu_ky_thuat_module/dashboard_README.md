# Dashboard — Streamlit Real-Time Crypto Monitoring & Benchmarks

Giao diện Bảng điều khiển Trực quan hóa dữ liệu thị trường tiền mã hóa thời gian thực (Top 10 USDT pairs) và Đánh giá Thực nghiệm 3 Benchmark cho hệ thống **Data Lakehouse Kiến trúc Lambda**.

---

## 🏛️ Tính Năng Nổi Bật

1. **Biểu Đồ Nến Tương Tác (Plotly Candlestick):**
   - Nến OHLCV đa khung thời gian (15m, 1h, 6h, 24h).
   - Đường kẻ chỉ số **VWAP** (Volume-Weighted Average Price) trực quan.
   - Thanh khối lượng giao dịch (**Volume Bar Chart**) khớp màu nến.
   - **Tô màu phân biệt học thuật:**
     - Nến **`Reconciled`**: Dữ liệu đã chốt chuẩn xác từ Batch Layer.
     - Nến **`Provisional`**: Dữ liệu tức thời từ Speed Layer.
   - Đánh dấu tam giác cảnh báo **Price Spike** (Sốc giá) nhấp nháy trên đỉnh nến.

2. **Thanh Trạng Thái Query Merger & System Watermark:**
   - Hiển thị mốc ranh giới **System Watermark** giữa 2 tầng.
   - Hiển thị kịch bản phân luồng: **Case 1 (History)**, **Case 2 (Realtime)**, **Case 3 (Hybrid)**.
   - Đo lường sai số hiệu chỉnh: $\Delta_{\text{reconciliation}} = |\text{VWAP}_{\text{speed}} - \text{VWAP}_{\text{batch}}|$.

3. **Bảng Đối Soát Dữ Liệu Hai Tầng (Benchmark 2):**
   - So sánh song song từng cây nến giữa Batch View và Speed View.
   - Thống kê sai số tuyệt đối trung bình (MAE) phục vụ đánh giá luận văn.

4. **Trực Quan Hóa 3 Benchmark Hệ Thống (`benchmark_charts.py`):**
   - **Tổng quan KPI Hero Cards**: Tóm tắt chỉ số đo kiểm P50/P95 latency, sai số đối soát MAPE, và hệ số tăng tốc Compaction so với SLA đồ án.
   - **BM1 (Query Latency)**: Box plot phân bố độ trễ 3 chiến lược (`batch_only`, `speed_only`, `lambda`), Bar chart P50/P95/P99, bộ lọc đa cặp coin (BTC, ETH, SOL, BNB, XRP).
   - **BM2 (Reconciliation Accuracy)**: Sai số MAPE (%) kèm ngưỡng SLA 1.0%, MAE/RMSE theo thang log, biểu đồ so sánh đường giá nến Speed VWAP vs Batch VWAP theo chuỗi thời gian.
   - **BM3 (Compaction Efficiency)**: Đánh giá cải thiện Small Files trên Apache Iceberg (tăng tốc 3.8x - 4.1x, giảm ~75% latency đọc).
   - **Biểu đồ gốc (Pre-generated Plots)**: Trình chiếu các biểu đồ PNG chuẩn hóa phục vụ báo cáo và slide thuyết trình.

5. **Tự Động Làm Mới (Auto-Refresh):**
   - Hỗ trợ công tắc cập nhật định kỳ mỗi 3s, 5s hoặc 10s.

---

## 📁 Cấu Trúc Thư Mục

| File | Chức năng |
| :--- | :--- |
| `app.py` | Ứng dụng Streamlit chính, quản trị bộ lọc, sidebar, gọi API và điều phối layout 4 tabs. |
| `components/candlestick.py` | Component vẽ biểu đồ nến Plotly chuyên nghiệp (Subplots Nến + Volume). |
| `components/metrics_cards.py` | Component hiển thị 5 thẻ KPI (Giá, VWAP, Query Case, Watermark, Sai số Δ). |
| `components/reconciliation_view.py` | Component bảng đối soát dữ liệu Benchmark 2 (Speed vs Batch). |
| `components/benchmark_charts.py` | Component trực quan hóa chuyên sâu 3 Benchmark với biểu đồ Plotly tương tác. |

---

## 🚀 Hướng Dẫn Khởi Chạy

```bash
# Khởi động ứng dụng Streamlit cục bộ:
streamlit run dashboard/app.py

# Hoặc khởi chạy thông qua Docker Compose:
docker compose up -d streamlit
```

Ứng dụng sẽ tự động mở tại trình duyệt: **http://localhost:8501**
