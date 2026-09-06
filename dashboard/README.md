# Dashboard — Streamlit Real-Time Crypto Monitoring

Giao diện Bảng điều khiển Trực quan hóa dữ liệu thị trường tiền mã hóa thời gian thực (Top 10 USDT pairs) cho hệ thống **Data Lakehouse Kiến trúc Lambda**.

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
   - Đo lường sai số hiệu chỉnh: $\\Delta_{\\text{reconciliation}} = |\\text{VWAP}_{\\text{speed}} - \\text{VWAP}_{\\text{batch}}|$.

3. **Bảng Đối Soát Dữ Liệu Hai Tầng (Benchmark 2):**
   - So sánh song song từng cây nến giữa Batch View và Speed View.
   - Thống kê sai số tuyệt đối trung bình (MAE) phục vụ đánh giá luận văn.

4. **Tự Động Làm Mới (Auto-Refresh):**
   - Hỗ trợ công tắc cập nhật định kỳ mỗi 3s, 5s hoặc 10s.

---

## 📁 Cấu Trúc Thư Mục

| File | Chức năng |
| :--- | :--- |
| `app.py` | Ứng dụng Streamlit chính, quản trị bộ lọc, sidebar, gọi API và điều phối layout. |
| `components/candlestick.py` | Component vẽ biểu đồ nến Plotly chuyên nghiệp (Subplots Nến + Volume). |
| `components/metrics_cards.py` | Component hiển thị 5 thẻ KPI (Giá, VWAP, Query Case, Watermark, Sai số Δ). |
| `components/reconciliation_view.py` | Component bảng đối soát dữ liệu Benchmark 2 (Speed vs Batch). |

---

## 🚀 Hướng Dẫn Khởi Chạy

```bash
# Khởi động ứng dụng Streamlit:
streamlit run dashboard/app.py
```

Ứng dụng sẽ tự động mở tại trình duyệt: **http://localhost:8501**
