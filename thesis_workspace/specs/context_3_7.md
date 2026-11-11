# ĐẶC TẢ KỸ THUẬT TIỂU MỤC 3.7: HIỆN THỰC GIAO DIỆN GIÁM SÁT (DASHBOARD UI) VÀ CẢNH BÁO CHỦ ĐỘNG
# (TECHNICAL SPECIFICATION: SECTION 3.7)

> **Mục lục liên kết:** Thuộc Chương 3 trong [master_outline.md](file:///thesis_workspace/master_outline.md)  
> **Các tiểu mục bao hàm:**  
> - `3.7.1. Xây dựng Dashboard tương tác thời gian thực với Streamlit`  
> - `3.7.2. Trực quan hóa nến OHLCV, đường VWAP và nhãn đối soát`  
> - `3.7.3. Bảng điều khiển giám sát Data Quality Gate, Quarantine và cảnh báo Price Spike`  
> **Nguồn mã nguồn & tệp cấu hình trích xuất:**  
> - [dashboard/app.py](file:///dashboard/app.py)  
> - [dashboard/components/candlestick.py](file:///dashboard/components/candlestick.py)  
> - [dashboard/components/reconciliation_view.py](file:///dashboard/components/reconciliation_view.py)  
> - [dashboard/components/dq_panel.py](file:///dashboard/components/dq_panel.py)  
> - [dashboard/components/watermark_status.py](file:///dashboard/components/watermark_status.py)  
> - [dashboard/components/system_status.py](file:///dashboard/components/system_status.py)  
> - [src/utils/alerting.py](file:///src/utils/alerting.py)  
> - [thesis_workspace/specs/00_global_constants.md](file:///thesis_workspace/specs/00_global_constants.md)

---

## 1. TIỂU MỤC 3.7.1: XÂY DỰNG DASHBOARD TƯƠNG TÁC THỜI GIAN THỰC VỚI STREAMLIT

### 1.1. Kiến Trúc Ứng Dụng Giám Sát Thị Trường
Phân hệ Trình diễn (Presentation Layer) được đóng gói trong container `lakehouse-streamlit` (cổng `8501`). Điểm nhập chính [dashboard/app.py](file:///dashboard/app.py) xây dựng giao diện phân tích tài chính đa thành phần:
* **Tự động làm mới dữ liệu (Auto-Refresh Loop):** Cơ chế định thời quét dữ liệu mỗi $3\text{ giây}$ (`time.sleep(3)` hoặc Streamlit Auto-refresh widget), gửi yêu cầu polling đến FastAPI Gateway (`/api/v1/market/ohlcv` và `/api/speed/latest`).
* **Hỗ trợ Top 10 Đồng Coin Thanh Khoản Cao:** Hỗ trợ lựa chọn linh hoạt danh mục tiền mã hóa hàng đầu: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BNBUSDT`, `XRPUSDT`, `DOGEUSDT`, `ADAUSDT`, `AVAXUSDT`, `LINKUSDT`, `SUIUSDT`.
* **Khung Thời Gian Linh Hoạt (Time Presets):** Cung cấp các nút bấm nhanh: `15m` (15 phút), `30m` (30 phút), `1H` (1 giờ), `3H` (3 giờ), `6H` (6 giờ).

### 1.2. Thẻ Chỉ Số Tổng Hợp Tức Thời (Metrics Cards)
Được triển khai trong `dashboard/components/metrics_cards.py`, bảng điều khiển hiển thị 4 khối thông số trọng tâm:
1. **Giá Giao Dịch Gần Nhất (Latest Close Price):** Kèm mức chênh lệch tuyệt đối và tỷ lệ phần trăm so với đầu phiên.
2. **Giá Trung Bình Gia Quyền Khối Lượng (VWAP):** Phản ánh đường cơ sở định giá của thị trường.
3. **Tổng Khối Lượng Giao Dịch (Total Volume):** Khối lượng coin cơ sở đã được khớp lệnh.
4. **Sai Lệch Đối Soát Hiện Tại ($\Delta_{\text{reconcile}}$):** Độ chênh lệch giữa Speed View và Batch View tại nến biên, gắn nhãn xanh (`Reconciled`) hoặc cam (`Provisional`).

---

## 2. TIỂU MỤC 3.7.2: TRỰC QUAN HÓA NẾN OHLCV, ĐƯỜNG VWAP VÀ NHÃN ĐỐI SOÁT

### 2.1. Cấu Trúc Biểu Đồ Nến Plotly Đa Tầng (Interactive Candlestick Subplots)
Trong [dashboard/components/candlestick.py](file:///dashboard/components/candlestick.py), hệ thống sử dụng thư viện Plotly dựng biểu đồ tương tác 2 tầng (*Two-Row Subplots*) với tỷ lệ chiều cao tối ưu $78\% : 22\%$:

* **Tầng 1 (Row 1 - Height 78%): Biểu Đồ Nến & Đường Chỉ Báo:**
  - `go.Candlestick`: Hiển thị nến xanh (Tăng: $\text{Close} \ge \text{Open}$) và nến đỏ (Giảm: $\text{Close} < \text{Open}$).
  - `go.Scatter` (VWAP Line): Đường chỉ báo VWAP màu vàng nổi bật thể hiện mức giá cân bằng khối lượng.
  - Cảnh báo sốc giá (`Price Spike Marker`): Các nến có cờ `is_spike = 1` được đánh dấu bằng biểu tượng hình tam giác/kim cương màu hổ phách (*Amber Marker*) tại đỉnh nến kèm chú thích phần trăm biến động.
* **Tầng 2 (Row 2 - Height 22%): Biểu Đồ Cột Khối Lượng (Volume Bars):**
  - `go.Bar`: Cột khối lượng đồng bộ trục thời gian $X$ với hàng trên, tô màu theo chiều hướng nến giá.

### 2.2. Phân Tách Trực Quan Vùng Chân Lý (Reconciled) và Vùng Tức Thời (Provisional)
Điểm độc đáo nhất của giao diện là khả năng thể hiện trực quan ranh giới của System Watermark ($W$):
1. **Đường Ranh Giới Thẳng Đứng (Watermark Line):** Một đường kẻ chấm màu hổ phách tại tọa độ $X = W$.
2. **Vùng Tô Nền Sau Watermark (Provisional Shading):**
   ```python
   fig.add_shape(
       type="rect", xref="x", yref="paper",
       x0=wm_ts, x1=x_max, y0=0, y1=1,
       fillcolor=COLORS["provisional"], opacity=0.05, line_width=0, layer="below"
   )
   ```
   Toàn bộ khu vực thời gian thực sau mốc $W$ được phủ một lớp màu mờ cảnh báo người dùng đây là dữ liệu tạm thời (`Provisional`), trong khi vùng trước $W$ là dữ liệu hồ dữ liệu bất biến đã qua đối soát (`Reconciled`).

---

## 3. TIỂU MỤC 3.7.3: BẢNG ĐIỀU KHIỂN GIÁM SÁT DATA QUALITY GATE, QUARANTINE VÀ CẢNH BÁO PRICE SPIKE

### 3.1. Bảng Điều Khiển Dữ Liệu Cách Ly (`render_dq_quarantine_panel`)
Được triển khai trong [dashboard/components/dq_panel.py](file:///dashboard/components/dq_panel.py), component này kết nối trực tiếp hoặc qua API truy vấn bảng ClickHouse `lakehouse.dq_quarantine`:
* **Thẻ Tổng Số Vi Phạm:** Đếm tổng số bản ghi bị cách ly theo phiên (`batch_run_id`).
* **Phân Phối Mã Lỗi (Error Code Breakdown):** Biểu đồ thanh ngang hoặc bảng phân bổ chi tiết các loại lỗi:
  - `ERR_PRICE_NONPOSITIVE`: Lỗi giá âm hoặc bằng 0.
  - `ERR_QTY_NONPOSITIVE`: Lỗi khối lượng rác.
  - `ERR_DUPLICATE_TRADE_ID`: Lỗi trùng lặp mã giao dịch.
  - `ERR_LATE_EVENT`: Lỗi dữ liệu đến quá trễ.
* **Bảng Kiểm Toán Bản Ghi Thô (Raw Payload Inspection):** Cho phép xem nội dung JSON gốc của từng bản ghi vi phạm phục vụ việc gỡ lỗi đường ống.

### 3.2. Hệ Thống Phát Hiện & Cảnh Báo Sốc Giá Chủ Động (`AlertManager`)
Trong [src/utils/alerting.py](file:///src/utils/alerting.py), lớp `AlertManager` giám sát liên tục 2 chỉ số an toàn hệ thống:
1. **Cảnh Báo Độ Trễ Dòng Luồng (Streaming Freshness Lag):**
   $$\text{Lag}_{\text{sec}} = T_{\text{current}} - T_{\text{watermark}}$$
   Nếu độ trễ vượt quá ngưỡng cảnh báo cấu hình ($> 900\text{s}$ - tương đương $15\text{ phút}$ trôi qua mà Batch chưa chốt mốc mới), hệ thống ghi log mức `WARNING` và hiển thị huy hiệu đỏ trên giao diện người dùng.
2. **Cảnh Báo Đột Biến Giá Bất Thường:**
   Khi phát hiện `is_spike = 1`, hệ thống lập tức xuất thông báo Toast trên Streamlit UI cảnh báo nhà giao dịch về nguy cơ biến động mạnh (Flash Crash / Pump & Dump).

