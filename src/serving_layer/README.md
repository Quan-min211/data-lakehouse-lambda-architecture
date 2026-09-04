# Serving Layer — Auto-Correcting Query Merger

Tầng Phục Vụ (Serving Layer) trong kiến trúc **Data Lakehouse Lambda**, chịu trách nhiệm cung cấp dữ liệu nến chuẩn xác và tức thời cho Dashboard và các ứng dụng phân tích thông qua thuật toán **Auto-Correcting Query Merger**.

---

## 🏛️ Thuật Toán Auto-Correcting Query Merger

Khi người dùng/Dashboard gửi yêu cầu lấy dữ liệu trong khoảng $[T_{\text{start}}, T_{\text{end}}]$, hệ thống tự động đối chiếu với mốc **System Watermark ($W$)** của Batch Layer và phân luồng theo 3 kịch bản:

```
                                  YÊU CẦU TRUY VẤN: [T_start, T_end]
                                                  │
                                                  ▼
                                      [ĐỐI CHIẾU WATERMARK: W]
                                                  │
                ┌─────────────────────────────────┼─────────────────────────────────┐
                ▼                                 ▼                                 ▼
      [CASE 1: HISTORY]                 [CASE 2: REALTIME]                  [CASE 3: HYBRID]
        T_end ≤ W                         T_start ≥ W                      T_start < W < T_end
                │                                 │                                 │
                ▼                                 ▼                                 ▼
       100% BATCH VIEW                    100% SPEED VIEW                  PHÂN TÁCH BIÊN WATERMARK
       • lakehouse.batch_agg             • lakehouse.speed_agg            • [T_start, W]  → BATCH
       • Nhãn: Reconciled                • Nhãn: Provisional              • (W, T_end]    → SPEED
       • Độ chính xác tuyệt đối          • Độ trễ tức thời (< 5s)         • ZERO DOUBLE-COUNTING!
                                                                          • Δ = |VWAP_speed - VWAP_batch|
```

---

## 📁 Cấu Trúc Thành Phần

| File | Chức năng |
| :--- | :--- |
| [`query_merger.py`](./query_merger.py) | **Thuật toán cốt lõi** phân luồng 3 Case, ghép nối dữ liệu không trùng lặp và tính sai số hiệu chỉnh $\Delta_{\text{reconciliation}}$. |
| [`api_routes.py`](./api_routes.py) | **FastAPI Application** cung cấp REST API endpoints, Swagger UI documentation và hỗ trợ CORS. |
| [`schemas.py`](./schemas.py) | **Pydantic Data Models** chuẩn hóa phản hồi (`MarketDataResponse`, `CandleData`, `ReconciliationDetail`). |
| [`watermark_reader.py`](./watermark_reader.py) | Đọc mốc Watermark mới nhất từ ClickHouse table `lakehouse.system_watermark`. |
| [`clickhouse_client.py`](./clickhouse_client.py) | Client tối ưu hóa truy vấn nến từ `batch_agg` và `speed_agg`. |

---

## 🚀 REST API Endpoints

| Method | Endpoint | Mô tả |
| :--- | :--- | :--- |
| `GET` | `/health` | Kiểm tra trạng thái sẵn sàng của dịch vụ. |
| `GET` | `/api/watermark` | Lấy mốc System Watermark hiện tại của Batch Layer. |
| `GET` | `/api/market?symbol=BTCUSDT` | Truy vấn nến nạp qua bộ ghép nối Auto-Correcting Query Merger. |
| `GET` | `/api/reconciliation?symbol=BTCUSDT` | Báo cáo đối soát chi tiết phục vụ Benchmark 2 (Sai số VWAP giữa Speed và Batch). |
| `GET` | `/docs` | Interactive Swagger UI API Documentation. |

---

## 💻 Cách Khởi Động API Server

```bash
# Chạy trực tiếp từ mã nguồn:
uvicorn src.serving_layer.api_routes:app --host 0.0.0.0 --port 8000 --reload

# Hoặc qua Docker:
docker compose up -d fastapi
```

Sau khi chạy, truy cập Swagger UI tại: [http://localhost:8000/docs](http://localhost:8000/docs).
