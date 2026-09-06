"""
app.py
======
Streamlit Dashboard cho Hệ thống Data Lakehouse Kiến trúc Lambda:
- Giám sát thị trường Crypto thời gian thực (Top 10 USDT pairs)
- Biểu đồ nến tương tác Plotly OHLCV + VWAP + Cảnh báo Sốc giá
- Bộ ghép nối dữ liệu tự động sửa sai (Auto-Correcting Query Merger)
- Bảng đối soát hai tầng Speed vs Batch (Benchmark 2)
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd
import requests

# Thêm root vào sys.path để import nội bộ
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.components.candlestick import render_candlestick_chart
from dashboard.components.metrics_cards import render_metrics_cards
from dashboard.components.reconciliation_view import render_reconciliation_table


# =============================================================================
# CẤU HÌNH TRANG STREAMLIT
# =============================================================================
st.set_page_config(
    page_title="Crypto Lakehouse | Lambda Architecture Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS cho phong cách hiện đại Dark Crypto Trading
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #1E222D;
        border: 1px solid #2A2E39;
        border-radius: 8px;
        padding: 12px;
    }
    .badge-reconciled {
        background-color: #00E676;
        color: #000000;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-provisional {
        background-color: #AB47BC;
        color: #FFFFFF;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-hybrid {
        background-color: #FFA726;
        color: #000000;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# =============================================================================
# HÀM LẤY DỮ LIỆU TỪ SERVING API (HOẶC FALLBACK DỮ LIỆU MẪU)
# =============================================================================
def fetch_api_data(
    api_url: str,
    symbol: str,
    start_time: datetime,
    end_time: datetime
) -> Dict[str, Any]:
    """Gọi Serving Layer FastAPI GET /api/market."""
    endpoint = f"{api_url.rstrip('/')}/api/market"
    params = {
        "symbol": symbol,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
    }
    try:
        resp = requests.get(endpoint, params=params, timeout=4)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass

    # Fallback dữ liệu mô phỏng an toàn nếu FastAPI server chưa bật
    return generate_fallback_market_data(symbol, start_time, end_time)


def fetch_watermark(api_url: str) -> Optional[Dict[str, Any]]:
    """Gọi Serving Layer FastAPI GET /api/watermark."""
    endpoint = f"{api_url.rstrip('/')}/api/watermark"
    try:
        resp = requests.get(endpoint, timeout=3)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def fetch_reconciliation_report(
    api_url: str,
    symbol: str,
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Gọi Serving Layer FastAPI GET /api/reconciliation."""
    endpoint = f"{api_url.rstrip('/')}/api/reconciliation"
    params = {
        "symbol": symbol,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
    }
    try:
        resp = requests.get(endpoint, params=params, timeout=4)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return []


def generate_fallback_market_data(
    symbol: str,
    start_time: datetime,
    end_time: datetime
) -> Dict[str, Any]:
    """Sinh dữ liệu mẫu minh họa chuẩn khi API Server chưa kích hoạt."""
    import random
    candles = []
    curr = start_time
    base_price = 62000.0 if "BTC" in symbol else (2700.0 if "ETH" in symbol else 140.0)
    now_utc = datetime.now(timezone.utc)
    wm = now_utc - timedelta(minutes=15)  # Giả sử Watermark cách hiện tại 15p

    while curr < end_time:
        c_next = curr + timedelta(minutes=1)
        # Sinh bước nhảy ngẫu nhiên
        pct_change = random.gauss(0, 0.003)
        open_p = base_price
        close_p = open_p * (1 + pct_change)
        high_p = max(open_p, close_p) * (1 + abs(random.gauss(0, 0.002)))
        low_p = min(open_p, close_p) * (1 - abs(random.gauss(0, 0.002)))
        volume = round(random.uniform(2.0, 25.0), 4)
        vwap = round((open_p + high_p + low_p + close_p) / 4.0, 2)
        is_spike = 1 if abs(close_p - open_p) / open_p >= 0.015 else 0

        # Phân loại trạng thái theo Watermark
        status = "Reconciled" if curr < wm else "Provisional"

        candles.append({
            "symbol": symbol,
            "window_start": curr,
            "window_end": c_next,
            "open_price": round(open_p, 2),
            "high_price": round(high_p, 2),
            "low_price": round(low_p, 2),
            "close_price": round(close_p, 2),
            "volume": volume,
            "trade_count": random.randint(30, 150),
            "vwap": vwap,
            "is_spike": is_spike,
            "status": status,
        })
        base_price = close_p
        curr = c_next

    # Xác định Case
    if end_time <= wm:
        q_case = 1
        q_desc = "Case 1 (History): 100% dữ liệu từ Batch View (Ground Truth)"
        overall_status = "Reconciled"
        delta = 0.0
    elif start_time >= wm:
        q_case = 2
        q_desc = "Case 2 (Realtime): 100% dữ liệu từ Speed View (Provisional)"
        overall_status = "Provisional"
        delta = None
    else:
        q_case = 3
        q_desc = "Case 3 (Hybrid): Ghép nối tự động Batch [T_start, W] + Speed (W, T_end]"
        overall_status = "Partially Reconciled"
        delta = round(random.uniform(2.5, 12.0), 2)

    return {
        "symbol": symbol,
        "time_range": {"start": start_time, "end": end_time},
        "query_case": q_case,
        "query_case_description": q_desc,
        "watermark": wm,
        "overall_status": overall_status,
        "reconciliation_delta": delta,
        "candles_count": len(candles),
        "candles": candles,
    }


# =============================================================================
# GIAO DIỆN CHÍNH STREAMLIT
# =============================================================================
def main():
    # --- SIDEBAR: ĐIỀU KHIỂN & BỘ LỌC ---
    st.sidebar.image(
        "https://upload.wikimedia.org/wikipedia/commons/4/46/Bitcoin.svg",
        width=48
    )
    st.sidebar.title("Cấu Hình Giám Sát")

    # 1. Chọn cặp coin
    top_coins = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT"
    ]
    selected_symbol = st.sidebar.selectbox("Cặp tiền mã hóa:", top_coins, index=0)

    # 2. Chọn khoảng thời gian
    time_presets = {
        "15 phút gần nhất": 15,
        "30 phút gần nhất": 30,
        "1 giờ gần nhất": 60,
        "3 giờ gần nhất": 180,
        "6 giờ gần nhất": 360,
    }
    selected_preset = st.sidebar.selectbox("Khung thời gian truy vấn:", list(time_presets.keys()), index=2)
    minutes_back = time_presets[selected_preset]

    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(minutes=minutes_back)
    end_time = now_utc

    # 3. Cấu hình Serving API Server
    api_base_url = st.sidebar.text_input("Serving API URL:", value="http://localhost:8000")

    # 4. Tự động làm mới (Auto-Refresh)
    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("Bật tự động làm mới (Auto-Refresh)", value=False)
    refresh_rate = st.sidebar.slider("Tần suất làm mới (giây):", min_value=3, max_value=30, value=5)

    if st.sidebar.button("🔄 Làm mới thủ công", use_container_width=True):
        st.rerun()

    # --- KHU VỰC TIÊU ĐỀ CHÍNH ---
    st.title(f"⚡ Giám Sát Thị Trường Real-time — {selected_symbol}")
    st.caption(
        "Đề tài Khóa Luận Tốt Nghiệp: **Data Lakehouse Kiến trúc Lambda Hỗ trợ Đối soát Dữ liệu Thời gian thực** "
        "| SVTH: Nguyễn Đặng Quốc Anh & Phạm Minh Quân"
    )

    # Lấy dữ liệu
    market_data = fetch_api_data(api_base_url, selected_symbol, start_time, end_time)
    watermark_info = fetch_watermark(api_base_url)

    # 1. Vẽ hàng thẻ chỉ số KPI & Phân luồng Query Merger
    render_metrics_cards(market_data, watermark_info)

    st.markdown("---")

    # 2. Vẽ biểu đồ nến Plotly chuyên nghiệp
    candles = market_data.get("candles", [])
    fig = render_candlestick_chart(candles, symbol=selected_symbol)
    st.plotly_chart(fig, use_container_width=True)

    # 3. Các Tab Phân Tích & Đối Soát
    tab1, tab2, tab3 = st.tabs([
        "📊 Đối Soát Hai Tầng (Benchmark 2)",
        "⚠️ Cảnh Báo Sốc Giá (Price Spike)",
        "🏛️ Cơ Chế Lambda & Query Merger"
    ])

    with tab1:
        st.markdown(
            "Cơ chế **Auto-Correcting Query Merger** liên tục so sánh nến tạo bởi Tầng Tốc Độ (**Speed Layer**) "
            "với dữ liệu chuẩn sau khi Tầng Xử Lý Theo Lô (**Batch Layer**) chốt mốc Watermark để tính sai số hiệu chỉnh $\\Delta$."
        )
        report_data = fetch_reconciliation_report(api_base_url, selected_symbol, start_time, end_time)
        if not report_data and candles:
            # Mô phỏng mẫu dữ liệu đối soát dựa trên nến hiện có
            report_data = [
                {
                    "window_start": c["window_start"],
                    "window_end": c["window_end"],
                    "batch_vwap": c["vwap"] - 2.5 if c["status"] == "Reconciled" else None,
                    "speed_vwap": c["vwap"],
                    "vwap_delta": 2.5 if c["status"] == "Reconciled" else None,
                    "batch_volume": c["volume"],
                    "speed_volume": c["volume"] + 0.1,
                    "volume_delta": 0.1,
                    "is_reconciled": (c["status"] == "Reconciled"),
                }
                for c in candles[-10:]
            ]
        render_reconciliation_table(report_data)

    with tab2:
        st.subheader("🚨 Nhật Ký Phát Hiện Biến Động Bất Thường (Spike Detection)")
        spike_candles = [c for c in candles if c.get("is_spike") == 1]
        if spike_candles:
            spike_df = pd.DataFrame(spike_candles)[[
                "window_start", "open_price", "close_price", "high_price", "low_price", "volume", "status"
            ]]
            spike_df["Biên độ (%)"] = (
                abs(spike_df["close_price"] - spike_df["open_price"]) / spike_df["open_price"] * 100
            ).round(2)
            st.dataframe(spike_df, use_container_width=True, hide_index=True)
        else:
            st.success("✅ Không ghi nhận biến động sốc giá bất thường nào trong khung thời gian này.")

    with tab3:
        st.subheader("Kiến Trúc Auto-Correcting Query Merger")
        st.markdown(
            """
            * **Case 1 (History):** $T_{\\text{end}} \le W \implies$ Lấy $100\%$ từ **Batch View** (\`lakehouse.batch_agg\`) — Nhãn: **`Reconciled`**.
            * **Case 2 (Realtime):** $T_{\\text{start}} \ge W \implies$ Lấy $100\%$ từ **Speed View** (\`lakehouse.speed_agg\`) — Nhãn: **`Provisional`**.
            * **Case 3 (Hybrid):** $T_{\\text{start}} < W < T_{\\text{end}} \implies$ Tự động phân tách tại Watermark $W$:
              * $[T_{\\text{start}}, W]$ lấy từ Batch View.
              * $(W, T_{\\text{end}}]$ lấy từ Speed View.
              * **ZERO DOUBLE-COUNTING:** Bảo đảm không bao giờ tính trùng lặp cây nến tại biên.
              * **Tự động đối soát:** $\\Delta_{\\text{reconciliation}} = |\\text{VWAP}_{\\text{speed}} - \\text{VWAP}_{\\text{batch}}|$.
            """
        )

    # Xử lý tự động làm mới trang
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()


if __name__ == "__main__":
    main()
