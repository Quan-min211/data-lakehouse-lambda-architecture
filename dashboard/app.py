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
from dashboard.components.benchmark_charts import (
    render_benchmark_overview,
    render_benchmark1_latency,
    render_benchmark2_reconciliation,
    render_benchmark3_compaction,
    render_benchmark_images,
)
from dashboard.components.dq_panel import render_dq_quarantine_panel
from dashboard.components.watermark_status import render_watermark_status_bar
from dashboard.components.system_status import render_system_status_panel


from dashboard.components.theme import (
    COLORS,
    chip,
    html_block,
    icon,
    inject_global_css,
    render_html,
    section_header,
    sidebar_label,
)
from dashboard.components.watermark_status import resolve_watermark

DEFAULT_API_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
TOP_COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT",
]
TIME_PRESETS = {"15m": 15, "30m": 30, "1H": 60, "3H": 180, "6H": 360}


def _configure_page() -> None:
    """Cấu hình trang Streamlit và nạp design system (chỉ gọi khi chạy app)."""
    st.set_page_config(
        page_title="Market Monitor · Lambda Lakehouse",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_global_css()


# =============================================================================
# HÀM LẤY DỮ LIỆU TỪ SERVING API (HOẶC FALLBACK DỮ LIỆU MẪU)
# =============================================================================
def fetch_api_data(
    api_url: str,
    symbol: str,
    start_time: datetime,
    end_time: datetime
) -> Dict[str, Any]:
    """Gọi Serving Layer FastAPI GET /api/market.

    - 200: dữ liệu thật (data_source = "live").
    - 4xx: lỗi tham số truy vấn -> KHÔNG chuyển sang demo, trả payload rỗng
      với data_source = "error" để giao diện báo lỗi.
    - 5xx hoặc lỗi kết nối mạng: chuyển sang dữ liệu demo (data_source = "demo").
    """
    endpoint = f"{api_url.rstrip('/')}/api/market"
    params = {
        "symbol": symbol,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
    }
    try:
        resp = requests.get(endpoint, params=params, timeout=4)
        if resp.status_code == 200:
            payload = resp.json()
            payload["data_source"] = "live"
            return payload
        if 400 <= resp.status_code < 500:
            try:
                detail = resp.json().get("detail", resp.text)
            except ValueError:
                detail = resp.text
            return {
                "symbol": symbol,
                "query_case": None,
                "overall_status": None,
                "reconciliation_delta": None,
                "candles_count": 0,
                "candles": [],
                "data_source": "error",
                "error": f"HTTP {resp.status_code}: {detail}",
            }
    except Exception:
        pass

    # Fallback dữ liệu mô phỏng khi FastAPI không kết nối được hoặc lỗi máy chủ (5xx)
    payload = generate_fallback_market_data(symbol, start_time, end_time)
    payload["data_source"] = "demo"
    return payload


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


def fetch_system_status(api_url: str) -> Optional[Dict[str, Any]]:
    """Gọi Serving Layer FastAPI GET /api/system/status."""
    endpoint = f"{api_url.rstrip('/')}/api/system/status"
    try:
        resp = requests.get(endpoint, timeout=3)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def fetch_latest_speed_candle(api_url: str, symbol: str) -> Optional[Dict[str, Any]]:
    """Gọi Serving Layer FastAPI GET /api/speed/latest."""
    endpoint = f"{api_url.rstrip('/')}/api/speed/latest"
    try:
        resp = requests.get(endpoint, params={"symbol": symbol}, timeout=3)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def fetch_system_alerts(api_url: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """Gọi Serving Layer FastAPI GET /api/system/alerts (None nếu không gọi được)."""
    endpoint = f"{api_url.rstrip('/')}/api/system/alerts"
    try:
        resp = requests.get(endpoint, params={"limit": limit}, timeout=3)
        if resp.status_code == 200:
            return resp.json().get("alerts", [])
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
        # Cùng ngưỡng với SpikeDetector: spike_threshold = 0.02, volatility_threshold = 0.03
        is_spike = 1 if abs(close_p - open_p) / open_p >= 0.02 or (high_p - low_p) / low_p >= 0.03 else 0

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
        q_desc = "Case 3 (Hybrid): Ghép nối tự động Batch [T_start, W) + Speed [W, T_end)"
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
def render_sidebar() -> Dict[str, Any]:
    """Render sidebar điều khiển và trả về cấu hình người dùng chọn.

    Returns:
        dict: symbol, minutes_back, api_base_url, auto_refresh, refresh_rate.
    """
    st.sidebar.markdown(
        html_block(
            f"""
            <div class="lh-brand" style="margin-bottom:6px;">
            <div class="lh-brand-mark">{icon("layers", 15)}</div>
            <div><div class="lh-brand-title">Lambda Lakehouse</div>
            <div class="lh-brand-sub">Market data platform</div></div>
            </div>
            """
        ),
        unsafe_allow_html=True,
    )

    sidebar_label("Instrument")
    symbol = st.sidebar.selectbox("Cặp giao dịch", TOP_COINS, index=0, label_visibility="collapsed")

    sidebar_label("Time range")
    preset = st.sidebar.radio(
        "Khung thời gian", list(TIME_PRESETS.keys()), index=2,
        horizontal=True, label_visibility="collapsed",
    )

    sidebar_label("Connection")
    api_base_url = st.sidebar.text_input("Serving API", value=DEFAULT_API_URL, label_visibility="collapsed")

    sidebar_label("Refresh")
    auto_refresh = st.sidebar.toggle("Tự động làm mới", value=False)
    refresh_rate = st.sidebar.slider("Chu kỳ (giây)", min_value=3, max_value=30, value=5, disabled=not auto_refresh)
    if st.sidebar.button("Làm mới ngay", use_container_width=True):
        st.rerun()

    return {
        "symbol": symbol,
        "minutes_back": TIME_PRESETS[preset],
        "range_label": preset,
        "api_base_url": api_base_url,
        "auto_refresh": auto_refresh,
        "refresh_rate": refresh_rate,
    }


def render_topbar(market_data: Dict[str, Any], system_status: Optional[Dict[str, Any]], now_utc: datetime) -> None:
    """Thanh trên cùng: tên sản phẩm, nguồn dữ liệu, tình trạng API và đồng hồ UTC."""
    data_source = market_data.get("data_source")
    if data_source == "live":
        source_chip = chip("Live · Serving API", COLORS["reconciled"])
    elif data_source == "error":
        source_chip = chip("API error · tham số không hợp lệ", COLORS["danger"])
    else:
        source_chip = chip("Demo data · API offline", COLORS["provisional"])
    if system_status:
        ok = str(system_status.get("status", "")).lower() == "ok"
        health_chip = chip("Systems nominal" if ok else "Degraded", COLORS["reconciled"] if ok else COLORS["provisional"])
    else:
        health_chip = chip("Health unknown", COLORS["text_muted"])

    render_html(
        f"""
        <div class="lh-topbar">
        <div class="lh-brand">
        <div class="lh-brand-mark">{icon("activity", 15)}</div>
        <div><div class="lh-brand-title">Market Monitor</div>
        <div class="lh-brand-sub">Speed + Batch serving · Auto-correcting query merger</div></div>
        </div>
        <div class="lh-topbar-right">
        {source_chip}{health_chip}
        <span class="lh-chip lh-num" style="color:var(--text)">{now_utc.strftime('%H:%M:%S')} UTC</span>
        </div>
        </div>
        """
    )


def build_demo_reconciliation(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sinh bảng đối soát minh họa từ nến khi API /api/reconciliation không khả dụng."""
    return [
        {
            "window_start": c["window_start"],
            "window_end": c["window_end"],
            "batch_vwap": c["vwap"] - 2.5 if c["status"] == "Reconciled" else None,
            "speed_vwap": c["vwap"],
            "vwap_delta": 2.5 if c["status"] == "Reconciled" else None,
            "batch_volume": c["volume"],
            "speed_volume": c["volume"] + 0.1,
            "volume_delta": 0.1,
            "volume_drift_pct": round(0.1 / c["volume"] * 100, 4) if c["volume"] else None,
            "reconciliation_status": "RECONCILED_AND_HEALED" if c["status"] == "Reconciled" else "PROVISIONAL_ONLY",
            "is_reconciled": (c["status"] == "Reconciled"),
        }
        for c in candles[-10:]
    ]


def render_spike_tab(candles: List[Dict[str, Any]]) -> None:
    """Tab nhật ký phát hiện biến động giá bất thường."""
    section_header(
        "Biến động bất thường",
        "Nến có biên độ |close − open| / open hoặc biên độ đỉnh–đáy (high − low) / low vượt ngưỡng của SpikeDetector trong Speed Layer.",
        "alert",
    )
    spike_candles = [c for c in candles if c.get("is_spike") == 1 or c.get("is_spike") is True]
    if not spike_candles:
        st.info("Không ghi nhận nến biến động bất thường trong khung thời gian đang xem.")
        return
    spike_df = pd.DataFrame(spike_candles)
    # |close − open| / open: cùng công thức với SpikeDetector và AlertManager.check_price_spike();
    # chiều tăng/giảm tách riêng sang cột direction.
    spike_df["amplitude_pct"] = (
        (spike_df["close_price"] - spike_df["open_price"]).abs() / spike_df["open_price"] * 100
    ).round(3)
    spike_df["direction"] = [
        "▲ tăng" if c >= o else "▼ giảm"
        for o, c in zip(spike_df["open_price"], spike_df["close_price"])
    ]
    spike_df["range_pct"] = (
        (spike_df["high_price"] - spike_df["low_price"]) / spike_df["low_price"] * 100
    ).round(3)
    st.dataframe(
        spike_df,
        use_container_width=True,
        hide_index=True,
        column_order=[
            "window_start",
            "open_price",
            "close_price",
            "high_price",
            "low_price",
            "amplitude_pct",
            "direction",
            "range_pct",
            "volume",
            "status",
        ],
        column_config={
            "window_start": st.column_config.DatetimeColumn("Thời điểm (UTC)", format="YYYY-MM-DD HH:mm"),
            "open_price": st.column_config.NumberColumn("Open", format="%.4f"),
            "close_price": st.column_config.NumberColumn("Close", format="%.4f"),
            "high_price": st.column_config.NumberColumn("High", format="%.4f"),
            "low_price": st.column_config.NumberColumn("Low", format="%.4f"),
            "amplitude_pct": st.column_config.NumberColumn("Biên độ |Đóng–Mở| (%)", format="%.3f"),
            "direction": st.column_config.TextColumn("Chiều"),
            "range_pct": st.column_config.NumberColumn("Biên độ Đỉnh–Đáy (%)", format="%.3f"),
            "volume": st.column_config.NumberColumn("Volume", format="%.4f"),
            "status": st.column_config.TextColumn("Trạng thái"),
        },
    )


def render_architecture_tab() -> None:
    """Tab mô tả cơ chế định tuyến của Auto-Correcting Query Merger."""
    section_header(
        "Cơ chế định tuyến truy vấn",
        "Query Merger chia khoảng truy vấn nửa mở [T_start, T_end) tại mốc Watermark W do Batch Layer "
        "công bố, đảm bảo mỗi cửa sổ 1 phút chỉ được lấy từ đúng một nguồn.",
        "merge",
    )
    cases = [
        ("Case 1 · History", "T_end ≤ W", COLORS["reconciled"], "Reconciled",
         "Toàn bộ dữ liệu đọc từ <code>lakehouse.batch_agg</code> (FINAL). Đây là ground truth sau DQ gate và dedup."),
        ("Case 2 · Realtime", "T_start ≥ W", COLORS["provisional"], "Provisional",
         "Toàn bộ dữ liệu đọc từ <code>lakehouse.speed_agg</code> do Spark Structured Streaming ghi mỗi micro-batch."),
        ("Case 3 · Hybrid", "T_start < W < T_end", COLORS["hybrid"], "Partially Reconciled",
         "<code>[T_start, W)</code> từ <code>batch_agg</code>, <code>[W, T_end)</code> từ <code>speed_agg</code>: "
         "hai khoảng nửa mở không giao nhau nên không double-count. Truy vấn thứ ba vào "
         "<code>speed_agg</code> trên <code>[T_start, W)</code> tính |VWAP speed − VWAP batch| tại biên."),
    ]
    cols = st.columns(3)
    for col, (title, cond, color, label, body) in zip(cols, cases):
        with col:
            render_html(
                f"""
                <div class="lh-case" style="border-top:2px solid {color};">
                <div class="lh-case-head"><span class="lh-case-title">{title}</span>
                <span class="lh-badge" style="background:{color}1F;color:{color};border:1px solid {color}55;">{label}</span></div>
                <div class="lh-num" style="font-size:12px;color:var(--text);margin-bottom:8px;">{cond}</div>
                <div class="lh-case-body">{body}</div>
                </div>
                """
            )


def main() -> None:
    """Điểm vào giao diện Streamlit."""
    _configure_page()
    cfg = render_sidebar()

    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(minutes=cfg["minutes_back"])
    end_time = now_utc
    api_base_url = cfg["api_base_url"]
    selected_symbol = cfg["symbol"]

    # Lấy dữ liệu
    market_data = fetch_api_data(api_base_url, selected_symbol, start_time, end_time)
    watermark_info = fetch_watermark(api_base_url)
    system_status = fetch_system_status(api_base_url)
    latest_speed = fetch_latest_speed_candle(api_base_url, selected_symbol)
    system_alerts = fetch_system_alerts(api_base_url)
    candles = market_data.get("candles", [])

    # 1. Top bar + header giá + dải trạng thái Lambda
    render_topbar(market_data, system_status, now_utc)
    if market_data.get("data_source") == "error":
        st.error(f"Serving API từ chối truy vấn: {market_data.get('error', '')}")
    render_metrics_cards(market_data, watermark_info)
    render_watermark_status_bar(watermark_info, market_data)

    # 2. Biểu đồ nến (vùng sau W được đánh dấu Provisional)
    fig = render_candlestick_chart(
        candles,
        symbol=selected_symbol,
        watermark=resolve_watermark(watermark_info, market_data),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False, "displayModeBar": "hover"})

    # 3. Tabs chi tiết
    tab_recon, tab_bench, tab_dq, tab_system, tab_spike, tab_arch = st.tabs([
        "Đối soát",
        "Benchmark",
        "Data quality",
        "Hệ thống",
        "Biến động",
        "Kiến trúc",
    ])

    with tab_recon:
        report_data = fetch_reconciliation_report(api_base_url, selected_symbol, start_time, end_time)
        # Chỉ dựng bảng minh họa khi đang ở chế độ demo (API offline). Khi API thật trả về
        # rỗng, hiển thị rỗng thay vì chèn số liệu bịa không gắn nhãn.
        if not report_data and candles and market_data.get("data_source") == "demo":
            st.caption("Bảng đối soát minh họa (dữ liệu demo, API offline).")
            report_data = build_demo_reconciliation(candles)
        render_reconciliation_table(report_data)

    with tab_bench:
        bench_sub = st.radio(
            "Benchmark",
            ["Tổng quan", "BM1 · Query latency", "BM2 · Reconciliation", "BM3 · Compaction", "Biểu đồ gốc"],
            horizontal=True,
            label_visibility="collapsed",
        )
        if bench_sub == "Tổng quan":
            render_benchmark_overview()
        elif bench_sub == "BM1 · Query latency":
            render_benchmark1_latency()
        elif bench_sub == "BM2 · Reconciliation":
            render_benchmark2_reconciliation()
        elif bench_sub == "BM3 · Compaction":
            render_benchmark3_compaction()
        else:
            render_benchmark_images()

    with tab_dq:
        # Thông tin kết nối (host/port/user/password/database) lấy từ biến môi trường CLICKHOUSE_*
        render_dq_quarantine_panel()

    with tab_system:
        render_system_status_panel(system_status, latest_speed=latest_speed, alerts=system_alerts)
        source_breakdown = market_data.get("source_breakdown", {})
        if source_breakdown:
            with st.expander("Query source breakdown (raw)"):
                st.json(source_breakdown)

    with tab_spike:
        render_spike_tab(candles)

    with tab_arch:
        render_architecture_tab()

    render_html(
        f"""
        <div class="lh-footnote">
        Khóa luận tốt nghiệp · Data Lakehouse kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực ·
        Nguyễn Đặng Quốc Anh, Phạm Minh Quân · Dữ liệu cập nhật {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC
        </div>
        """
    )

    # Xử lý tự động làm mới trang
    if cfg["auto_refresh"]:
        time.sleep(cfg["refresh_rate"])
        st.rerun()


if __name__ == "__main__":
    main()
