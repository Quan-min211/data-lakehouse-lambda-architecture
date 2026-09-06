"""
metrics_cards.py
================
Hiển thị các thẻ chỉ số KPI tài chính và trạng thái Lambda Serving:
- Giá mới nhất & Biến động
- VWAP thời gian thực
- Kịch bản phân luồng Query Merger (Case 1 / 2 / 3)
- Mốc System Watermark
- Sai số đối soát (Reconciliation Delta)
"""

import streamlit as st
from typing import Dict, Any, Optional


def render_metrics_cards(market_data: Dict[str, Any], watermark_info: Optional[Dict[str, Any]] = None):
    """Vẽ hàng thẻ chỉ số KPI tổng quan trên đầu trang Dashboard."""
    candles = market_data.get("candles", [])
    query_case = market_data.get("query_case", 1)
    reconcil_delta = market_data.get("reconciliation_delta")
    overall_status = market_data.get("overall_status", "Provisional")

    latest_price = 0.0
    price_change_str = "--"
    delta_color = "normal"
    latest_vwap = 0.0

    if candles:
        last_c = candles[-1]
        latest_price = float(last_c.get("close_price", 0.0))
        latest_vwap = float(last_c.get("vwap", 0.0))

        if len(candles) >= 2:
            prev_c = candles[-2]
            prev_close = float(prev_c.get("close_price", 0.0))
            if prev_close > 0:
                diff = latest_price - prev_close
                diff_pct = (diff / prev_close) * 100
                price_change_str = f"{diff:+.2f} USDT ({diff_pct:+.2f}%)"
                delta_color = "normal" if diff >= 0 else "inverse"

    case_badges = {
        1: ("🟢 CASE 1: HISTORY", "100% Batch View (Reconciled)", "off"),
        2: ("🟣 CASE 2: REALTIME", "100% Speed View (Provisional)", "normal"),
        3: ("🟠 CASE 3: HYBRID", "Cắt ghép Batch + Speed (Partially Reconciled)", "inverse"),
    }
    case_title, case_desc, _ = case_badges.get(
        query_case, ("XÁC ĐỊNH...", "Đang phân tích", "off")
    )

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="Giá Hiện Tại",
            value=f"${latest_price:,.2f}" if latest_price > 0 else "--",
            delta=price_change_str,
            delta_color=delta_color
        )

    with col2:
        st.metric(
            label="Chỉ Số VWAP",
            value=f"${latest_vwap:,.2f}" if latest_vwap > 0 else "--",
            help="Volume-Weighted Average Price tính từ trọng số khối lượng"
        )

    with col3:
        st.metric(
            label="Phân Luồng Query Merger",
            value=case_title,
            delta=overall_status,
            delta_color="normal" if overall_status == "Reconciled" else "off",
            help=case_desc
        )

    with col4:
        wm_str = "--"
        if watermark_info and "watermark_time" in watermark_info:
            wm_str = str(watermark_info["watermark_time"]).replace("T", " ")[:19]
        elif market_data.get("watermark"):
            wm_str = str(market_data["watermark"]).replace("T", " ")[:19]

        st.metric(
            label="System Watermark",
            value=wm_str,
            help="Mốc ranh giới thời gian mà Batch Layer đã chốt dữ liệu chuẩn xác"
        )

    with col5:
        delta_val = f"${reconcil_delta:,.4f}" if reconcil_delta is not None else "0.0000"
        status_note = "Khớp 100%" if reconcil_delta == 0.0 else ("Chưa đối soát" if reconcil_delta is None else "Sai lệch biên")
        st.metric(
            label="Sai Số Đối Soát (Δ)",
            value=delta_val,
            delta=status_note,
            delta_color="normal" if reconcil_delta == 0.0 else "inverse",
            help="|VWAP_speed - VWAP_batch| tại vùng giao thoa giữa 2 tầng"
        )
