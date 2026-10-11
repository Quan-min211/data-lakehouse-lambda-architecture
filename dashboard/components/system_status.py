"""
system_status.py
================
Streamlit component for FastAPI /api/system/status readiness output.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import streamlit as st


def format_lag(seconds: Optional[float]) -> str:
    """Format freshness lag seconds into a compact human-readable string."""
    if seconds is None:
        return "--"
    if seconds >= 86400:
        return f"{seconds / 86400:.1f} ngày"
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} giờ"
    if seconds >= 60:
        return f"{seconds / 60:.1f} phút"
    return f"{int(seconds)} giây"


def render_system_status_panel(
    status: Optional[Dict[str, Any]],
    latest_speed: Optional[Dict[str, Any]] = None,
) -> None:
    """Render system readiness/freshness summary."""
    if not status:
        st.warning("Serving API chưa trả về trạng thái hệ thống. Kiểm tra FastAPI hoặc cấu hình URL.")
        return

    overall = str(status.get("status", "degraded")).lower()
    is_ok = overall == "ok"
    banner = "Hệ thống sẵn sàng" if is_ok else "Hệ thống degraded"
    st.success(banner) if is_ok else st.warning(banner)

    watermark = status.get("watermark", {}) or {}
    lag = format_lag(watermark.get("freshness_lag_seconds"))
    wm_time = str(watermark.get("watermark_time", "--")).replace("T", " ")[:19]

    dep_items = status.get("dependencies", []) or []
    dep_map = {str(item.get("name", "")).lower(): item for item in dep_items}

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("ClickHouse", dep_map.get("clickhouse", {}).get("status", "unknown"))
    with col2:
        st.metric("Kafka", dep_map.get("kafka", {}).get("status", "unknown"))
    with col3:
        st.metric("Redis", dep_map.get("redis", {}).get("status", "unknown"))
    with col4:
        st.metric("Watermark Lag", lag)

    st.caption(f"Batch Watermark: {wm_time} UTC")

    if latest_speed:
        cache_status = latest_speed.get("cache_status", "unknown")
        st.markdown("##### Redis Latest Speed Candle")
        if cache_status == "hit" and latest_speed.get("candle"):
            candle = latest_speed["candle"]
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("Symbol", candle.get("symbol", "--"))
            with c2:
                st.metric("Close", f"{float(candle.get('close_price', 0.0)):,.4f}")
            with c3:
                st.metric("VWAP", f"{float(candle.get('vwap', 0.0)):,.4f}")
            with c4:
                st.metric("Trades", int(candle.get("trade_count", 0)))
            st.caption(f"Redis key: {latest_speed.get('redis_key', '--')}")
        elif cache_status == "miss":
            st.info(f"Redis cache chưa có latest candle cho key `{latest_speed.get('redis_key', '--')}`.")
        else:
            st.warning(f"Redis latest candle unavailable: {latest_speed.get('error', 'unknown error')}")
