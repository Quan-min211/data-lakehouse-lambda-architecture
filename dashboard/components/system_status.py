"""
system_status.py
================
Streamlit component for FastAPI /api/system/status readiness output.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import streamlit as st

from dashboard.components.theme import COLORS, fmt_price, icon, render_html, section_header


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


def _status_color(status: str) -> str:
    """Map trạng thái dependency sang màu ngữ nghĩa."""
    s = str(status).lower()
    if s in ("ok", "up", "healthy", "connected"):
        return COLORS["reconciled"]
    if s in ("degraded", "slow", "warning"):
        return COLORS["provisional"]
    if s in ("unknown", "--", ""):
        return COLORS["text_muted"]
    return COLORS["danger"]


def _service_cell(name: str, role: str, status: str, icon_name: str) -> str:
    """HTML cho một ô dịch vụ."""
    color = _status_color(status)
    return (
        f'<div class="lh-cell">'
        f'<div class="lh-cell-label">{icon(icon_name, 13)}{name}</div>'
        f'<div style="display:flex;align-items:center;gap:8px;">'
        f'<span class="lh-dot" style="background:{color};width:8px;height:8px;"></span>'
        f'<span class="lh-cell-value" style="font-size:14px;color:{color}">{str(status).upper()}</span></div>'
        f'<div class="lh-cell-note">{role}</div></div>'
    )


def render_system_status_panel(
    status: Optional[Dict[str, Any]],
    latest_speed: Optional[Dict[str, Any]] = None,
) -> None:
    """Render system readiness/freshness summary."""
    section_header(
        "Tình trạng hệ thống",
        "Trạng thái kết nối của các thành phần phục vụ truy vấn và độ tươi của Batch watermark.",
        "server",
    )
    if not status:
        st.warning("Serving API chưa trả về trạng thái hệ thống. Kiểm tra FastAPI hoặc cấu hình URL.")
        return

    watermark = status.get("watermark", {}) or {}
    lag = format_lag(watermark.get("freshness_lag_seconds"))
    wm_time = str(watermark.get("watermark_time", "--")).replace("T", " ")[:19]

    dep_items = status.get("dependencies", []) or []
    dep_map = {str(item.get("name", "")).lower(): item for item in dep_items}

    def dep(name: str) -> str:
        return str(dep_map.get(name, {}).get("status", "unknown"))

    overall = str(status.get("status", "degraded")).lower()
    render_html(
        '<div class="lh-grid" style="grid-template-columns: repeat(5, 1fr);">'
        + _service_cell("Serving API", "FastAPI · Query Merger", overall, "activity")
        + _service_cell("ClickHouse", "Batch & Speed views", dep("clickhouse"), "database")
        + _service_cell("Kafka", "crypto_trades_raw", dep("kafka"), "layers")
        + _service_cell("Redis", "Hot cache · latest candle", dep("redis"), "zap")
        + f'<div class="lh-cell"><div class="lh-cell-label">{icon("clock", 13)}Watermark lag</div>'
          f'<div class="lh-cell-value">{lag}</div><div class="lh-cell-note lh-num">{wm_time} UTC</div></div>'
        + "</div>"
    )

    if latest_speed:
        cache_status = latest_speed.get("cache_status", "unknown")
        section_header("Redis · nến Speed mới nhất", f"Key <code>{latest_speed.get('redis_key', '--')}</code>", "zap")
        if cache_status == "hit" and latest_speed.get("candle"):
            candle = latest_speed["candle"]

            def cell(label: str, value: str) -> str:
                return (
                    f'<div class="lh-cell"><div class="lh-cell-label">{label}</div>'
                    f'<div class="lh-cell-value">{value}</div></div>'
                )

            render_html(
                '<div class="lh-grid" style="grid-template-columns: repeat(4, 1fr);">'
                + cell("Symbol", str(candle.get("symbol", "--")))
                + cell("Close", fmt_price(candle.get("close_price")))
                + cell("VWAP", fmt_price(candle.get("vwap")))
                + cell("Trades", f"{int(candle.get('trade_count', 0) or 0):,}")
                + "</div>"
            )
        elif cache_status == "miss":
            st.info("Redis chưa có nến mới nhất cho key này — Speed Layer có thể chưa xử lý micro-batch đầu tiên.")
        else:
            st.warning(f"Không đọc được Redis: {latest_speed.get('error', 'unknown error')}")
