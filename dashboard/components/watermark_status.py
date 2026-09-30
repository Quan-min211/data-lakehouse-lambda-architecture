"""
watermark_status.py
===================
Component hiển thị trạng thái System Watermark và Badge dữ liệu thời gian thực:
- Trạng thái nến: Reconciled (Chuẩn hóa) vs Provisional (Tức thời) vs Partially Reconciled (Lai)
- Huy hiệu (Badge) động với hiệu ứng nhấp nháy (Pulsing Dot)
- Thanh trạng thái Watermark (Status Ribbon) phân tích độ trễ chốt sổ giữa Batch Layer và Speed Layer
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
import streamlit as st


# ── CSS STYLES CHO BADGES VÀ HIỆU ỨNG PULSE ──────────────────────────────────
BADGE_CSS = """
<style>
.badge-container {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 14px;
    border-radius: 20px;
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.3px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.15);
}
.badge-dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    display: inline-block;
}
.pulse-green {
    background-color: #00E676;
    box-shadow: 0 0 0 0 rgba(0, 230, 118, 0.7);
    animation: pulse-green-anim 2s infinite;
}
.pulse-purple {
    background-color: #D500F9;
    box-shadow: 0 0 0 0 rgba(213, 0, 249, 0.7);
    animation: pulse-purple-anim 2s infinite;
}
.pulse-orange {
    background-color: #FF9100;
    box-shadow: 0 0 0 0 rgba(255, 145, 0, 0.7);
    animation: pulse-orange-anim 2s infinite;
}
@keyframes pulse-green-anim {
    0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 230, 118, 0.7); }
    70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(0, 230, 118, 0); }
    100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 230, 118, 0); }
}
@keyframes pulse-purple-anim {
    0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(213, 0, 249, 0.7); }
    70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(213, 0, 249, 0); }
    100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(213, 0, 249, 0); }
}
@keyframes pulse-orange-anim {
    0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(255, 145, 0, 0.7); }
    70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(255, 145, 0, 0); }
    100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(255, 145, 0, 0); }
}

.status-ribbon {
    background: linear-gradient(90deg, #1A1F2C 0%, #131722 100%);
    border: 1px solid #2A2E39;
    border-radius: 10px;
    padding: 12px 18px;
    margin-bottom: 15px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
}
.ribbon-item {
    display: flex;
    flex-direction: column;
}
.ribbon-label {
    font-size: 11px;
    color: #848E9C;
    text-transform: uppercase;
    font-weight: 600;
}
.ribbon-val {
    font-size: 14px;
    color: #EAECEF;
    font-weight: bold;
    font-family: monospace;
}
</style>
"""


def get_badge_html(status: str) -> str:
    """Trả về đoạn HTML badge tương ứng với trạng thái Lambda.

    Args:
        status: 'Reconciled', 'Provisional' hoặc 'Partially Reconciled'.

    Returns:
        Chuỗi HTML hiển thị badge.
    """
    clean_status = str(status).strip()

    if "Reconciled" in clean_status and "Partially" not in clean_status:
        bg_color = "rgba(0, 230, 118, 0.15)"
        border_color = "#00E676"
        text_color = "#00E676"
        dot_class = "pulse-green"
        label = "RECONCILED (Ground Truth)"
    elif "Provisional" in clean_status:
        bg_color = "rgba(213, 0, 249, 0.15)"
        border_color = "#D500F9"
        text_color = "#EA80FC"
        dot_class = "pulse-purple"
        label = "PROVISIONAL (Speed Layer)"
    else:  # Partially Reconciled / Hybrid
        bg_color = "rgba(255, 145, 0, 0.15)"
        border_color = "#FF9100"
        text_color = "#FFB74D"
        dot_class = "pulse-orange"
        label = "PARTIALLY RECONCILED (Hybrid)"

    return (
        f'{BADGE_CSS}'
        f'<div class="badge-container" style="background:{bg_color}; border: 1px solid {border_color}; color:{text_color};">'
        f'<span class="badge-dot {dot_class}"></span>'
        f'<span>{label}</span>'
        f'</div>'
    )


def render_watermark_badge(status: str) -> None:
    """Render trực tiếp badge trạng thái vào giao diện Streamlit."""
    html = get_badge_html(status)
    st.markdown(html, unsafe_allow_html=True)


def render_watermark_status_bar(
    watermark_info: Optional[Dict[str, Any]],
    market_data: Dict[str, Any],
) -> None:
    """Render thanh trạng thái Watermark chi tiết (Status Ribbon) trên đầu giao diện.

    Hiển thị:
      - Trạng thái dữ liệu hiện hành (Badge)
      - Mốc System Watermark W
      - Phân luồng Query Merger (Case 1 / 2 / 3)
      - Độ trễ chốt sổ (Watermark Lag)
    """
    overall_status = market_data.get("overall_status", "Provisional")
    query_case = market_data.get("query_case", 1)

    # 1. Parse Watermark Time
    wm_time_str = "--"
    wm_dt: Optional[datetime] = None
    if watermark_info and watermark_info.get("watermark_time"):
        raw_wm = watermark_info["watermark_time"]
        if isinstance(raw_wm, str):
            wm_time_str = raw_wm.replace("T", " ")[:19]
            try:
                wm_dt = datetime.fromisoformat(raw_wm)
            except Exception:
                pass
        elif isinstance(raw_wm, datetime):
            wm_dt = raw_wm
            wm_time_str = wm_dt.strftime("%Y-%m-%d %H:%M:%S")
    elif market_data.get("watermark"):
        wm_time_str = str(market_data["watermark"]).replace("T", " ")[:19]

    # 2. Tính Watermark Lag
    lag_str = "Thời gian thực"
    if wm_dt:
        if wm_dt.tzinfo is None:
            wm_dt = wm_dt.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        diff_sec = (now_utc - wm_dt).total_seconds()
        if diff_sec > 86400:
            lag_str = f"{diff_sec / 86400:.1f} ngày trước"
        elif diff_sec > 3600:
            lag_str = f"{diff_sec / 3600:.1f} giờ trước"
        elif diff_sec > 60:
            lag_str = f"{diff_sec / 60:.1f} phút trước"
        else:
            lag_str = f"{int(diff_sec)}s lag"

    # 3. Text mô tả Case
    case_desc_map = {
        1: "Case 1: 100% Batch View",
        2: "Case 2: 100% Speed View",
        3: "Case 3: Hybrid (Zero Double-Counting)",
    }
    case_str = case_desc_map.get(query_case, f"Case {query_case}")

    badge_html = get_badge_html(overall_status)

    ribbon_html = f"""
    {BADGE_CSS}
    <div class="status-ribbon">
        <div class="ribbon-item">
            <span class="ribbon-label">Trạng Thái Dữ Liệu</span>
            <div style="margin-top: 4px;">{badge_html}</div>
        </div>
        <div class="ribbon-item">
            <span class="ribbon-label">System Watermark (W)</span>
            <span class="ribbon-val" style="color: #64B5F6;">{wm_time_str} UTC</span>
        </div>
        <div class="ribbon-item">
            <span class="ribbon-label">Độ Trễ Chốt Sổ (Lag)</span>
            <span class="ribbon-val" style="color: #FFB74D;">{lag_str}</span>
        </div>
        <div class="ribbon-item">
            <span class="ribbon-label">Phân Luồng Query Merger</span>
            <span class="ribbon-val" style="color: #81C784;">{case_str}</span>
        </div>
    </div>
    """
    st.markdown(ribbon_html, unsafe_allow_html=True)
