"""
watermark_status.py
===================
Component hiển thị trạng thái System Watermark và Badge dữ liệu thời gian thực:
- Trạng thái nến: Reconciled (Chuẩn hóa) vs Provisional (Tức thời) vs Partially Reconciled (Lai)
- Huy hiệu (Badge) trạng thái dạng phẳng, màu mang ngữ nghĩa
- Dải trạng thái Lambda (Serving route, Watermark, độ trễ chốt sổ, sai số đối soát)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from dashboard.components.theme import COLORS, icon, render_html

STATUS_STYLES = {
    "reconciled": (COLORS["reconciled"], "Reconciled"),
    "provisional": (COLORS["provisional"], "Provisional"),
    "hybrid": (COLORS["hybrid"], "Partially Reconciled"),
}


def _status_key(status: str) -> str:
    """Chuẩn hóa chuỗi trạng thái về khóa 'reconciled' | 'provisional' | 'hybrid'."""
    clean_status = str(status).strip()
    if "Reconciled" in clean_status and "Partially" not in clean_status:
        return "reconciled"
    if "Provisional" in clean_status:
        return "provisional"
    return "hybrid"


def get_badge_html(status: str) -> str:
    """Trả về đoạn HTML badge tương ứng với trạng thái Lambda.

    Args:
        status: 'Reconciled', 'Provisional' hoặc 'Partially Reconciled'.

    Returns:
        Chuỗi HTML hiển thị badge.
    """
    color, label = STATUS_STYLES[_status_key(status)]
    return (
        f'<span class="lh-badge" style="background:{color}1F;color:{color};border:1px solid {color}55;">'
        f'<span class="lh-dot" style="background:{color}"></span>{label}</span>'
    )


def render_watermark_badge(status: str) -> None:
    """Render trực tiếp badge trạng thái vào giao diện Streamlit."""
    st.markdown(get_badge_html(status), unsafe_allow_html=True)


def _parse_dt(raw: Any) -> Optional[datetime]:
    """Parse datetime/ISO string về datetime có timezone UTC."""
    dt: Optional[datetime] = None
    if isinstance(raw, datetime):
        dt = raw
    elif isinstance(raw, str) and raw:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt is not None and dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def resolve_watermark(
    watermark_info: Optional[Dict[str, Any]],
    market_data: Dict[str, Any],
) -> Optional[datetime]:
    """Lấy mốc Watermark ưu tiên từ /api/watermark, fallback sang payload /api/market."""
    if watermark_info and watermark_info.get("watermark_time"):
        return _parse_dt(watermark_info["watermark_time"])
    return _parse_dt(market_data.get("watermark"))


def _format_lag(seconds: float) -> str:
    """Định dạng độ trễ giây thành chuỗi ngắn gọn."""
    if seconds >= 86400:
        return f"{seconds / 86400:.1f} ngày"
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} giờ"
    if seconds >= 60:
        return f"{seconds / 60:.1f} phút"
    return f"{int(seconds)} giây"


def _source_split(candles: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Đếm số nến Reconciled (Batch) và Provisional (Speed)."""
    batch = sum(1 for c in candles if _status_key(c.get("status", "")) == "reconciled")
    return batch, len(candles) - batch


def render_watermark_status_bar(
    watermark_info: Optional[Dict[str, Any]],
    market_data: Dict[str, Any],
) -> None:
    """Render dải trạng thái Lambda Serving dưới header giá.

    Hiển thị:
      - Tuyến truy vấn Query Merger (Case 1 / 2 / 3) + tỷ lệ nến Batch/Speed
      - Mốc System Watermark W
      - Độ trễ chốt sổ (Watermark Lag)
      - Sai số đối soát tại biên (Reconciliation Δ)
    """
    overall_status = market_data.get("overall_status", "Provisional")
    query_case = market_data.get("query_case", 1)
    try:
        query_case = int(query_case) if query_case is not None else None
    except (TypeError, ValueError):
        query_case = None
    candles = market_data.get("candles", []) or []
    delta = market_data.get("reconciliation_delta")

    # 1. Serving route + split bar
    case_names = {1: "Batch view", 2: "Speed view", 3: "Hybrid merge"}
    case_name = case_names.get(query_case, "Unknown")
    case_label = f"Case {query_case}" if query_case else "Case --"
    status_badge = get_badge_html(overall_status) if overall_status else ""
    batch_n, speed_n = _source_split(candles)
    total = max(batch_n + speed_n, 1)
    batch_pct = batch_n / total * 100
    split_bar = (
        f'<div class="lh-split">'
        f'<span style="width:{batch_pct:.1f}%;background:{COLORS["reconciled"]}"></span>'
        f'<span style="width:{100 - batch_pct:.1f}%;background:{COLORS["provisional"]}"></span>'
        f'</div>'
    )

    # 2. Watermark + lag
    wm_dt = resolve_watermark(watermark_info, market_data)
    wm_str = wm_dt.strftime("%H:%M:%S") if wm_dt else "--:--:--"
    wm_date = wm_dt.strftime("%Y-%m-%d UTC") if wm_dt else "Chưa có mốc chốt sổ"
    if wm_dt:
        lag_sec = max((datetime.now(timezone.utc) - wm_dt).total_seconds(), 0.0)
        lag_str = _format_lag(lag_sec)
        lag_color = COLORS["text"] if lag_sec < 1800 else COLORS["provisional"]
    else:
        lag_str, lag_color = "--", COLORS["text_muted"]

    # 3. Reconciliation delta. Case 1 dat delta = 0.0 theo quy uoc (khong co phep do
    # nao), nen khong duoc hien thi nhu "khop hoan toan".
    if query_case == 1:
        delta_str = "N/A"
        delta_note = "Dữ liệu Lịch sử Chuẩn tắc (100% Batch - Không phát sinh đối soát)"
        delta_color = COLORS["reconciled"]
    elif delta is None:
        delta_str, delta_note, delta_color = "--", "Chưa có vùng giao thoa", COLORS["text_muted"]
    elif float(delta) == 0.0:
        delta_str, delta_note, delta_color = "0.0000", "Khớp hoàn toàn tại biên W", COLORS["reconciled"]
    else:
        delta_str, delta_note, delta_color = f"{float(delta):,.4f}", "|VWAP speed − VWAP batch|", COLORS["provisional"]

    render_html(
        f"""
        <div class="lh-grid" style="grid-template-columns: 1.3fr 1fr 1fr 1fr;">
        <div class="lh-cell">
        <div class="lh-cell-label">{icon("merge", 13)}Serving route</div>
        <div style="display:flex;align-items:center;gap:10px;">
        <span class="lh-cell-value" style="font-family:var(--sans);font-weight:600;">{case_label} · {case_name}</span>
        {status_badge}
        </div>
        {split_bar}
        <div class="lh-cell-note"><span class="lh-num">{batch_n}</span> nến batch · <span class="lh-num">{speed_n}</span> nến speed</div>
        </div>
        <div class="lh-cell">
        <div class="lh-cell-label">{icon("database", 13)}Batch watermark</div>
        <div class="lh-cell-value">{wm_str}</div>
        <div class="lh-cell-note">{wm_date}</div>
        </div>
        <div class="lh-cell">
        <div class="lh-cell-label">{icon("clock", 13)}Độ trễ chốt sổ</div>
        <div class="lh-cell-value" style="color:{lag_color}">{lag_str}</div>
        <div class="lh-cell-note">Khoảng cách now − W</div>
        </div>
        <div class="lh-cell">
        <div class="lh-cell-label">{icon("scale", 13)}Sai số đối soát Δ</div>
        <div class="lh-cell-value" style="color:{delta_color}">{delta_str}</div>
        <div class="lh-cell-note">{delta_note}</div>
        </div>
        </div>
        """
    )
