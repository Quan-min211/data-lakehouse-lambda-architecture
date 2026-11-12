"""
reconciliation_view.py
======================
Bảng đối soát chi tiết (Audit & Benchmark 2 Panel):
So sánh trực tiếp từng cây nến giữa Batch Layer (Ground Truth) và Speed Layer (Provisional).
"""

from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from dashboard.components.theme import COLORS, icon, render_html, section_header


def render_reconciliation_table(report_items: List[Dict[str, Any]]) -> None:
    """Hiển thị bảng đối soát chi tiết giữa Speed View và Batch View.

    Args:
        report_items: Danh sách bản ghi đối soát từ /api/reconciliation.
    """
    section_header(
        "Đối soát Speed view và Batch view",
        "Mỗi cửa sổ 1 phút được so sánh giữa nến tạm tính của Speed Layer và nến chuẩn của "
        "Batch Layer sau khi Watermark được nâng. Δ là sai lệch tuyệt đối dùng cho Benchmark 2.",
        "scale",
    )

    if not report_items:
        st.info("Chưa có cửa sổ nào được đối soát trong khoảng thời gian này — Batch Layer chưa chốt mốc tương ứng.")
        return

    df = pd.DataFrame(report_items)

    # Thống kê tóm tắt
    total = len(df)
    reconciled = int(df["is_reconciled"].fillna(False).astype(bool).sum()) if "is_reconciled" in df.columns else 0
    deltas = df["vwap_delta"].dropna() if "vwap_delta" in df.columns else pd.Series(dtype=float)
    mean_delta = f"{deltas.abs().mean():,.4f}" if not deltas.empty else "--"
    max_delta = f"{deltas.abs().max():,.4f}" if not deltas.empty else "--"
    coverage = reconciled / total * 100 if total else 0.0
    status_counts = (
        df["reconciliation_status"].value_counts().to_dict()
        if "reconciliation_status" in df.columns else {}
    )
    matched = int(status_counts.get("MATCHED", 0))
    healed = int(status_counts.get("RECONCILED_AND_HEALED", 0))

    def cell(label: str, value: str, note: str, color: str = COLORS["text"]) -> str:
        return (
            f'<div class="lh-cell"><div class="lh-cell-label">{label}</div>'
            f'<div class="lh-cell-value" style="color:{color}">{value}</div>'
            f'<div class="lh-cell-note">{note}</div></div>'
        )

    render_html(
        '<div class="lh-grid" style="grid-template-columns: repeat(4, 1fr);">'
        + cell("Cửa sổ so sánh", f"{total}", "Số nến trong khung")
        + cell("Đã đối soát", f"{reconciled}",
               f"{coverage:.1f}% coverage · {matched} MATCHED · {healed} HEALED", COLORS["reconciled"])
        + cell("Δ VWAP trung bình", mean_delta, "USDT")
        + cell("Δ VWAP lớn nhất", max_delta, "USDT")
        + "</div>"
    )

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_order=[
            c for c in [
                "window_start", "window_end", "reconciliation_status", "batch_vwap", "speed_vwap",
                "vwap_delta", "batch_volume", "speed_volume", "volume_delta", "volume_drift_pct",
                "is_reconciled",
            ] if c in df.columns
        ],
        column_config={
            "window_start": st.column_config.DatetimeColumn("Bắt đầu (UTC)", format="HH:mm:ss"),
            "window_end": st.column_config.DatetimeColumn("Kết thúc (UTC)", format="HH:mm:ss"),
            "batch_vwap": st.column_config.NumberColumn("VWAP batch", format="%.4f"),
            "speed_vwap": st.column_config.NumberColumn("VWAP speed", format="%.4f"),
            "vwap_delta": st.column_config.NumberColumn("Δ VWAP", format="%.4f"),
            "batch_volume": st.column_config.NumberColumn("Vol batch", format="%.4f"),
            "speed_volume": st.column_config.NumberColumn("Vol speed", format="%.4f"),
            "volume_delta": st.column_config.NumberColumn("Δ Vol", format="%.4f"),
            "volume_drift_pct": st.column_config.NumberColumn("ΔV (%)", format="%.4f"),
            "reconciliation_status": st.column_config.TextColumn(
                "Trạng thái đối soát",
                help="MATCHED · RECONCILED_AND_HEALED · BATCH_ONLY · PROVISIONAL_ONLY "
                     "(src/utils/reconciliation.py: |ΔVWAP| < 1e-4 và ΔV% < 1e-4 thì MATCHED)",
            ),
            "is_reconciled": st.column_config.CheckboxColumn("Đã chốt"),
        },
    )
    render_html(
        f'<div class="lh-cell-note" style="display:flex;align-items:center;gap:6px;">'
        f'{icon("check", 12, COLORS["reconciled"])} Nguồn: <code>lakehouse.batch_agg</code> FINAL '
        f'đối chiếu <code>lakehouse.speed_agg</code></div>'
    )
