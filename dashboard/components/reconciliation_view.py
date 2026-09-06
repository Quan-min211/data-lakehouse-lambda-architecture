"""
reconciliation_view.py
======================
Bảng đối soát chi tiết (Audit & Benchmark 2 Panel):
So sánh trực tiếp từng cây nến giữa Batch Layer (Ground Truth) và Speed Layer (Provisional).
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any


def render_reconciliation_table(report_items: List[Dict[str, Any]]):
    """Hiển thị bảng đối soát chi tiết giữa Speed View và Batch View."""
    st.subheader("📋 Bảng Đối Soát Dữ Liệu Hai Tầng (Benchmark 2: Reconciliation Accuracy)")

    if not report_items:
        st.info("Chưa có sự kiện đối soát trong khoảng thời gian này hoặc Batch Layer chưa chạy mốc đối soát tương ứng.")
        return

    df = pd.DataFrame(report_items)

    column_mapping = {
        "window_start": "Bắt đầu (UTC)",
        "window_end": "Kết thúc (UTC)",
        "batch_vwap": "VWAP (Batch)",
        "speed_vwap": "VWAP (Speed)",
        "vwap_delta": "Sai lệch Δ (USDT)",
        "batch_volume": "Vol (Batch)",
        "speed_volume": "Vol (Speed)",
        "volume_delta": "Sai lệch Vol",
        "is_reconciled": "Đã Đối Soát?"
    }

    display_df = df.rename(columns=column_mapping)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    if "vwap_delta" in df.columns:
        valid_deltas = df["vwap_delta"].dropna()
        if not valid_deltas.empty:
            mean_delta = valid_deltas.mean()
            max_delta = valid_deltas.max()
            st.caption(f"**Thống kê sai số:** Sai số VWAP trung bình: **${mean_delta:.4f} USDT** | Sai số lớn nhất: **${max_delta:.4f} USDT**")
