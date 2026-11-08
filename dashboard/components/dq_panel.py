"""
dq_panel.py
===========
Component quan ly va giam sat chat luong du lieu (Data Quality & Quarantine Panel):
- Truy van truc tiep tu ClickHouse table 'lakehouse.dq_quarantine'
- Fallback du lieu mau khi ClickHouse offline hoac chua co ban ghi cach ly
- Bieu do Donut / Pie Chart phan bo cac loai vi pham DQ Gate
- Bieu do Bar Chart so luong vi pham theo cap Coin
- Bang danh sach ban ghi bi cach ly kem bo loc da chieu (Error type, Symbol, Search)
- Hop thoai kiem tra chi tiet payload goc (Raw Record Inspection)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components.theme import CATEGORICAL, PLOTLY_TEMPLATE, section_header

# ── FALLBACK SYNTHETIC DATA GENERATOR ─────────────────────────────────────────
def _generate_fallback_quarantine_records() -> List[Dict[str, Any]]:
    """Sinh du lieu cach ly mau thuc te khi ClickHouse offline hoac chua co ban ghi."""
    now_utc = datetime.now(timezone.utc)
    sample_errors = [
        ("price_non_positive", -50000.0, 0.5, "BTCUSDT", "100234812"),
        ("quantity_non_positive", 65420.0, -0.05, "BTCUSDT", "100234815"),
        ("price_non_positive", -3200.0, 1.2, "ETHUSDT", "200145920"),
        ("missing required fields: price", None, 2.5, "ETHUSDT", "200145925"),
        ("trade_id_negative", 145.0, 10.0, "SOLUSDT", "-999120"),
        ("quantity_non_positive", 590.0, 0.0, "BNBUSDT", "300891230"),
        ("missing required fields: symbol", 0.62, 500.0, "", "400512390"),
        ("trade_time_non_positive", 64800.0, 0.1, "BTCUSDT", "100234900"),
        ("price_non_positive", -142.5, 5.0, "SOLUSDT", "500129840"),
        ("missing required fields: price", None, 1.0, "BNBUSDT", "300891280"),
    ]

    records = []
    for i, (err, price, qty, sym, tid) in enumerate(sample_errors * 3):
        t_quarantine = now_utc - timedelta(minutes=(30 - i))
        sym_val = sym if sym else "UNKNOWN"
        raw_payload = {
            "trade_id": tid,
            "symbol": sym_val,
            "price": price,
            "quantity": qty,
            "trade_time": int(t_quarantine.timestamp() * 1000),
            "is_injected": True,
            "fault_type": err,
        }
        records.append({
            "batch_run_id": f"batch_demo_{t_quarantine.strftime('%Y%m%d_%H00')}",
            "trade_id": str(tid),
            "symbol": sym_val,
            "price": str(price) if price is not None else "NULL",
            "quantity": str(qty),
            "trade_time": str(int(t_quarantine.timestamp() * 1000)),
            "dq_error": err,
            "raw_record": json.dumps(raw_payload),
            "quarantined_at": t_quarantine.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


# ── CLICKHOUSE QUERY HELPER ───────────────────────────────────────────────────
def fetch_quarantine_data_from_clickhouse(
    host: str = "localhost",
    port: int = 8123,
    database: str = "lakehouse",
    limit: int = 1000,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Truy van danh sach ban ghi bi cach ly tu ClickHouse table lakehouse.dq_quarantine.

    Returns:
        Tuple of (records_list, is_live_connected).
    """
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=host, port=port, database=database, connect_timeout=3
        )
        query = f"""
        SELECT
            batch_run_id,
            trade_id,
            symbol,
            price,
            quantity,
            trade_time,
            dq_error,
            raw_record,
            toString(quarantined_at) AS quarantined_at
        FROM {database}.dq_quarantine
        ORDER BY quarantined_at DESC
        LIMIT {limit}
        """
        res = client.query(query)
        cols = res.column_names
        rows = [dict(zip(cols, row)) for row in res.result_rows]

        if rows:
            return rows, True
        return _generate_fallback_quarantine_records(), False
    except Exception:
        # Fallback sang tap mau khi khong ket noi duoc
        return _generate_fallback_quarantine_records(), False


# ── MAIN COMPONENT RENDER ─────────────────────────────────────────────────────
def render_dq_quarantine_panel(
    clickhouse_host: str = "localhost",
    clickhouse_port: int = 8123,
    clickhouse_db: str = "lakehouse",
) -> None:
    """Render Tab Data Quality & Quarantine tren Streamlit Dashboard."""
    section_header(
        "Data quality gate & quarantine",
        "Bản ghi vi phạm data contract được tách vào <code>lakehouse.dq_quarantine</code> thay vì làm dừng pipeline, "
        "bảo vệ tính toàn vẹn của tầng Silver và Gold.",
        "shield",
    )

    records, is_live = fetch_quarantine_data_from_clickhouse(
        host=clickhouse_host, port=clickhouse_port, database=clickhouse_db
    )

    if not is_live:
        st.info("Đang hiển thị dữ liệu mẫu: ClickHouse không khả dụng hoặc chưa có bản ghi bị cách ly.")

    if not records:
        st.success("Không có bản ghi nào bị cách ly trong kỳ quan sát.")
        return

    df = pd.DataFrame(records)

    # ── 1. KPI SUMMARY CARDS ──────────────────────────────────────────────────
    total_quarantined = len(df)
    unique_errors = df["dq_error"].nunique()
    unique_symbols = df["symbol"].nunique()
    latest_time = df["quarantined_at"].iloc[0] if "quarantined_at" in df.columns and len(df) > 0 else "--"

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Bản ghi bị cách ly",
            value=f"{total_quarantined:,}",
            help="Số lượng bản ghi vi phạm Data Quality Gate đã bị chặn lại",
        )
    with col2:
        st.metric(
            label="Loại vi phạm",
            value=f"{unique_errors}",
            help="Phân loại các vi phạm schema, giá âm, khối lượng âm...",
        )
    with col3:
        st.metric(
            label="Cặp bị ảnh hưởng",
            value=f"{unique_symbols}",
            help="Các thị trường phát hiện dữ liệu lỗi",
        )
    with col4:
        st.metric(
            label="Cách ly gần nhất",
            value=str(latest_time)[:16],
            help="Thời điểm phát hiện bản ghi lỗi mới nhất",
        )

    st.markdown("---")

    # ── 2. CHARTS: PIE & BAR VISUALIZATION ─────────────────────────────────────
    chart_col1, chart_col2 = st.columns([1, 1])

    with chart_col1:
        section_header("Phân bố loại vi phạm")
        error_counts = df["dq_error"].value_counts().reset_index()
        error_counts.columns = ["dq_error", "count"]

        # Color palette cho cac loai loi
        colors = CATEGORICAL
        fig_pie = go.Figure(
            data=[
                go.Pie(
                    labels=error_counts["dq_error"],
                    values=error_counts["count"],
                    hole=0.45,
                    marker=dict(colors=colors[:len(error_counts)]),
                    textinfo="percent",
                    marker_line=dict(color="#0B0E11", width=2),
                )
            ]
        )
        fig_pie.update_layout(
            template=PLOTLY_TEMPLATE,
            margin=dict(l=10, r=10, t=10, b=10),
            height=320,
            showlegend=True,
            legend=dict(orientation="v", x=1.0, y=0.5),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with chart_col2:
        section_header("Số bản ghi lỗi theo cặp")
        sym_counts = df["symbol"].value_counts().reset_index()
        sym_counts.columns = ["symbol", "count"]

        fig_bar = px.bar(
            sym_counts,
            x="symbol",
            y="count",
            text="count",
            template=PLOTLY_TEMPLATE,
            labels={"symbol": "Cặp Coin", "count": "Số Bản Ghi"},
        )
        fig_bar.update_layout(
            margin=dict(l=10, r=10, t=10, b=10),
            height=320,
            showlegend=False,
        )
        fig_bar.update_traces(textposition="outside", marker_color=CATEGORICAL[0], marker_line_width=0)
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # ── 3. FILTER CONTROLS & RECORDS TABLE ────────────────────────────────────
    section_header("Bản ghi đang bị cách ly")

    f_col1, f_col2, f_col3 = st.columns([2, 2, 3])

    with f_col1:
        all_errors = ["Tất cả"] + sorted(list(df["dq_error"].unique()))
        selected_error = st.selectbox("Lọc theo loại lỗi:", all_errors)

    with f_col2:
        all_syms = ["Tất cả"] + sorted(list(df["symbol"].unique()))
        selected_sym = st.selectbox("Lọc theo cặp Coin:", all_syms)

    with f_col3:
        search_kw = st.text_input("Tìm kiếm Trade ID / Batch Run ID:", value="", placeholder="Nhập từ khóa...")

    # Áp dụng bộ lọc
    filtered_df = df.copy()
    if selected_error != "Tất cả":
        filtered_df = filtered_df[filtered_df["dq_error"] == selected_error]
    if selected_sym != "Tất cả":
        filtered_df = filtered_df[filtered_df["symbol"] == selected_sym]
    if search_kw:
        mask = (
            filtered_df["trade_id"].astype(str).str.contains(search_kw, case=False, na=False)
            | filtered_df["batch_run_id"].astype(str).str.contains(search_kw, case=False, na=False)
        )
        filtered_df = filtered_df[mask]

    # Hiển thị bảng
    display_cols = {
        "quarantined_at": "Thời Điểm Cách Ly (UTC)",
        "trade_id": "Trade ID",
        "symbol": "Cặp Coin",
        "price": "Giá (Price)",
        "quantity": "Khối Lượng (Qty)",
        "dq_error": "Mã Lỗi Vi Phạm",
        "batch_run_id": "Batch Run ID",
    }
    present_cols = [c for c in display_cols.keys() if c in filtered_df.columns]
    renamed_df = filtered_df[present_cols].rename(columns=display_cols)

    st.dataframe(
        renamed_df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(f"Đang hiển thị **{len(filtered_df)} / {len(df)}** bản ghi bị cách ly.")

    # ── 4. INSPECT RAW RECORD PAYLOAD & EXPORT ────────────────────────────────
    act_col1, act_col2 = st.columns([3, 1])

    with act_col1:
        with st.expander("Raw payload của bản ghi đầu tiên"):
            if not filtered_df.empty:
                first_row = filtered_df.iloc[0]
                raw_txt = first_row.get("raw_record", "{}")
                try:
                    parsed_json = json.loads(raw_txt) if isinstance(raw_txt, str) else raw_txt
                    st.json(parsed_json)
                except Exception:
                    st.text(raw_txt)
            else:
                st.write("Không có bản ghi phù hợp bộ lọc.")

    with act_col2:
        csv_data = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Xuất CSV",
            data=csv_data,
            file_name=f"quarantine_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
