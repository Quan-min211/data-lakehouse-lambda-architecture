"""
benchmark_charts.py
===================
Trực quan hóa kết quả 3 Benchmark cốt lõi của đề tài Data Lakehouse Lambda Architecture:
- Benchmark 1: Query Latency (P50, P95, P99) — So sánh Batch-only, Speed-only và Lambda
- Benchmark 2: Reconciliation Accuracy (MAPE, MAE, RMSE) — Sai số Speed Layer vs Batch Layer
- Benchmark 3: Compaction Efficiency — Tốc độ đọc trước và sau khi nén file Apache Iceberg
Hỗ trợ trực quan hóa đa cặp tiền mã hóa (Top coins: BTC, ETH, SOL, BNB, XRP...).
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from typing import Optional, List, Dict

from dashboard.components.theme import (
    CATEGORICAL,
    COLORS as THEME_COLORS,
    PLOTLY_TEMPLATE,
    render_html,
    section_header,
)


def _find_results_dir() -> str:
    """Tìm đường dẫn thư mục results dù chạy trên Windows host hay Docker container."""
    candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "results")),
        os.path.abspath("/app/results"),
        "c:/StudyZone/PODE/data-lakehouse-lambda-architecture/results",
        os.path.join(os.getcwd(), "results"),
    ]
    for c in candidates:
        if os.path.exists(c) and os.path.isdir(c):
            return c
    return candidates[0]


RESULTS_DIR = _find_results_dir()
LOGS_DIR = os.path.join(RESULTS_DIR, "logs")
PLOTS_DIR = os.path.join(RESULTS_DIR, "plots")

# Bảng mã màu thương hiệu crypto chuẩn quốc tế
CRYPTO_COLORS: Dict[str, str] = {
    "BTCUSDT": "#F7931A",  # Bitcoin Orange
    "ETHUSDT": "#627EEA",  # Ethereum Slate Blue
    "SOLUSDT": "#14F195",  # Solana Neon Green
    "BNBUSDT": "#F3BA2F",  # Binance Yellow
    "XRPUSDT": "#00AAE4",  # Ripple Cyan
    "DOGEUSDT": "#C2A633",
    "ADAUSDT": "#0033AD",
    "AVAXUSDT": "#E84142",
    "LINKUSDT": "#375BD2",
    "SUIUSDT": "#4DA2FF",
}


def _get_crypto_color(sym: str) -> str:
    """Màu cột theo symbol: dùng một màu trung tính để so sánh công bằng giữa các cặp."""
    return CATEGORICAL[0]


def _show_image(path: str, caption: Optional[str] = None):
    """Hiển thị ảnh tương thích cả Streamlit cũ (use_column_width) và mới (use_container_width)."""
    try:
        st.image(path, caption=caption, use_column_width=True)
    except TypeError:
        try:
            st.image(path, caption=caption, use_container_width=True)
        except Exception:
            st.image(path, caption=caption)


def _load_csv(filename: str) -> Optional[pd.DataFrame]:
    """Đọc file CSV benchmark từ LOGS_DIR hoặc fallback thư mục khác."""
    path = os.path.join(LOGS_DIR, filename)
    if not os.path.exists(path):
        path = os.path.join(RESULTS_DIR, "logs", filename)
    if os.path.exists(path) and os.path.getsize(path) > 50:
        try:
            df = pd.read_csv(path)
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception:
            return None
    return None


def _overview_cell(label: str, value: str, note: str, color: Optional[str] = None) -> str:
    """HTML cho một ô chỉ số trong lưới tổng quan."""
    style = f' style="color:{color}"' if color else ""
    return (
        f'<div class="lh-cell"><div class="lh-cell-label">{label}</div>'
        f'<div class="lh-cell-value"{style}>{value}</div>'
        f'<div class="lh-cell-note">{note}</div></div>'
    )


def _verdict(passed: Optional[bool]) -> str:
    """Nhãn đánh giá SLA dựa trên kết quả đo thực tế."""
    if passed is None:
        return "Chưa đủ dữ liệu"
    return "Đạt" if passed else "Không đạt"


def render_benchmark_overview():
    """Tổng quan KPI 3 Benchmark cốt lõi (mọi con số được tính trực tiếp từ results/logs)."""
    section_header(
        "Tổng quan 3 benchmark",
        "Các chỉ số dưới đây được tính trực tiếp từ file CSV trong <code>results/logs/</code>; "
        "ô trống nghĩa là benchmark tương ứng chưa được chạy.",
        "activity",
    )

    df_lat = _load_csv("bench_latency.csv")
    df_rec = _load_csv("bench_reprocess_summary.csv")
    df_comp = _load_csv("bench_compaction.csv")

    # BM1 — Query latency (strategy = lambda)
    p50 = p95 = p99 = None
    lat_syms = 0
    if df_lat is not None and len(df_lat) > 0 and "strategy" in df_lat.columns:
        lambda_rows = df_lat[df_lat["strategy"] == "lambda"]
        if len(lambda_rows) > 0:
            p50 = float(lambda_rows["latency_ms"].quantile(0.50))
            p95 = float(lambda_rows["latency_ms"].quantile(0.95))
            p99 = float(lambda_rows["latency_ms"].quantile(0.99))
            lat_syms = int(lambda_rows["symbol"].nunique()) if "symbol" in lambda_rows.columns else 0

    # BM2 — Reconciliation accuracy
    avg_mape = avg_cov = None
    rec_syms = 0
    if df_rec is not None and len(df_rec) > 0 and "matched_candles" in df_rec.columns:
        valid = df_rec[df_rec["matched_candles"] > 0]
        if len(valid) > 0:
            latest_valid = valid.sort_values("ts").groupby("symbol").last().reset_index()
            avg_mape = float(latest_valid["mape_pct"].mean())
            avg_cov = float(latest_valid["coverage_pct"].mean())
            rec_syms = len(latest_valid)

    # BM3 — Compaction
    avg_b = avg_a = speedup = None
    if df_comp is not None and len(df_comp) > 0 and "phase" in df_comp.columns:
        before = df_comp[df_comp["phase"] == "before"]["read_latency_ms"]
        after = df_comp[df_comp["phase"] == "after"]["read_latency_ms"]
        if len(before) > 0 and len(after) > 0:
            avg_b, avg_a = float(before.mean()), float(after.mean())
            speedup = avg_b / avg_a if avg_a > 0 else None

    def fmt(v: Optional[float], pattern: str) -> str:
        return pattern.format(v) if v is not None else "--"

    ok, warn = THEME_COLORS["reconciled"], THEME_COLORS["provisional"]
    bm1_pass = (p95 < 500.0) if p95 is not None else None
    bm2_pass = (avg_mape < 1.0) if avg_mape is not None else None
    bm3_pass = (speedup >= 2.0) if speedup is not None else None

    def tone(passed: Optional[bool]) -> Optional[str]:
        return None if passed is None else (ok if passed else warn)

    render_html(
        '<div class="lh-grid" style="grid-template-columns: repeat(3, 1fr);">'
        + _overview_cell("BM1 · Lambda P95 latency", fmt(p95, "{:,.1f} ms"),
                         f"P50 {fmt(p50, '{:,.1f}')} · P99 {fmt(p99, '{:,.1f}')} ms · {lat_syms} cặp", tone(bm1_pass))
        + _overview_cell("BM2 · VWAP MAPE", fmt(avg_mape, "{:.4f} %"),
                         f"Coverage {fmt(avg_cov, '{:.0f}')}% · {rec_syms} cặp", tone(bm2_pass))
        + _overview_cell("BM3 · Compaction speedup", fmt(speedup, "{:.1f}x"),
                         f"{fmt(avg_b, '{:,.0f}')} → {fmt(avg_a, '{:,.0f}')} ms đọc", tone(bm3_pass))
        + "</div>"
    )

    section_header("Đối chiếu chỉ tiêu", "Đánh giá tự động so với ngưỡng SLA của đề tài.")
    summary_table = pd.DataFrame([
        {
            "Benchmark": "BM1 · Query latency (Lambda P95)",
            "Ngưỡng": "< 500 ms",
            "Kết quả đo": fmt(p95, "{:,.1f} ms"),
            "Đánh giá": _verdict(bm1_pass),
        },
        {
            "Benchmark": "BM2 · Sai số đối soát VWAP (MAPE)",
            "Ngưỡng": "< 1.00 %",
            "Kết quả đo": fmt(avg_mape, "{:.4f} %"),
            "Đánh giá": _verdict(bm2_pass),
        },
        {
            "Benchmark": "BM3 · Tăng tốc đọc sau compaction",
            "Ngưỡng": ">= 2.0x",
            "Kết quả đo": fmt(speedup, "{:.1f}x"),
            "Đánh giá": _verdict(bm3_pass),
        },
    ])
    st.dataframe(summary_table, use_container_width=True, hide_index=True)


def render_benchmark1_latency():
    """Chi tiết Benchmark 1: Query Latency Distribution."""
    section_header(
        "Benchmark 1 · Query latency",
        "Độ trễ truy vấn qua FastAPI Serving Layer với 3 chiến lược: <code>batch_only</code>, "
        "<code>speed_only</code> và <code>lambda</code> (ghép tự động theo Watermark).",
        "clock",
    )

    df = _load_csv("bench_latency.csv")
    if df is None or len(df) == 0:
        img_path = os.path.join(PLOTS_DIR, "benchmark1_latency.png")
        if os.path.exists(img_path):
            _show_image(img_path, caption="Benchmark 1: Query Latency Distribution (Plot có sẵn)")
        else:
            st.info("Chưa có dữ liệu Benchmark 1.")
        return

    strategy_colors = {
        "batch_only": THEME_COLORS["text_muted"],
        "speed_only": THEME_COLORS["provisional"],
        "lambda": THEME_COLORS["accent"],
    }
    strategy_labels = {
        "batch_only": "Batch-only (Cold Storage)",
        "speed_only": "Speed-only (Realtime Cache)",
        "lambda": "Lambda (Auto-Correcting Hybrid)",
    }

    # Bộ lọc Symbol
    available_symbols = ["Tất cả (5 Cặp Coin)"] + sorted(df["symbol"].dropna().unique().tolist())
    selected_sym = st.selectbox("Lọc theo Cặp Coin:", available_symbols, index=0)
    filtered_df = df if "Tất cả" in selected_sym else df[df["symbol"] == selected_sym]

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(
            f"Phân Bố Latency theo Chiến Lược ({len(filtered_df):,} mẫu)",
            "Các Mức Phân Vị (P50, P95, P99) Latency (ms)",
        ),
        column_widths=[0.55, 0.45],
    )

    for strat in ["batch_only", "speed_only", "lambda"]:
        sdf = filtered_df[filtered_df["strategy"] == strat]
        if len(sdf) > 0:
            fig.add_trace(
                go.Box(
                    y=sdf["latency_ms"],
                    name=strategy_labels.get(strat, strat),
                    marker_color=strategy_colors.get(strat, "#888"),
                    boxmean=True,
                ),
                row=1, col=1,
            )

    percentiles = []
    for strat in ["batch_only", "speed_only", "lambda"]:
        sdf = filtered_df[filtered_df["strategy"] == strat]
        if len(sdf) > 0:
            p50 = float(sdf["latency_ms"].quantile(0.50))
            p95 = float(sdf["latency_ms"].quantile(0.95))
            p99 = float(sdf["latency_ms"].quantile(0.99))
            avg = float(sdf["latency_ms"].mean())
            percentiles.append({
                "Chiến Lược": strategy_labels.get(strat, strat),
                "Số Mẫu": len(sdf),
                "P50 (ms)": round(p50, 2),
                "P95 (ms)": round(p95, 2),
                "P99 (ms)": round(p99, 2),
                "Trung Bình (ms)": round(avg, 2),
            })

    if percentiles:
        pdf = pd.DataFrame(percentiles)
        for metric, color in [("P50 (ms)", THEME_COLORS["accent"]), ("P95 (ms)", THEME_COLORS["provisional"]), ("P99 (ms)", THEME_COLORS["down"])]:
            fig.add_trace(
                go.Bar(
                    x=pdf["Chiến Lược"],
                    y=pdf[metric],
                    name=metric,
                    marker_color=color,
                    text=pdf[metric].apply(lambda v: f"{v:.1f}ms"),
                    textposition="outside",
                ),
                row=1, col=2,
            )

    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        height=480,
        showlegend=True,
        legend=dict(orientation="h", y=-0.15),
        yaxis_title="Latency (ms)",
        yaxis2_title="Latency (ms)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Biểu đồ so sánh P95 theo từng Symbol
    if "Tất cả" in selected_sym and df["symbol"].nunique() > 1:
        st.markdown("---")
        section_header("P95 latency của chiến lược Lambda theo cặp")
        lambda_df = df[df["strategy"] == "lambda"]
        p95_by_sym = lambda_df.groupby("symbol")["latency_ms"].quantile(0.95).reset_index()
        p95_by_sym.columns = ["symbol", "p95_latency"]
        
        fig_sym = go.Figure()
        fig_sym.add_trace(go.Bar(
            x=p95_by_sym["symbol"],
            y=p95_by_sym["p95_latency"],
            marker_color=[_get_crypto_color(s) for s in p95_by_sym["symbol"]],
            text=p95_by_sym["p95_latency"].apply(lambda v: f"{v:.1f} ms"),
            textposition="outside",
        ))
        fig_sym.add_hline(y=500.0, line_dash="dash", line_color="#EF5350",
                          annotation_text="Ngưỡng SLA Max: 500 ms", annotation_position="top left")
        fig_sym.update_layout(
            template=PLOTLY_TEMPLATE,
            height=360,
            yaxis_title="P95 Latency (ms)",
            title="P95 Latency (Lambda Hybrid) của các Cặp Coin",
        )
        st.plotly_chart(fig_sym, use_container_width=True)

    if percentiles:
        section_header("Bảng phân vị độ trễ")
        st.dataframe(pd.DataFrame(percentiles), use_container_width=True, hide_index=True)


def render_benchmark2_reconciliation():
    """Chi tiết Benchmark 2: Reconciliation Accuracy."""
    section_header(
        "Benchmark 2 · Reconciliation accuracy",
        "Sai số VWAP của Speed Layer (tính online) so với Batch Layer (chuẩn hóa đầy đủ sau khi chốt Watermark).",
        "scale",
    )

    df_sum = _load_csv("bench_reprocess_summary.csv")
    df_det = _load_csv("bench_reprocess.csv")

    if df_sum is None or len(df_sum) == 0:
        img_path = os.path.join(PLOTS_DIR, "benchmark2_reconciliation.png")
        if os.path.exists(img_path):
            _show_image(img_path, caption="Benchmark 2: Reconciliation Accuracy (Plot có sẵn)")
        else:
            st.info("Chưa có dữ liệu Benchmark 2.")
        return

    valid = df_sum[df_sum["matched_candles"] > 0].copy()
    if len(valid) == 0:
        st.warning("Tất cả các lần chạy trong file summary đều ghi nhận matched_candles = 0.")
        img_path = os.path.join(PLOTS_DIR, "benchmark2_reconciliation.png")
        if os.path.exists(img_path):
            _show_image(img_path, caption="Benchmark 2: Reconciliation Accuracy (Plot có sẵn)")
        return

    latest_valid = valid.sort_values("ts").groupby("symbol").last().reset_index()

    col_l, col_r = st.columns(2)
    with col_l:
        fig_mape = go.Figure()
        fig_mape.add_trace(go.Bar(
            x=latest_valid["symbol"],
            y=latest_valid["mape_pct"],
            text=latest_valid["mape_pct"].apply(lambda v: f"{v:.4f}%"),
            textposition="outside",
            marker_color=[_get_crypto_color(s) for s in latest_valid["symbol"]],
        ))
        fig_mape.add_hline(y=1.0, line_dash="dash", line_color="#EF5350",
                           annotation_text="Ngưỡng SLA Max: 1.0%", annotation_position="top left")
        fig_mape.update_layout(
            template=PLOTLY_TEMPLATE,
            title="Sai Số Phần Trăm Tuyệt Đối Trung Bình (MAPE %)",
            yaxis_title="MAPE (%)",
            height=380,
        )
        st.plotly_chart(fig_mape, use_container_width=True)

    with col_r:
        fig_err = go.Figure()
        fig_err.add_trace(go.Bar(
            name="MAE (Sai số tuyệt đối)",
            x=latest_valid["symbol"],
            y=latest_valid["mae"],
            text=latest_valid["mae"].apply(lambda v: f"${v:.4f}" if v < 1 else f"${v:.2f}"),
            textposition="outside",
            marker_color=THEME_COLORS["accent"],
        ))
        fig_err.add_trace(go.Bar(
            name="RMSE (Căn bậc hai sai số)",
            x=latest_valid["symbol"],
            y=latest_valid["rmse"],
            text=latest_valid["rmse"].apply(lambda v: f"${v:.4f}" if v < 1 else f"${v:.2f}"),
            textposition="outside",
            marker_color=THEME_COLORS["text_muted"],
        ))
        fig_err.update_layout(
            template=PLOTLY_TEMPLATE,
            title="Sai Số Giá Thực Tế: MAE & RMSE (USDT)",
            yaxis_title="USDT (log scale)",
            yaxis_type="log",
            barmode="group",
            height=380,
        )
        st.plotly_chart(fig_err, use_container_width=True)

    if df_det is not None and len(df_det) > 0 and "vwap_batch" in df_det.columns and "vwap_speed" in df_det.columns:
        valid_candles = df_det.dropna(subset=["vwap_batch", "vwap_speed"]).copy()
        if len(valid_candles) > 0:
            st.markdown("---")
            section_header("Speed VWAP và Batch VWAP theo từng nến")
            sym_list = sorted(valid_candles["symbol"].unique().tolist())
            picked_sym = st.selectbox("Chọn Cặp Coin Đối Soát Chi Tiết:", sym_list, index=0)
            sym_data = valid_candles[valid_candles["symbol"] == picked_sym].copy()

            if len(sym_data) > 0:
                fig_compare = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.08,
                    subplot_titles=(f"Đường Giá VWAP: Batch vs Speed ({picked_sym})", "Sai Số Phần Trăm (% Error)"),
                    row_heights=[0.7, 0.3],
                )
                fig_compare.add_trace(
                    go.Scatter(
                        x=sym_data["window_start"],
                        y=sym_data["vwap_batch"],
                        name="Batch Ground Truth",
                        line=dict(color=THEME_COLORS["reconciled"], width=1.5),
                        mode="lines+markers",
                    ),
                    row=1, col=1,
                )
                fig_compare.add_trace(
                    go.Scatter(
                        x=sym_data["window_start"],
                        y=sym_data["vwap_speed"],
                        name="Speed Provisional",
                        line=dict(color=THEME_COLORS["provisional"], width=1.25, dash="dot"),
                        mode="lines+markers",
                    ),
                    row=1, col=1,
                )
                fig_compare.add_trace(
                    go.Bar(
                        x=sym_data["window_start"],
                        y=sym_data["pct_error"],
                        name="|% Sai Số|",
                        marker_color="#EF5350",
                    ),
                    row=2, col=1,
                )
                fig_compare.update_layout(
                    template=PLOTLY_TEMPLATE,
                    height=520,
                    showlegend=True,
                    legend=dict(orientation="h", y=-0.12),
                )
                st.plotly_chart(fig_compare, use_container_width=True)

    section_header("Kết quả đối soát theo cặp")
    st.dataframe(
        latest_valid[["symbol", "matched_candles", "coverage_pct", "mae", "mape_pct", "rmse", "ts"]].rename(columns={
            "symbol": "Cặp Tiền",
            "matched_candles": "Số Nến Đối Soát",
            "coverage_pct": "Độ Phủ (Coverage %)",
            "mae": "MAE (USDT)",
            "mape_pct": "MAPE (%)",
            "rmse": "RMSE (USDT)",
            "ts": "Thời Điểm Đo Kiểm",
        }),
        use_container_width=True,
        hide_index=True,
    )


def render_benchmark3_compaction():
    """Chi tiết Benchmark 3: Iceberg Compaction Efficiency."""
    section_header(
        "Benchmark 3 · Iceberg compaction",
        "Suy giảm hiệu năng đọc do small files khi streaming ghi Parquet liên tục, và mức cải thiện sau "
        "<code>rewrite_data_files</code>.",
        "database",
    )

    df = _load_csv("bench_compaction.csv")
    if df is None or len(df) == 0:
        img_path = os.path.join(PLOTS_DIR, "benchmark3_compaction.png")
        if os.path.exists(img_path):
            _show_image(img_path, caption="Benchmark 3: Compaction Efficiency (Plot có sẵn)")
        else:
            st.info("Chưa có dữ liệu Benchmark 3.")
        return

    sym_col = "symbol_filter" if "symbol_filter" in df.columns else "symbol"
    symbols_available = ["Tất cả (5 Cặp Coin)"] + sorted(df[sym_col].dropna().unique().tolist())
    picked_sym = st.selectbox("Lọc Compaction theo Cặp Coin:", symbols_available, index=0)

    filtered_df = df if "Tất cả" in picked_sym else df[df[sym_col] == picked_sym]

    before_df = filtered_df[filtered_df["phase"] == "before"]
    after_df = filtered_df[filtered_df["phase"] == "after"]

    col1, col2, col3, col4 = st.columns(4)
    avg_b = float(before_df["read_latency_ms"].mean()) if len(before_df) > 0 else 0
    avg_a = float(after_df["read_latency_ms"].mean()) if len(after_df) > 0 else 0
    speedup = avg_b / avg_a if avg_a > 0 else 0
    reduction = ((avg_b - avg_a) / avg_b * 100) if avg_b > 0 else 0

    files_b = int(before_df["file_count"].iloc[0]) if len(before_df) > 0 and "file_count" in before_df.columns else 0
    files_a = int(after_df["file_count"].iloc[0]) if len(after_df) > 0 and "file_count" in after_df.columns else 0

    col1.metric("Độ Trễ Trước Compaction", f"{avg_b:.0f} ms", f"{files_b} files nhỏ")
    col2.metric("Độ Trễ Sau Compaction", f"{avg_a:.0f} ms", f"{files_a} files gom")
    col3.metric("Tốc Độ Cải Thiện", f"{speedup:.1f}x")
    col4.metric("Giảm Độ Trễ", f"-{reduction:.1f}%")

    if "Tất cả" in picked_sym and df[sym_col].nunique() > 1:
        sym_perf = df.groupby([sym_col, "phase"])["read_latency_ms"].mean().unstack()
        if "before" in sym_perf.columns and "after" in sym_perf.columns:
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                name="Trước Compaction (Nhiều file nhỏ)",
                x=sym_perf.index,
                y=sym_perf["before"],
                marker_color="#EF5350",
                text=sym_perf["before"].apply(lambda v: f"{v:.0f}ms"),
                textposition="outside",
            ))
            fig_bar.add_trace(go.Bar(
                name="Sau Compaction (File tối ưu)",
                x=sym_perf.index,
                y=sym_perf["after"],
                marker_color="#26A69A",
                text=sym_perf["after"].apply(lambda v: f"{v:.0f}ms"),
                textposition="outside",
            ))
            fig_bar.update_layout(
                template=PLOTLY_TEMPLATE,
                title="So Sánh Tốc Độ Đọc Trước vs Sau Compaction Theo Cặp Coin",
                yaxis_title="Read Latency (ms)",
                barmode="group",
                height=420,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    fig = go.Figure()
    if len(before_df) > 0:
        fig.add_trace(go.Box(
            y=before_df["read_latency_ms"],
            name=f"Trước Compaction ({files_b} files)",
            marker_color="#EF5350",
            boxmean=True,
        ))
    if len(after_df) > 0:
        fig.add_trace(go.Box(
            y=after_df["read_latency_ms"],
            name=f"Sau Compaction ({files_a} files)",
            marker_color="#26A69A",
            boxmean=True,
        ))

    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        title=f"Phân Bố Read Latency ({picked_sym}): Trước vs Sau Compaction (ms)",
        yaxis_title="Read Latency (ms)",
        height=450,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_benchmark_images():
    """Hiển thị các biểu đồ PNG gốc do script sinh ra."""
    section_header(
        "Biểu đồ gốc",
        "Ảnh PNG sinh từ <code>scripts/benchmarks/</code> dùng cho báo cáo và slide.",
        "layers",
    )

    img_files = [
        ("benchmark_summary_dashboard.png", "Tổng Hợp Cả 3 Benchmark (Summary Dashboard)"),
        ("benchmark1_latency.png", "Benchmark 1: Query Latency (P50, P95, P99)"),
        ("benchmark2_reconciliation.png", "Benchmark 2: Reconciliation Accuracy (MAPE, MAE, RMSE)"),
        ("benchmark3_compaction.png", "Benchmark 3: Apache Iceberg Compaction Efficiency"),
    ]

    for fname, caption in img_files:
        path = os.path.join(PLOTS_DIR, fname)
        if os.path.exists(path):
            _show_image(path, caption=caption)
            st.markdown("---")
