"""
candlestick.py
==============
Vẽ biểu đồ nến tương tác (Interactive Plotly Candlestick Chart) chuẩn TradingView
cho hệ thống Data Lakehouse Lambda Architecture:
- Subplot 1: Nến OHLCV + Đường VWAP + Dấu hiệu Cảnh báo Sốc giá (Price Spike)
- Subplot 2: Khối lượng giao dịch (Volume Bars)
- Phân biệt trực quan trạng thái: Reconciled (Đã chốt từ Batch) vs Provisional (Tức thời từ Speed)
  bằng vùng tô nền sau mốc Watermark W và đường ranh giới W.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard.components.theme import COLORS, FONT_MONO, PLOTLY_TEMPLATE


def _to_utc_timestamp(value: Any) -> Optional[pd.Timestamp]:
    """Chuyển datetime / ISO string về pandas Timestamp UTC (hoặc None)."""
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def render_candlestick_chart(
    candles: List[Dict[str, Any]],
    symbol: str = "BTCUSDT",
    watermark: Optional[datetime] = None,
) -> go.Figure:
    """
    Tạo biểu đồ Plotly Candlestick từ danh sách nến trả về bởi Serving API.

    Args:
        candles: Danh sách nến OHLCV (window_start, open/high/low/close_price, volume, vwap, ...).
        symbol: Mã cặp giao dịch.
        watermark: Mốc System Watermark W; vùng sau W được tô nền là Provisional.

    Returns:
        go.Figure đã cấu hình theme dashboard.
    """
    if not candles:
        fig = go.Figure()
        fig.add_annotation(
            text="Chưa có dữ liệu nến cho khoảng thời gian này",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=13, color=COLORS["text_muted"])
        )
        fig.update_layout(
            template=PLOTLY_TEMPLATE,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            height=520
        )
        return fig

    df = pd.DataFrame(candles)

    # Đảm bảo các cột số có kiểu dữ liệu đúng
    numeric_cols = ["open_price", "high_price", "low_price", "close_price", "volume", "vwap", "is_spike"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")

    # Tạo Subplots: Row 1 = Nến + VWAP (78% height), Row 2 = Volume (22% height)
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.78, 0.22],
    )

    # 0. Vùng Provisional sau Watermark W + đường ranh giới
    wm_ts = _to_utc_timestamp(watermark)
    x_min, x_max = df["window_start"].min(), df["window_start"].max() + pd.Timedelta(minutes=1)
    if wm_ts is not None and x_min <= wm_ts <= x_max:
        fig.add_shape(
            type="rect", xref="x", yref="paper",
            x0=wm_ts, x1=x_max, y0=0, y1=1,
            fillcolor=COLORS["provisional"], opacity=0.05, line_width=0, layer="below",
        )
        fig.add_shape(
            type="line", xref="x", yref="paper",
            x0=wm_ts, x1=wm_ts, y0=0, y1=1,
            line=dict(color=COLORS["provisional"], width=1, dash="dot"),
        )
        fig.add_annotation(
            x=wm_ts, y=1, xref="x", yref="paper",
            text=f"W {wm_ts.strftime('%H:%M')}  ·  provisional →",
            showarrow=False, xanchor="left", yanchor="top", xshift=6, yshift=-4,
            font=dict(size=10, color=COLORS["provisional"], family=FONT_MONO),
        )

    statuses = df["status"].tolist() if "status" in df.columns else [""] * len(df)

    # 1. Vẽ Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["window_start"],
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name=symbol.upper(),
            increasing=dict(line=dict(color=COLORS["up"], width=1), fillcolor=COLORS["up"]),
            decreasing=dict(line=dict(color=COLORS["down"], width=1), fillcolor=COLORS["down"]),
            text=[f"Status: {s}" for s in statuses],
            whiskerwidth=0,
        ),
        row=1, col=1
    )

    # 2. Vẽ đường VWAP (Volume-Weighted Average Price)
    if "vwap" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["window_start"],
                y=df["vwap"],
                mode="lines",
                name="VWAP",
                line=dict(color=COLORS["vwap"], width=1.25),
                hovertemplate="%{y:,.4f}<extra>VWAP</extra>",
            ),
            row=1, col=1
        )

    # 3. Đánh dấu các nến có Sốc Giá (Price Spike)
    if "is_spike" in df.columns:
        spike_df = df[df["is_spike"] == 1]
        if not spike_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=spike_df["window_start"],
                    y=spike_df["high_price"] * 1.0015,  # Đặt ngay trên đỉnh nến
                    mode="markers",
                    name="Price spike",
                    marker=dict(
                        symbol="triangle-down",
                        size=9,
                        color=COLORS["danger"],
                        line=dict(width=0),
                    ),
                    hovertemplate="Spike @ %{x|%H:%M}<extra></extra>",
                ),
                row=1, col=1
            )

    # 4. Vẽ Khối lượng giao dịch (Volume Bar Chart)
    volume_colors = [
        COLORS["up"] if c >= o else COLORS["down"]
        for o, c in zip(df["open_price"], df["close_price"])
    ]
    fig.add_trace(
        go.Bar(
            x=df["window_start"],
            y=df["volume"],
            name="Volume",
            marker=dict(color=volume_colors, opacity=0.45, line_width=0),
            hovertemplate="%{y:,.4f}<extra>Volume</extra>",
            showlegend=False,
        ),
        row=2, col=1
    )

    # Cấu hình giao diện
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.0,
            xanchor="left", x=0,
            font=dict(size=11),
        ),
        xaxis=dict(rangeslider=dict(visible=False), showgrid=False),
        xaxis2=dict(showgrid=False, tickformat="%H:%M"),
        yaxis=dict(side="right", tickformat=",.2f", showgrid=True),
        yaxis2=dict(side="right", showgrid=False, tickformat="~s"),
        margin=dict(l=8, r=8, t=28, b=8),
        height=540,
        hovermode="x unified",
        bargap=0.15,
    )
    fig.update_xaxes(
        showspikes=True, spikemode="across", spikesnap="cursor",
        spikecolor=COLORS["border_strong"], spikethickness=1, spikedash="solid",
    )

    return fig
