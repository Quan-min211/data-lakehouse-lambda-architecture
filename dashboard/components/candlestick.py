"""
candlestick.py
==============
Vẽ biểu đồ nến tương tác (Interactive Plotly Candlestick Chart) chuẩn TradingView
cho hệ thống Data Lakehouse Lambda Architecture:
- Subplot 1: Nến OHLCV + Đường VWAP + Dấu hiệu Cảnh báo Sốc giá (Price Spike)
- Subplot 2: Khối lượng giao dịch (Volume Bars)
- Phân biệt trực quan trạng thái: Reconciled (Đã chốt từ Batch) vs Provisional (Tức thời từ Speed)
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Dict, Any


def render_candlestick_chart(candles: List[Dict[str, Any]], symbol: str = "BTCUSDT") -> go.Figure:
    """
    Tạo biểu đồ Plotly Candlestick chuyên nghiệp từ danh sách nến trả về bởi Serving API.
    """
    if not candles:
        fig = go.Figure()
        fig.add_annotation(
            text="Chưa có dữ liệu nến cho khoảng thời gian này.<br>Vui lòng chọn khoảng thời gian khác hoặc kiểm tra API.",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#9E9E9E")
        )
        fig.update_layout(
            template="plotly_dark",
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(showgrid=False, zeroline=False),
            height=500
        )
        return fig

    df = pd.DataFrame(candles)

    # Đảm bảo các cột số có kiểu dữ liệu đúng
    numeric_cols = ["open_price", "high_price", "low_price", "close_price", "volume", "vwap", "is_spike"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Tạo Subplots: Row 1 = Nến + VWAP (75% height), Row 2 = Volume (25% height)
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
    )

    # 1. Vẽ Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["window_start"],
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name="OHLCV",
            increasing=dict(line=dict(color="#00E676", width=1.5), fillcolor="#00E676"),
            decreasing=dict(line=dict(color="#FF5252", width=1.5), fillcolor="#FF5252"),
            hovertext=[
                f"Status: {s}<br>Spike: {'CÓ' if spk else 'Không'}"
                for s, spk in zip(df.get("status", [""] * len(df)), df.get("is_spike", [0] * len(df)))
            ]
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
                name="VWAP (Tỷ trọng khối lượng)",
                line=dict(color="#FFD600", width=2, dash="dot"),
                hoverinfo="x+y+name"
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
                    y=spike_df["high_price"] * 1.002,  # Đặt ngay trên đỉnh nến
                    mode="markers+text",
                    name="Cảnh báo Price Spike",
                    marker=dict(
                        symbol="triangle-down",
                        size=12,
                        color="#FF1744",
                        line=dict(width=1, color="#FFFFFF")
                    ),
                    text=["⚠️ SPIKE"] * len(spike_df),
                    textposition="top center",
                    textfont=dict(color="#FF5252", size=10),
                    hoverinfo="x+y+name"
                ),
                row=1, col=1
            )

    # 4. Vẽ Khối lượng giao dịch (Volume Bar Chart)
    volume_colors = [
        "#00E676" if c >= o else "#FF5252"
        for o, c in zip(df["open_price"], df["close_price"])
    ]
    fig.add_trace(
        go.Bar(
            x=df["window_start"],
            y=df["volume"],
            name="Khối lượng",
            marker=dict(color=volume_colors, opacity=0.8),
            hoverinfo="x+y+name"
        ),
        row=2, col=1
    )

    # Cấu hình giao diện chuẩn Dark Mode Crypto Trading
    fig.update_layout(
        template="plotly_dark",
        title=dict(
            text=f"<b>{symbol.upper()}</b> — Biểu đồ Nến Thời gian thực & VWAP",
            font=dict(size=20, color="#FFFFFF"),
            x=0.01, y=0.98
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=11)
        ),
        xaxis=dict(rangeslider=dict(visible=False), showgrid=True, gridcolor="#263238"),
        xaxis2=dict(showgrid=True, gridcolor="#263238", title="Thời gian (UTC)"),
        yaxis=dict(showgrid=True, gridcolor="#263238", title="Giá (USDT)", side="right"),
        yaxis2=dict(showgrid=True, gridcolor="#263238", title="Volume", side="right"),
        margin=dict(l=20, r=60, t=60, b=20),
        height=620,
        hovermode="x unified"
    )

    return fig
