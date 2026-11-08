"""
theme.py
========
Design system dùng chung cho Streamlit Dashboard (Lambda Lakehouse Market Monitor).

Nguyên tắc thiết kế (tham khảo ui-ux-pro-max: Fintech / Data-Dense Dashboard):
  - Dark surface trung tính (không gradient tím/hồng), phân lớp bằng border mảnh.
  - Màu chỉ mang ngữ nghĩa: tăng/giảm giá, trạng thái Reconciled/Provisional/Hybrid.
  - Inter cho chữ giao diện, JetBrains Mono + tabular-nums cho mọi con số.
  - Không dùng emoji làm icon; dùng SVG stroke icon (Lucide) đồng nhất.
"""

from __future__ import annotations

import math
from textwrap import dedent
from typing import Optional

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

# ── DESIGN TOKENS ─────────────────────────────────────────────────────────────
COLORS = {
    "bg": "#0B0E11",
    "surface": "#12161C",
    "surface_2": "#181D25",
    "border": "#232A34",
    "border_strong": "#2F3844",
    "text": "#E6E8EB",
    "text_muted": "#8B949E",
    "text_subtle": "#6B7480",
    "accent": "#4C8DF6",
    "up": "#26A69A",
    "down": "#EF5350",
    "reconciled": "#3FB68B",
    "provisional": "#E3A008",
    "hybrid": "#4C8DF6",
    "vwap": "#C9A227",
    "danger": "#EF5350",
}

FONT_SANS = "Inter, -apple-system, 'Segoe UI', Roboto, sans-serif"
FONT_MONO = "'JetBrains Mono', 'SF Mono', Consolas, monospace"

PLOTLY_TEMPLATE = "lakehouse"

# Thứ tự màu cho chuỗi dữ liệu phân loại (trầm, phân biệt tốt trên nền tối)
CATEGORICAL = ["#4C8DF6", "#3FB68B", "#E3A008", "#EF5350", "#8B949E", "#5FB3C8", "#B07CC6"]


# ── SVG ICONS (Lucide, stroke 1.75) ───────────────────────────────────────────
_ICON_PATHS = {
    "activity": '<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
    "database": '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/>',
    "zap": '<path d="M13 2 3 14h9l-1 8 10-12h-9l1-8z"/>',
    "shield": '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
    "server": '<rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><path d="M6 6h.01M6 18h.01"/>',
    "clock": '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
    "merge": '<circle cx="18" cy="18" r="3"/><circle cx="6" cy="6" r="3"/><path d="M6 21V9a9 9 0 0 0 9 9"/>',
    "layers": '<path d="m12 2 10 5-10 5L2 7l10-5z"/><path d="m2 17 10 5 10-5"/><path d="m2 12 10 5 10-5"/>',
    "alert": '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3z"/><path d="M12 9v4M12 17h.01"/>',
    "check": '<path d="M20 6 9 17l-5-5"/>',
    "scale": '<path d="m16 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="m2 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1z"/><path d="M7 21h10M12 3v18M3 7h2c2 0 5-1 7-2 2 1 5 2 7 2h2"/>',
}


def icon(name: str, size: int = 16, color: Optional[str] = None) -> str:
    """Trả về SVG icon inline.

    Args:
        name: Tên icon trong bộ Lucide rút gọn (_ICON_PATHS).
        size: Kích thước icon (px).
        color: Màu stroke; mặc định kế thừa currentColor.

    Returns:
        Chuỗi SVG.
    """
    paths = _ICON_PATHS.get(name, "")
    stroke = color or "currentColor"
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 24 24" '
        f'fill="none" stroke="{stroke}" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" '
        f'style="flex-shrink:0;vertical-align:-0.15em">{paths}</svg>'
    )


# ── GLOBAL CSS ────────────────────────────────────────────────────────────────
GLOBAL_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');
:root {{
--bg:{COLORS['bg']};--surface:{COLORS['surface']};--surface-2:{COLORS['surface_2']};
--border:{COLORS['border']};--border-strong:{COLORS['border_strong']};
--text:{COLORS['text']};--muted:{COLORS['text_muted']};--subtle:{COLORS['text_subtle']};
--accent:{COLORS['accent']};--up:{COLORS['up']};--down:{COLORS['down']};
--sans:{FONT_SANS};--mono:{FONT_MONO};
}}
html, body, [class*="css"], .stApp, .stMarkdown, p, label, input, textarea, select, button {{ font-family: var(--sans); }}
.stApp {{ background: var(--bg); color: var(--text); }}
[data-testid="stHeader"] {{ background: transparent; height: 0; }}
[data-testid="stToolbar"] {{ right: 1rem; }}
.block-container {{ padding-top: 1.25rem; padding-bottom: 2rem; max-width: 1560px; }}
h1, h2, h3, h4, h5 {{ font-family: var(--sans); letter-spacing: -0.01em; color: var(--text); }}
code {{ font-family: var(--mono); font-size: 0.85em; color: #C9D1D9; background: var(--surface-2); border-radius: 4px; padding: 1px 5px; }}
hr {{ border-color: var(--border) !important; margin: 1rem 0 !important; }}

/* Sidebar */
[data-testid="stSidebar"] {{ background: var(--surface); border-right: 1px solid var(--border); }}
[data-testid="stSidebar"] .block-container {{ padding-top: 1rem; }}
[data-testid="stSidebar"] label p {{ font-size: 12px; color: var(--muted); font-weight: 500; }}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {{ gap: 0; border-bottom: 1px solid var(--border); }}
.stTabs [data-baseweb="tab"] {{ height: 40px; padding: 0 16px; background: transparent; color: var(--muted); font-size: 13px; font-weight: 500; }}
.stTabs [data-baseweb="tab"]:hover {{ color: var(--text); }}
.stTabs [aria-selected="true"] {{ color: var(--text) !important; }}
.stTabs [data-baseweb="tab-highlight"] {{ background-color: var(--accent); height: 2px; }}
.stTabs [data-baseweb="tab-border"] {{ display: none; }}

/* Native metrics */
[data-testid="stMetric"] {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }}
[data-testid="stMetricLabel"] p {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); font-weight: 600; }}
[data-testid="stMetricValue"] {{ font-family: var(--mono); font-variant-numeric: tabular-nums; font-size: 22px; font-weight: 500; }}

/* Tables, inputs, buttons */
[data-testid="stDataFrame"] {{ border: 1px solid var(--border); border-radius: 8px; }}
.stButton > button, .stDownloadButton > button {{ background: var(--surface-2); border: 1px solid var(--border-strong); color: var(--text); border-radius: 6px; font-size: 13px; font-weight: 500; transition: border-color 150ms ease, background 150ms ease; }}
.stButton > button:hover, .stDownloadButton > button:hover {{ border-color: var(--accent); color: var(--text); background: var(--surface-2); }}
[data-testid="stExpander"] {{ border: 1px solid var(--border); border-radius: 8px; background: var(--surface); }}
[data-testid="stAlert"] {{ border-radius: 8px; }}

/* ── Custom components ── */
.lh-num {{ font-family: var(--mono); font-variant-numeric: tabular-nums; }}
.lh-up {{ color: var(--up); }} .lh-down {{ color: var(--down); }} .lh-muted {{ color: var(--muted); }}
.lh-topbar {{ display:flex; align-items:center; justify-content:space-between; gap:16px; padding: 0 0 14px 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; }}
.lh-brand {{ display:flex; align-items:center; gap:10px; }}
.lh-brand-mark {{ width:28px; height:28px; border-radius:6px; background: var(--surface-2); border:1px solid var(--border-strong); display:flex; align-items:center; justify-content:center; color: var(--accent); }}
.lh-brand-title {{ font-size:14px; font-weight:600; color: var(--text); line-height:1.1; }}
.lh-brand-sub {{ font-size:11px; color: var(--muted); }}
.lh-topbar-right {{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
.lh-chip {{ display:inline-flex; align-items:center; gap:6px; height:24px; padding:0 9px; border-radius:4px; border:1px solid var(--border-strong); background: var(--surface); font-size:11px; font-weight:600; letter-spacing:0.04em; text-transform:uppercase; color: var(--muted); white-space:nowrap; }}
.lh-dot {{ width:6px; height:6px; border-radius:50%; display:inline-block; }}

.lh-ticker {{ display:flex; align-items:flex-end; gap:32px; flex-wrap:wrap; margin-bottom: 16px; }}
.lh-ticker-main {{ display:flex; flex-direction:column; gap:2px; }}
.lh-symbol {{ font-size:13px; color: var(--muted); font-weight:600; letter-spacing:0.02em; }}
.lh-symbol b {{ color: var(--text); font-size:15px; }}
.lh-price-row {{ display:flex; align-items:baseline; gap:12px; }}
.lh-price {{ font-family: var(--mono); font-variant-numeric: tabular-nums; font-size:34px; font-weight:500; line-height:1.1; color: var(--text); }}
.lh-change {{ font-family: var(--mono); font-variant-numeric: tabular-nums; font-size:13px; font-weight:500; padding:2px 8px; border-radius:4px; }}
.lh-stats {{ display:flex; gap:28px; flex-wrap:wrap; padding-bottom:4px; }}
.lh-stat {{ display:flex; flex-direction:column; gap:3px; }}
.lh-stat-label {{ font-size:11px; color: var(--muted); text-transform:uppercase; letter-spacing:0.06em; font-weight:500; }}
.lh-stat-value {{ font-family: var(--mono); font-variant-numeric: tabular-nums; font-size:14px; color: var(--text); }}

.lh-grid {{ display:grid; gap:1px; background: var(--border); border:1px solid var(--border); border-radius:8px; overflow:hidden; margin-bottom: 14px; }}
.lh-cell {{ background: var(--surface); padding: 12px 16px; display:flex; flex-direction:column; gap:6px; min-width:0; }}
.lh-cell-label {{ display:flex; align-items:center; gap:6px; font-size:11px; color: var(--muted); text-transform:uppercase; letter-spacing:0.06em; font-weight:600; }}
.lh-cell-value {{ font-family: var(--mono); font-variant-numeric: tabular-nums; font-size:16px; color: var(--text); font-weight:500; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.lh-cell-note {{ font-size:12px; color: var(--muted); }}
.lh-split {{ display:flex; height:4px; border-radius:2px; overflow:hidden; background: var(--surface-2); margin-top:2px; }}

.lh-badge {{ display:inline-flex; align-items:center; gap:6px; height:22px; padding:0 8px; border-radius:4px; font-size:12px; font-weight:600; white-space:nowrap; }}

.lh-section {{ display:flex; align-items:flex-start; justify-content:space-between; gap:12px; margin: 6px 0 12px 0; }}
.lh-section-title {{ display:flex; align-items:center; gap:8px; font-size:15px; font-weight:600; color: var(--text); }}
.lh-section-title svg {{ color: var(--muted); }}
.lh-section-sub {{ font-size:12.5px; color: var(--muted); margin-top:3px; max-width: 900px; line-height:1.5; }}

.lh-side-label {{ font-size:10.5px; font-weight:600; letter-spacing:0.08em; text-transform:uppercase; color: var(--subtle); margin: 14px 0 4px 0; }}
.lh-footnote {{ font-size:11.5px; color: var(--subtle); border-top:1px solid var(--border); padding-top:10px; margin-top:24px; }}

.lh-case {{ background: var(--surface); border:1px solid var(--border); border-radius:8px; padding:14px 16px; height:100%; }}
.lh-case-head {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:8px; }}
.lh-case-title {{ font-size:13px; font-weight:600; color: var(--text); }}
.lh-case-body {{ font-size:12.5px; color: var(--muted); line-height:1.6; }}
</style>
"""


def html_block(markup: str) -> str:
    """Chuẩn hóa chuỗi HTML để Streamlit markdown không biến dòng thụt lề thành code block.

    Args:
        markup: Chuỗi HTML nhiều dòng.

    Returns:
        Chuỗi HTML đã loại bỏ thụt lề và dòng trống.
    """
    lines = [line.strip() for line in dedent(markup).splitlines()]
    return "".join(line for line in lines if line)


def render_html(markup: str) -> None:
    """Render HTML an toàn qua st.markdown."""
    st.markdown(html_block(markup), unsafe_allow_html=True)


def inject_global_css() -> None:
    """Chèn CSS toàn cục và đăng ký Plotly template của dashboard."""
    register_plotly_template()
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


# ── FORMAT HELPERS ────────────────────────────────────────────────────────────
def fmt_price(value: Optional[float]) -> str:
    """Định dạng giá với số chữ số thập phân thích ứng theo độ lớn.

    Args:
        value: Giá trị giá (USDT).

    Returns:
        Chuỗi giá đã định dạng, hoặc '--' nếu không hợp lệ.
    """
    try:
        v = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "--"
    if math.isnan(v) or v <= 0:
        return "--"
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:,.6f}"


def fmt_compact(value: Optional[float]) -> str:
    """Định dạng số lớn dạng rút gọn (K, M, B)."""
    try:
        v = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "--"
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(v) >= div:
            return f"{v / div:,.2f}{unit}"
    return f"{v:,.2f}"


# ── SMALL UI PRIMITIVES ───────────────────────────────────────────────────────
def chip(label: str, color: Optional[str] = None) -> str:
    """Chip trạng thái nhỏ với chấm màu."""
    dot = f'<span class="lh-dot" style="background:{color}"></span>' if color else ""
    return f'<span class="lh-chip">{dot}{label}</span>'


def section_header(title: str, subtitle: Optional[str] = None, icon_name: Optional[str] = None) -> None:
    """Render tiêu đề section đồng nhất (thay cho st.subheader + emoji).

    Args:
        title: Tiêu đề chính.
        subtitle: Mô tả ngắn bên dưới.
        icon_name: Tên icon SVG (tùy chọn).
    """
    ico = icon(icon_name, 16) if icon_name else ""
    sub = f'<div class="lh-section-sub">{subtitle}</div>' if subtitle else ""
    render_html(
        f"""
        <div class="lh-section"><div>
        <div class="lh-section-title">{ico}<span>{title}</span></div>
        {sub}
        </div></div>
        """
    )


def sidebar_label(text: str) -> None:
    """Nhãn nhóm nhỏ trong sidebar."""
    st.sidebar.markdown(f'<div class="lh-side-label">{text}</div>', unsafe_allow_html=True)


# ── PLOTLY TEMPLATE ───────────────────────────────────────────────────────────
def register_plotly_template() -> None:
    """Đăng ký Plotly template 'lakehouse' đồng bộ với design tokens."""
    if PLOTLY_TEMPLATE in pio.templates:
        return
    axis = dict(
        gridcolor="#1A2028",
        linecolor=COLORS["border"],
        zerolinecolor=COLORS["border"],
        tickfont=dict(color=COLORS["text_muted"], size=11, family=FONT_MONO),
        title=dict(font=dict(color=COLORS["text_muted"], size=11)),
    )
    tpl = go.layout.Template(pio.templates["plotly_dark"])
    tpl.layout.update(
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(family=FONT_SANS, color=COLORS["text"], size=12),
        colorway=CATEGORICAL,
        xaxis=axis,
        yaxis=axis,
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=COLORS["text_muted"], size=11)),
        hoverlabel=dict(
            bgcolor=COLORS["surface_2"],
            bordercolor=COLORS["border_strong"],
            font=dict(family=FONT_MONO, color=COLORS["text"], size=12),
        ),
        title=dict(font=dict(size=14, color=COLORS["text"])),
        margin=dict(l=16, r=16, t=40, b=16),
    )
    pio.templates[PLOTLY_TEMPLATE] = tpl


register_plotly_template()

