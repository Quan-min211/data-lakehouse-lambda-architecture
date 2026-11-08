"""
metrics_cards.py
================
Header thông tin công cụ giao dịch (Instrument Ticker Header) kiểu sàn giao dịch:
- Giá mới nhất & biến động trong khung thời gian truy vấn
- Thống kê khung: Open / High / Low / Volume / VWAP / Số giao dịch
"""

from typing import Any, Dict, Optional

from dashboard.components.theme import COLORS, fmt_compact, fmt_price, render_html


def _symbol_pair(symbol: str) -> str:
    """Tách 'BTCUSDT' thành 'BTC / USDT' để hiển thị."""
    sym = symbol.upper()
    for quote in ("USDT", "USDC", "BUSD", "BTC", "ETH"):
        if sym.endswith(quote) and len(sym) > len(quote):
            return f"<b>{sym[:-len(quote)]}</b> / {quote}"
    return f"<b>{sym}</b>"


def render_metrics_cards(market_data: Dict[str, Any], watermark_info: Optional[Dict[str, Any]] = None) -> None:
    """Vẽ header giá & thống kê khung thời gian ở đầu Dashboard.

    Args:
        market_data: Payload trả về từ Serving API /api/market.
        watermark_info: Payload /api/watermark (giữ tham số để tương thích, không bắt buộc).
    """
    candles = market_data.get("candles", []) or []
    symbol = str(market_data.get("symbol", "--"))

    latest_price = open_price = high = low = vwap = None
    volume = 0.0
    trades = 0
    if candles:
        last_c = candles[-1]
        latest_price = float(last_c.get("close_price", 0.0) or 0.0)
        vwap = float(last_c.get("vwap", 0.0) or 0.0)
        open_price = float(candles[0].get("open_price", 0.0) or 0.0)
        high = max(float(c.get("high_price", 0.0) or 0.0) for c in candles)
        low = min(float(c.get("low_price", 0.0) or 0.0) for c in candles)
        volume = sum(float(c.get("volume", 0.0) or 0.0) for c in candles)
        trades = sum(int(c.get("trade_count", 0) or 0) for c in candles)

    change_html = '<span class="lh-change lh-muted">--</span>'
    price_color = COLORS["text"]
    if latest_price and open_price:
        diff = latest_price - open_price
        diff_pct = diff / open_price * 100
        tone = COLORS["up"] if diff >= 0 else COLORS["down"]
        price_color = tone
        sign = "+" if diff >= 0 else "−"
        change_html = (
            f'<span class="lh-change" style="color:{tone};background:{tone}1A;">'
            f'{sign}{fmt_price(abs(diff))} ({sign}{abs(diff_pct):.2f}%)</span>'
        )

    def stat(label: str, value: str) -> str:
        return (
            f'<div class="lh-stat"><span class="lh-stat-label">{label}</span>'
            f'<span class="lh-stat-value">{value}</span></div>'
        )

    stats_html = "".join([
        stat("Open", fmt_price(open_price)),
        stat("High", fmt_price(high)),
        stat("Low", fmt_price(low)),
        stat("VWAP", fmt_price(vwap)),
        stat("Volume", fmt_compact(volume) if candles else "--"),
        stat("Trades", f"{trades:,}" if candles else "--"),
    ])

    render_html(
        f"""
        <div class="lh-ticker">
        <div class="lh-ticker-main">
        <span class="lh-symbol">{_symbol_pair(symbol)} &nbsp;·&nbsp; 1m</span>
        <div class="lh-price-row">
        <span class="lh-price" style="color:{price_color}">{fmt_price(latest_price)}</span>
        {change_html}
        </div>
        </div>
        <div class="lh-stats">{stats_html}</div>
        </div>
        """
    )
