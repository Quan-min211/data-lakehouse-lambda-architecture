"""
test_dashboard_components.py
============================
Unit test kiểm thử các component của Dashboard Streamlit:
1. Vẽ biểu đồ nến Plotly Candlestick
2. Dữ liệu fallback sinh chuẩn xác
3. Khớp định dạng trả về từ Serving Layer
"""

import unittest
from datetime import datetime, timezone, timedelta
import plotly.graph_objects as go

from dashboard.components.candlestick import render_candlestick_chart
from dashboard.app import generate_fallback_market_data


class TestDashboardComponents(unittest.TestCase):
    """Kiểm thử các thành phần trực quan hóa của Dashboard."""

    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.sample_candles = [
            {
                "symbol": "BTCUSDT",
                "window_start": self.now - timedelta(minutes=2),
                "window_end": self.now - timedelta(minutes=1),
                "open_price": 60000.0,
                "high_price": 60500.0,
                "low_price": 59800.0,
                "close_price": 60400.0,
                "volume": 10.5,
                "trade_count": 80,
                "vwap": 60250.0,
                "is_spike": 0,
                "status": "Reconciled",
            },
            {
                "symbol": "BTCUSDT",
                "window_start": self.now - timedelta(minutes=1),
                "window_end": self.now,
                "open_price": 60400.0,
                "high_price": 62000.0,
                "low_price": 60300.0,
                "close_price": 61800.0,
                "volume": 25.0,
                "trade_count": 150,
                "vwap": 61200.0,
                "is_spike": 1,  # Spike
                "status": "Provisional",
            },
        ]

    def test_render_candlestick_empty(self):
        """Biểu đồ với danh sách rỗng không bị crash."""
        fig = render_candlestick_chart([], symbol="BTCUSDT")
        self.assertIsInstance(fig, go.Figure)

    def test_render_candlestick_with_data(self):
        """Biểu đồ có nến, vwap, spike markers và volume bars."""
        fig = render_candlestick_chart(self.sample_candles, symbol="BTCUSDT")
        self.assertIsInstance(fig, go.Figure)
        # Kiểm tra có ít nhất 4 traces (Candlestick, VWAP line, Spike marker, Volume bar)
        self.assertGreaterEqual(len(fig.data), 4)

    def test_generate_fallback_data(self):
        """Hàm sinh dữ liệu dự phòng tạo đúng cấu trúc."""
        start = self.now - timedelta(minutes=10)
        end = self.now
        data = generate_fallback_market_data("BTCUSDT", start, end)

        self.assertEqual(data["symbol"], "BTCUSDT")
        self.assertIn(data["query_case"], [1, 2, 3])
        self.assertIn(data["overall_status"], ["Reconciled", "Provisional", "Partially Reconciled"])
        self.assertGreater(len(data["candles"]), 0)


if __name__ == "__main__":
    unittest.main()
