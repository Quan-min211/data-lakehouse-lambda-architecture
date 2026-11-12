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


from dashboard.components.watermark_status import get_badge_html, render_watermark_badge
from dashboard.components.dq_panel import (
    _generate_fallback_quarantine_records,
    fetch_quarantine_data_from_clickhouse,
)


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

    def test_watermark_status_badge(self):
        """Kiểm tra badge watermark trạng thái Provisional / Reconciled."""
        badge_reconciled = get_badge_html("Reconciled")
        self.assertIn("Reconciled", badge_reconciled)
        self.assertIn("#3FB68B", badge_reconciled)

        badge_provisional = get_badge_html("Provisional")
        self.assertIn("Provisional", badge_provisional)
        self.assertIn("#E3A008", badge_provisional)

        badge_partial = get_badge_html("Partially Reconciled")
        self.assertIn("Partially Reconciled", badge_partial)
        self.assertIn("#4C8DF6", badge_partial)

    def test_dq_quarantine_fallback_and_schema(self):
        """Kiểm tra fallback generator và trường dữ liệu của DQ quarantine."""
        fallback_records = _generate_fallback_quarantine_records()
        self.assertIsInstance(fallback_records, list)
        self.assertGreater(len(fallback_records), 0)

        required_keys = {"batch_run_id", "trade_id", "symbol", "price", "quantity", "dq_error", "raw_record", "quarantined_at"}
        first_record = fallback_records[0]
        self.assertTrue(required_keys.issubset(first_record.keys()))

        # Use a closed local port so connection failure is immediate and does
        # not depend on DNS retry behaviour of the host operating system.
        records, is_live = fetch_quarantine_data_from_clickhouse(host="127.0.0.1", port=1)
        self.assertFalse(is_live)
        self.assertGreater(len(records), 0)


class TestUiBackendSync(unittest.TestCase):
    """Kiểm thử bản vá đồng bộ Dashboard / AlertManager với Backend."""

    def test_dq_empty_table_is_live_and_clean(self):
        """Kết nối được nhưng bảng rỗng -> (rỗng, True), không đổ dữ liệu mẫu."""
        from unittest import mock
        fake_client = mock.MagicMock()
        fake_client.query.return_value = mock.MagicMock(column_names=["batch_run_id"], result_rows=[])
        fake_module = mock.MagicMock(get_client=mock.MagicMock(return_value=fake_client))
        with mock.patch.dict("sys.modules", {"clickhouse_connect": fake_module}):
            records, is_live = fetch_quarantine_data_from_clickhouse(
                host="ch", port=8123, user="u", password="p", database="lakehouse"
            )
        self.assertTrue(is_live)
        self.assertEqual(records, [])
        kwargs = fake_module.get_client.call_args.kwargs
        self.assertEqual(kwargs["username"], "u")
        self.assertEqual(kwargs["password"], "p")
        self.assertEqual(kwargs["database"], "lakehouse")

    def test_market_4xx_does_not_fall_back_to_demo(self):
        """Lỗi tham số 4xx -> data_source = error; lỗi 5xx -> demo."""
        from unittest import mock
        from dashboard.app import fetch_api_data
        now = datetime.now(timezone.utc)
        resp_400 = mock.MagicMock(status_code=400)
        resp_400.json.return_value = {"detail": "bad range"}
        with mock.patch("dashboard.app.requests.get", return_value=resp_400):
            payload = fetch_api_data("http://x", "BTCUSDT", now - timedelta(minutes=5), now)
        self.assertEqual(payload["data_source"], "error")
        self.assertEqual(payload["candles"], [])
        with mock.patch("dashboard.app.requests.get", return_value=mock.MagicMock(status_code=503)):
            payload = fetch_api_data("http://x", "BTCUSDT", now - timedelta(minutes=5), now)
        self.assertEqual(payload["data_source"], "demo")

    def test_watermark_lag_thresholds(self):
        """Lag <= 900 s im lặng; > 900 s WARNING; >= 1800 s CRITICAL."""
        from src.utils.alerting import AlertManager
        manager = AlertManager()
        self.assertIsNone(manager.check_streaming_lag("BTCUSDT", 120.0))
        self.assertIsNone(manager.check_streaming_lag("BTCUSDT", 900.0))
        self.assertEqual(manager.check_streaming_lag("BTCUSDT", 901.0).severity, "WARNING")
        self.assertEqual(manager.check_streaming_lag("BTCUSDT", 1800.0).severity, "CRITICAL")

    def test_quarantine_spike_alerts_once_per_batch_run(self):
        """Mỗi batch_run_id chỉ phát QUARANTINE_SPIKE một lần."""
        from src.utils.alerting import AlertManager
        manager = AlertManager()
        self.assertIsNone(manager.check_quarantine_spike(10, 1000, threshold=100, batch_run_id="b1"))
        first = manager.check_quarantine_spike(150, 1000, threshold=100, batch_run_id="b1")
        self.assertIsNotNone(first)
        self.assertEqual(first.metrics["rejection_pct"], 15.0)
        self.assertIsNone(manager.check_quarantine_spike(150, 1000, threshold=100, batch_run_id="b1"))
        self.assertIsNotNone(manager.check_quarantine_spike(150, 1000, threshold=100, batch_run_id="b2"))

    def test_sequence_gap_alert(self):
        """gap = 0 im lặng; gap > 0 phát một lần mỗi (run_id, symbol); R_gap >= 1% là CRITICAL."""
        from src.utils.alerting import AlertManager
        manager = AlertManager()
        self.assertIsNone(manager.check_sequence_gap("BTCUSDT", 0, 0.0, run_id="r1"))
        warn = manager.check_sequence_gap("BTCUSDT", 5, 0.2, run_id="r1")
        self.assertEqual(warn.alert_type, "SEQUENCE_GAP")
        self.assertEqual(warn.severity, "WARNING")
        self.assertIsNone(manager.check_sequence_gap("BTCUSDT", 5, 0.2, run_id="r1"))
        self.assertEqual(manager.check_sequence_gap("ETHUSDT", 50, 2.5, run_id="r1").severity, "CRITICAL")


if __name__ == "__main__":
    unittest.main()

