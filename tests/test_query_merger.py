"""
test_query_merger.py
=====================
Bộ Unit Test kiểm thử thuật toán Auto-Correcting Query Merger:
1. Case 1 (History): T_end <= Watermark -> 100% Batch (Reconciled)
2. Case 2 (Realtime): T_start > Watermark -> 100% Speed (Provisional)
3. Case 3 (Hybrid): T_start <= Watermark < T_end -> Cắt ghép không trùng lặp (Partially Reconciled)
4. Cam kết Zero Double-Counting
5. Tính đúng sai số đối soát (Reconciliation Delta)
6. Kiểm thử FastAPI Endpoints (TestClient)
"""

import unittest
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient

from src.serving_layer.schemas import CandleStatus, QueryCase
from src.serving_layer.watermark_reader import WatermarkReader
from src.serving_layer.clickhouse_client import ClickHouseQueryClient
from src.serving_layer.query_merger import AutoCorrectingQueryMerger
from src.serving_layer.api_routes import app, query_merger


class TestAutoCorrectingQueryMerger(unittest.TestCase):
    """Kiểm thử thuật toán Query Merger với 3 Case kinh điển."""

    def setUp(self):
        # Mốc Watermark giả lập: 12:00:00 UTC
        self.watermark = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)
        self.wm_reader = WatermarkReader(mock_watermark=self.watermark)
        self.ch_client = ClickHouseQueryClient()

        # Dữ liệu mẫu Batch Layer (Lịch sử: 11:58 -> 12:00)
        self.batch_candles = [
            {
                "symbol": "BTCUSDT",
                "window_start": datetime(2026, 9, 4, 11, 58, 0, tzinfo=timezone.utc),
                "window_end": datetime(2026, 9, 4, 11, 59, 0, tzinfo=timezone.utc),
                "open_price": 60000.0,
                "high_price": 60200.0,
                "low_price": 59900.0,
                "close_price": 60100.0,
                "volume": 10.0,
                "trade_count": 50,
                "vwap": 60050.0,
                "is_spike": 0,
            },
            {
                "symbol": "BTCUSDT",
                "window_start": datetime(2026, 9, 4, 11, 59, 0, tzinfo=timezone.utc),
                "window_end": datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc),
                "open_price": 60100.0,
                "high_price": 60300.0,
                "low_price": 60050.0,
                "close_price": 60250.0,
                "volume": 15.0,
                "trade_count": 70,
                "vwap": 60200.0,  # Batch VWAP = 60200.0
                "is_spike": 0,
            },
        ]

        # Dữ liệu mẫu Speed Layer (Thời gian thực: 11:59 -> 12:02)
        self.speed_candles = [
            # Nến 11:59 ở Speed Layer (chưa qua DQ Gate, giả sử có late trade làm vwap = 60215.0)
            {
                "symbol": "BTCUSDT",
                "window_start": datetime(2026, 9, 4, 11, 59, 0, tzinfo=timezone.utc),
                "window_end": datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc),
                "open_price": 60100.0,
                "high_price": 60350.0,
                "low_price": 60050.0,
                "close_price": 60250.0,
                "volume": 15.2,
                "trade_count": 72,
                "vwap": 60215.0,  # Speed VWAP = 60215.0 (lệch +15 USDT so với Batch)
                "is_spike": 0,
            },
            # Nến sau Watermark (12:00 -> 12:01)
            {
                "symbol": "BTCUSDT",
                "window_start": datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc),
                "window_end": datetime(2026, 9, 4, 12, 1, 0, tzinfo=timezone.utc),
                "open_price": 60250.0,
                "high_price": 60400.0,
                "low_price": 60200.0,
                "close_price": 60350.0,
                "volume": 8.0,
                "trade_count": 40,
                "vwap": 60300.0,
                "is_spike": 0,
            },
            # Nến sau Watermark (12:01 -> 12:02)
            {
                "symbol": "BTCUSDT",
                "window_start": datetime(2026, 9, 4, 12, 1, 0, tzinfo=timezone.utc),
                "window_end": datetime(2026, 9, 4, 12, 2, 0, tzinfo=timezone.utc),
                "open_price": 60350.0,
                "high_price": 60500.0,
                "low_price": 60300.0,
                "close_price": 60450.0,
                "volume": 12.0,
                "trade_count": 55,
                "vwap": 60400.0,
                "is_spike": 0,
            },
        ]

        self.ch_client.set_mock_data("batch_agg", self.batch_candles)
        self.ch_client.set_mock_data("speed_agg", self.speed_candles)

        self.merger = AutoCorrectingQueryMerger(
            watermark_reader=self.wm_reader,
            ch_client=self.ch_client,
        )

    def test_case_1_history_query(self):
        """Case 1: T_end <= Watermark -> 100% dữ liệu từ Batch View (Reconciled)."""
        t_start = datetime(2026, 9, 4, 11, 58, 0, tzinfo=timezone.utc)
        t_end = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)

        resp = self.merger.merge_query("BTCUSDT", t_start, t_end)

        self.assertEqual(resp.query_case, QueryCase.CASE_1_HISTORY)
        self.assertEqual(resp.overall_status, CandleStatus.RECONCILED)
        self.assertEqual(resp.reconciliation_delta, 0.0)
        self.assertEqual(resp.candles_count, 2)
        for candle in resp.candles:
            self.assertEqual(candle.status, CandleStatus.RECONCILED)

    def test_case_2_realtime_query(self):
        """Case 2: T_start > Watermark -> 100% dữ liệu từ Speed View (Provisional)."""
        t_start = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)
        t_end = datetime(2026, 9, 4, 12, 2, 0, tzinfo=timezone.utc)

        resp = self.merger.merge_query("BTCUSDT", t_start, t_end)

        self.assertEqual(resp.query_case, QueryCase.CASE_2_REALTIME)
        self.assertEqual(resp.overall_status, CandleStatus.PROVISIONAL)
        self.assertIsNone(resp.reconciliation_delta)
        self.assertEqual(resp.candles_count, 2)
        for candle in resp.candles:
            self.assertEqual(candle.status, CandleStatus.PROVISIONAL)

    def test_case_3_hybrid_query_and_zero_double_counting(self):
        """
        Case 3: T_start <= Watermark < T_end -> Cắt ghép Batch + Speed.
        Kiểm tra:
        - Đúng 4 nến (2 nến Batch 11:58, 11:59 + 2 nến Speed 12:00, 12:01)
        - Nến 11:59 chỉ lấy từ Batch (Reconciled), không bị nhân đôi với Speed!
        - Tính đúng delta = |60215.0 - 60200.0| = 15.0
        """
        t_start = datetime(2026, 9, 4, 11, 58, 0, tzinfo=timezone.utc)
        t_end = datetime(2026, 9, 4, 12, 2, 0, tzinfo=timezone.utc)

        resp = self.merger.merge_query("BTCUSDT", t_start, t_end)

        self.assertEqual(resp.query_case, QueryCase.CASE_3_HYBRID)
        self.assertEqual(resp.overall_status, CandleStatus.PARTIALLY_RECONCILED)
        self.assertEqual(resp.candles_count, 4)

        # Kiểm tra 2 nến đầu là Reconciled từ Batch
        self.assertEqual(resp.candles[0].status, CandleStatus.RECONCILED)
        self.assertEqual(resp.candles[1].status, CandleStatus.RECONCILED)
        self.assertEqual(resp.candles[1].vwap, 60200.0)  # Chuẩn từ Batch

        # Kiểm tra 2 nến sau là Provisional từ Speed
        self.assertEqual(resp.candles[2].status, CandleStatus.PROVISIONAL)
        self.assertEqual(resp.candles[3].status, CandleStatus.PROVISIONAL)

        # Kiểm tra sai số đối soát tại biên
        self.assertEqual(resp.reconciliation_delta, 15.0)

    def test_reconciliation_report(self):
        """Kiểm tra bảng báo cáo đối soát Benchmark 2."""
        t_start = datetime(2026, 9, 4, 11, 59, 0, tzinfo=timezone.utc)
        t_end = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)

        report = self.merger.get_reconciliation_report("BTCUSDT", t_start, t_end)
        self.assertEqual(len(report), 1)
        item = report[0]
        self.assertEqual(item.batch_vwap, 60200.0)
        self.assertEqual(item.speed_vwap, 60215.0)
        self.assertEqual(item.vwap_delta, 15.0)
        self.assertTrue(item.is_reconciled)


class TestFastAPIRoutes(unittest.TestCase):
    """Kiểm thử API Endpoints qua FastAPI TestClient."""

    def setUp(self):
        # Thiết lập mock reader & client vào global merger của app để test siêu tốc (< 0.01s)
        self.mock_wm = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)
        query_merger.watermark_reader.set_mock_watermark(self.mock_wm)
        query_merger.ch_client.set_mock_data("batch_agg", [
            {
                "symbol": "BTCUSDT",
                "window_start": datetime(2026, 9, 4, 11, 0, 0, tzinfo=timezone.utc),
                "window_end": datetime(2026, 9, 4, 11, 1, 0, tzinfo=timezone.utc),
                "open_price": 60000.0, "high_price": 60100.0, "low_price": 59900.0,
                "close_price": 60050.0, "volume": 5.0, "trade_count": 20,
                "vwap": 60020.0, "is_spike": 0,
            }
        ])
        query_merger.ch_client.set_mock_data("speed_agg", [])
        self.client = TestClient(app)

    def test_health_check(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["service"], "serving_layer")

    def test_watermark_endpoint(self):
        resp = self.client.get("/api/watermark")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["layer"], "batch_layer")
        self.assertIn("watermark_time", data)

    def test_market_endpoint(self):
        resp = self.client.get("/api/market?symbol=BTCUSDT")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["symbol"], "BTCUSDT")
        self.assertIn("query_case", data)
        self.assertIn("candles", data)


if __name__ == "__main__":
    unittest.main()
