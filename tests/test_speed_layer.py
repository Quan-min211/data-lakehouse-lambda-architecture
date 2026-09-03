"""
test_speed_layer.py
===================
Bộ Unit Test kiểm thử logic nghiệp vụ của Tầng Tốc Độ (Speed Layer):
1. Tính toán nến OHLCV
2. Tính toán VWAP (Volume-Weighted Average Price)
3. Phát hiện biến động giá bất thường (Price Spike Detection)
4. Chuẩn hóa bản ghi trước khi nạp vào ClickHouse speed_agg
5. Khớp Data Contract schema
"""

import unittest
from datetime import datetime, timezone

from src.speed_layer.metrics_calculator import MetricsCalculator
from src.speed_layer.spike_detector import SpikeDetector
from src.speed_layer.window_aggregator import TRADE_EVENT_SPARK_SCHEMA


class TestMetricsCalculator(unittest.TestCase):
    """Kiểm thử tính toán chỉ số OHLCV và VWAP."""

    def setUp(self):
        # 3 giao dịch mẫu trong 1 phút
        self.sample_trades = [
            {"price": 60000.0, "quantity": 1.0, "trade_time": 1700000000000},  # Open: 60000
            {"price": 61500.0, "quantity": 2.0, "trade_time": 1700000020000},  # High: 61500
            {"price": 59500.0, "quantity": 1.0, "trade_time": 1700000040000},  # Low: 59500
            {"price": 61000.0, "quantity": 1.0, "trade_time": 1700000059000},  # Close: 61000
        ]

    def test_vwap_calculation(self):
        # Total Value: (60000*1) + (61500*2) + (59500*1) + (61000*1) = 60000 + 123000 + 59500 + 61000 = 303500
        # Total Quantity: 1 + 2 + 1 + 1 = 5
        # Expected VWAP = 303500 / 5 = 60700.0
        vwap = MetricsCalculator.calculate_vwap_python(self.sample_trades)
        self.assertEqual(vwap, 60700.0)

    def test_vwap_empty_trades(self):
        vwap = MetricsCalculator.calculate_vwap_python([])
        self.assertEqual(vwap, 0.0)

    def test_ohlcv_calculation(self):
        ohlcv = MetricsCalculator.calculate_ohlcv_python(self.sample_trades)
        self.assertEqual(ohlcv["open"], 60000.0)
        self.assertEqual(ohlcv["high"], 61500.0)
        self.assertEqual(ohlcv["low"], 59500.0)
        self.assertEqual(ohlcv["close"], 61000.0)
        self.assertEqual(ohlcv["volume"], 5.0)
        self.assertEqual(ohlcv["trade_count"], 4)
        self.assertEqual(ohlcv["vwap"], 60700.0)

    def test_ohlcv_out_of_order_input(self):
        # Đảo thứ tự thời gian để kiểm tra hàm tự sắp xếp theo trade_time
        shuffled = [
            self.sample_trades[2], # 59500
            self.sample_trades[0], # 60000
            self.sample_trades[3], # 61000
            self.sample_trades[1], # 61500
        ]
        ohlcv = MetricsCalculator.calculate_ohlcv_python(shuffled)
        self.assertEqual(ohlcv["open"], 60000.0)
        self.assertEqual(ohlcv["close"], 61000.0)


class TestSpikeDetector(unittest.TestCase):
    """Kiểm thử thuật toán phát hiện Price Spike."""

    def setUp(self):
        self.detector = SpikeDetector(price_change_threshold=0.02, range_threshold=0.03)

    def test_normal_candle(self):
        # Open: 60000, Close: 60500 (+0.83% -> < 2%)
        # High: 60800, Low: 59800 (Range = 1000/59800 = 1.67% -> < 3%)
        candle = {"open": 60000.0, "high": 60800.0, "low": 59800.0, "close": 60500.0}
        self.assertEqual(self.detector.is_spike_python(candle), 0)

    def test_price_jump_spike(self):
        # Open: 60000, Close: 61500 (+2.5% -> >= 2% threshold)
        candle = {"open": 60000.0, "high": 61600.0, "low": 59900.0, "close": 61500.0}
        self.assertEqual(self.detector.is_spike_python(candle), 1)

    def test_price_dump_spike(self):
        # Open: 60000, Close: 58500 (-2.5% -> >= 2% threshold)
        candle = {"open": 60000.0, "high": 60100.0, "low": 58400.0, "close": 58500.0}
        self.assertEqual(self.detector.is_spike_python(candle), 1)

    def test_range_volatility_spike(self):
        # Open: 60000, Close: 60100 (ít đổi) nhưng High: 62500, Low: 60000 (Range 4.16% >= 3%)
        candle = {"open": 60000.0, "high": 62500.0, "low": 60000.0, "close": 60100.0}
        self.assertEqual(self.detector.is_spike_python(candle), 1)


class TestSchemaAndFormatting(unittest.TestCase):
    """Kiểm thử cấu trúc Schema."""

    def test_spark_schema_field_names(self):
        field_names = [f.name for f in TRADE_EVENT_SPARK_SCHEMA.fields]
        expected = [
            "trade_id",
            "symbol",
            "price",
            "quantity",
            "trade_time",
            "is_buyer_maker",
            "ingestion_time",
            "is_injected",
            "fault_type",
        ]
        self.assertEqual(field_names, expected)


if __name__ == "__main__":
    unittest.main()
