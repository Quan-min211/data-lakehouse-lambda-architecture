"""
Speed Layer Package
===================
Các thành phần xử lý thời gian thực bằng Spark Structured Streaming:
- MetricsCalculator: Tính toán OHLCV và VWAP
- SpikeDetector: Phát hiện biến động giá bất thường
- WindowAggregator: Phân tích cửa sổ thời gian có Watermark
- ClickHouseSpeedWriter: Ghi kết quả vào ClickHouse
- SpeedLayerStreamingJob: Vận hành streaming pipeline
"""

from src.speed_layer.metrics_calculator import MetricsCalculator
from src.speed_layer.spike_detector import SpikeDetector
from src.speed_layer.clickhouse_writer import ClickHouseSpeedWriter
from src.speed_layer.window_aggregator import WindowAggregator

__all__ = [
    "MetricsCalculator",
    "SpikeDetector",
    "ClickHouseSpeedWriter",
    "WindowAggregator",
]
