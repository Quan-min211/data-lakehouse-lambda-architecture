"""
spike_detector.py
=================
Module phát hiện bất thường biến động giá (Price Spike Detection) trong thời gian thực.
Cảnh báo các biến động giá vượt ngưỡng (ví dụ: biến động đột ngột > 2% hoặc biên độ High-Low > 3% trong 1 phút).
"""

from typing import Dict, Any
from pyspark.sql import functions as F
from pyspark.sql.column import Column


class SpikeDetector:
    """Phát hiện Price Spike dựa trên ngưỡng biến động tương đối."""

    DEFAULT_PRICE_CHANGE_THRESHOLD = 0.02   # 2.0% thay đổi giữa Close và Open
    DEFAULT_RANGE_THRESHOLD = 0.03          # 3.0% biên độ giữa High và Low

    def __init__(
        self,
        price_change_threshold: float = DEFAULT_PRICE_CHANGE_THRESHOLD,
        range_threshold: float = DEFAULT_RANGE_THRESHOLD,
    ):
        self.price_change_threshold = price_change_threshold
        self.range_threshold = range_threshold

    def is_spike_python(self, ohlcv: Dict[str, Any]) -> int:
        """
        Kiểm tra một bản ghi OHLCV có phải là Price Spike hay không (Pure Python).
        Trả về 1 nếu là Spike, 0 nếu bình thường.
        """
        open_p = float(ohlcv.get("open", ohlcv.get("open_price", 0.0)))
        close_p = float(ohlcv.get("close", ohlcv.get("close_price", 0.0)))
        high_p = float(ohlcv.get("high", ohlcv.get("high_price", 0.0)))
        low_p = float(ohlcv.get("low", ohlcv.get("low_price", 0.0)))

        if open_p <= 0 or low_p <= 0:
            return 0

        # Kiểm tra bước nhảy giá Open -> Close
        price_change = abs(close_p - open_p) / open_p
        # Kiểm tra biên độ dao động High -> Low
        volatility_range = (high_p - low_p) / low_p

        if price_change >= self.price_change_threshold or volatility_range >= self.range_threshold:
            return 1
        return 0

    def get_spark_spike_expr(self) -> Column:
        """
        Trả về biểu thức Spark Column tính toán cột 'is_spike' (0 hoặc 1).
        """
        price_change_cond = (
            F.abs(F.col("close_price") - F.col("open_price")) / F.col("open_price")
            >= self.price_change_threshold
        )
        range_cond = (
            (F.col("high_price") - F.col("low_price")) / F.col("low_price")
            >= self.range_threshold
        )

        return (
            F.when(price_change_cond | range_cond, F.lit(1))
            .otherwise(F.lit(0))
            .cast("int")
            .alias("is_spike")
        )
