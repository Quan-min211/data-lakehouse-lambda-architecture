"""
metrics_calculator.py
=====================
Module tính toán các chỉ số nến OHLCV (Open, High, Low, Close, Volume)
và VWAP (Volume-Weighted Average Price) cho Speed Layer trong kiến trúc Lambda.

Hỗ trợ cả:
1. Pure Python / Pandas (dành cho Unit testing độc lập không cần Spark JVM)
2. PySpark Column expressions (dành cho Spark Structured Streaming trên cluster)
"""

from typing import Dict, Any, List
from pyspark.sql import functions as F
from pyspark.sql.column import Column


class MetricsCalculator:
    """Tính toán OHLCV và VWAP cho các cửa sổ thời gian (Tumbling Windows)."""

    @staticmethod
    def calculate_vwap_python(trades: List[Dict[str, Any]]) -> float:
        """
        Tính VWAP từ danh sách các giao dịch (Pure Python).
        Công thức: VWAP = sum(price * quantity) / sum(quantity)
        """
        if not trades:
            return 0.0
        total_value = sum(float(t["price"]) * float(t["quantity"]) for t in trades)
        total_volume = sum(float(t["quantity"]) for t in trades)
        return round(total_value / total_volume, 6) if total_volume > 0 else 0.0

    @staticmethod
    def calculate_ohlcv_python(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Tính OHLCV từ danh sách giao dịch được sắp xếp theo thời gian (Pure Python).
        """
        if not trades:
            return {
                "open": 0.0,
                "high": 0.0,
                "low": 0.0,
                "close": 0.0,
                "volume": 0.0,
                "trade_count": 0,
                "vwap": 0.0,
            }

        sorted_trades = sorted(trades, key=lambda t: t["trade_time"])
        prices = [float(t["price"]) for t in sorted_trades]
        quantities = [float(t["quantity"]) for t in sorted_trades]

        open_p = prices[0]
        close_p = prices[-1]
        high_p = max(prices)
        low_p = min(prices)
        total_vol = sum(quantities)
        total_val = sum(p * q for p, q in zip(prices, quantities))
        vwap = round(total_val / total_vol, 6) if total_vol > 0 else 0.0

        return {
            "open": round(open_p, 4),
            "high": round(high_p, 4),
            "low": round(low_p, 4),
            "close": round(close_p, 4),
            "volume": round(total_vol, 6),
            "trade_count": len(sorted_trades),
            "vwap": vwap,
        }

    @staticmethod
    def get_spark_aggregation_exprs() -> List[Column]:
        """
        Trả về danh sách các Spark Aggregation Expressions cho Window Aggregation.
        Sử dụng kỹ thuật struct(trade_time, price) để trích xuất chính xác Open và Close.
        """
        # Struct kết hợp trade_time và price
        time_price_struct = F.struct(F.col("trade_time"), F.col("price"))

        return [
            # Open: giá của giao dịch có trade_time sớm nhất trong window
            F.min(time_price_struct).getItem("price").alias("open_price"),
            # High: giá cao nhất trong window
            F.max(F.col("price")).alias("high_price"),
            # Low: giá thấp nhất trong window
            F.min(F.col("price")).alias("low_price"),
            # Close: giá của giao dịch có trade_time muộn nhất trong window
            F.max(time_price_struct).getItem("price").alias("close_price"),
            # Volume: tổng khối lượng giao dịch
            F.sum(F.col("quantity")).alias("volume"),
            # Trade count: tổng số giao dịch
            F.count(F.lit(1)).alias("trade_count"),
            # VWAP = sum(price * quantity) / sum(quantity)
            (F.sum(F.col("price") * F.col("quantity")) / F.sum(F.col("quantity"))).alias("vwap"),
        ]
