"""
window_aggregator.py
====================
Phu trach xu ly luong su kien giao dich voi co che:
- Event-time processing (dua tren trade_time tu san Binance)
- Watermarking (1-2 phut) de tiep nhan late data hop le va loai bo data qua tre
- Tumbling Window (1 phut va 5 phut)
- Tich hop MetricsCalculator & SpikeDetector

Luu y PySpark import:
  - PySpark chi duoc import khi WindowAggregator.parse_kafka_stream() / aggregate_tumbling_window()
    duoc goi (lazy import). Dieu nay cho phep unit test chay ma khong can JVM.
  - TRADE_EVENT_SPARK_SCHEMA van export duoc (qua trade_schema.py) ma khong can Spark.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from src.speed_layer.metrics_calculator import MetricsCalculator
from src.speed_layer.spike_detector import SpikeDetector
from src.speed_layer.trade_schema import (
    get_trade_event_spark_schema,
    TRADE_EVENT_FIELD_NAMES,
)

if TYPE_CHECKING:
    # Chi import khi check type (mypy/IDE), khong chay khi runtime
    from pyspark.sql import DataFrame


# ── Public re-export: TRADE_EVENT_SPARK_SCHEMA (lazy) ────────────────────────
# Dung cho: unit tests, benchmarks — goi get() chi khi can Spark
class _LazySchema:
    """Proxy object: cho phep import `TRADE_EVENT_SPARK_SCHEMA` ma khong khoi dong JVM."""

    def __getattr__(self, name: str):
        schema = get_trade_event_spark_schema()
        return getattr(schema, name)

    @property
    def fields(self):
        return get_trade_event_spark_schema().fields


TRADE_EVENT_SPARK_SCHEMA = _LazySchema()


class WindowAggregator:
    """Xu ly aggregation cua so thoi gian (Tumbling Window) tren Spark Streaming."""

    def __init__(
        self,
        watermark_duration: str = "1 minute",
        window_duration: str = "1 minute",
        spike_threshold: float = 0.02,
        volatility_threshold: float = 0.03,
    ):
        self.watermark_duration = watermark_duration
        self.window_duration = window_duration
        self.spike_detector = SpikeDetector(
            price_change_threshold=spike_threshold,
            range_threshold=volatility_threshold,
        )

    @staticmethod
    def parse_kafka_stream(raw_kafka_df: "DataFrame") -> "DataFrame":
        """
        Parse raw Kafka messages (key, value) thanh DataFrame co cau truc theo TradeEvent schema.
        Them cot 'event_time' chuyen tu Unix timestamp (epoch ms) sang TimestampType.
        """
        # Lazy import PySpark — chi khi Spark Session dang chay
        from pyspark.sql import functions as F

        schema = get_trade_event_spark_schema()

        parsed_df = (
            raw_kafka_df.selectExpr("CAST(value AS STRING) as json_str")
            .select(F.from_json(F.col("json_str"), schema).alias("data"))
            .select("data.*")
        )

        df_with_time = parsed_df.withColumn(
            "event_time", (F.col("trade_time") / 1000.0).cast("timestamp")
        )

        return df_with_time

    def aggregate_tumbling_window(self, events_df: "DataFrame") -> "DataFrame":
        """
        Thuc hien Watermarking va Window Aggregation:
        1. withWatermark(event_time, watermark_duration)
        2. groupBy(window(event_time, window_duration), symbol)
        3. Tinh OHLCV va VWAP
        4. Gan co Price Spike (is_spike)
        """
        # Lazy import PySpark
        from pyspark.sql import functions as F

        # 1. Thiet lap Watermark xu ly late data
        watermarked_df = events_df.withWatermark("event_time", self.watermark_duration)

        # 2. Group by cua so thoi gian va Symbol
        agg_exprs = MetricsCalculator.get_spark_aggregation_exprs()

        aggregated_df = (
            watermarked_df.groupBy(
                F.window(F.col("event_time"), self.window_duration),
                F.col("symbol"),
            )
            .agg(*agg_exprs)
        )

        # 3. Phat hien Price Spike
        spike_expr = self.spike_detector.get_spark_spike_expr()
        final_df = aggregated_df.withColumn("is_spike", spike_expr)

        # 4. Trich xuat window_start, window_end va format cot chuan ClickHouse
        result_df = (
            final_df.withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .select(
                "symbol",
                "window_start",
                "window_end",
                F.round(F.col("open_price"), 4).alias("open_price"),
                F.round(F.col("high_price"), 4).alias("high_price"),
                F.round(F.col("low_price"), 4).alias("low_price"),
                F.round(F.col("close_price"), 4).alias("close_price"),
                F.round(F.col("volume"), 6).alias("volume"),
                F.col("trade_count").cast("long").alias("trade_count"),
                F.round(F.col("vwap"), 4).alias("vwap"),
                F.col("is_spike").cast("int").alias("is_spike"),
            )
        )

        return result_df
