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

    @staticmethod
    def filter_and_deduplicate_python(records: list[dict]) -> list[dict]:
        """
        Loc bo ban ghi vi pham Data Contract (gia/khoi luong <= 0, thieu truong)
        va khu trung lap theo (symbol, trade_id) cho luong Speed Layer (Pure Python).
        """
        seen: set[tuple[str, int]] = set()
        valid: list[dict] = []
        for r in records:
            try:
                tid = int(r["trade_id"])
                sym = str(r["symbol"]).strip().upper()
                price = float(r["price"])
                qty = float(r["quantity"])
                ttime = int(r["trade_time"])
            except (KeyError, TypeError, ValueError):
                continue
            if tid < 0 or not sym or price <= 0 or qty <= 0 or ttime <= 0:
                continue
            key = (sym, tid)
            if key in seen:
                continue
            seen.add(key)
            cleaned = dict(r)
            cleaned["trade_id"] = tid
            cleaned["symbol"] = sym
            cleaned["price"] = price
            cleaned["quantity"] = qty
            cleaned["trade_time"] = ttime
            valid.append(cleaned)
        return valid

    def aggregate_tumbling_window(self, events_df: "DataFrame") -> "DataFrame":
        """
        Thuc hien Streaming DQ Filter, Watermarking, Bounded Deduplication va Window Aggregation:
        1. Filter ban ghi hop le (price > 0, quantity > 0, trade_id >= 0, symbol != '')
        2. withWatermark(event_time, watermark_duration)
        3. dropDuplicatesWithinWatermark(['symbol', 'trade_id']) chong nhan doi volume
        4. groupBy(window(event_time, window_duration), symbol)
        5. Tinh OHLCV va VWAP
        6. Gan co Price Spike (is_spike)
        """
        # Lazy import PySpark
        from pyspark.sql import functions as F

        # 1. Streaming Data Quality Filter (chan gia am, khoi luong <= 0, null fields)
        valid_events_df = events_df.filter(
            F.col("trade_id").isNotNull()
            & (F.col("trade_id") >= 0)
            & F.col("symbol").isNotNull()
            & (F.length(F.trim(F.col("symbol"))) > 0)
            & F.col("price").isNotNull()
            & (F.col("price") > 0)
            & F.col("quantity").isNotNull()
            & (F.col("quantity") > 0)
            & F.col("trade_time").isNotNull()
            & (F.col("trade_time") > 0)
        )

        # 2. Thiet lap Watermark xu ly late data & gioi han bo nho state
        watermarked_df = valid_events_df.withWatermark("event_time", self.watermark_duration)

        # 3. Khu trung lap trong cua so Watermark (chong duplicate seam / retry)
        if hasattr(watermarked_df, "dropDuplicatesWithinWatermark"):
            deduped_df = watermarked_df.dropDuplicatesWithinWatermark(["symbol", "trade_id"])
        else:
            deduped_df = watermarked_df.dropDuplicates(["symbol", "trade_id"])

        # 4. Group by cua so thoi gian va Symbol
        agg_exprs = MetricsCalculator.get_spark_aggregation_exprs()

        aggregated_df = (
            deduped_df.groupBy(
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
