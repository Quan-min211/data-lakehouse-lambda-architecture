"""
window_aggregator.py
====================
Phụ trách xử lý luồng sự kiện giao dịch với cơ chế:
- Event-time processing (dựa trên trade_time từ sàn Binance)
- Watermarking (1-2 phút) để tiếp nhận late data hợp lệ và loại bỏ data quá trễ
- Tumbling Window (1 phút và 5 phút)
- Tích hợp MetricsCalculator & SpikeDetector
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
)

from src.speed_layer.metrics_calculator import MetricsCalculator
from src.speed_layer.spike_detector import SpikeDetector


# Schema khớp chính xác 100% với Data Contract: datasets/schemas/trade_event_schema.json
TRADE_EVENT_SPARK_SCHEMA = StructType([
    StructField("trade_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", DoubleType(), False),
    StructField("trade_time", LongType(), False),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("ingestion_time", LongType(), True),
    StructField("is_injected", BooleanType(), True),
    StructField("fault_type", StringType(), True),
])


class WindowAggregator:
    """Xử lý aggregation cửa sổ thời gian (Tumbling Window) trên Spark Streaming."""

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
    def parse_kafka_stream(raw_kafka_df: DataFrame) -> DataFrame:
        """
        Parse raw Kafka messages (key, value) thành DataFrame có cấu trúc theo TradeEvent schema.
        Thêm cột 'event_time' chuyển từ Unix timestamp (epoch ms) sang TimestampType.
        """
        # Parse JSON string từ Kafka value
        parsed_df = (
            raw_kafka_df.selectExpr("CAST(value AS STRING) as json_str")
            .select(F.from_json(F.col("json_str"), TRADE_EVENT_SPARK_SCHEMA).alias("data"))
            .select("data.*")
        )

        # Chuyển trade_time (epoch ms) thành TimestampType chuẩn hóa
        df_with_time = parsed_df.withColumn(
            "event_time", (F.col("trade_time") / 1000.0).cast("timestamp")
        )

        return df_with_time

    def aggregate_tumbling_window(self, events_df: DataFrame) -> DataFrame:
        """
        Thực hiện Watermarking và Window Aggregation:
        1. withWatermark(event_time, watermark_duration)
        2. groupBy(window(event_time, window_duration), symbol)
        3. Tính OHLCV và VWAP
        4. Gắn cờ Price Spike (is_spike)
        """
        # 1. Thiết lập Watermark xử lý late data
        watermarked_df = events_df.withWatermark("event_time", self.watermark_duration)

        # 2. Group by cửa sổ thời gian và Symbol
        agg_exprs = MetricsCalculator.get_spark_aggregation_exprs()

        aggregated_df = (
            watermarked_df.groupBy(
                F.window(F.col("event_time"), self.window_duration),
                F.col("symbol"),
            )
            .agg(*agg_exprs)
        )

        # 3. Phát hiện Price Spike
        spike_expr = self.spike_detector.get_spark_spike_expr()
        final_df = aggregated_df.withColumn("is_spike", spike_expr)

        # 4. Trích xuất window_start, window_end và format cột chuẩn ClickHouse
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
