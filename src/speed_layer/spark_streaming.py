"""
spark_streaming.py
==================
Entry point chính cho Tầng Tốc Độ (Speed Layer) trong kiến trúc Lambda.
Sử dụng Spark Structured Streaming để:
1. Tiêu thụ luồng sự kiện giao dịch từ Kafka topic 'crypto_trades_raw'
2. Phân tích cửa sổ thời gian (Tumbling Window 1m) với Watermark (1m)
3. Tính toán nến OHLCV, VWAP và phát hiện Price Spike
4. Ghi micro-batch định kỳ mỗi 5 giây vào ClickHouse table 'lakehouse.speed_agg' (Trạng thái: Provisional)

Đạt SLA: Latency end-to-end < 5 giây.
"""

import os
import sys
import argparse
from typing import Optional

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.utils.logger import setup_logger
from src.utils.config import get_kafka_config, get_clickhouse_config
from src.speed_layer.window_aggregator import WindowAggregator
from src.speed_layer.clickhouse_writer import ClickHouseSpeedWriter

logger = setup_logger("spark_streaming")


def create_spark_session(app_name: str = "CryptoLakehouse-SpeedLayer") -> SparkSession:
    """Khởi tạo SparkSession tối ưu hóa cho Structured Streaming độ trễ thấp."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[2]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # Nạp Kafka SQL connector nếu chạy với maven packages
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession cho Speed Layer đã được khởi tạo thành công!")
    return spark


class SpeedLayerStreamingJob:
    """Quản lý vòng đời của Spark Structured Streaming Job."""

    def __init__(
        self,
        spark: SparkSession,
        kafka_bootstrap: Optional[str] = None,
        topic: Optional[str] = None,
        checkpoint_dir: Optional[str] = None,
        watermark: str = "1 minute",
        window_duration: str = "1 minute",
    ):
        self.spark = spark
        kafka_cfg = get_kafka_config()
        self.kafka_bootstrap = kafka_bootstrap or kafka_cfg.bootstrap_servers
        self.topic = topic or kafka_cfg.topic_raw
        self.checkpoint_dir = checkpoint_dir or os.path.join(
            os.getcwd(), ".checkpoints", "speed_layer"
        )
        self.aggregator = WindowAggregator(
            watermark_duration=watermark,
            window_duration=window_duration,
        )
        self.ch_writer = ClickHouseSpeedWriter()

    def run(self, trigger_interval: str = "5 seconds", once: bool = False):
        """Khởi động luồng Structured Streaming."""
        logger.info(f"Bắt đầu đọc từ Kafka topic: '{self.topic}' tại {self.kafka_bootstrap}...")

        # 1. Đọc luồng dữ liệu từ Kafka
        raw_stream = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # 2. Parse cấu trúc dữ liệu theo Data Contract
        parsed_events = self.aggregator.parse_kafka_stream(raw_stream)

        # 3. Thực hiện Tumbling Window Aggregation + Spike Detection
        aggregated_stream = self.aggregator.aggregate_tumbling_window(parsed_events)

        # 4. Ghi micro-batch vào ClickHouse
        # Đảm bảo bảng ClickHouse đã tồn tại
        try:
            self.ch_writer.initialize_tables()
        except Exception as e:
            logger.warning(f"Chưa thể kết nối tới ClickHouse để tạo bảng trước: {e}")

        logger.info(f"Cấu hình checkpoint location: {self.checkpoint_dir}")
        logger.info(f"Thiết lập Trigger interval: {trigger_interval} (SLA < 5s)")

        writer_query = (
            aggregated_stream.writeStream
            .outputMode("update")
            .foreachBatch(self.ch_writer.write_batch_from_spark)
            .option("checkpointLocation", self.checkpoint_dir)
        )

        if once:
            query = writer_query.trigger(availableNow=True).start()
        else:
            query = writer_query.trigger(processingTime=trigger_interval).start()

        logger.info("⚡ Speed Layer Streaming Query đang chạy... Nhấn Ctrl+C để dừng.")
        return query


def main():
    parser = argparse.ArgumentParser(description="Speed Layer Spark Streaming Job")
    parser.add_argument("--kafka", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=None, help="Kafka topic name")
    parser.add_argument("--watermark", default="1 minute", help="Watermark duration (e.g. '1 minute')")
    parser.add_argument("--window", default="1 minute", help="Window duration (e.g. '1 minute')")
    parser.add_argument("--trigger", default="5 seconds", help="Micro-batch trigger interval")
    parser.add_argument("--once", action="store_true", help="Chạy một micro-batch duy nhất rồi dừng (test mode)")

    args = parser.parse_args()

    spark = create_spark_session()
    job = SpeedLayerStreamingJob(
        spark=spark,
        kafka_bootstrap=args.kafka,
        topic=args.topic,
        watermark=args.watermark,
        window_duration=args.window,
    )

    query = job.run(trigger_interval=args.trigger, once=args.once)
    query.awaitTermination()


if __name__ == "__main__":
    main()
