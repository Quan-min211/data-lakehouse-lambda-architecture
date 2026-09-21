"""
spark_streaming.py
==================
Entry point chinh cho Tang Toc Do (Speed Layer) trong kien truc Lambda.
Su dung Spark Structured Streaming de:
1. Tieu thu luong su kien giao dich tu Kafka topic 'crypto_trades_raw'
2. Phan tich cua so thoi gian (Tumbling Window 1m) voi Watermark (1m)
3. Tinh toan nen OHLCV, VWAP va phat hien Price Spike
4. Ghi micro-batch dinh ky moi 5 giay vao ClickHouse table 'lakehouse.speed_agg'

Dat SLA: Latency end-to-end < 5 giay.

Cach chay (khuyen nghi -- Docker, tranh loi winutils.exe tren Windows):
  docker compose up -d speed-layer
  docker compose logs -f speed-layer

Cach chay thu cong tren Windows (can HADOOP_HOME va winutils.exe):
  Dat winutils.exe vao: <project_root>/hadoop/bin/winutils.exe
  python src/speed_layer/spark_streaming.py --once

Bien moi truong:
  SPARK_MASTER_URL        -- URL Spark Master (mac dinh: local[2])
  KAFKA_BOOTSTRAP_SERVERS -- Kafka brokers
  KAFKA_TOPIC_RAW         -- Topic de doc (mac dinh: crypto_trades_raw)
  CLICKHOUSE_HOST         -- Host ClickHouse
  CHECKPOINT_DIR          -- Thu muc luu Structured Streaming checkpoint
  SPEED_TRIGGER_INTERVAL  -- Chu ky micro-batch (mac dinh: 5 seconds)
"""

from __future__ import annotations

import argparse
import os
import platform
import sys
from pathlib import Path
from typing import Optional

# ── Windows environment fix ──────────────────────────────────────────────────
# Spark Structured Streaming tren Windows yeu cau winutils.exe (Hadoop binary).
# Neu HADOOP_HOME chua duoc set, tu dong tim va cau hinh.
def _configure_windows_hadoop() -> None:
    """
    Try to configure HADOOP_HOME for Windows so winutils.exe can be found.
    If no valid Hadoop installation is found, patch Spark to skip Hadoop native calls.
    """
    if platform.system() != "Windows":
        return  # Khong can xu ly tren Linux/macOS (Docker container)

    # 1. Neu HADOOP_HOME da duoc set boi nguoi dung -> giu nguyen
    if os.environ.get("HADOOP_HOME"):
        return

    # 2. Tim hadoop/ trong goc project
    project_root = Path(__file__).resolve().parents[2]
    candidate = project_root / "hadoop"
    winutils_path = candidate / "bin" / "winutils.exe"

    if winutils_path.exists():
        os.environ["HADOOP_HOME"] = str(candidate)
        os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")
        import logging
        logging.getLogger("spark_streaming").info(
            "Windows: Da cau hinh HADOOP_HOME=%s", candidate
        )
        return

    # 3. Khong tim thay winutils -> ghi canh bao va tao thu muc gia de Spark khoi panic
    import logging
    log = logging.getLogger("spark_streaming")
    log.warning(
        "Windows: Khong tim thay winutils.exe tai %s.\n"
        "Speed Layer SE CHAY TRONG DOCKER (khuyen nghi):\n"
        "  docker compose up -d speed-layer\n"
        "Neu muon chay tren Windows host:\n"
        "  1. Tai winutils.exe tu: https://github.com/cdarlint/winutils\n"
        "  2. Dat vao thu muc: %s\n"
        "  3. Chay lai lenh nay.",
        winutils_path, candidate / "bin",
    )
    # Tao thu muc gia de Spark khoi panic ngay lap tuc
    fake_hadoop = Path(os.environ.get("TEMP", "C:\\Temp")) / "hadoop_fake"
    (fake_hadoop / "bin").mkdir(parents=True, exist_ok=True)
    os.environ["HADOOP_HOME"] = str(fake_hadoop)


# Goi ngay khi import module (truoc ca pyspark import)
_configure_windows_hadoop()


def _configure_spark_python_path() -> None:
    """
    Tu dong tim va them PySpark & Py4J vao sys.path neu chay trong Spark container (/opt/spark).
    Dieu nay giup import pyspark thanh cong ngay ca khi PYTHONPATH bi ghi de boi Docker environment.
    """
    candidates = [
        os.environ.get("SPARK_HOME", "/opt/spark"),
        "/opt/spark",
        "/usr/local/spark",
        "/opt/bitnami/spark",
    ]
    for base in candidates:
        p = Path(base) / "python"
        if p.exists():
            if str(p) not in sys.path:
                sys.path.insert(0, str(p))
            lib_dir = p / "lib"
            if lib_dir.exists():
                for z in sorted(lib_dir.glob("*.zip")):
                    if str(z) not in sys.path:
                        sys.path.insert(0, str(z))
            break


_configure_spark_python_path()

# ── imports ──────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.utils.logger import setup_logger
from src.utils.config import get_kafka_config, get_clickhouse_config  # noqa: F401
from src.speed_layer.window_aggregator import WindowAggregator
from src.speed_layer.clickhouse_writer import ClickHouseSpeedWriter

logger = setup_logger("spark_streaming")

# ── environment variables ─────────────────────────────────────────────────────
_DEFAULT_CHECKPOINT = os.path.join(os.getcwd(), ".checkpoints", "speed_layer")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", _DEFAULT_CHECKPOINT)
SPEED_TRIGGER_INTERVAL = os.getenv("SPEED_TRIGGER_INTERVAL", "5 seconds")


# ── environment validation ────────────────────────────────────────────────────
def validate_environment() -> bool:
    """
    Kiem tra cac dependency truoc khi khoi dong Spark.

    Returns:
        True neu moi truong hop le, False neu co loi nghiem trong.
    """
    ok = True

    try:
        import pyspark  # noqa: F401
    except ImportError:
        logger.error(
            "PySpark chua duoc cai dat. Chay: pip install pyspark==3.5.1\n"
            "Hoac su dung Docker: docker compose up -d speed-layer"
        )
        ok = False

    try:
        import clickhouse_connect  # noqa: F401
    except ImportError:
        logger.error(
            "clickhouse-connect chua duoc cai dat. Chay: pip install clickhouse-connect"
        )
        ok = False

    if platform.system() == "Windows":
        hadoop_home = os.environ.get("HADOOP_HOME", "")
        winutils = Path(hadoop_home) / "bin" / "winutils.exe" if hadoop_home else Path(".")
        if not winutils.exists():
            logger.warning(
                "Windows: winutils.exe khong tim thay. Spark Streaming co the bi loi.\n"
                "Khuyen nghi chay qua Docker: docker compose up -d speed-layer"
            )

    return ok


# ── SparkSession factory ──────────────────────────────────────────────────────
def create_spark_session(app_name: str = "CryptoLakehouse-SpeedLayer"):
    """
    Khoi tao SparkSession toi uu hoa cho Structured Streaming do tre thap.

    Returns:
        SparkSession da duoc cau hinh.
    """
    from pyspark.sql import SparkSession

    master_url = os.getenv("SPARK_MASTER_URL", "local[2]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # Kafka SQL connector -- tai ve tu Maven khi khoi dong
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
    )

    # Tren Windows: giam thieu cac tinh nang Hadoop native
    if platform.system() == "Windows":
        builder = builder.config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession cho Speed Layer da khoi tao thanh cong! Master=%s", master_url
    )
    return spark


# ── streaming job class ───────────────────────────────────────────────────────
class SpeedLayerStreamingJob:
    """Quan ly vong doi cua Spark Structured Streaming Job."""

    def __init__(
        self,
        spark,
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
        self.checkpoint_dir = checkpoint_dir or CHECKPOINT_DIR
        self.aggregator = WindowAggregator(
            watermark_duration=watermark,
            window_duration=window_duration,
        )
        self.ch_writer = ClickHouseSpeedWriter()

    def _ensure_topic_exists(self) -> None:
        """Kiem tra va tu dong tao Kafka topic neu chua ton tai."""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError

            admin = KafkaAdminClient(
                bootstrap_servers=self.kafka_bootstrap,
                client_id="speed_layer_admin",
                request_timeout_ms=5000,
            )
            existing = admin.list_topics()
            if self.topic not in existing:
                logger.info("Kafka topic '%s' chua ton tai. Dang tao moi...", self.topic)
                new_topic = NewTopic(name=self.topic, num_partitions=3, replication_factor=1)
                admin.create_topics(new_topics=[new_topic], validate_only=False)
                logger.info("Da tao thanh cong Kafka topic '%s' (3 partitions)!", self.topic)
                import time
                time.sleep(2)  # Cho metadata propagate tren cac broker
            admin.close()
        except Exception as e:
            logger.warning("Khong the tu dong tao topic qua KafkaAdminClient: %s", e)

    def run(self, trigger_interval: Optional[str] = None, once: bool = False):
        """
        Khoi dong luong Structured Streaming.

        Args:
            trigger_interval: Chu ky micro-batch (vi du: '5 seconds').
                              Mac dinh lay tu bien moi truong SPEED_TRIGGER_INTERVAL.
            once: Neu True, chay mot micro-batch duy nhat roi dung (test mode).

        Returns:
            StreamingQuery dang chay.
        """
        _trigger = trigger_interval or SPEED_TRIGGER_INTERVAL

        # 0. Dam bao topic Kafka ton tai truoc khi Spark readStream
        self._ensure_topic_exists()

        logger.info(
            "Bat dau doc tu Kafka topic '%s' tai %s...",
            self.topic, self.kafka_bootstrap,
        )

        # 1. Doc luong du lieu tu Kafka
        raw_stream = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # 2. Parse cau truc du lieu theo Data Contract
        parsed_events = self.aggregator.parse_kafka_stream(raw_stream)

        # 3. Tumbling Window Aggregation + Spike Detection
        aggregated_stream = self.aggregator.aggregate_tumbling_window(parsed_events)

        # 4. Khoi tao bang ClickHouse (neu chua ton tai)
        try:
            self.ch_writer.initialize_tables()
        except Exception as e:
            logger.warning("Chua the ket noi toi ClickHouse de tao bang truoc: %s", e)

        logger.info("Checkpoint location: %s", self.checkpoint_dir)
        logger.info("Trigger interval: %s (SLA < 5s)", _trigger)

        writer_query = (
            aggregated_stream.writeStream
            .outputMode("update")
            .foreachBatch(self.ch_writer.write_batch_from_spark)
            .option("checkpointLocation", self.checkpoint_dir)
        )

        if once:
            query = writer_query.trigger(availableNow=True).start()
        else:
            query = writer_query.trigger(processingTime=_trigger).start()

        logger.info(
            "Speed Layer Streaming Query dang chay... "
            "Nhan Ctrl+C de dung hoac: docker compose stop speed-layer"
        )
        return query


# ── CLI entry point ───────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Speed Layer -- Spark Structured Streaming Job\n"
            "Khuyen nghi chay qua Docker: docker compose up -d speed-layer"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--kafka", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=None, help="Kafka topic name")
    parser.add_argument("--watermark", default="1 minute", help="Watermark duration")
    parser.add_argument("--window", default="1 minute", help="Window duration")
    parser.add_argument("--trigger", default=None, help="Micro-batch trigger interval")
    parser.add_argument(
        "--once", action="store_true",
        help="Chay mot micro-batch duy nhat roi dung (test mode)",
    )
    parser.add_argument(
        "--skip-validation", action="store_true",
        help="Bo qua kiem tra moi truong truoc khi chay.",
    )
    args = parser.parse_args()

    if not args.skip_validation:
        if not validate_environment():
            logger.error(
                "Moi truong khong hop le. Khuyen nghi chay qua Docker:\n"
                "  docker compose up -d speed-layer\n"
                "  docker compose logs -f speed-layer"
            )
            sys.exit(1)

    spark = create_spark_session()
    job = SpeedLayerStreamingJob(
        spark=spark,
        kafka_bootstrap=args.kafka,
        topic=args.topic,
        watermark=args.watermark,
        window_duration=args.window,
    )

    query = job.run(trigger_interval=args.trigger, once=args.once)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Nhan tin hieu dung. Dang tat Speed Layer Streaming Query...")
        query.stop()
        logger.info("Speed Layer da dung.")


if __name__ == "__main__":
    main()
