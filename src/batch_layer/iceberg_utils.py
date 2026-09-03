"""
iceberg_utils.py
================
Module tiện ích cho Apache Iceberg — Batch Layer Foundation.

Chức năng chính:
  - Khởi tạo Spark Session với cấu hình Iceberg REST Catalog + MinIO S3
  - Tạo và quản lý Bronze Table (bronze.crypto_trades)
  - Ghi dữ liệu TradeEvent vào Bronze (Append-only, ACID)
  - Đọc Bronze với filter theo partition (symbol, date range)
  - Truy vấn Snapshot history (Time Travel)

Sử dụng:
  from src.batch_layer.iceberg_utils import IcebergTableManager, get_spark_session

  spark = get_spark_session("BronzeWriter")
  mgr   = IcebergTableManager(spark)
  mgr.create_bronze_table()
  mgr.write_to_bronze(df, batch_run_id="batch_001")
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, BooleanType, TimestampType
)

from ..utils.config import config
from ..utils.logger import setup_logger

logger = setup_logger("iceberg_utils")

# ── Catalog & Table Constants ─────────────────────────────────────────────────
CATALOG_NAME     = "iceberg_catalog"
BRONZE_NAMESPACE = "bronze"
BRONZE_TABLE     = "crypto_trades"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE   = "gold"

BRONZE_FULL_NAME = f"{CATALOG_NAME}.{BRONZE_NAMESPACE}.{BRONZE_TABLE}"

# ── Bronze Table Schema ───────────────────────────────────────────────────────
# Khớp hoàn toàn với src/ingestion/models.py — TradeEvent dataclass
BRONZE_SCHEMA = StructType([
    StructField("trade_id",         LongType(),      nullable=False),
    StructField("symbol",           StringType(),    nullable=False),
    StructField("price",            DoubleType(),    nullable=False),
    StructField("quantity",         DoubleType(),    nullable=False),
    StructField("trade_time",       LongType(),      nullable=False),  # epoch ms
    StructField("trade_time_ts",    TimestampType(), nullable=False),  # Partition column
    StructField("is_buyer_maker",   BooleanType(),   nullable=True),
    StructField("ingestion_time",   LongType(),      nullable=True),
    StructField("is_injected",      BooleanType(),   nullable=True),
    StructField("fault_type",       StringType(),    nullable=True),
    StructField("batch_run_id",     StringType(),    nullable=True),
    StructField("bronze_written_at", TimestampType(), nullable=True),
])

# DDL cho Spark SQL (dùng khi tạo bảng lần đầu)
BRONZE_CREATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_FULL_NAME} (
    trade_id         BIGINT        NOT NULL  COMMENT 'Aggregate Trade ID từ Binance — khóa dedup',
    symbol           STRING        NOT NULL  COMMENT 'Cặp giao dịch uppercase: BTCUSDT, ETHUSDT...',
    price            DOUBLE        NOT NULL  COMMENT 'Giá khớp lệnh',
    quantity         DOUBLE        NOT NULL  COMMENT 'Khối lượng aggregate trade',
    trade_time       BIGINT        NOT NULL  COMMENT 'Thời điểm khớp lệnh — epoch milliseconds UTC',
    trade_time_ts    TIMESTAMP     NOT NULL  COMMENT 'trade_time dạng Timestamp — dùng để partition',
    is_buyer_maker   BOOLEAN                 COMMENT 'True: Taker bán (short), False: Taker mua',
    ingestion_time   BIGINT                  COMMENT 'Thời điểm producer nhận message — epoch ms',
    is_injected      BOOLEAN                 COMMENT 'True: bản ghi được tiêm lỗi bởi FaultInjector',
    fault_type       STRING                  COMMENT 'duplicate | late_data | out_of_order | schema_invalid',
    batch_run_id     STRING                  COMMENT 'ID của Batch Job lần ghi',
    bronze_written_at TIMESTAMP              COMMENT 'Thời điểm ghi vào Bronze table'
)
USING iceberg
PARTITIONED BY (days(trade_time_ts))
TBLPROPERTIES (
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes'    = '134217728',
    'write.distribution-mode'         = 'hash',
    'history.expire.max-snapshot-age-ms' = '604800000',
    'write.metadata.metrics.default'  = 'truncate(16)',
    'format-version'                  = '2'
)
"""


# ── Spark Session Factory ──────────────────────────────────────────────────────

def get_spark_session(app_name: str = "LambdaLakehouse-Batch") -> SparkSession:
    """
    Khởi tạo Spark Session với đầy đủ cấu hình Iceberg REST Catalog + MinIO S3A.

    Đọc thông tin kết nối từ biến môi trường / config:
      - MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
      - ICEBERG_REST_URI

    Args:
        app_name: Tên Spark Application hiển thị trên Spark UI.

    Returns:
        SparkSession đã cấu hình đầy đủ.
    """
    minio_endpoint  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    iceberg_uri     = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")

    logger.info(f"Khởi tạo SparkSession: {app_name}")
    logger.info(f"  Iceberg REST  : {iceberg_uri}")
    logger.info(f"  MinIO endpoint: {minio_endpoint}")

    spark = (
        SparkSession.builder
        .appName(app_name)

        # ── Iceberg Extensions ──────────────────────────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        # ── Iceberg REST Catalog ────────────────────────────────────────────
        .config(f"spark.sql.catalog.{CATALOG_NAME}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", iceberg_uri)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", "s3://warehouse/")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id", minio_access)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key", minio_secret)

        # ── Hadoop S3A (dùng spark.read.parquet, v.v.) ─────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",              minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",            minio_access)
        .config("spark.hadoop.fs.s3a.secret.key",            minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ── Tài nguyên Spark ────────────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism",    "4")
        .config("spark.driver.memory",          "1g")
        .config("spark.executor.memory",        "2g")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✓ SparkSession khởi tạo thành công")
    return spark


# ── IcebergTableManager ────────────────────────────────────────────────────────

class IcebergTableManager:
    """
    Quản lý vòng đời của các Iceberg Tables trong Lambda Architecture.

    Chịu trách nhiệm:
    - Tạo namespace và bảng Bronze / Silver / Gold
    - Ghi dữ liệu TradeEvent vào Bronze (Append-only, ACID)
    - Đọc Bronze với filter theo partition
    - Truy vấn Snapshot history (Time Travel, Audit)
    """

    def __init__(self, spark: SparkSession):
        """
        Args:
            spark: SparkSession đã được cấu hình Iceberg catalog.
        """
        self.spark = spark
        logger.info(f"IcebergTableManager khởi tạo — Catalog: {CATALOG_NAME}")

    # ── Namespace Management ──────────────────────────────────────────────────

    def create_namespace_if_not_exists(self, namespace: str) -> None:
        """Tạo Iceberg namespace nếu chưa tồn tại.

        Args:
            namespace: Tên namespace (ví dụ: 'bronze', 'silver', 'gold').
        """
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{namespace}")
            logger.info(f"✓ Namespace sẵn sàng: {CATALOG_NAME}.{namespace}")
        except Exception as e:
            logger.error(f"Lỗi tạo namespace {namespace}: {e}")
            raise

    def create_all_namespaces(self) -> None:
        """Tạo tất cả namespaces cần thiết (bronze, silver, gold)."""
        for ns in [BRONZE_NAMESPACE, SILVER_NAMESPACE, GOLD_NAMESPACE]:
            self.create_namespace_if_not_exists(ns)

    # ── Bronze Table ──────────────────────────────────────────────────────────

    def create_bronze_table(self) -> None:
        """
        Tạo Bronze Table `bronze.crypto_trades` với Iceberg format version 2.

        Chiến lược phân vùng: days(trade_time_ts)
        - Mỗi partition = 1 ngày giao dịch (UTC)
        - Hidden Partition — Iceberg tự quản lý tên folder
        - Tránh Small File Problem bằng target-file-size 128MB

        Snapshot Isolation:
        - Format version 2 hỗ trợ Row-level Delete (position + equality deletes)
        - Mỗi lần ghi tạo 1 snapshot mới — có thể rollback về bất kỳ điểm nào
        """
        logger.info(f"Đang tạo Bronze table: {BRONZE_FULL_NAME}")
        try:
            self.create_namespace_if_not_exists(BRONZE_NAMESPACE)
            self.spark.sql(BRONZE_CREATE_DDL)
            logger.info(f"✓ Bronze table sẵn sàng: {BRONZE_FULL_NAME}")
        except Exception as e:
            logger.error(f"Lỗi tạo Bronze table: {e}")
            raise

    def bronze_table_exists(self) -> bool:
        """Kiểm tra Bronze table đã tồn tại chưa."""
        try:
            self.spark.sql(f"DESCRIBE TABLE {BRONZE_FULL_NAME}")
            return True
        except Exception:
            return False

    # ── Write Operations ──────────────────────────────────────────────────────

    def _enrich_for_bronze(self, df: DataFrame, batch_run_id: str) -> DataFrame:
        """
        Làm giàu DataFrame trước khi ghi vào Bronze:
        - Thêm cột `trade_time_ts` (Timestamp) từ `trade_time` (epoch ms)
        - Thêm `batch_run_id` để truy vết batch job
        - Thêm `bronze_written_at` timestamp hiện tại

        Args:
            df:           DataFrame chứa raw TradeEvent records.
            batch_run_id: ID unique của batch job (dùng để audit).

        Returns:
            DataFrame đã được làm giàu, sẵn sàng ghi vào Iceberg.
        """
        return (
            df
            .withColumn(
                "trade_time_ts",
                (F.col("trade_time") / 1000).cast(TimestampType())
            )
            .withColumn("batch_run_id",      F.lit(batch_run_id))
            .withColumn("bronze_written_at", F.current_timestamp())
            .select([field.name for field in BRONZE_SCHEMA.fields])
        )

    def write_to_bronze(
        self,
        df: DataFrame,
        batch_run_id: Optional[str] = None,
    ) -> dict:
        """
        Ghi DataFrame TradeEvent vào Bronze Iceberg table (Append-only).

        Tính năng:
        - Tạo bảng tự động nếu chưa tồn tại
        - Append-only (không xóa dữ liệu cũ — immutable raw truth)
        - Mỗi lần ghi tạo 1 Snapshot mới (Snapshot Isolation)

        Args:
            df:           DataFrame có schema TradeEvent (từ Kafka hoặc CSV).
            batch_run_id: ID batch job (auto-generate nếu None).

        Returns:
            dict với thông tin về snapshot vừa tạo.
        """
        if batch_run_id is None:
            batch_run_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        if not self.bronze_table_exists():
            logger.info("Bronze table chưa tồn tại — tự động tạo...")
            self.create_bronze_table()

        logger.info(f"Bắt đầu ghi vào Bronze — batch_run_id: {batch_run_id}")
        t_start = time.time()

        try:
            enriched_df = self._enrich_for_bronze(df, batch_run_id)
            record_count = enriched_df.count()

            enriched_df.writeTo(BRONZE_FULL_NAME).append()

            elapsed = time.time() - t_start
            snapshot_id = self._get_latest_snapshot_id()

            logger.info(f"✓ Ghi Bronze thành công: {record_count:,} records | "
                        f"{elapsed:.1f}s | snapshot_id={snapshot_id}")

            return {
                "batch_run_id":  batch_run_id,
                "record_count":  record_count,
                "snapshot_id":   snapshot_id,
                "elapsed_s":     round(elapsed, 2),
                "written_at":    datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Lỗi ghi vào Bronze: {e}")
            raise

    # ── Read Operations ───────────────────────────────────────────────────────

    def read_bronze(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        snapshot_id: Optional[int] = None,
    ) -> DataFrame:
        """
        Đọc dữ liệu từ Bronze table với filter tùy chọn.

        Hỗ trợ Time Travel (đọc theo snapshot_id cụ thể) cho audit và rollback.

        Args:
            symbol:      Lọc theo symbol (ví dụ: 'BTCUSDT'). None = tất cả.
            start_date:  Lọc từ ngày (ISO date: '2026-01-01'). None = không giới hạn.
            end_date:    Lọc đến ngày (ISO date: '2026-01-31'). None = không giới hạn.
            snapshot_id: Đọc theo snapshot cụ thể (Time Travel). None = latest.

        Returns:
            DataFrame với dữ liệu Bronze đã được filter.
        """
        logger.info(f"Đọc Bronze: symbol={symbol}, from={start_date}, to={end_date}, "
                    f"snapshot={snapshot_id}")
        try:
            if snapshot_id:
                # Time Travel — đọc theo snapshot_id cụ thể
                df = (
                    self.spark.read
                    .option("snapshot-id", str(snapshot_id))
                    .format("iceberg")
                    .load(BRONZE_FULL_NAME)
                )
            else:
                df = self.spark.table(BRONZE_FULL_NAME)

            # Áp dụng filters
            if symbol:
                df = df.filter(F.col("symbol") == symbol.upper())

            if start_date:
                df = df.filter(F.col("trade_time_ts") >= F.lit(start_date).cast(TimestampType()))

            if end_date:
                df = df.filter(F.col("trade_time_ts") <= F.lit(end_date).cast(TimestampType()))

            return df

        except Exception as e:
            logger.error(f"Lỗi đọc Bronze: {e}")
            raise

    # ── Snapshot & Metadata ───────────────────────────────────────────────────

    def _get_latest_snapshot_id(self) -> Optional[int]:
        """Lấy snapshot_id mới nhất của Bronze table."""
        try:
            row = (
                self.spark.sql(
                    f"SELECT snapshot_id FROM {CATALOG_NAME}.{BRONZE_NAMESPACE}"
                    f".{BRONZE_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1"
                )
                .collect()
            )
            return int(row[0]["snapshot_id"]) if row else None
        except Exception:
            return None

    def get_snapshot_history(self, limit: int = 10) -> DataFrame:
        """
        Lấy lịch sử Snapshots của Bronze table.

        Mỗi snapshot tương ứng với 1 lần ghi (Append), 1 lần Compaction,
        hoặc 1 lần xóa snapshot cũ.

        Args:
            limit: Số snapshot tối đa hiển thị (newest first).

        Returns:
            DataFrame gồm: snapshot_id, committed_at, operation, summary.
        """
        return self.spark.sql(
            f"SELECT snapshot_id, committed_at, operation, summary "
            f"FROM {CATALOG_NAME}.{BRONZE_NAMESPACE}.{BRONZE_TABLE}.snapshots "
            f"ORDER BY committed_at DESC "
            f"LIMIT {limit}"
        )

    def get_file_stats(self) -> dict:
        """
        Lấy thông tin về data files trong Bronze table.
        Dùng để so sánh TRƯỚC và SAU compaction (Benchmark 3).

        Returns:
            dict với: file_count, total_size_mb, avg_size_mb, min_size_mb, max_size_mb
        """
        try:
            stats_df = self.spark.sql(
                f"SELECT "
                f"  COUNT(*) as file_count, "
                f"  SUM(file_size_in_bytes) / 1048576.0 as total_size_mb, "
                f"  AVG(file_size_in_bytes) / 1048576.0 as avg_size_mb, "
                f"  MIN(file_size_in_bytes) / 1048576.0 as min_size_mb, "
                f"  MAX(file_size_in_bytes) / 1048576.0 as max_size_mb "
                f"FROM {CATALOG_NAME}.{BRONZE_NAMESPACE}.{BRONZE_TABLE}.files"
            ).collect()[0]

            result = {
                "file_count":    int(stats_df["file_count"]),
                "total_size_mb": round(float(stats_df["total_size_mb"] or 0), 2),
                "avg_size_mb":   round(float(stats_df["avg_size_mb"] or 0), 2),
                "min_size_mb":   round(float(stats_df["min_size_mb"] or 0), 2),
                "max_size_mb":   round(float(stats_df["max_size_mb"] or 0), 2),
            }
            logger.info(f"File stats: {result}")
            return result

        except Exception as e:
            logger.error(f"Lỗi lấy file stats: {e}")
            return {}

    def get_partition_stats(self) -> DataFrame:
        """
        Lấy thống kê theo từng partition (ngày giao dịch).

        Returns:
            DataFrame gồm: partition, record_count, file_count, total_size_mb
        """
        return self.spark.sql(
            f"SELECT "
            f"  partition, "
            f"  record_count, "
            f"  file_count, "
            f"  ROUND(total_data_file_size_in_bytes / 1048576.0, 2) as total_size_mb "
            f"FROM {CATALOG_NAME}.{BRONZE_NAMESPACE}.{BRONZE_TABLE}.partitions "
            f"ORDER BY partition DESC"
        )

    def get_table_row_count(self) -> int:
        """Đếm tổng số records trong Bronze table."""
        try:
            count = self.spark.sql(
                f"SELECT COUNT(*) as cnt FROM {BRONZE_FULL_NAME}"
            ).collect()[0]["cnt"]
            logger.info(f"Bronze table row count: {count:,}")
            return int(count)
        except Exception as e:
            logger.error(f"Lỗi đếm records: {e}")
            return -1

    # ── Utility ───────────────────────────────────────────────────────────────

    def load_csv_to_dataframe(self, csv_path: str) -> DataFrame:
        """
        Đọc file CSV (klines hoặc mock data) vào Spark DataFrame.
        Tiện ích để seed Bronze table từ datasets/raw/ hoặc datasets/mock/.

        Args:
            csv_path: Đường dẫn tới file CSV (local hoặc s3a://).

        Returns:
            DataFrame với schema phù hợp để ghi vào Bronze.
        """
        raw_df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_path)
        )

        # Đảm bảo các cột bắt buộc tồn tại và đúng kiểu
        required_cols = ["trade_id", "symbol", "price", "quantity", "trade_time"]
        for col in required_cols:
            if col not in raw_df.columns:
                raise ValueError(f"File CSV thiếu cột bắt buộc: {col}")

        # Thêm các cột tùy chọn nếu chưa có
        if "is_injected" not in raw_df.columns:
            raw_df = raw_df.withColumn("is_injected", F.lit(False))
        if "fault_type" not in raw_df.columns:
            raw_df = raw_df.withColumn("fault_type", F.lit(None).cast(StringType()))
        if "ingestion_time" not in raw_df.columns:
            raw_df = raw_df.withColumn("ingestion_time", F.lit(None).cast(LongType()))
        if "is_buyer_maker" not in raw_df.columns:
            raw_df = raw_df.withColumn("is_buyer_maker", F.lit(False))

        return raw_df.select(
            F.col("trade_id").cast(LongType()),
            F.col("symbol").cast(StringType()),
            F.col("price").cast(DoubleType()),
            F.col("quantity").cast(DoubleType()),
            F.col("trade_time").cast(LongType()),
            F.col("is_buyer_maker").cast(BooleanType()),
            F.col("ingestion_time").cast(LongType()),
            F.col("is_injected").cast(BooleanType()),
            F.col("fault_type").cast(StringType()),
        )
