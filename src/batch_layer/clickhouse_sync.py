"""
clickhouse_sync.py
==================
ClickHouse synchronization helpers for the Batch Layer.

The sync writes reconciled Gold candles into ``lakehouse.batch_agg`` and updates
``lakehouse.system_watermark`` for the Serving Layer query merger.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None

from src.utils.config import get_clickhouse_config
from src.utils.logger import setup_logger

logger = setup_logger("clickhouse_batch_sync")


class ClickHouseBatchSync:
    """Write reconciled batch aggregates and watermarks to ClickHouse."""

    DDL_INIT_DATABASE = "CREATE DATABASE IF NOT EXISTS lakehouse;"

    DDL_INIT_BATCH_AGG = """
    CREATE TABLE IF NOT EXISTS lakehouse.batch_agg
    (
        symbol          LowCardinality(String),
        window_start    DateTime64(3, 'UTC'),
        window_end      DateTime64(3, 'UTC'),
        open_price      Float64,
        high_price      Float64,
        low_price       Float64,
        close_price     Float64,
        volume          Float64,
        trade_count     UInt64,
        vwap            Float64,
        is_spike        UInt8 DEFAULT 0,
        batch_run_id    String,
        created_at      DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMMDD(window_start)
    ORDER BY (symbol, window_start);
    """

    DDL_INIT_WATERMARK = """
    CREATE TABLE IF NOT EXISTS lakehouse.system_watermark
    (
        layer           String,
        watermark_time  DateTime64(3, 'UTC'),
        updated_at      DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (layer);
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        cfg = get_clickhouse_config()
        self.host = host or cfg.host
        self.port = port or cfg.port
        self.user = user or cfg.user
        self.password = password or cfg.password
        self.database = database or cfg.database
        self._client = None

    def get_client(self):
        """Return a ClickHouse client or None when unavailable."""
        if clickhouse_connect is None:
            logger.warning("clickhouse-connect is not installed; skipping ClickHouse sync")
            return None
        if self._client is None:
            try:
                self._client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                )
            except Exception as exc:
                logger.warning("Cannot connect to ClickHouse at %s:%s: %s", self.host, self.port, exc)
                return None
        return self._client

    def initialize_tables(self) -> bool:
        """Create required ClickHouse tables when the database is reachable."""
        client = self.get_client()
        if client is None:
            return False
        client.command(self.DDL_INIT_DATABASE)
        client.command(self.DDL_INIT_BATCH_AGG)
        client.command(self.DDL_INIT_WATERMARK)
        return True

    def insert_batch_aggregates(self, candles: Iterable[Dict[str, Any]], batch_run_id: str) -> int:
        """Insert Gold candles into ``lakehouse.batch_agg``.

        Args:
            candles: Reconciled OHLCV records.
            batch_run_id: Identifier of the batch execution.

        Returns:
            Number of inserted rows. Returns 0 if ClickHouse is unavailable.
        """
        rows = list(candles)
        if not rows:
            return 0

        client = self.get_client()
        if client is None:
            return 0

        self.initialize_tables()
        now_utc = datetime.now(timezone.utc)
        data: List[List[Any]] = []
        for row in rows:
            data.append([
                row["symbol"],
                row["window_start"],
                row["window_end"],
                float(row["open_price"]),
                float(row["high_price"]),
                float(row["low_price"]),
                float(row["close_price"]),
                float(row["volume"]),
                int(row["trade_count"]),
                float(row["vwap"]),
                int(row.get("is_spike", 0)),
                batch_run_id,
                now_utc,
            ])

        client.insert(
            "lakehouse.batch_agg",
            data,
            column_names=[
                "symbol",
                "window_start",
                "window_end",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "trade_count",
                "vwap",
                "is_spike",
                "batch_run_id",
                "created_at",
            ],
        )
        logger.info("Inserted %s batch candles into lakehouse.batch_agg", len(data))
        return len(data)

    def update_watermark(self, watermark_time: datetime, layer: str = "batch_layer") -> bool:
        """Insert a new batch watermark row."""
        client = self.get_client()
        if client is None:
            return False

        self.initialize_tables()
        if watermark_time.tzinfo is None:
            watermark_time = watermark_time.replace(tzinfo=timezone.utc)
        client.insert(
            "lakehouse.system_watermark",
            [[layer, watermark_time, datetime.now(timezone.utc)]],
            column_names=["layer", "watermark_time", "updated_at"],
        )
        logger.info("Updated %s watermark to %s", layer, watermark_time.isoformat())
        return True
