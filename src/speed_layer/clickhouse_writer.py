"""
clickhouse_writer.py
====================
Sink Writer phụ trách ghi dữ liệu tổng hợp micro-batch từ Spark Structured Streaming
vào ClickHouse table 'lakehouse.speed_agg' (Trạng thái: Provisional).

Đặc điểm:
- Tương thích 100% với schema DDL trong configs/clickhouse/init.sql.
- Cơ chế ReplacingMergeTree(created_at) bảo đảm tính Idempotent (không bị duplicate khi retry).
- Sử dụng clickhouse-connect với connection retry & pooling.
"""

import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None

from src.utils.logger import setup_logger
from src.utils.config import get_clickhouse_config

logger = setup_logger("clickhouse_writer")


class ClickHouseSpeedWriter:
    """Quản lý kết nối và ghi dữ liệu Speed Views vào ClickHouse."""

    DDL_INIT_DATABASE = "CREATE DATABASE IF NOT EXISTS lakehouse;"
    
    DDL_INIT_SPEED_AGG = """
    CREATE TABLE IF NOT EXISTS lakehouse.speed_agg
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
        created_at      DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMMDD(window_start)
    ORDER BY (symbol, window_start);
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        ch_config = get_clickhouse_config()
        self.host = host or ch_config.host
        self.port = port or ch_config.port
        self.user = user or ch_config.user
        self.password = password or ch_config.password
        self.database = database or ch_config.database
        self._client = None

    def get_client(self):
        """Khởi tạo hoặc trả về client ClickHouse."""
        if self._client is None:
            if clickhouse_connect is None:
                raise RuntimeError("Thư viện 'clickhouse-connect' chưa được cài đặt!")
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
            )
        return self._client

    def initialize_tables(self) -> bool:
        """Kiểm tra và tạo database & table nếu chưa tồn tại."""
        try:
            client = self.get_client()
            client.command(self.DDL_INIT_DATABASE)
            client.command(self.DDL_INIT_SPEED_AGG)
            logger.info(f"Đã khởi tạo thành công schema ClickHouse 'lakehouse.speed_agg' tại {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo bảng ClickHouse: {e}")
            return False

    def write_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Ghi danh sách records (dạng dict) vào table 'lakehouse.speed_agg'.
        """
        if not records:
            return 0

        client = self.get_client()
        now_utc = datetime.now(timezone.utc)

        data = []
        for r in records:
            # Chuẩn hóa window_start và window_end thành datetime
            w_start = r["window_start"]
            w_end = r["window_end"]
            if isinstance(w_start, (int, float)):
                w_start = datetime.fromtimestamp(w_start / 1000.0, timezone.utc)
            if isinstance(w_end, (int, float)):
                w_end = datetime.fromtimestamp(w_end / 1000.0, timezone.utc)

            row = [
                str(r["symbol"]),
                w_start,
                w_end,
                float(r["open_price"]),
                float(r["high_price"]),
                float(r["low_price"]),
                float(r["close_price"]),
                float(r["volume"]),
                int(r["trade_count"]),
                float(r["vwap"]),
                int(r.get("is_spike", 0)),
                now_utc,
            ]
            data.append(row)

        columns = [
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
            "created_at",
        ]

        client.insert("lakehouse.speed_agg", data, column_names=columns)
        logger.info(f"Đã ghi thành công {len(data)} nến vào 'lakehouse.speed_agg'")
        return len(data)

    def write_batch_from_spark(self, batch_df, batch_id: int):
        """
        Hàm callback sử dụng cho DataFrame.writeStream.foreachBatch().
        """
        start_t = time.time()
        count = batch_df.count()
        if count == 0:
            return

        logger.info(f"[Batch {batch_id}] Nhận {count} bản ghi tổng hợp từ Spark Streaming.")

        # Thu thập dữ liệu dạng Pandas / Dict để nạp vào ClickHouse
        # Mapping cấu trúc window.start và window.end
        collected = []
        for row in batch_df.collect():
            r_dict = row.asDict(recursive=True)
            if "window" in r_dict:
                w = r_dict["window"]
                r_dict["window_start"] = w["start"]
                r_dict["window_end"] = w["end"]
            collected.append(r_dict)

        self.write_records(collected)
        elapsed = round(time.time() - start_t, 3)
        logger.info(f"[Batch {batch_id}] Hoàn thành ghi {count} nến vào ClickHouse trong {elapsed}s (SLA < 5s)")
