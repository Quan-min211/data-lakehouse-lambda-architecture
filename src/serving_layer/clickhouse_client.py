"""
clickhouse_client.py
====================
Client truy vấn dữ liệu nến từ ClickHouse:
- lakehouse.batch_agg (Ground Truth - Reconciled)
- lakehouse.speed_agg (Near-realtime - Provisional)
"""

import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None

from src.utils.logger import setup_logger
from src.utils.config import get_clickhouse_config

logger = setup_logger("serving_ch_client")


class ClickHouseQueryClient:
    """Truy vấn các bảng tổng hợp trong ClickHouse."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: str = "lakehouse",
        mock_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ):
        ch_config = get_clickhouse_config()
        self.host = host or ch_config.host
        self.port = port or ch_config.port
        self.user = user or ch_config.user
        self.password = password or ch_config.password
        self.database = database
        self._mock_data = mock_data or {}
        self._client = None

    def set_mock_data(self, table: str, records: List[Dict[str, Any]]):
        """Thiết lập dữ liệu mock cho unit test."""
        self._mock_data[table] = records

    def get_client(self):
        """Khởi tạo kết nối ClickHouse."""
        if self._client is None:
            if clickhouse_connect is None:
                return None
            try:
                self._client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                    database=self.database,
                )
            except Exception as e:
                logger.warning(f"Chưa thể kết nối ClickHouse tại {self.host}:{self.port}: {e}")
                return None
        return self._client

    def query_candles(
        self,
        table: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Truy vấn nến từ table (batch_agg hoặc speed_agg) trong khoảng [start_time, end_time].
        """
        # Nếu có mock data cho table này (trong unit test)
        if table in self._mock_data:
            results = []
            for r in self._mock_data[table]:
                if r["symbol"].upper() != symbol.upper():
                    continue
                w_start = r["window_start"]
                w_end = r["window_end"]
                if isinstance(w_start, str):
                    w_start = datetime.fromisoformat(w_start)
                if isinstance(w_end, str):
                    w_end = datetime.fromisoformat(w_end)
                if w_start.tzinfo is None:
                    w_start = w_start.replace(tzinfo=timezone.utc)
                if w_end.tzinfo is None:
                    w_end = w_end.replace(tzinfo=timezone.utc)

                # Điều kiện lọc cửa sổ: window_start >= start_time và window_start < end_time
                if w_start >= start_time and w_start < end_time:
                    candle = dict(r)
                    candle["window_start"] = w_start
                    candle["window_end"] = w_end
                    results.append(candle)
            return sorted(results, key=lambda x: x["window_start"])

        # Truy vấn trực tiếp từ ClickHouse
        client = self.get_client()
        if client is None:
            return []

        # Format ISO UTC string cho ClickHouse
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

        query = f"""
        SELECT
            symbol,
            window_start,
            window_end,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            trade_count,
            vwap,
            is_spike
        FROM {self.database}.{table}
        WHERE symbol = '{symbol.upper()}'
          AND window_start >= toDateTime64('{start_str}', 3, 'UTC')
          AND window_start < toDateTime64('{end_str}', 3, 'UTC')
        ORDER BY window_start ASC
        """
        try:
            res = client.query(query)
            candles = []
            for row in res.result_rows:
                w_s = row[1]
                w_e = row[2]
                if isinstance(w_s, datetime) and w_s.tzinfo is None:
                    w_s = w_s.replace(tzinfo=timezone.utc)
                if isinstance(w_e, datetime) and w_e.tzinfo is None:
                    w_e = w_e.replace(tzinfo=timezone.utc)

                candles.append({
                    "symbol": row[0],
                    "window_start": w_s,
                    "window_end": w_e,
                    "open_price": float(row[3]),
                    "high_price": float(row[4]),
                    "low_price": float(row[5]),
                    "close_price": float(row[6]),
                    "volume": float(row[7]),
                    "trade_count": int(row[8]),
                    "vwap": float(row[9]),
                    "is_spike": int(row[10]),
                })
            return candles
        except Exception as e:
            logger.error(f"Lỗi khi query bảng {table}: {e}")
            return []
