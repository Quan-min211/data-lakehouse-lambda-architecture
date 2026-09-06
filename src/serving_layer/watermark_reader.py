"""
watermark_reader.py
===================
Đọc và quản lý mốc System Watermark từ ClickHouse table 'lakehouse.system_watermark'.
Mốc Watermark đại diện cho thời điểm dữ liệu đã được Batch Layer xử lý chuẩn xác (Ground Truth).
"""

import os
from datetime import datetime, timezone
from typing import Optional

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None

from src.utils.logger import setup_logger
from src.utils.config import get_clickhouse_config

logger = setup_logger("watermark_reader")


class WatermarkReader:
    """Đọc mốc Watermark từ ClickHouse hoặc từ bộ nhớ (khi test)."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        mock_watermark: Optional[datetime] = None,
    ):
        ch_config = get_clickhouse_config()
        self.host = host or ch_config.host
        self.port = port or ch_config.port
        self.user = user or ch_config.user
        self.password = password or ch_config.password
        self._mock_watermark = mock_watermark
        self._client = None

    def set_mock_watermark(self, wm: Optional[datetime]):
        """Dùng cho Unit Test hoặc môi trường Offline."""
        self._mock_watermark = wm

    def get_client(self):
        """Khởi tạo ClickHouse client."""
        if self._client is None:
            if clickhouse_connect is None:
                return None
            try:
                self._client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                )
            except Exception as e:
                logger.warning(f"Không thể kết nối ClickHouse để đọc Watermark: {e}")
                return None
        return self._client

    def get_watermark(self, layer: str = "batch_layer") -> datetime:
        """
        Lấy mốc Watermark mới nhất của layer được chỉ định.
        Mặc định: 'batch_layer'.
        """
        if self._mock_watermark is not None:
            return self._mock_watermark

        client = self.get_client()
        if client is not None:
            try:
                query = f"""
                SELECT watermark_time, updated_at
                FROM lakehouse.system_watermark
                WHERE layer = '{layer}'
                ORDER BY updated_at DESC
                LIMIT 1
                """
                res = client.query(query)
                if res.result_rows:
                    wm_time = res.result_rows[0][0]
                    if isinstance(wm_time, datetime):
                        if wm_time.tzinfo is None:
                            wm_time = wm_time.replace(tzinfo=timezone.utc)
                        return wm_time
            except Exception as e:
                logger.warning(f"Lỗi khi đọc bảng system_watermark: {e}")

        # Fallback an toàn: Trả về Epoch 1970 nếu chưa có watermark trong DB
        return datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
