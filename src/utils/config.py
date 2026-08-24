"""
config.py
=========
Module đọc và quản lý các biến môi trường cấu hình cho các dịch vụ.
Đảm bảo tính bảo mật và giá trị mặc định an toàn.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class KafkaConfig:
    """Cấu hình kết nối Apache Kafka."""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    external_servers: str = os.getenv("KAFKA_EXTERNAL_SERVERS", "localhost:9094")
    topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "crypto_trades_raw")
    topic_dlq: str = os.getenv("KAFKA_TOPIC_DLQ", "crypto_trades_dlq")


@dataclass
class BinanceConfig:
    """Cấu hình kết nối Binance API và WebSocket."""
    rest_url: str = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
    ws_url: str = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
    top_n_coins: int = int(os.getenv("TOP_N_COINS", "10"))
    default_symbols_str: str = os.getenv(
        "DEFAULT_SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,DOTUSDT"
    )

    @property
    def symbol_list(self) -> List[str]:
        """Danh sách symbol phân tách từ chuỗi cấu hình."""
        return [s.strip().upper() for s in self.default_symbols_str.split(",") if s.strip()]


@dataclass
class MinIOConfig:
    """Cấu hình kết nối MinIO Object Storage (S3 Lakehouse)."""
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    bucket_name: str = os.getenv("MINIO_BUCKET_NAME", "lakehouse-bronze")


@dataclass
class ClickHouseConfig:
    """Cấu hình kết nối ClickHouse OLAP Database."""
    host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    database: str = os.getenv("CLICKHOUSE_DB", "lakehouse")


@dataclass
class AppConfig:
    """Tổng hợp cấu hình toàn bộ hệ thống."""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


config = AppConfig()
