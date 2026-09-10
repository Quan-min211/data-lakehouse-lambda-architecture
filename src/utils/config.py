"""
config.py
=========
Module doc va quan ly cac bien moi truong cau hinh cho cac dich vu.
Tu dong nhan dien moi truong Host (Windows/macOS) va Container (Linux) de fallback dung Port/Host.
"""

import os
from dataclasses import dataclass, field
from typing import List


def _resolve_host(env_var: str, default_host: str) -> str:
    val = os.getenv(env_var, default_host)
    # Neu chay tren Windows Host ma env tro vao container name -> tu dong chuyen ve localhost
    if os.name == "nt" and val in ["clickhouse", "kafka", "minio", "redis", "iceberg-rest"]:
        return "localhost"
    return val


@dataclass
class KafkaConfig:
    """Cau hinh ket noi Apache Kafka."""
    bootstrap_servers: str = "localhost:9094" if os.name == "nt" and os.getenv("KAFKA_BOOTSTRAP_SERVERS") in [None, "kafka:9092"] else os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    external_servers: str = os.getenv("KAFKA_EXTERNAL_SERVERS", "localhost:9094")
    topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "crypto_trades_raw")
    topic_dlq: str = os.getenv("KAFKA_TOPIC_DLQ", "crypto_trades_dlq")


@dataclass
class BinanceConfig:
    """Cau hinh ket noi Binance API va WebSocket."""
    rest_url: str = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
    ws_url: str = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
    top_n_coins: int = int(os.getenv("TOP_N_COINS", "10"))
    default_symbols_str: str = os.getenv(
        "DEFAULT_SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,DOTUSDT"
    )

    @property
    def symbol_list(self) -> List[str]:
        """Danh sach symbol phan tich tu chuoi cau hinh."""
        return [s.strip().upper() for s in self.default_symbols_str.split(",") if s.strip()]


@dataclass
class MinIOConfig:
    """Cau hinh ket noi MinIO Object Storage (S3 Lakehouse)."""
    endpoint: str = "http://localhost:9000" if os.name == "nt" and "minio:9000" in os.getenv("MINIO_ENDPOINT", "") else os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket_name: str = os.getenv("MINIO_BUCKET_NAME", "lakehouse-bronze")


@dataclass
class ClickHouseConfig:
    """Cau hinh ket noi ClickHouse OLAP Database."""
    host: str = _resolve_host("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    database: str = os.getenv("CLICKHOUSE_DB", "lakehouse")


@dataclass
class AppConfig:
    """Tong hop cau hinh toan bo he thong."""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


config = AppConfig()


def get_kafka_config() -> KafkaConfig:
    return KafkaConfig()


def get_clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig()


def get_binance_config() -> BinanceConfig:
    return BinanceConfig()


def get_minio_config() -> MinIOConfig:
    return MinIOConfig()
