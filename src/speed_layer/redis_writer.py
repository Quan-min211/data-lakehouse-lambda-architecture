"""
redis_writer.py
===============
Hot-cache writer for Speed Layer candles.

The Speed Layer still writes durable provisional candles to ClickHouse, while
Redis stores the latest low-latency copy for dashboard and serving extensions.
Connection failures are logged and do not fail the streaming micro-batch.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import redis
except ImportError:
    redis = None

from src.utils.config import get_redis_config
from src.utils.logger import setup_logger

logger = setup_logger("redis_speed_writer")


class RedisSpeedWriter:
    """Write provisional Speed View candles into Redis with TTL."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        client: Any = None,
        key_prefix: str = "speed:candle",
    ):
        cfg = get_redis_config()
        self.host = host or cfg.host
        self.port = port or cfg.port
        self.password = password if password is not None else cfg.password
        self.ttl_seconds = ttl_seconds or cfg.ttl_seconds
        self.key_prefix = key_prefix
        self._client = client
        self._available = True

    def get_client(self):
        """Return a Redis client, or None when Redis is unavailable."""
        if self._client is not None:
            return self._client
        if redis is None:
            logger.warning("redis package is not installed; Speed Redis cache is disabled")
            self._available = False
            return None
        if not self._available:
            return None
        try:
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password or None,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            self._client.ping()
            return self._client
        except Exception as exc:
            logger.warning("Redis Speed cache unavailable at %s:%s: %s", self.host, self.port, exc)
            self._available = False
            return None

    @staticmethod
    def _to_datetime(value: Any) -> datetime:
        """Normalize timestamps from Spark/Python records to timezone-aware UTC datetimes."""
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value) / 1000.0, timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        raise ValueError(f"Unsupported timestamp value: {value!r}")

    def make_key(self, symbol: str, window_start: datetime) -> str:
        """Build the Redis key for one candle."""
        epoch_ms = int(window_start.timestamp() * 1000)
        return f"{self.key_prefix}:{symbol.upper()}:{epoch_ms}"

    def normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a candle record to JSON-safe primitive values."""
        window_start = self._to_datetime(record["window_start"])
        window_end = self._to_datetime(record["window_end"])
        return {
            "symbol": str(record["symbol"]).upper(),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "open_price": float(record["open_price"]),
            "high_price": float(record["high_price"]),
            "low_price": float(record["low_price"]),
            "close_price": float(record["close_price"]),
            "volume": float(record["volume"]),
            "trade_count": int(record["trade_count"]),
            "vwap": float(record["vwap"]),
            "is_spike": int(record.get("is_spike", 0)),
            "status": "provisional",
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }

    def write_records(self, records: List[Dict[str, Any]]) -> int:
        """Write candle records to Redis and return the number cached."""
        if not records:
            return 0

        client = self.get_client()
        if client is None:
            return 0

        written = 0
        try:
            pipe = client.pipeline()
            for record in records:
                normalized = self.normalize_record(record)
                window_start = self._to_datetime(record["window_start"])
                key = self.make_key(normalized["symbol"], window_start)
                latest_key = f"{self.key_prefix}:{normalized['symbol']}:latest"
                payload = json.dumps(normalized, separators=(",", ":"))

                pipe.set(key, payload, ex=self.ttl_seconds)
                pipe.set(latest_key, payload, ex=self.ttl_seconds)
                written += 1
            pipe.execute()
            logger.info("Cached %d Speed candles in Redis", written)
            return written
        except Exception as exc:
            logger.warning("Failed to write Speed candles to Redis: %s", exc)
            return 0
