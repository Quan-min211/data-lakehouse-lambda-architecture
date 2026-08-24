"""
src/ingestion
=============
Module thu thập, chuẩn hóa và nạp dữ liệu giao dịch tiền mã hóa.
Bao gồm WebSocket Producer, Fault Injector, Kafka Producer và Historical Backfill.
"""

from .models import TradeEvent, DLQEvent
from .fault_injector import FaultInjector
from .kafka_producer import ResilientKafkaProducer
from .binance_ws import BinanceWebSocketProducer
from .historical_backfill import HistoricalBackfillProducer

__all__ = [
    "TradeEvent",
    "DLQEvent",
    "FaultInjector",
    "ResilientKafkaProducer",
    "BinanceWebSocketProducer",
    "HistoricalBackfillProducer"
]
