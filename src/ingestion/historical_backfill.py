"""
historical_backfill.py
======================
Module nạp dữ liệu lịch sử (Historical Backfill) từ Binance REST API hoặc file CSV/Parquet mẫu.
Phục vụ chạy Spark Batch Layer và bài toán kiểm thử đối soát.
"""

import time
import requests
from typing import List, Optional
from .models import TradeEvent
from .kafka_producer import ResilientKafkaProducer
from ..utils.config import config
from ..utils.logger import setup_logger

logger = setup_logger("historical_backfill")


class HistoricalBackfillProducer:
    """Nạp dữ liệu lịch sử vào hệ thống."""

    def __init__(self, kafka_producer: Optional[ResilientKafkaProducer] = None):
        self.producer = kafka_producer

    def fetch_historical_agg_trades(
        self,
        symbol: str = "BTCUSDT",
        limit: int = 1000,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None
    ) -> List[TradeEvent]:
        """Lấy các giao dịch lịch sử từ Binance /api/v3/aggTrades.

        Args:
            symbol (str): Cặp giao dịch (ví dụ: BTCUSDT).
            limit (int): Số lượng bản ghi tối đa (tối đa 1000).
            start_time_ms (int, optional): Mốc thời gian bắt đầu (epoch ms).
            end_time_ms (int, optional): Mốc thời gian kết thúc (epoch ms).

        Returns:
            List[TradeEvent]: Danh sách các sự kiện giao dịch chuẩn hóa.
        """
        url = f"{config.binance.rest_url}/api/v3/aggTrades"
        params = {
            "symbol": symbol.upper(),
            "limit": min(limit, 1000)
        }
        if start_time_ms:
            params["startTime"] = start_time_ms
        if end_time_ms:
            params["endTime"] = end_time_ms

        logger.info(f"Đang lấy dữ liệu lịch sử từ Binance: {params}")
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            trades_raw = resp.json()

            events: List[TradeEvent] = []
            for t in trades_raw:
                # Format aggTrades: a=aggTradeId, p=price, q=qty, f=firstTradeId, l=lastTradeId, T=time, m=isBuyerMaker
                evt = TradeEvent(
                    trade_id=int(t["a"]),
                    symbol=symbol.upper(),
                    price=float(t["p"]),
                    quantity=float(t["q"]),
                    trade_time=int(t["T"]),
                    is_buyer_maker=bool(t["m"]),
                    ingestion_time=int(time.time() * 1000),
                    is_injected=False
                )
                events.append(evt)

            logger.info(f"Đã tải thành công {len(events)} bản ghi lịch sử cho {symbol}.")
            return events
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu lịch sử: {e}")
            return []

    def backfill_to_kafka(self, events: List[TradeEvent]) -> int:
        """Gửi danh sách sự kiện lịch sử vào Kafka topic."""
        if not self.producer:
            self.producer = ResilientKafkaProducer()

        success_count = 0
        for evt in events:
            if self.producer.send_trade(evt):
                success_count += 1

        self.producer.flush()
        logger.info(f"Đã hoàn thành nạp {success_count}/{len(events)} bản ghi lịch sử vào Kafka.")
        return success_count
