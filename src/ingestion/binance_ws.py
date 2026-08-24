"""
binance_ws.py
=============
Module kết nối trực tiếp WebSocket sàn Binance (Live Trade Stream).
Tự động lấy Top N coin có thanh khoản cao nhất và thiết lập multiplexed connection.
"""

import json
import time
import requests
import websocket
from typing import List, Optional

from .models import TradeEvent, DLQEvent
from .fault_injector import FaultInjector
from .kafka_producer import ResilientKafkaProducer
from ..utils.config import config
from ..utils.logger import setup_logger

logger = setup_logger("binance_ws")


class BinanceWebSocketProducer:
    """Client WebSocket thu thập dữ liệu giao dịch thời gian thực từ Binance."""

    def __init__(
        self,
        fault_injector: Optional[FaultInjector] = None,
        kafka_producer: Optional[ResilientKafkaProducer] = None,
        top_n: int = 10
    ):
        self.top_n = top_n
        self.fault_injector = fault_injector or FaultInjector()
        self.producer = kafka_producer
        self.ws: Optional[websocket.WebSocketApp] = None
        self.symbols: List[str] = []
        self.total_messages_received = 0
        self.total_events_published = 0

    def fetch_top_symbols(self) -> List[str]:
        """Gọi Binance REST API /api/v3/ticker/24hr để lấy Top N cặp coin có quoteVolume lớn nhất."""
        url = f"{config.binance.rest_url}/api/v3/ticker/24hr"
        logger.info(f"Đang lấy danh sách Top {self.top_n} coin thanh khoản cao nhất từ Binance API...")
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            tickers = resp.json()

            # Lọc chỉ các cặp USDT (loại trừ UP, DOWN, BEAR, BULL tokens)
            usdt_pairs = [
                t for t in tickers
                if t["symbol"].endswith("USDT")
                and not any(x in t["symbol"] for x in ["UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT"])
            ]
            # Sắp xếp giảm dần theo giá trị giao dịch 24h
            usdt_pairs.sort(key=lambda x: float(x.get("quoteVolume", 0.0)), reverse=True)
            top_pairs = [p["symbol"] for p in usdt_pairs[:self.top_n]]

            logger.info(f"Top {len(top_pairs)} coin được chọn: {top_pairs}")
            self.symbols = top_pairs
            return top_pairs
        except Exception as e:
            logger.warning(f"Không thể gọi Binance REST API ({e}), sử dụng danh sách mặc định.")
            self.symbols = config.binance.symbol_list[:self.top_n]
            return self.symbols

    def build_ws_url(self) -> str:
        """Xây dựng Combined Stream URL cho nhiều coin cùng lúc."""
        if not self.symbols:
            self.fetch_top_symbols()
        streams = "/".join(f"{s.lower()}@aggTrade" for s in self.symbols)
        ws_url = f"{config.binance.ws_url}?streams={streams}"
        logger.info(f"WebSocket URL được khởi tạo: {ws_url}")
        return ws_url

    def on_message(self, ws, message: str):
        """Xử lý sự kiện tin nhắn nhận được từ Binance WebSocket."""
        self.total_messages_received += 1
        try:
            payload = json.loads(message)
            # Dạng combined stream: {"stream": "btcusdt@aggTrade", "data": {...}}
            data = payload.get("data", payload)

            if not isinstance(data, dict) or "s" not in data or "p" not in data:
                # Đẩy vào DLQ nếu cấu trúc không chuẩn
                if self.producer:
                    self.producer.send_dlq(
                        DLQEvent(
                            raw_payload=message,
                            error_reason="Missing required trade fields (s, p, q, T)",
                            source_stream="binance_ws"
                        )
                    )
                return

            # Chuẩn hóa sang TradeEvent
            trade_event = TradeEvent.from_binance_raw(data)

            # Chạy qua module Fault Injector (nếu có cấu hình)
            events_to_send = self.fault_injector.process_event(trade_event)

            # Đẩy vào Kafka
            if self.producer:
                for evt in events_to_send:
                    self.producer.send_trade(evt)
                    self.total_events_published += 1
            else:
                self.total_events_published += len(events_to_send)

            if self.total_messages_received % 100 == 0:
                logger.info(f"Đã xử lý {self.total_messages_received} ticks | Đã phát {self.total_events_published} events vào Kafka.")

        except json.JSONDecodeError as jde:
            logger.error(f"Lỗi JSON Decode: {jde}")
            if self.producer:
                self.producer.send_dlq(
                    DLQEvent(
                        raw_payload=message,
                        error_reason=f"JSONDecodeError: {str(jde)}",
                        source_stream="binance_ws"
                    )
                )
        except Exception as ex:
            logger.error(f"Lỗi trong quá trình xử lý tick: {ex}")

    def on_error(self, ws, error):
        """Xử lý lỗi kết nối WebSocket."""
        logger.error(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Xử lý khi đóng kết nối WebSocket."""
        logger.warning(f"WebSocket đóng kết nối (code={close_status_code}, msg={close_msg}). Sẵn sàng kết nối lại...")

    def on_open(self, ws):
        """Xử lý khi mở kết nối WebSocket thành công."""
        logger.info(f"Kết nối Binance WebSocket thành công cho {len(self.symbols)} pairs!")

    def start_streaming(self):
        """Bắt đầu vòng lặp thu thập dữ liệu với cơ chế Auto-reconnect."""
        if not self.producer:
            self.producer = ResilientKafkaProducer()

        retry_count = 0
        while True:
            try:
                ws_url = self.build_ws_url()
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                logger.info("Bắt đầu lắng nghe dòng sự kiện giao dịch trực tiếp...")
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
                retry_count = 0
            except KeyboardInterrupt:
                logger.info("Nhận tín hiệu dừng từ người dùng.")
                break
            except Exception as e:
                retry_count += 1
                wait_sec = min(2 ** retry_count, 60)
                logger.error(f"Mất kết nối WebSocket ({e}). Thử lại sau {wait_sec} giây...")
                time.sleep(wait_sec)
            finally:
                if self.producer:
                    self.producer.flush()
