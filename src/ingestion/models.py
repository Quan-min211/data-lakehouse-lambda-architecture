"""
models.py
=========
Định nghĩa Data Models chuẩn hóa cho tầng Ingestion.
Sử dụng dataclasses và kiểm tra tính hợp lệ kiểu dữ liệu.
"""

import json
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any


@dataclass
class TradeEvent:
    """Sự kiện giao dịch tiền mã hóa đã được chuẩn hóa.

    Attributes:
        trade_id (int): Mã định danh giao dịch duy nhất từ sàn Binance (aggTrade ID).
        symbol (str): Cặp giao dịch (ví dụ: BTCUSDT).
        price (float): Mức giá khớp lệnh.
        quantity (float): Khối lượng coin khớp lệnh.
        trade_time (int): Thời điểm khớp lệnh từ sàn (Event-time ms).
        is_buyer_maker (bool): True nếu lệnh Maker bán (Taker mua), False ngược lại.
        ingestion_time (int): Thời điểm hệ thống Ingestion nhận tin (ms).
        is_injected (bool): Đánh dấu nếu đây là bản ghi được tiêm lỗi thực nghiệm.
        fault_type (Optional[str]): Loại lỗi tiêm (duplicate, late_data, out_of_order, schema_invalid).
    """
    trade_id: int
    symbol: str
    price: float
    quantity: float
    trade_time: int
    is_buyer_maker: bool
    ingestion_time: int = 0
    is_injected: bool = False
    fault_type: Optional[str] = None

    def __post_init__(self):
        if self.ingestion_time == 0:
            self.ingestion_time = int(time.time() * 1000)

    def to_dict(self) -> Dict[str, Any]:
        """Chuyển đổi đối tượng sang dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Chuyển đổi đối tượng sang chuỗi JSON."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TradeEvent":
        """Khởi tạo TradeEvent từ dictionary."""
        return cls(
            trade_id=int(data["trade_id"]),
            symbol=str(data["symbol"]).upper(),
            price=float(data["price"]),
            quantity=float(data["quantity"]),
            trade_time=int(data["trade_time"]),
            is_buyer_maker=bool(data["is_buyer_maker"]),
            ingestion_time=int(data.get("ingestion_time", int(time.time() * 1000))),
            is_injected=bool(data.get("is_injected", False)),
            fault_type=data.get("fault_type", None)
        )

    @classmethod
    def from_binance_raw(cls, tick: Dict[str, Any]) -> "TradeEvent":
        """Parse và chuẩn hóa từ payload thô aggTrade của Binance WebSocket.

        Payload Binance aggTrade:
            - 's': symbol (ví dụ: BTCUSDT)
            - 'a': aggregate trade ID
            - 'p': price string
            - 'q': quantity string
            - 'T': trade time (epoch ms)
            - 'm': is_buyer_maker boolean
        """
        symbol = tick.get("s", "UNKNOWN").upper()
        trade_id = int(tick.get("a", tick.get("t", 0)))
        price = float(tick.get("p", 0.0))
        quantity = float(tick.get("q", 0.0))
        trade_time = int(tick.get("T", tick.get("E", int(time.time() * 1000))))
        is_buyer_maker = bool(tick.get("m", False))

        return cls(
            trade_id=trade_id,
            symbol=symbol,
            price=price,
            quantity=quantity,
            trade_time=trade_time,
            is_buyer_maker=is_buyer_maker,
            ingestion_time=int(time.time() * 1000),
            is_injected=False,
            fault_type=None
        )


@dataclass
class DLQEvent:
    """Sự kiện lỗi chuyển vào Dead-Letter Queue (DLQ).

    Attributes:
        raw_payload (str): Chuỗi nội dung gốc không parse được.
        error_reason (str): Lý do phát sinh lỗi.
        source_stream (str): Tên nguồn stream.
        received_at (int): Thời điểm nhận tin lỗi (epoch ms).
    """
    raw_payload: str
    error_reason: str
    source_stream: str = "binance_websocket"
    received_at: int = 0

    def __post_init__(self):
        if self.received_at == 0:
            self.received_at = int(time.time() * 1000)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
