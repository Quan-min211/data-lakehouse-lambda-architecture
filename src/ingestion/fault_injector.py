"""
fault_injector.py
=================
Module chủ động bơm các loại lỗi phân tán (Fault Injection) có kiểm soát.
Mục tiêu: Tạo Ground Truth cho các bài Benchmark định lượng (Data Quality, Deduplication, Late Events).
"""

import random
import time
from typing import List, Optional
from .models import TradeEvent
from ..utils.logger import setup_logger

logger = setup_logger("fault_injector")


class FaultInjector:
    """Bộ tạo và chèn lỗi có kiểm soát cho luồng dữ liệu sự kiện giao dịch.

    Hỗ trợ 4 loại lỗi:
        1. Duplicate: Nhân đôi bản ghi giữ nguyên trade_id.
        2. Late Data: Giảm trade_time lùi lại 1-5 phút (60.000 - 300.000 ms).
        3. Out-of-Order: Đảo trật tự phát sự kiện.
        4. Schema Invalid: Giá âm, khối lượng bằng 0 hoặc thiếu trường.
    """

    def __init__(
        self,
        duplicate_rate: float = 0.0,
        late_data_rate: float = 0.0,
        out_of_order_rate: float = 0.0,
        schema_invalid_rate: float = 0.0,
        late_min_seconds: int = 60,
        late_max_seconds: int = 300
    ):
        """Khởi tạo FaultInjector với tỷ lệ tiêm lỗi cấu hình.

        Args:
            duplicate_rate (float): Tỷ lệ nhân bản lặp (0.0 đến 1.0). Ví dụ: 0.10 (10%).
            late_data_rate (float): Tỷ lệ dữ liệu đến trễ (0.0 đến 1.0).
            out_of_order_rate (float): Tỷ lệ đảo trật tự sự kiện (0.0 đến 1.0).
            schema_invalid_rate (float): Tỷ lệ bản ghi sai schema / giá âm (0.0 đến 1.0).
            late_min_seconds (int): Độ trễ tối thiểu tính bằng giây.
            late_max_seconds (int): Độ trễ tối đa tính bằng giây.
        """
        self.duplicate_rate = duplicate_rate
        self.late_data_rate = late_data_rate
        self.out_of_order_rate = out_of_order_rate
        self.schema_invalid_rate = schema_invalid_rate
        self.late_min_seconds = late_min_seconds
        self.late_max_seconds = late_max_seconds
        self.pending_out_of_order: List[TradeEvent] = []

    def process_event(self, event: TradeEvent) -> List[TradeEvent]:
        """Xử lý một sự kiện gốc và quyết định có chèn lỗi hay không.

        Args:
            event (TradeEvent): Sự kiện giao dịch sạch từ sàn.

        Returns:
            List[TradeEvent]: Danh sách các sự kiện (có thể gồm bản ghi gốc + bản ghi lỗi).
        """
        results: List[TradeEvent] = []
        rand = random.random()

        # 1. Chèn lỗi Schema Invalid (Giá âm / Khối lượng 0)
        if self.schema_invalid_rate > 0 and rand < self.schema_invalid_rate:
            corrupt_event = TradeEvent(
                trade_id=event.trade_id,
                symbol=event.symbol,
                price=-abs(event.price),  # Giá âm không hợp lệ
                quantity=0.0,             # Khối lượng rác
                trade_time=event.trade_time,
                is_buyer_maker=event.is_buyer_maker,
                ingestion_time=int(time.time() * 1000),
                is_injected=True,
                fault_type="schema_invalid"
            )
            results.append(corrupt_event)
            logger.debug(f"[Fault Injected] Schema Invalid on trade_id={event.trade_id}")
            return results

        # 2. Chèn lỗi Late Data (Lùi thời gian giao dịch)
        if self.late_data_rate > 0 and rand < self.late_data_rate:
            delay_ms = random.randint(self.late_min_seconds * 1000, self.late_max_seconds * 1000)
            late_event = TradeEvent(
                trade_id=event.trade_id,
                symbol=event.symbol,
                price=event.price,
                quantity=event.quantity,
                trade_time=event.trade_time - delay_ms,  # Trễ 1 - 5 phút trong quá khứ
                is_buyer_maker=event.is_buyer_maker,
                ingestion_time=int(time.time() * 1000),
                is_injected=True,
                fault_type="late_data"
            )
            results.append(late_event)
            logger.debug(f"[Fault Injected] Late Data delay={delay_ms}ms on trade_id={event.trade_id}")
            return results

        # 3. Chèn lỗi Out-of-Order (Giữ lại và phát sau)
        if self.out_of_order_rate > 0 and rand < self.out_of_order_rate:
            ooo_event = TradeEvent(
                trade_id=event.trade_id,
                symbol=event.symbol,
                price=event.price,
                quantity=event.quantity,
                trade_time=event.trade_time,
                is_buyer_maker=event.is_buyer_maker,
                ingestion_time=int(time.time() * 1000),
                is_injected=True,
                fault_type="out_of_order"
            )
            self.pending_out_of_order.append(ooo_event)
            logger.debug(f"[Fault Injected] Out-of-Order buffer trade_id={event.trade_id}")
            return results

        # Sự kiện bình thường
        results.append(event)

        # 4. Chèn lỗi Duplicate (Gửi kèm thêm 1 bản ghi trùng ID)
        if self.duplicate_rate > 0 and rand < self.duplicate_rate:
            dup_event = TradeEvent(
                trade_id=event.trade_id,
                symbol=event.symbol,
                price=event.price,
                quantity=event.quantity,
                trade_time=event.trade_time,
                is_buyer_maker=event.is_buyer_maker,
                ingestion_time=int(time.time() * 1000) + random.randint(10, 500),
                is_injected=True,
                fault_type="duplicate"
            )
            results.append(dup_event)
            logger.debug(f"[Fault Injected] Duplicate emitted on trade_id={event.trade_id}")

        # Xả buffer out-of-order nếu có tích lũy
        if self.pending_out_of_order and random.random() < 0.3:
            released = self.pending_out_of_order.pop(0)
            results.append(released)

        return results
