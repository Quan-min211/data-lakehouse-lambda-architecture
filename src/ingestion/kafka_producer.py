"""
kafka_producer.py
=================
Đóng gói Kafka Producer an toàn, hỗ trợ cơ chế retry exponential backoff và DLQ routing.
"""

import json
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log

from .models import TradeEvent, DLQEvent
from ..utils.config import config
from ..utils.logger import setup_logger

logger = setup_logger("kafka_producer")


class ResilientKafkaProducer:
    """Kafka Producer bền bỉ với khả năng tự phục hồi và đẩy lỗi vào DLQ."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """Khởi tạo Producer với danh sách Kafka Brokers."""
        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self._producer: Optional[KafkaProducer] = None
        self._connect()

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, 20)  # 20 = logging.INFO
    )
    def _connect(self):
        """Kết nối đến Kafka Broker với cơ chế thử lại tự động."""
        logger.info(f"Đang kết nối Kafka Producer tới: {self.bootstrap_servers}")
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
                acks="all",              # Đảm bảo ghi thành công vào các in-sync replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type="gzip",
                linger_ms=10,
                batch_size=16384
            )
            logger.info("Kết nối Kafka Producer thành công!")
        except Exception as e:
            logger.warning(f"Chưa thể kết nối Kafka ({e}), đang thử lại...")
            raise e

    def send_trade(self, event: TradeEvent, topic: Optional[str] = None) -> bool:
        """Gửi sự kiện giao dịch chuẩn vào Kafka topic.

        Args:
            event (TradeEvent): Sự kiện giao dịch đã chuẩn hóa.
            topic (str, optional): Tên topic (mặc định lấy từ config: crypto_trades_raw).

        Returns:
            bool: True nếu gửi thành công, False nếu thất bại.
        """
        if not self._producer:
            logger.error("Producer chưa sẵn sàng kết nối!")
            return False

        target_topic = topic or config.kafka.topic_raw
        try:
            # Key partition theo symbol (BTCUSDT, ETHUSDT) để cùng symbol luôn vào cùng 1 partition
            self._producer.send(
                topic=target_topic,
                key=event.symbol,
                value=event.to_dict()
            )
            return True
        except KafkaError as ke:
            logger.error(f"Lỗi gửi tin tới topic {target_topic}: {ke}")
            return False
        except Exception as ex:
            logger.error(f"Lỗi không xác định khi gửi Kafka: {ex}")
            return False

    def send_dlq(self, dlq_event: DLQEvent, topic: Optional[str] = None) -> bool:
        """Gửi message lỗi vào Dead-Letter Queue (DLQ).

        Args:
            dlq_event (DLQEvent): Sự kiện lỗi.
            topic (str, optional): Tên DLQ topic (mặc định: crypto_trades_dlq).

        Returns:
            bool: True nếu gửi thành công.
        """
        if not self._producer:
            return False

        target_topic = topic or config.kafka.topic_dlq
        try:
            self._producer.send(
                topic=target_topic,
                key=dlq_event.source_stream,
                value=dlq_event.to_dict()
            )
            logger.warning(f"Đã chuyển bản ghi lỗi vào DLQ ({target_topic}): {dlq_event.error_reason}")
            return True
        except Exception as ex:
            logger.error(f"Lỗi gửi vào DLQ: {ex}")
            return False

    def flush(self):
        """Đẩy toàn bộ buffer tin nhắn xuống Kafka."""
        if self._producer:
            self._producer.flush()

    def close(self):
        """Đóng kết nối Kafka Producer an toàn."""
        if self._producer:
            self._producer.flush()
            self._producer.close()
            logger.info("Đã đóng kết nối Kafka Producer.")
