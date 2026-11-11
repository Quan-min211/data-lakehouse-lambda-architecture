"""
test_ingestion.py
=================
Unit tests cho các module thuộc tầng Ingestion:
  - Models: TradeEvent & DLQEvent
  - FaultInjector: Tiêm 4 loại lỗi kiểm thử
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.ingestion.models import TradeEvent, DLQEvent
from src.ingestion.fault_injector import FaultInjector


class TestTradeEventModels(unittest.TestCase):
    """Kiểm thử khởi tạo và serialize dữ liệu TradeEvent."""

    def test_trade_event_creation(self):
        evt = TradeEvent(
            trade_id=123456789,
            symbol="BTCUSDT",
            price=65432.10,
            quantity=0.5432,
            trade_time=1700000000000,
            is_buyer_maker=True
        )
        self.assertEqual(evt.trade_id, 123456789)
        self.assertEqual(evt.symbol, "BTCUSDT")
        self.assertEqual(evt.price, 65432.10)
        self.assertEqual(evt.quantity, 0.5432)
        self.assertTrue(evt.is_buyer_maker)
        self.assertFalse(evt.is_injected)
        self.assertGreater(evt.ingestion_time, 0)

    def test_to_dict_and_to_json(self):
        evt = TradeEvent(
            trade_id=999,
            symbol="ETHUSDT",
            price=3500.5,
            quantity=1.2,
            trade_time=1700000001000,
            is_buyer_maker=False
        )
        d = evt.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["symbol"], "ETHUSDT")

        json_str = evt.to_json()
        self.assertIsInstance(json_str, str)
        self.assertIn('"symbol": "ETHUSDT"', json_str)

    def test_from_binance_raw(self):
        raw_tick = {
            "s": "solusdt",
            "a": 555666777,
            "p": "145.25",
            "q": "10.5",
            "T": 1700000050000,
            "m": False
        }
        evt = TradeEvent.from_binance_raw(raw_tick)
        self.assertEqual(evt.symbol, "SOLUSDT")
        self.assertEqual(evt.trade_id, 555666777)
        self.assertEqual(evt.price, 145.25)
        self.assertEqual(evt.quantity, 10.5)
        self.assertFalse(evt.is_buyer_maker)

    def test_from_binance_raw_missing_fields_use_rejectable_sentinels(self):
        """Thiếu trường không làm sập parser, và sentinel luôn bị DQ Gate chặn."""
        from src.data_quality.dq_checks import clean_and_deduplicate

        evt = TradeEvent.from_binance_raw({"s": "btcusdt", "p": "64000.5"})
        self.assertEqual(evt.trade_id, -1)
        self.assertEqual(evt.quantity, 0.0)
        # Không dùng đồng hồ xử lý thay cho thời gian sự kiện
        self.assertEqual(evt.trade_time, 0)

        clean, rejected, _ = clean_and_deduplicate([evt.to_dict()])
        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)

    def test_dlq_event(self):
        dlq = DLQEvent(
            raw_payload="corrupted_json{",
            error_reason="JSONDecodeError"
        )
        self.assertEqual(dlq.raw_payload, "corrupted_json{")
        self.assertEqual(dlq.source_stream, "binance_ws")
        self.assertGreater(dlq.received_at, 0)


class TestFaultInjector(unittest.TestCase):
    """Kiểm thử module tiêm lỗi FaultInjector."""

    def setUp(self):
        self.sample_event = TradeEvent(
            trade_id=1001,
            symbol="BTCUSDT",
            price=64000.0,
            quantity=0.1,
            trade_time=1700000000000,
            is_buyer_maker=False
        )

    def test_normal_pass_through(self):
        injector = FaultInjector(
            duplicate_rate=0.0,
            late_data_rate=0.0,
            schema_invalid_rate=0.0
        )
        results = injector.process_event(self.sample_event)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].trade_id, 1001)
        self.assertFalse(results[0].is_injected)

    def test_duplicate_injection(self):
        injector = FaultInjector(duplicate_rate=1.0)  # 100% duplicate
        results = injector.process_event(self.sample_event)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].trade_id, 1001)
        self.assertEqual(results[1].trade_id, 1001)
        self.assertTrue(results[1].is_injected)
        self.assertEqual(results[1].fault_type, "duplicate")

    def test_late_data_injection(self):
        injector = FaultInjector(
            late_data_rate=1.0,
            late_min_seconds=60,
            late_max_seconds=120
        )  # 100% late
        results = injector.process_event(self.sample_event)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].is_injected)
        self.assertEqual(results[0].fault_type, "late_data")
        self.assertLess(results[0].trade_time, self.sample_event.trade_time)

    def test_schema_invalid_injection(self):
        injector = FaultInjector(schema_invalid_rate=1.0)  # 100% invalid
        results = injector.process_event(self.sample_event)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].is_injected)
        self.assertEqual(results[0].fault_type, "schema_invalid")
        self.assertLess(results[0].price, 0)

    def test_out_of_order_injection(self):
        injector = FaultInjector(out_of_order_rate=1.0)  # 100% out of order
        results = injector.process_event(self.sample_event)
        self.assertEqual(len(results), 0)
        self.assertEqual(len(injector.pending_out_of_order), 1)
        self.assertEqual(injector.pending_out_of_order[0].fault_type, "out_of_order")

    def test_drain_pending_releases_buffered_out_of_order_events(self):
        injector = FaultInjector(out_of_order_rate=1.0)
        injector.process_event(self.sample_event)
        drained = injector.drain_pending()
        self.assertEqual(len(drained), 1)
        self.assertEqual(drained[0].fault_type, "out_of_order")
        self.assertEqual(injector.pending_out_of_order, [])


class _FakeFuture:
    """Future tối giản ghi lại errback để kiểm thử lỗi gửi bất đồng bộ."""

    def __init__(self):
        self.errbacks = []

    def add_errback(self, fn):
        self.errbacks.append(fn)
        return self


class _FakeKafkaProducer:
    def __init__(self):
        self.futures = []

    def send(self, topic, key=None, value=None):
        future = _FakeFuture()
        self.futures.append(future)
        return future


class TestResilientKafkaProducer(unittest.TestCase):
    """Lỗi broker xảy ra sau khi send() trả về phải được ghi nhận."""

    def test_async_send_failure_is_counted(self):
        from src.ingestion.kafka_producer import ResilientKafkaProducer

        producer = ResilientKafkaProducer.__new__(ResilientKafkaProducer)  # bỏ qua kết nối thật
        producer._producer = _FakeKafkaProducer()
        producer.async_send_failures = 0

        event = TradeEvent(trade_id=1, symbol="BTCUSDT", price=1.0, quantity=1.0,
                           trade_time=1700000000000, is_buyer_maker=False)
        self.assertTrue(producer.send_trade(event))
        future = producer._producer.futures[0]
        self.assertEqual(len(future.errbacks), 1)

        future.errbacks[0](RuntimeError("NotEnoughReplicas"))
        self.assertEqual(producer.async_send_failures, 1)


if __name__ == "__main__":
    unittest.main()
