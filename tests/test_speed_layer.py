"""
test_speed_layer.py
===================
Bộ Unit Test kiểm thử logic nghiệp vụ của Tầng Tốc Độ (Speed Layer):
1. Tính toán nến OHLCV
2. Tính toán VWAP (Volume-Weighted Average Price)
3. Phát hiện biến động giá bất thường (Price Spike Detection)
4. Chuẩn hóa bản ghi trước khi nạp vào ClickHouse speed_agg
5. Khớp Data Contract schema
"""

import unittest
from datetime import datetime, timezone

from src.speed_layer.metrics_calculator import MetricsCalculator
from src.speed_layer.spike_detector import SpikeDetector
from src.speed_layer.trade_schema import TRADE_EVENT_FIELD_NAMES
from src.speed_layer.redis_writer import RedisSpeedWriter
from src.speed_layer.spark_streaming import kafka_source_options
from src.speed_layer.window_aggregator import WindowAggregator


class FakeRedisPipeline:
    """Minimal Redis pipeline fake for unit tests."""

    def __init__(self):
        self.commands = []

    def set(self, key, value, ex=None):
        self.commands.append((key, value, ex))
        return self

    def execute(self):
        return [True for _ in self.commands]


class FakeRedisClient:
    """Minimal Redis client fake for unit tests."""

    def __init__(self):
        self.last_pipeline = None

    def pipeline(self):
        self.last_pipeline = FakeRedisPipeline()
        return self.last_pipeline


class TestMetricsCalculator(unittest.TestCase):
    """Kiểm thử tính toán chỉ số OHLCV và VWAP."""

    def setUp(self):
        # 3 giao dịch mẫu trong 1 phút
        self.sample_trades = [
            {"price": 60000.0, "quantity": 1.0, "trade_time": 1700000000000},  # Open: 60000
            {"price": 61500.0, "quantity": 2.0, "trade_time": 1700000020000},  # High: 61500
            {"price": 59500.0, "quantity": 1.0, "trade_time": 1700000040000},  # Low: 59500
            {"price": 61000.0, "quantity": 1.0, "trade_time": 1700000059000},  # Close: 61000
        ]

    def test_vwap_calculation(self):
        # Total Value: (60000*1) + (61500*2) + (59500*1) + (61000*1) = 60000 + 123000 + 59500 + 61000 = 303500
        # Total Quantity: 1 + 2 + 1 + 1 = 5
        # Expected VWAP = 303500 / 5 = 60700.0
        vwap = MetricsCalculator.calculate_vwap_python(self.sample_trades)
        self.assertEqual(vwap, 60700.0)

    def test_vwap_empty_trades(self):
        vwap = MetricsCalculator.calculate_vwap_python([])
        self.assertEqual(vwap, 0.0)

    def test_ohlcv_calculation(self):
        ohlcv = MetricsCalculator.calculate_ohlcv_python(self.sample_trades)
        self.assertEqual(ohlcv["open"], 60000.0)
        self.assertEqual(ohlcv["high"], 61500.0)
        self.assertEqual(ohlcv["low"], 59500.0)
        self.assertEqual(ohlcv["close"], 61000.0)
        self.assertEqual(ohlcv["volume"], 5.0)
        self.assertEqual(ohlcv["trade_count"], 4)
        self.assertEqual(ohlcv["vwap"], 60700.0)

    def test_ohlcv_out_of_order_input(self):
        # Đảo thứ tự thời gian để kiểm tra hàm tự sắp xếp theo trade_time
        shuffled = [
            self.sample_trades[2], # 59500
            self.sample_trades[0], # 60000
            self.sample_trades[3], # 61000
            self.sample_trades[1], # 61500
        ]
        ohlcv = MetricsCalculator.calculate_ohlcv_python(shuffled)
        self.assertEqual(ohlcv["open"], 60000.0)
        self.assertEqual(ohlcv["close"], 61000.0)

    def test_ohlcv_same_millisecond_tie_breaker_by_trade_id(self):
        # 2 giao dịch cùng chính xác 1 mili-giây nhưng khác trade_id và giá
        same_ms_trades = [
            {"trade_id": 105, "price": 60500.0, "quantity": 1.0, "trade_time": 1700000000000},
            {"trade_id": 101, "price": 60100.0, "quantity": 1.0, "trade_time": 1700000000000},
        ]
        ohlcv = MetricsCalculator.calculate_ohlcv_python(same_ms_trades)
        self.assertEqual(ohlcv["open"], 60100.0)
        self.assertEqual(ohlcv["close"], 60500.0)

    def test_streaming_filter_and_deduplicate_python(self):
        raw_stream = [
            {"trade_id": 1, "symbol": "btcusdt", "price": 60000.0, "quantity": 1.0, "trade_time": 1000},
            {"trade_id": 1, "symbol": "BTCUSDT", "price": 60000.0, "quantity": 1.0, "trade_time": 1000},  # dup
            {"trade_id": 2, "symbol": "BTCUSDT", "price": -50.0, "quantity": 1.0, "trade_time": 1001},    # bad price
            {"trade_id": 3, "symbol": "BTCUSDT", "price": 60100.0, "quantity": 0.0, "trade_time": 1002},  # bad qty
        ]
        cleaned = WindowAggregator.filter_and_deduplicate_python(raw_stream)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["trade_id"], 1)
        self.assertEqual(cleaned[0]["symbol"], "BTCUSDT")


class TestSpikeDetector(unittest.TestCase):
    """Kiểm thử thuật toán phát hiện Price Spike."""

    def setUp(self):
        self.detector = SpikeDetector(price_change_threshold=0.02, range_threshold=0.03)

    def test_normal_candle(self):
        # Open: 60000, Close: 60500 (+0.83% -> < 2%)
        # High: 60800, Low: 59800 (Range = 1000/59800 = 1.67% -> < 3%)
        candle = {"open": 60000.0, "high": 60800.0, "low": 59800.0, "close": 60500.0}
        self.assertEqual(self.detector.is_spike_python(candle), 0)

    def test_price_jump_spike(self):
        # Open: 60000, Close: 61500 (+2.5% -> >= 2% threshold)
        candle = {"open": 60000.0, "high": 61600.0, "low": 59900.0, "close": 61500.0}
        self.assertEqual(self.detector.is_spike_python(candle), 1)

    def test_price_dump_spike(self):
        # Open: 60000, Close: 58500 (-2.5% -> >= 2% threshold)
        candle = {"open": 60000.0, "high": 60100.0, "low": 58400.0, "close": 58500.0}
        self.assertEqual(self.detector.is_spike_python(candle), 1)

    def test_range_volatility_spike(self):
        # Open: 60000, Close: 60100 (ít đổi) nhưng High: 62500, Low: 60000 (Range 4.16% >= 3%)
        candle = {"open": 60000.0, "high": 62500.0, "low": 60000.0, "close": 60100.0}
        self.assertEqual(self.detector.is_spike_python(candle), 1)


class TestSchemaAndFormatting(unittest.TestCase):
    """Kiem thu cau truc Schema (khong can PySpark/JVM)."""

    def test_spark_schema_field_names(self):
        """Kiem tra danh sach ten cot dung theo Data Contract."""
        expected = [
            "trade_id",
            "symbol",
            "price",
            "quantity",
            "trade_time",
            "is_buyer_maker",
            "ingestion_time",
            "is_injected",
            "fault_type",
        ]
        # Dung TRADE_EVENT_FIELD_NAMES tu trade_schema (khong can Spark JVM)
        self.assertEqual(TRADE_EVENT_FIELD_NAMES, expected)


class TestRedisSpeedWriter(unittest.TestCase):
    """Kiem thu Redis hot-cache writer khong can Redis server that."""

    def test_write_records_uses_symbol_and_window_key(self):
        fake_client = FakeRedisClient()
        writer = RedisSpeedWriter(client=fake_client, ttl_seconds=60)
        record = {
            "symbol": "btcusdt",
            "window_start": datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc),
            "window_end": datetime(2026, 9, 4, 12, 1, 0, tzinfo=timezone.utc),
            "open_price": 60000.0,
            "high_price": 60100.0,
            "low_price": 59900.0,
            "close_price": 60050.0,
            "volume": 5.0,
            "trade_count": 20,
            "vwap": 60020.0,
            "is_spike": 0,
        }

        written = writer.write_records([record])

        self.assertEqual(written, 1)
        commands = fake_client.last_pipeline.commands
        self.assertEqual(len(commands), 2)
        self.assertEqual(commands[0][0], "speed:candle:BTCUSDT:1788523200000")
        self.assertEqual(commands[1][0], "speed:candle:BTCUSDT:latest")
        self.assertEqual(commands[0][2], 60)

    def test_write_records_latest_key_picks_max_window_start(self):
        fake_client = FakeRedisClient()
        writer = RedisSpeedWriter(client=fake_client, ttl_seconds=60)
        # 2 bản ghi cùng symbol nhưng thứ tự truyền vào là bản ghi mới trước, bản ghi cũ sau
        r_newer = {
            "symbol": "BTCUSDT",
            "window_start": datetime(2026, 9, 4, 12, 1, 0, tzinfo=timezone.utc),
            "window_end": datetime(2026, 9, 4, 12, 2, 0, tzinfo=timezone.utc),
            "open_price": 60100.0,
            "high_price": 60200.0,
            "low_price": 60050.0,
            "close_price": 60150.0,
            "volume": 6.0,
            "trade_count": 25,
            "vwap": 60120.0,
            "is_spike": 0,
        }
        r_older = {
            "symbol": "BTCUSDT",
            "window_start": datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc),
            "window_end": datetime(2026, 9, 4, 12, 1, 0, tzinfo=timezone.utc),
            "open_price": 60000.0,
            "high_price": 60100.0,
            "low_price": 59900.0,
            "close_price": 60050.0,
            "volume": 5.0,
            "trade_count": 20,
            "vwap": 60020.0,
            "is_spike": 0,
        }

        # Gửi r_newer trước, r_older sau
        written = writer.write_records([r_newer, r_older])
        self.assertEqual(written, 2)
        commands = fake_client.last_pipeline.commands
        # commands: [set window 12:01, set window 12:00, set latest]
        self.assertEqual(len(commands), 3)
        latest_cmd = [c for c in commands if c[0] == "speed:candle:BTCUSDT:latest"][0]
        # latest_cmd payload phải chứa thông tin của r_newer (12:01)
        import json
        payload = json.loads(latest_cmd[1])
        self.assertEqual(payload["window_start"], "2026-09-04T12:01:00+00:00")
        self.assertEqual(payload["close_price"], 60150.0)

    def test_auto_reconnect_cooldown(self):
        writer = RedisSpeedWriter(port=63799, reconnect_cooldown_seconds=30.0)
        writer._available = False
        import time
        from unittest.mock import patch
        writer._last_error_time = time.time() - 10.0  # Mới 10s trôi qua (< 30s)
        self.assertIsNone(writer.get_client())

        # Giả lập đã qua 35s (> 30s cooldown)
        writer._last_error_time = time.time() - 35.0
        with patch("redis.Redis", side_effect=Exception("Mock connection failure")):
            self.assertIsNone(writer.get_client())
        self.assertFalse(writer._available)
        self.assertGreater(writer._last_error_time, time.time() - 2.0)


class TestYamlConfigLoading(unittest.TestCase):
    """configs/kafka/topics.yaml và configs/redis/redis.yaml phải được mã nạp thật."""

    def test_topic_specs_loaded_from_yaml(self):
        from src.utils.config import get_kafka_topic_specs

        specs = {s["name"]: s for s in get_kafka_topic_specs()}
        self.assertIn("crypto_trades_raw", specs)
        self.assertIn("crypto_trades_dlq", specs)
        self.assertEqual(int(specs["crypto_trades_raw"]["partitions"]), 3)

    def test_redis_writer_prefix_comes_from_config(self):
        from src.utils.config import get_redis_config

        writer = RedisSpeedWriter(client=FakeRedisClient())
        self.assertEqual(writer.key_prefix, get_redis_config().key_prefix)
        self.assertEqual(writer.key_prefix, "speed:candle")

    def test_ensure_topic_exists_creates_missing_topics_from_yaml(self):
        from unittest import mock
        from src.speed_layer.spark_streaming import SpeedLayerStreamingJob

        created = []

        class FakeAdmin:
            def __init__(self, **kwargs):
                pass

            def list_topics(self):
                return ["crypto_trades_raw"]

            def create_topics(self, new_topics, validate_only=False):
                created.extend(new_topics)

            def close(self):
                pass

        job = SpeedLayerStreamingJob.__new__(SpeedLayerStreamingJob)  # không cần SparkSession
        job.kafka_bootstrap = "kafka:9092"
        job.topic = "crypto_trades_raw"
        with mock.patch("kafka.admin.KafkaAdminClient", FakeAdmin), mock.patch("time.sleep"):
            job._ensure_topic_exists()

        self.assertEqual([t.name for t in created], ["crypto_trades_dlq"])
        self.assertEqual(created[0].num_partitions, 3)
        self.assertEqual(created[0].topic_configs["retention.ms"], "86400000")
        self.assertEqual(created[0].topic_configs["cleanup.policy"], "delete")


class TestKafkaSourceGuardrails(unittest.TestCase):
    """Kafka source must fail closed so retention loss is never silent."""

    def test_kafka_source_defaults_to_integrity_preserving_options(self):
        options = kafka_source_options(
            kafka_bootstrap="kafka:9092",
            topic="crypto_trades_raw",
            max_offsets_per_trigger=50000,
            starting_offsets="latest",
            fail_on_data_loss=True,
        )

        self.assertEqual(options["failOnDataLoss"], "true")
        self.assertEqual(options["startingOffsets"], "latest")
        self.assertEqual(options["maxOffsetsPerTrigger"], "50000")

    def test_kafka_source_rejects_empty_recovery_offset(self):
        with self.assertRaises(ValueError):
            kafka_source_options(
                kafka_bootstrap="kafka:9092",
                topic="crypto_trades_raw",
                max_offsets_per_trigger=None,
                starting_offsets=" ",
                fail_on_data_loss=True,
            )


if __name__ == "__main__":
    unittest.main()
