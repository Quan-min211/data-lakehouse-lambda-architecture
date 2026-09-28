"""
test_integration.py
===================
Integration Tests - Chạy với Docker services thật.

Yêu cầu:
    docker compose up -d clickhouse kafka redis

Chạy:
    # Chỉ integration tests:
    pytest tests/test_integration.py -v -m integration

    # Toàn bộ (bỏ qua nếu services không chạy):
    pytest tests/ -v

Các bài kiểm thử:
    1. [ClickHouse] Kết nối, tạo bảng, insert, SELECT
    2. [ClickHouse] batch_agg có dữ liệu hợp lệ
    3. [ClickHouse] speed_agg tồn tại và query được
    4. [ClickHouse] dq_quarantine insert & SELECT
    5. [ClickHouse] system_watermark tồn tại
    6. [Kafka]      Kết nối broker, list topics
    7. [Kafka]      Produce + Consume 1 message round-trip
    8. [Redis]      Kết nối ping
    9. [Redis]      Set + Get key round-trip
   10. [E2E]        Query Merger kết nối ClickHouse thật, trả về response hợp lệ
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

import pytest

# ── path setup ────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# ── env overrides for Docker networking ──────────────────────────────────────
# Khi chạy ngoài Docker (host machine), dùng localhost.
# Khi chạy bên trong container, các biến môi trường đã được set sẵn.
os.environ.setdefault("CLICKHOUSE_HOST", "localhost")
os.environ.setdefault("CLICKHOUSE_PORT", "8123")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")

pytestmark = pytest.mark.integration


# ══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def ch_client():
    """Trả về clickhouse_connect client, skip nếu không kết nối được."""
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ["CLICKHOUSE_PORT"]),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        )
        client.ping()
        return client
    except Exception as exc:
        pytest.skip(f"ClickHouse không kết nối được: {exc}")


@pytest.fixture(scope="module")
def kafka_producer():
    """Trả về KafkaProducer, skip nếu không kết nối được."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        return producer
    except Exception as exc:
        pytest.skip(f"Kafka không kết nối được: {exc}")


@pytest.fixture(scope="module")
def redis_client():
    """Trả về redis client, skip nếu không kết nối được."""
    try:
        import redis
        client = redis.Redis(
            host=os.environ["REDIS_HOST"],
            port=int(os.environ["REDIS_PORT"]),
            decode_responses=True,
            socket_connect_timeout=3,
        )
        client.ping()
        return client
    except Exception as exc:
        pytest.skip(f"Redis không kết nối được: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# 1-5: CLICKHOUSE INTEGRATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestClickHouseConnectivity:
    """Kiểm thử kết nối và các bảng cốt lõi trong ClickHouse."""

    def test_ch_ping(self, ch_client):
        """[CH-01] Ping ClickHouse thành công."""
        assert ch_client.ping() is True

    def test_ch_server_version(self, ch_client):
        """[CH-02] Server version trả về string không rỗng."""
        version = ch_client.server_version
        assert isinstance(version, str)
        assert len(version) > 0
        print(f"\n  ClickHouse version: {version}")

    def test_ch_lakehouse_database_exists(self, ch_client):
        """[CH-03] Database 'lakehouse' phải tồn tại."""
        rows = ch_client.query("SHOW DATABASES").result_rows
        db_names = [r[0] for r in rows]
        assert "lakehouse" in db_names, f"Database lakehouse không tìm thấy. Found: {db_names}"

    def test_ch_batch_agg_has_data(self, ch_client):
        """[CH-04] batch_agg phải có ít nhất 1 hàng."""
        count = ch_client.query("SELECT count(*) FROM lakehouse.batch_agg").result_rows[0][0]
        assert count > 0, "batch_agg trống. Hãy chạy Batch pipeline trước."
        print(f"\n  batch_agg rows: {count:,}")

    def test_ch_batch_agg_schema(self, ch_client):
        """[CH-05] batch_agg có đủ các cột theo Data Contract."""
        required_cols = {
            "symbol", "window_start", "window_end",
            "open_price", "high_price", "low_price", "close_price",
            "volume", "trade_count", "vwap", "batch_run_id",
        }
        rows = ch_client.query(
            "SELECT name FROM system.columns "
            "WHERE database='lakehouse' AND table='batch_agg'"
        ).result_rows
        actual_cols = {r[0] for r in rows}
        missing = required_cols - actual_cols
        assert not missing, f"batch_agg thiếu cột: {missing}"

    def test_ch_speed_agg_queryable(self, ch_client):
        """[CH-06] speed_agg tồn tại và có thể truy vấn (cho phép rỗng)."""
        count = ch_client.query("SELECT count(*) FROM lakehouse.speed_agg").result_rows[0][0]
        print(f"\n  speed_agg rows: {count}")
        assert count >= 0  # Cho phép 0 nếu Speed Layer chưa chạy

    def test_ch_system_watermark_exists(self, ch_client):
        """[CH-07] system_watermark tồn tại."""
        result = ch_client.query(
            "SELECT count(*) FROM lakehouse.system_watermark"
        ).result_rows
        assert result[0][0] >= 0

    def test_ch_dq_quarantine_insert_and_select(self, ch_client):
        """[CH-08] Insert 1 record vào dq_quarantine rồi SELECT lại."""
        run_id = f"integ_test_{uuid.uuid4().hex[:8]}"
        now_utc = datetime.now(timezone.utc)

        # Đảm bảo bảng tồn tại
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS lakehouse.dq_quarantine
            (
                batch_run_id   String,
                trade_id       String,
                symbol         String,
                price          String,
                quantity       String,
                trade_time     String,
                dq_error       String,
                raw_record     String,
                quarantined_at DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(quarantined_at)
            ORDER BY (batch_run_id, quarantined_at)
        """)

        ch_client.insert(
            "lakehouse.dq_quarantine",
            [[run_id, "99999", "TESTUSDT", "-1.0", "0.0", "0",
              "integration_test_error", '{"test": true}', now_utc]],
            column_names=[
                "batch_run_id", "trade_id", "symbol", "price", "quantity",
                "trade_time", "dq_error", "raw_record", "quarantined_at",
            ],
        )

        time.sleep(0.5)  # Chờ ClickHouse flush
        rows = ch_client.query(
            "SELECT trade_id, dq_error FROM lakehouse.dq_quarantine "
            "WHERE batch_run_id = %(run_id)s",
            parameters={"run_id": run_id},
        ).result_rows

        assert len(rows) == 1, f"Expect 1 row, got {len(rows)}"
        assert rows[0][0] == "99999"
        assert rows[0][1] == "integration_test_error"

    def test_ch_batch_agg_data_freshness(self, ch_client):
        """[CH-09] batch_agg có dữ liệu với window_start hợp lệ (> 2020-01-01)."""
        result = ch_client.query(
            "SELECT min(window_start), max(window_start) FROM lakehouse.batch_agg"
        ).result_rows[0]
        min_ws, max_ws = result
        assert min_ws is not None
        assert max_ws is not None
        assert min_ws > datetime(2020, 1, 1), f"window_start quá cũ: {min_ws}"
        print(f"\n  batch_agg range: {min_ws} → {max_ws}")

    def test_ch_candle_values_valid(self, ch_client):
        """[CH-10] Các nến trong batch_agg có giá trị hợp lệ (high >= low, volume > 0)."""
        rows = ch_client.query(
            "SELECT count(*) FROM lakehouse.batch_agg "
            "WHERE high_price < low_price OR volume <= 0 OR trade_count <= 0"
        ).result_rows[0][0]
        assert rows == 0, f"Phát hiện {rows} nến với giá trị không hợp lệ trong batch_agg"


# ══════════════════════════════════════════════════════════════════════════════
# 6-7: KAFKA INTEGRATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestKafkaConnectivity:
    """Kiểm thử kết nối và round-trip message với Kafka broker."""

    def test_kafka_list_topics(self, kafka_producer):
        """[KF-01] Có thể lấy danh sách topics từ broker."""
        from kafka import KafkaAdminClient
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
            )
            topics = admin.list_topics()
            admin.close()
            print(f"\n  Kafka topics: {list(topics)}")
            assert isinstance(topics, (list, set))
        except Exception as exc:
            pytest.skip(f"KafkaAdminClient không kết nối được: {exc}")

    def test_kafka_produce_message(self, kafka_producer):
        """[KF-02] Produce 1 message vào test topic thành công."""
        test_message = {
            "trade_id": 9999999,
            "symbol": "TESTUSDT",
            "price": 1.0,
            "quantity": 1.0,
            "trade_time": int(time.time() * 1000),
            "is_injected": True,
            "test_run": True,
        }
        future = kafka_producer.send("crypto_trades_raw", value=test_message)
        record_metadata = future.get(timeout=10)
        kafka_producer.flush()
        assert record_metadata.topic == "crypto_trades_raw"
        assert record_metadata.partition >= 0
        assert record_metadata.offset >= 0
        print(f"\n  Produced to partition={record_metadata.partition}, offset={record_metadata.offset}")


# ══════════════════════════════════════════════════════════════════════════════
# 8-9: REDIS INTEGRATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestRedisConnectivity:
    """Kiểm thử kết nối và thao tác cơ bản với Redis."""

    def test_redis_ping(self, redis_client):
        """[RD-01] Redis ping trả về True."""
        assert redis_client.ping() is True

    def test_redis_set_get_roundtrip(self, redis_client):
        """[RD-02] SET rồi GET lại đúng giá trị."""
        key = f"integ_test:{uuid.uuid4().hex}"
        value = f"test_value_{int(time.time())}"
        redis_client.set(key, value, ex=60)  # TTL 60s
        retrieved = redis_client.get(key)
        assert retrieved == value
        redis_client.delete(key)  # Dọn dẹp

    def test_redis_json_roundtrip(self, redis_client):
        """[RD-03] Lưu JSON object vào Redis và đọc lại đúng."""
        key = f"integ_test:candle:{uuid.uuid4().hex}"
        candle_data = {
            "symbol": "BTCUSDT",
            "window_start": "2026-09-28T13:00:00",
            "vwap": 65432.10,
            "volume": 12.5,
            "trade_count": 42,
        }
        redis_client.set(key, json.dumps(candle_data), ex=60)
        raw = redis_client.get(key)
        assert raw is not None
        retrieved = json.loads(raw)
        assert retrieved["symbol"] == "BTCUSDT"
        assert abs(retrieved["vwap"] - 65432.10) < 0.001
        redis_client.delete(key)


# ══════════════════════════════════════════════════════════════════════════════
# 10: E2E - Query Merger với ClickHouse thật
# ══════════════════════════════════════════════════════════════════════════════

class TestQueryMergerIntegration:
    """[E2E] Kiểm thử AutoCorrectingQueryMerger với ClickHouse Docker thật."""

    def test_query_merger_case1_batch_only(self, ch_client):
        """[E2E-01] Merger trả về CASE_1_HISTORY khi query nằm hoàn toàn trong batch window."""
        from src.serving_layer.clickhouse_client import ClickHouseQueryClient
        from src.serving_layer.watermark_reader import WatermarkReader
        from src.serving_layer.query_merger import AutoCorrectingQueryMerger
        from src.serving_layer.schemas import QueryCase

        # Lấy max window_start từ batch_agg để đặt watermark SAU dữ liệu
        max_ws_row = ch_client.query(
            "SELECT max(window_start) FROM lakehouse.batch_agg"
        ).result_rows[0][0]
        if max_ws_row is None:
            pytest.skip("batch_agg trống")

        if max_ws_row.tzinfo is None:
            max_ws_row = max_ws_row.replace(tzinfo=timezone.utc)

        # Watermark = max_window_start + 2h (batch data nằm TRƯỚC watermark → Case 1)
        mock_watermark = max_ws_row + timedelta(hours=2)

        ch_query_client = ClickHouseQueryClient(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ["CLICKHOUSE_PORT"]),
        )
        wm_reader = WatermarkReader(mock_watermark=mock_watermark)
        merger = AutoCorrectingQueryMerger(
            watermark_reader=wm_reader,
            ch_client=ch_query_client,
        )

        # Query 1 giờ nằm gọn trong batch data
        t_end = max_ws_row
        t_start = t_end - timedelta(hours=1)

        resp = merger.merge_query("BTCUSDT", t_start, t_end)
        assert resp.query_case == QueryCase.CASE_1_HISTORY, (
            f"Expected CASE_1_HISTORY, got {resp.query_case}"
        )
        assert resp.candles_count >= 0
        print(f"\n  [E2E-01] CASE_1_HISTORY | candles={resp.candles_count} | watermark={mock_watermark}")

    def test_query_merger_returns_valid_response_structure(self, ch_client):
        """[E2E-02] Merger luôn trả về MarketDataResponse có đủ các trường chuẩn."""
        from src.serving_layer.clickhouse_client import ClickHouseQueryClient
        from src.serving_layer.watermark_reader import WatermarkReader
        from src.serving_layer.query_merger import AutoCorrectingQueryMerger

        now = datetime.now(timezone.utc)
        mock_watermark = now - timedelta(hours=1)

        ch_query_client = ClickHouseQueryClient(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ["CLICKHOUSE_PORT"]),
        )
        wm_reader = WatermarkReader(mock_watermark=mock_watermark)
        merger = AutoCorrectingQueryMerger(
            watermark_reader=wm_reader,
            ch_client=ch_query_client,
        )

        t_start = now - timedelta(minutes=30)
        t_end = now

        resp = merger.merge_query("BTCUSDT", t_start, t_end)

        # Kiểm tra cấu trúc response
        assert hasattr(resp, "symbol")
        assert hasattr(resp, "query_case")
        assert hasattr(resp, "overall_status")
        assert hasattr(resp, "candles")
        assert hasattr(resp, "candles_count")
        assert resp.symbol == "BTCUSDT"
        assert resp.candles_count == len(resp.candles)
        print(f"\n  [E2E-02] query_case={resp.query_case} | status={resp.overall_status} | candles={resp.candles_count}")

    def test_ch_dq_quarantine_end_to_end(self, ch_client):
        """[E2E-03] DQ pipeline: reject records -> insert quarantine -> verify SELECT."""
        from src.data_quality.dq_checks import clean_and_deduplicate
        from src.batch_layer.clickhouse_sync import ClickHouseBatchSync

        bad_records = [
            {"trade_id": 1, "symbol": "BTCUSDT", "price": -1.0,
             "quantity": 1.0, "trade_time": 1700000000000},  # price_non_positive
            {"trade_id": 2, "symbol": "ETHUSDT", "price": 3000.0,
             "quantity": 0.0, "trade_time": 1700000060000},  # quantity_non_positive
        ]

        _, rejected, report = clean_and_deduplicate(bad_records)
        assert report.rejected_records == 2

        batch_run_id = f"integ_e2e_{uuid.uuid4().hex[:8]}"
        sync = ClickHouseBatchSync(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ["CLICKHOUSE_PORT"]),
        )
        inserted = sync.insert_quarantine_records(rejected, batch_run_id=batch_run_id)
        assert inserted == 2

        time.sleep(0.5)
        rows = ch_client.query(
            "SELECT count(*) FROM lakehouse.dq_quarantine WHERE batch_run_id = %(rid)s",
            parameters={"rid": batch_run_id},
        ).result_rows[0][0]
        assert rows == 2, f"Expected 2 quarantine rows, got {rows}"
        print(f"\n  [E2E-03] DQ quarantine E2E: {inserted} rejected records verified in ClickHouse")
