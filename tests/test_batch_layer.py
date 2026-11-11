"""
test_batch_layer.py
===================
Unit tests cho cac ban sua loi o Batch Layer (khong can Docker):
  1. SilverTradeStore: cua so bi chia qua hai lan chay / su kien tre van cho nen day du
  2. Watermark: ung vien theo do tre cho phep, va quy tac don dieu tang khi ghi
  3. Kafka offset: chi commit sau khi ghi ben vung, khong fallback sang du lieu mock
  4. Quy tac doi soat MATCHED dung chung
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.batch_layer import spark_batch_jobs as sbj
from src.batch_layer.clickhouse_sync import ClickHouseBatchSync
from src.batch_layer.silver_store import SilverTradeStore
from src.utils.reconciliation import (
    STATUS_BATCH_ONLY,
    STATUS_HEALED,
    STATUS_MATCHED,
    STATUS_PROVISIONAL_ONLY,
    classify_window,
    volume_drift_pct,
)

T0 = 1_700_000_040_000  # 2023-11-14 22:14:00 UTC, dung dau phut


def trade(tid, price, qty, t_ms, ingest=None, symbol="BTCUSDT"):
    return {
        "trade_id": tid, "symbol": symbol, "price": price, "quantity": qty,
        "trade_time": t_ms, "ingestion_time": ingest or t_ms,
        "is_buyer_maker": False, "is_injected": False, "fault_type": None,
    }


class TestIncrementalGoldCandles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SilverTradeStore(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_window_split_across_two_runs_gives_full_candle(self):
        run1 = [trade(1, 100.0, 1.0, T0 + 1_000), trade(2, 110.0, 1.0, T0 + 20_000)]
        run2 = [trade(3, 90.0, 2.0, T0 + 50_000)]
        sbj.build_incremental_gold_candles(run1, self.store)
        candles, _ = sbj.build_incremental_gold_candles(run2, self.store)

        self.assertEqual(len(candles), 1)
        c = candles[0]
        self.assertEqual(c["trade_count"], 3)
        self.assertEqual(c["open_price"], 100.0)
        self.assertEqual(c["close_price"], 90.0)
        self.assertEqual(c["high_price"], 110.0)
        self.assertEqual(c["low_price"], 90.0)
        self.assertAlmostEqual(c["volume"], 4.0)
        self.assertAlmostEqual(c["vwap"], (100 + 110 + 180) / 4.0, places=4)

    def test_late_event_recomputes_only_its_window(self):
        sbj.build_incremental_gold_candles(
            [trade(1, 100.0, 1.0, T0 + 1_000), trade(2, 200.0, 1.0, T0 + 61_000)], self.store
        )
        candles, _ = sbj.build_incremental_gold_candles([trade(9, 104.0, 1.0, T0 + 30_000)], self.store)
        self.assertEqual(len(candles), 1)  # chi cua so phut dau bi anh huong
        self.assertEqual(candles[0]["trade_count"], 2)
        self.assertEqual(candles[0]["close_price"], 104.0)

    def test_rerun_same_batch_is_idempotent(self):
        batch = [trade(1, 100.0, 1.0, T0 + 1_000), trade(2, 101.0, 1.0, T0 + 2_000)]
        first, dups_first = sbj.build_incremental_gold_candles(batch, self.store)
        again, dups_again = sbj.build_incremental_gold_candles(batch, self.store)
        self.assertEqual(dups_first, 0)
        self.assertEqual(dups_again, 2)
        self.assertEqual(first[0]["trade_count"], again[0]["trade_count"])
        self.assertEqual(again[0]["volume"], 2.0)

    def test_newer_ingestion_time_replaces_stored_record(self):
        sbj.build_incremental_gold_candles([trade(1, 100.0, 1.0, T0 + 1_000, ingest=1)], self.store)
        candles, dups = sbj.build_incremental_gold_candles(
            [trade(1, 100.0, 3.0, T0 + 1_000, ingest=2)], self.store
        )
        self.assertEqual(dups, 1)
        self.assertEqual(candles[0]["trade_count"], 1)
        self.assertEqual(candles[0]["volume"], 3.0)


class TestWatermarkCandidate(unittest.TestCase):
    def test_file_mode_uses_max_window_end(self):
        records = [trade(1, 1.0, 1.0, T0 + 5_000)]
        candles = sbj.aggregate_gold_candles(records)
        wm = sbj.compute_watermark_candidate(records, candles, incremental=False)
        self.assertEqual(wm, candles[0]["window_end"])

    def test_incremental_mode_holds_back_open_minute(self):
        records = [trade(1, 1.0, 1.0, T0 + 30_000), trade(2, 1.0, 1.0, T0 + 150_000)]
        candles = sbj.aggregate_gold_candles(records)
        wm = sbj.compute_watermark_candidate(records, candles, incremental=True, lateness_seconds=60)
        # max event = T0+150s -> tru 60s = T0+90s -> lam tron phut = T0+60s
        self.assertEqual(wm, datetime.fromtimestamp((T0 + 60_000) / 1000, timezone.utc))
        self.assertLess(wm, max(c["window_end"] for c in candles))

    def test_no_candles_no_watermark(self):
        self.assertIsNone(sbj.compute_watermark_candidate([], [], incremental=True))


class _FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class _FakeCHClient:
    """ClickHouse gia lap luu bang system_watermark trong bo nho."""

    def __init__(self):
        self.watermarks = []

    def command(self, _sql):
        return None

    def query(self, _sql, parameters=None):
        if not self.watermarks:
            return _FakeResult([(datetime(1970, 1, 1, tzinfo=timezone.utc), 0)])
        return _FakeResult([(max(self.watermarks), len(self.watermarks))])

    def insert(self, table, data, column_names=None):
        if table == "lakehouse.system_watermark":
            self.watermarks.append(data[0][1])


class TestMonotonicWatermark(unittest.TestCase):
    def test_watermark_never_moves_backwards(self):
        sync = ClickHouseBatchSync()
        sync._client = _FakeCHClient()
        newer = datetime(2026, 10, 17, 10, 0, tzinfo=timezone.utc)
        older = newer - timedelta(hours=3)

        self.assertTrue(sync.update_watermark(newer))
        self.assertFalse(sync.update_watermark(older))   # lan chay chi co du lieu tre
        self.assertFalse(sync.update_watermark(newer))   # khong tien -> khong ghi
        self.assertEqual(sync.get_current_watermark(), newer)
        self.assertTrue(sync.update_watermark(newer + timedelta(minutes=1)))
        self.assertEqual(len(sync._client.watermarks), 2)


class _FakeSync:
    """Thay ClickHouseBatchSync trong run_batch_pipeline; `available` gia lap ClickHouse song/chet."""

    available = True

    def __init__(self):
        self.inserted = []

    def get_client(self):
        return object() if self.available else None

    def insert_quarantine_records(self, rows, batch_run_id):
        return len(list(rows)) if self.available else 0

    def insert_batch_aggregates(self, candles, batch_run_id):
        if not self.available:
            return 0
        self.inserted.extend(candles)
        return len(candles)

    def update_watermark(self, wm):
        return self.available

    def get_current_watermark(self):
        return None


class TestKafkaOffsetCommit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.offset_file = Path(self.tmp.name) / "offsets.json"
        self.store_dir = Path(self.tmp.name) / "silver"
        patches = [
            mock.patch.dict(os.environ, {"BATCH_SILVER_STORE_DIR": str(self.store_dir)}),
            mock.patch.object(sbj, "ClickHouseBatchSync", _FakeSync),
            mock.patch.object(sbj, "sync_reconciliation_audit", return_value=0),
            mock.patch.object(sbj, "write_batch_run_log", return_value=Path(self.tmp.name) / "log.csv"),
            # Neu pipeline con fallback sang du lieu mock thi test se that bai ngay
            mock.patch.object(sbj, "load_default_records", side_effect=AssertionError("fallback to mock data")),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        self.tmp.cleanup()

    def _kafka(self, records):
        return sbj.KafkaReadResult(
            records=records,
            offsets={"crypto_trades_raw:0": len(records)},
            checkpoint_path=self.offset_file,
            advanced=bool(records),
        )

    def test_offsets_committed_only_after_clickhouse_write(self):
        _FakeSync.available = True
        with mock.patch.object(sbj, "read_kafka_records", return_value=self._kafka([trade(1, 1.0, 1.0, T0)])):
            summary = sbj.run_batch_pipeline(from_kafka=True)
        self.assertTrue(summary["kafka_offsets_committed"])
        self.assertEqual(json.loads(self.offset_file.read_text())["crypto_trades_raw:0"], 1)

    def test_offsets_not_committed_when_clickhouse_down(self):
        _FakeSync.available = False
        try:
            with mock.patch.object(sbj, "read_kafka_records", return_value=self._kafka([trade(1, 1.0, 1.0, T0)])):
                summary = sbj.run_batch_pipeline(from_kafka=True)
        finally:
            _FakeSync.available = True
        self.assertFalse(summary["kafka_offsets_committed"])
        self.assertFalse(self.offset_file.exists())

    def test_empty_kafka_does_not_fall_back_to_mock(self):
        with mock.patch.object(sbj, "read_kafka_records", return_value=self._kafka([])):
            summary = sbj.run_batch_pipeline(from_kafka=True)
        self.assertEqual(summary["total_records"], 0)
        self.assertIsNone(summary["watermark"])


class TestLoadDefaultRecordsNoFallback(unittest.TestCase):
    def test_kafka_mode_returns_empty_instead_of_mock_data(self):
        with mock.patch.object(sbj, "consume_kafka_records", return_value=[]), \
             mock.patch.object(sbj, "load_records_from_path", side_effect=AssertionError("fallback")):
            self.assertEqual(sbj.load_default_records(from_kafka=True), [])


class TestReconciliationRules(unittest.TestCase):
    def test_classification(self):
        self.assertEqual(classify_window(True, True, 0.0, 0.0), STATUS_MATCHED)
        self.assertEqual(classify_window(True, True, 0.5, 0.0), STATUS_HEALED)
        self.assertEqual(classify_window(True, True, 0.0, 2.5), STATUS_HEALED)
        self.assertEqual(classify_window(True, False, None, None), STATUS_BATCH_ONLY)
        self.assertEqual(classify_window(False, True, None, None), STATUS_PROVISIONAL_ONLY)

    def test_volume_drift_is_relative(self):
        # Cung do lech tuong doi 0.5% cho BTC (khoi luong nho) va XRP (khoi luong lon)
        self.assertEqual(volume_drift_pct(2.0, 2.01), 0.5)
        self.assertEqual(volume_drift_pct(200000.0, 201000.0), 0.5)
        self.assertEqual(volume_drift_pct(0.0, 0.0), 0.0)
        self.assertIsNone(volume_drift_pct(0.0, 1.0))


if __name__ == "__main__":
    unittest.main()
