"""
test_data_quality.py
====================
Unit tests cho Data Quality Module:
  1. dq_checks.py   - Normalization, Validation, Clean & Deduplication
  2. quarantine.py  - Quarantine Record wrapping, formatting, error summary
  3. dq_metrics.py  - Basic metrics, Fault injection evaluation (Precision/Recall/F1), CSV logging
"""

import os
import tempfile
import unittest
from pathlib import Path

from src.data_quality.dq_checks import (
    QualityReport,
    clean_and_deduplicate,
    normalize_trade_record,
    validate_trade_record,
)
from src.data_quality.quarantine import (
    QuarantineManager,
    QuarantineRecord,
)
from src.data_quality.dq_metrics import (
    DQMetricsCollector,
    DQMetricsSummary,
)


class TestDQChecks(unittest.TestCase):
    """Kiem tra cac ham rang buoc du lieu trong dq_checks.py."""

    def test_normalize_valid_record(self):
        raw = {
            "trade_id": "1001",
            "symbol": "btcusdt",
            "price": "65000.5",
            "quantity": "0.25",
            "trade_time": "1700000000000",
            "is_buyer_maker": True,
        }
        norm = normalize_trade_record(raw)
        self.assertEqual(norm["trade_id"], 1001)
        self.assertEqual(norm["symbol"], "BTCUSDT")
        self.assertEqual(norm["price"], 65000.5)
        self.assertEqual(norm["quantity"], 0.25)
        self.assertTrue(norm["is_buyer_maker"])

    def test_normalize_missing_field_raises(self):
        raw = {"trade_id": 1001, "price": 65000.0}
        with self.assertRaises(ValueError):
            normalize_trade_record(raw)

    def test_validate_rules(self):
        # Valid
        ok, reason = validate_trade_record({
            "trade_id": 1, "symbol": "BTCUSDT", "price": 50000.0, "quantity": 1.0, "trade_time": 1000
        })
        self.assertTrue(ok)
        self.assertIsNone(reason)

        # Invalid price
        ok, reason = validate_trade_record({
            "trade_id": 1, "symbol": "BTCUSDT", "price": -5.0, "quantity": 1.0, "trade_time": 1000
        })
        self.assertFalse(ok)
        self.assertEqual(reason, "price_non_positive")

        # Invalid quantity
        ok, reason = validate_trade_record({
            "trade_id": 1, "symbol": "BTCUSDT", "price": 50000.0, "quantity": 0.0, "trade_time": 1000
        })
        self.assertFalse(ok)
        self.assertEqual(reason, "quantity_non_positive")

    def test_clean_and_deduplicate(self):
        records = [
            {"trade_id": 1, "symbol": "BTCUSDT", "price": 60000.0, "quantity": 1.0, "trade_time": 1000, "ingestion_time": 1000},
            {"trade_id": 1, "symbol": "BTCUSDT", "price": 60000.0, "quantity": 1.0, "trade_time": 1000, "ingestion_time": 2000},  # duplicate
            {"trade_id": 2, "symbol": "BTCUSDT", "price": -100.0, "quantity": 1.0, "trade_time": 1000},  # invalid price
            {"trade_id": 3, "symbol": "ETHUSDT", "price": 3000.0, "quantity": 2.0, "trade_time": 1001},  # valid
        ]
        clean, rejected, report = clean_and_deduplicate(records)
        self.assertEqual(len(clean), 2)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(report.duplicates_removed, 1)
        self.assertEqual(report.total_records, 4)
        self.assertEqual(report.pass_rate, 50.0)


class TestQuarantineManager(unittest.TestCase):
    """Kiem tra QuarantineManager trong quarantine.py."""

    def test_wrap_record(self):
        raw = {"trade_id": 999, "symbol": "BTCUSDT", "price": -1.0}
        qr = QuarantineManager.wrap_record(raw, dq_error="price_non_positive", batch_run_id="run_01")
        self.assertEqual(qr.trade_id, "999")
        self.assertEqual(qr.symbol, "BTCUSDT")
        self.assertEqual(qr.dq_error, "price_non_positive")
        self.assertEqual(qr.batch_run_id, "run_01")

    def test_format_for_clickhouse(self):
        qm = QuarantineManager()
        rejected = [
            {"trade_id": 1, "symbol": "BTCUSDT", "price": -1.0, "quantity": 1.0, "trade_time": 1000, "dq_error": "price_non_positive"},
            {"trade_id": 2, "symbol": "ETHUSDT", "price": 3000.0, "quantity": -0.5, "trade_time": 1000, "dq_error": "quantity_non_positive"},
        ]
        rows = qm.format_for_clickhouse(rejected, batch_run_id="batch_123")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "batch_123")
        self.assertEqual(rows[0][1], "1")
        self.assertEqual(rows[0][6], "price_non_positive")

    def test_get_error_summary(self):
        rejected = [
            {"symbol": "BTCUSDT", "dq_error": "price_non_positive"},
            {"symbol": "BTCUSDT", "dq_error": "price_non_positive"},
            {"symbol": "ETHUSDT", "dq_error": "quantity_non_positive"},
        ]
        summary = QuarantineManager.get_error_summary(rejected)
        self.assertEqual(summary["total_quarantined"], 3)
        self.assertEqual(summary["by_error"]["price_non_positive"], 2)
        self.assertEqual(summary["by_symbol"]["BTCUSDT"], 2)


class TestDQMetricsCollector(unittest.TestCase):
    """Kiem tra DQMetricsCollector trong dq_metrics.py."""

    def test_compute_basic_metrics(self):
        report = QualityReport(
            total_records=100,
            valid_records=85,
            rejected_records=10,
            duplicates_removed=5,
        )
        summary = DQMetricsCollector.compute_basic_metrics(report, batch_run_id="test_run")
        self.assertEqual(summary.total_records, 100)
        self.assertEqual(summary.pass_rate, 85.0)
        self.assertEqual(summary.rejection_rate, 10.0)
        self.assertEqual(summary.duplicate_rate, 5.0)

    def test_evaluate_fault_injection(self):
        # 10 clean records
        clean = [
            {"trade_id": i, "is_injected": False} for i in range(10)
        ]
        # 5 injected faults correctly caught in rejected (TP = 5)
        # 1 clean record wrongly rejected (FP = 1)
        rejected = [
            {"trade_id": 100 + i, "is_injected": True} for i in range(5)
        ] + [{"trade_id": 999, "is_injected": False}]

        summary = DQMetricsCollector.evaluate_fault_injection(clean, rejected, batch_run_id="eval_run")
        self.assertEqual(summary.injected_detected, 5)
        self.assertEqual(summary.injected_leaked, 0)
        self.assertEqual(summary.recall, 1.0)  # 5/5 = 100% recall
        self.assertEqual(summary.precision, round(5 / 6, 4))  # 5/(5+1) = 83.33%
        self.assertGreater(summary.f1_score, 0.9)

    def test_log_to_csv_and_markdown(self):
        summary = DQMetricsSummary(
            batch_run_id="run_csv_test",
            total_records=1000,
            valid_records=950,
            rejected_records=50,
            duplicates_removed=20,
            pass_rate=95.0,
            rejection_rate=5.0,
            duplicate_rate=2.0,
            precision=0.98,
            recall=1.0,
            f1_score=0.9899,
            injected_count=50,
            injected_detected=49,
            injected_leaked=1,
            timestamp="2026-09-30T14:00:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test_metrics.csv"
            DQMetricsCollector.log_to_csv(summary, output_csv=csv_path)
            self.assertTrue(csv_path.exists())

        md = DQMetricsCollector.format_markdown_report(summary)
        self.assertIn("Precision", md)
        self.assertIn("Recall", md)
        self.assertIn("95.0%", md)


if __name__ == "__main__":
    unittest.main()
