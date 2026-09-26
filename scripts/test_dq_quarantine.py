"""
test_dq_quarantine.py
=====================
End-to-end test: verify that rejected DQ records are persisted to
``lakehouse.dq_quarantine`` in ClickHouse.

Run from project root:
    python scripts/test_dq_quarantine.py

Exit code 0 = PASS, non-zero = FAIL.
"""

from __future__ import annotations

import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from datetime import datetime, timezone

from src.data_quality.dq_checks import clean_and_deduplicate
from src.batch_layer.clickhouse_sync import ClickHouseBatchSync

# ---------------------------------------------------------------------------
# 1.  Synthetic dataset — mix of valid and intentionally bad records
# ---------------------------------------------------------------------------

BATCH_RUN_ID = f"dq_test_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

RAW_RECORDS = [
    # valid ✅
    {"trade_id": 1001, "symbol": "BTCUSDT", "price": 65000.0, "quantity": 0.5,
     "trade_time": 1700000000000, "is_buyer_maker": False},
    {"trade_id": 1002, "symbol": "ETHUSDT", "price": 3400.0, "quantity": 1.2,
     "trade_time": 1700000060000, "is_buyer_maker": True},
    {"trade_id": 1003, "symbol": "BNBUSDT", "price": 420.0, "quantity": 5.0,
     "trade_time": 1700000120000, "is_buyer_maker": False},

    # ❌ price <= 0
    {"trade_id": 2001, "symbol": "BTCUSDT", "price": -1.0, "quantity": 0.1,
     "trade_time": 1700000180000},

    # ❌ quantity <= 0
    {"trade_id": 2002, "symbol": "ETHUSDT", "price": 3400.0, "quantity": 0.0,
     "trade_time": 1700000240000},

    # ❌ missing required field (no price)
    {"trade_id": 2003, "symbol": "SOLUSDT", "quantity": 10.0,
     "trade_time": 1700000300000},

    # ❌ trade_time <= 0
    {"trade_id": 2004, "symbol": "XRPUSDT", "price": 0.65, "quantity": 100.0,
     "trade_time": -1},

    # ❌ missing symbol
    {"trade_id": 2005, "price": 100.0, "quantity": 1.0,
     "trade_time": 1700000360000},

    # valid ✅ — duplicate of 1001, older ingestion_time → should be deduped
    {"trade_id": 1001, "symbol": "BTCUSDT", "price": 64999.0, "quantity": 0.5,
     "trade_time": 1700000000000, "ingestion_time": 1699999990000},
]

# ---------------------------------------------------------------------------
# 2.  Run DQ pipeline
# ---------------------------------------------------------------------------

print(f"\n{'='*60}")
print(f"  DQ Quarantine Test   run_id={BATCH_RUN_ID}")
print(f"{'='*60}\n")

clean, rejected, report = clean_and_deduplicate(RAW_RECORDS)

print(f"[DQ] Total input    : {report.total_records}")
print(f"[DQ] Valid records  : {report.valid_records}")
print(f"[DQ] Rejected       : {report.rejected_records}")
print(f"[DQ] Duplicates     : {report.duplicates_removed}")
print(f"[DQ] Pass rate      : {report.pass_rate:.1f}%\n")

assert report.rejected_records >= 5, (
    f"Expected at least 5 rejected records, got {report.rejected_records}"
)
print("[DQ] ✅ Rejection count assertion passed.\n")

print("[DQ] Rejected record dq_errors:")
for r in rejected:
    print(f"      trade_id={r.get('trade_id', '?'):>6}  symbol={r.get('symbol','?'):<10}  "
          f"error={r.get('dq_error')}")

# ---------------------------------------------------------------------------
# 3.  Insert rejected records into ClickHouse quarantine table
# ---------------------------------------------------------------------------

print("\n[CH] Connecting to ClickHouse...")
sync = ClickHouseBatchSync()
client = sync.get_client()

if client is None:
    print("[CH] ⚠️  ClickHouse unavailable — skipping insert & verify. "
          "Make sure ClickHouse container is running.")
    sys.exit(0)

print("[CH] Connected ✅")
rows_inserted = sync.insert_quarantine_records(rejected, batch_run_id=BATCH_RUN_ID)
print(f"[CH] Inserted {rows_inserted} quarantine rows.\n")

assert rows_inserted == report.rejected_records, (
    f"Insert count mismatch: inserted={rows_inserted} expected={report.rejected_records}"
)
print("[CH] ✅ Insert count assertion passed.\n")

# ---------------------------------------------------------------------------
# 4.  Verify via SELECT — read back and confirm
# ---------------------------------------------------------------------------

print("[CH] Verifying via SELECT from lakehouse.dq_quarantine ...")
result = client.query(
    "SELECT batch_run_id, trade_id, symbol, dq_error, quarantined_at "
    "FROM lakehouse.dq_quarantine "
    "WHERE batch_run_id = %(run_id)s "
    "ORDER BY quarantined_at",
    parameters={"run_id": BATCH_RUN_ID},
)

rows = result.result_rows
print(f"[CH] SELECT returned {len(rows)} rows:\n")
print(f"  {'batch_run_id':<32}  {'trade_id':<8}  {'symbol':<12}  {'dq_error':<30}  quarantined_at")
print(f"  {'-'*32}  {'-'*8}  {'-'*12}  {'-'*30}  {'-'*24}")
for row in rows:
    print(f"  {str(row[0]):<32}  {str(row[1]):<8}  {str(row[2]):<12}  {str(row[3]):<30}  {row[4]}")

assert len(rows) == report.rejected_records, (
    f"SELECT count mismatch: got={len(rows)} expected={report.rejected_records}"
)

# ---------------------------------------------------------------------------
# 5.  Summary
# ---------------------------------------------------------------------------

print(f"\n{'='*60}")
print("  ✅  DQ QUARANTINE TEST PASSED")
print(f"  - {report.rejected_records} records quarantined in lakehouse.dq_quarantine")
print(f"  - batch_run_id: {BATCH_RUN_ID}")
print(f"{'='*60}\n")
