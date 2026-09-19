"""
run_tests.py
============
Script chay toan bo Unit Test noi bo ma KHONG can shell command.
Su dung: python scripts/run_tests.py

Chay tung test suite theo thu tu:
  1. test_ingestion    -- Models, FaultInjector (khong can Spark, Kafka)
  2. test_speed_layer  -- MetricsCalculator, SpikeDetector, Schema (khong can Spark)
  3. test_query_merger -- AutoCorrectingQueryMerger (mock ClickHouse, mock Watermark)
  4. test_dagster_pipeline -- Dagster assets (khong can Docker)
  5. test_dashboard_components -- Streamlit UI logic

Cac test co nhan [SKIP_DOCKER] se tu dong bo qua neu ClickHouse / Redis / Kafka
chua duoc chay (docker compose up -d).
"""

from __future__ import annotations

import os
import sys
import unittest
import time
from pathlib import Path

# ── PYTHONPATH ────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

# ── Color output ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

SUITES = [
    ("Ingestion Models & FaultInjector", "tests.test_ingestion"),
    ("Speed Layer: OHLCV / VWAP / SpikeDetector", "tests.test_speed_layer"),
    ("Query Merger: 3-Case Lambda Logic", "tests.test_query_merger"),
    ("Dagster Pipeline Assets", "tests.test_dagster_pipeline"),
    ("Dashboard Components", "tests.test_dashboard_components"),
]


def run_suite(label: str, module_path: str) -> dict:
    print(f"\n{BOLD}{'─'*60}{RESET}")
    print(f"{BOLD}[TEST] {label}{RESET}")
    print(f"{'─'*60}")

    try:
        loader = unittest.TestLoader()
        suite  = loader.loadTestsFromName(module_path)
    except Exception as exc:
        print(f"{RED}[LOAD ERROR] {exc}{RESET}")
        return {"label": label, "status": "load_error", "error": str(exc), "ran": 0, "failures": 0, "errors": 1}

    result = unittest.TextTestRunner(verbosity=2, failfast=False).run(suite)
    status = "PASS" if result.wasSuccessful() else "FAIL"
    color  = GREEN if status == "PASS" else RED

    print(f"\n{color}[{status}] {label} — ran={result.testsRun} failures={len(result.failures)} errors={len(result.errors)}{RESET}")
    return {
        "label": label,
        "status": status,
        "ran": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
    }


def main():
    start = time.time()
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  DATA LAKEHOUSE — LOCAL TEST RUNNER{RESET}")
    print(f"{BOLD}  {ROOT_DIR}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")

    results = []
    for label, module in SUITES:
        r = run_suite(label, module)
        results.append(r)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - start
    total_ran      = sum(r.get("ran", 0) for r in results)
    total_failures = sum(r.get("failures", 0) for r in results)
    total_errors   = sum(r.get("errors", 0) for r in results)
    passed_suites  = sum(1 for r in results if r["status"] == "PASS")

    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  SUMMARY{RESET}")
    print(f"{'─'*60}")
    print(f"  {'Suite':<45} {'Status':>8}")
    print(f"{'─'*60}")
    for r in results:
        color = GREEN if r["status"] == "PASS" else RED
        print(f"  {r['label']:<45} {color}{r['status']:>8}{RESET}")
    print(f"{'─'*60}")
    overall_color = GREEN if total_failures == 0 and total_errors == 0 else RED
    print(f"  {overall_color}{BOLD}Suites: {passed_suites}/{len(results)} passed | Tests: {total_ran} ran | Failures: {total_failures} | Errors: {total_errors} | {elapsed:.1f}s{RESET}")
    print(f"{BOLD}{'='*60}{RESET}\n")

    if total_failures > 0 or total_errors > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
