"""
run_all_benchmarks.py
=====================
Chay toan bo 3 Benchmark theo thu tu:
  1. bench_latency    -- Query Latency (Batch / Speed / Lambda)
  2. bench_reprocess  -- Reprocessing Correctness (MAE / MAPE / RMSE)
  3. bench_compaction -- Iceberg Compaction Efficiency

Ket qua CSV duoc ghi vao results/logs/:
  bench_latency.csv
  bench_reprocess.csv
  bench_reprocess_summary.csv
  bench_compaction.csv

Su dung:
  python scripts/benchmarks/run_all_benchmarks.py
  python scripts/benchmarks/run_all_benchmarks.py --runs 50 --symbols BTCUSDT ETHUSDT SOLUSDT

Luu y:
  - Benchmark 1 va 2 yeu cau ClickHouse dang chay.
  - Benchmark 3 tu dong chuyen sang Simulation Mode neu Spark khong san sang.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── path setup ──────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.utils.logger import setup_logger

logger = setup_logger("run_all_benchmarks")


def _separator(title: str) -> None:
    width = 70
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def main() -> None:
    parser = argparse.ArgumentParser(description="Chay toan bo 3 benchmark")
    parser.add_argument(
        "--runs", type=int, default=30,
        help="So lan lap lai cho Benchmark 1 va 3. Mac dinh: 30",
    )
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="Danh sach coin symbols. Mac dinh: BTCUSDT ETHUSDT",
    )
    parser.add_argument(
        "--watermark-offset-hours", type=int, default=24,
        help="Mock watermark offset (gio) cho Benchmark 1. Mac dinh: 24",
    )
    parser.add_argument(
        "--hours-back", type=int, default=2,
        help="Bat dau cua so so sanh cua Benchmark 2 (gio truoc now). Mac dinh: 2",
    )
    parser.add_argument(
        "--window-hours", type=int, default=1,
        help="Do rong cua so so sanh cua Benchmark 2 (gio). Mac dinh: 1",
    )
    parser.add_argument(
        "--simulate-compaction", action="store_true",
        help="Buoc Benchmark 3 chay Simulation Mode.",
    )
    args = parser.parse_args()

    start_time = datetime.now(timezone.utc)
    logger.info(
        "Bat dau chay toan bo 3 Benchmark | ts=%s", start_time.isoformat()
    )

    results = {}

    # ─────────────────────────────────────────────────────────────────────────
    # BENCHMARK 1: Query Latency
    # ─────────────────────────────────────────────────────────────────────────
    _separator("BENCHMARK 1: QUERY LATENCY  (Batch vs Speed vs Lambda)")
    t0 = time.perf_counter()
    try:
        from scripts.benchmarks.bench_latency import (
            OUTPUT_CSV as LAT_CSV,
            run_benchmark as lat_run,
            write_csv as lat_write,
            print_summary as lat_print,
        )
        rows_lat = lat_run(
            symbols=args.symbols,
            runs=args.runs,
            watermark_offset_hours=args.watermark_offset_hours,
        )
        if rows_lat:
            lat_write(rows_lat, LAT_CSV)
            lat_print(rows_lat)
        results["benchmark_1"] = {"status": "ok", "rows": len(rows_lat)}
    except SystemExit:
        logger.error("Benchmark 1 dung lai (ClickHouse khong san sang).")
        results["benchmark_1"] = {"status": "skipped"}
    except Exception as exc:
        logger.error("Benchmark 1 loi: %s", exc)
        results["benchmark_1"] = {"status": "error", "msg": str(exc)}
    results["benchmark_1"]["duration_s"] = round(time.perf_counter() - t0, 2)

    # ─────────────────────────────────────────────────────────────────────────
    # BENCHMARK 2: Reprocessing Correctness
    # ─────────────────────────────────────────────────────────────────────────
    _separator("BENCHMARK 2: REPROCESSING CORRECTNESS  (MAE / MAPE / RMSE)")
    t0 = time.perf_counter()
    try:
        from scripts.benchmarks.bench_reprocess import (
            OUTPUT_CSV as REP_CSV,
            run_benchmark as rep_run,
            write_candle_csv,
            write_summary_csv,
            print_summary as rep_print,
        )
        candle_rows, summary_rows = rep_run(
            symbols=args.symbols,
            hours_back=args.hours_back,
            window_hours=args.window_hours,
        )
        if candle_rows:
            write_candle_csv(candle_rows, REP_CSV)
        if summary_rows:
            write_summary_csv(summary_rows, REP_CSV)
            rep_print(summary_rows)
        results["benchmark_2"] = {
            "status": "ok",
            "candle_rows": len(candle_rows),
            "summary_rows": len(summary_rows),
        }
    except SystemExit:
        logger.error("Benchmark 2 dung lai (ClickHouse khong san sang).")
        results["benchmark_2"] = {"status": "skipped"}
    except Exception as exc:
        logger.error("Benchmark 2 loi: %s", exc)
        results["benchmark_2"] = {"status": "error", "msg": str(exc)}
    results["benchmark_2"]["duration_s"] = round(time.perf_counter() - t0, 2)

    # ─────────────────────────────────────────────────────────────────────────
    # BENCHMARK 3: Iceberg Compaction Efficiency
    # ─────────────────────────────────────────────────────────────────────────
    _separator("BENCHMARK 3: ICEBERG COMPACTION EFFICIENCY")
    t0 = time.perf_counter()
    try:
        from scripts.benchmarks.bench_compaction import (
            OUTPUT_CSV as COMP_CSV,
            run_benchmark as comp_run,
            write_csv as comp_write,
            print_summary as comp_print,
        )
        # Dung symbol dau tien trong danh sach
        symbol_for_compact = args.symbols[0] if args.symbols else "BTCUSDT"
        rows_comp = comp_run(
            symbol=symbol_for_compact,
            runs=args.runs,
            force_simulate=args.simulate_compaction,
        )
        if rows_comp:
            comp_write(rows_comp, COMP_CSV)
            comp_print(rows_comp)
        results["benchmark_3"] = {"status": "ok", "rows": len(rows_comp)}
    except Exception as exc:
        logger.error("Benchmark 3 loi: %s", exc)
        results["benchmark_3"] = {"status": "error", "msg": str(exc)}
    results["benchmark_3"]["duration_s"] = round(time.perf_counter() - t0, 2)

    # ─────────────────────────────────────────────────────────────────────────
    # FINAL REPORT
    # ─────────────────────────────────────────────────────────────────────────
    total_s = (datetime.now(timezone.utc) - start_time).total_seconds()
    _separator("TONG KET CHAY BENCHMARK")
    print(f"  {'Benchmark':<25} {'Trang thai':<12} {'Thoi gian':>10}")
    print("-" * 55)
    for key, info in results.items():
        status_icon = {"ok": "[OK]", "skipped": "[SKIP]", "error": "[ERR]"}.get(
            info.get("status", "error"), "[?]"
        )
        dur = info.get("duration_s", 0.0)
        print(f"  {key:<25} {status_icon:<12} {dur:>9.1f}s")
    print("-" * 55)
    print(f"  {'Tong cong':<25} {'':12} {total_s:>9.1f}s")
    print(f"\n  Ket qua CSV: {ROOT_DIR / 'results' / 'logs'}\n")


if __name__ == "__main__":
    main()
