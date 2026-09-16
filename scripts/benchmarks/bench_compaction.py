"""
bench_compaction.py
===================
Benchmark 3: Iceberg Compaction Efficiency

Muc tieu:
  Do luong hieu qua cua Iceberg Compaction (Bin-Packing) bang cach:
    1. Chay mot loat query doc du lieu tu Bronze Iceberg table TRUOC compaction
    2. Kich hoat IcebergCompactionJob (Bin-Packing)
    3. Chay lai cung loat query do NACH compaction
    4. So sanh: thoi gian doc, so file Parquet, kich thuoc manifest

Chi so do:
  - read_latency_ms       : thoi gian doc trung binh (ms) truoc/sau
  - file_count            : so luong file Parquet trong partition
  - speedup_ratio         : latency_before / latency_after
  - compaction_duration_s : thoi gian thuc thi compaction

Kich ban:
  - Neu Spark/Iceberg chua san sang (khong co Docker), script chay Simulation Mode:
    * Sinh du lieu gia lap truoc/sau (based on real compaction math)
    * Ghi ket qua simulation ra CSV voi co `simulated=True`
    * Log ro rang rang day la simulation, khong phai ket qua thuc

Dau ra:
  results/logs/bench_compaction.csv
  Cot: run_id, mode, phase, symbol_filter, read_latency_ms, file_count,
       compaction_duration_s, speedup_ratio, simulated, ts

Su dung:
  python scripts/benchmarks/bench_compaction.py
  python scripts/benchmarks/bench_compaction.py --rows 10000 --runs 20
  python scripts/benchmarks/bench_compaction.py --simulate   (buoc ep simulation)

Luu y:
  - Mode thuc yeu cau: docker compose up -d spark iceberg-rest minio
  - Mode simulation khong yeu cau bat ky Docker service nao.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── path setup ──────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.utils.logger import setup_logger

logger = setup_logger("bench_compaction")

# ── constants ────────────────────────────────────────────────────────────────
RESULT_DIR = ROOT_DIR / "results" / "logs"
OUTPUT_CSV = RESULT_DIR / "bench_compaction.csv"

FIELDNAMES = [
    "run_id",
    "mode",
    "phase",
    "symbol_filter",
    "read_latency_ms",
    "file_count",
    "compaction_duration_s",
    "speedup_ratio",
    "simulated",
    "ts",
]

# Simulation constants (based on typical Iceberg compaction outcomes)
# Truoc compact: nhieu file nho -> I/O cao -> doc cham
_SIM_BEFORE_FILE_COUNT_MIN = 80
_SIM_BEFORE_FILE_COUNT_MAX = 200
_SIM_BEFORE_LATENCY_BASE_MS = 850.0   # Co the bien dong nhieu
_SIM_BEFORE_LATENCY_NOISE_MS = 150.0

# Sau compact: it file lon -> I/O giam -> doc nhanh hon 3-5x
_SIM_AFTER_FILE_COUNT_MIN = 3
_SIM_AFTER_FILE_COUNT_MAX = 10
_SIM_AFTER_LATENCY_BASE_MS = 220.0
_SIM_AFTER_LATENCY_NOISE_MS = 40.0

_SIM_COMPACTION_DURATION_S = 45.0    # Thoi gian compact gia lap (giay)


# ── helpers ──────────────────────────────────────────────────────────────────
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _noise(base: float, noise: float) -> float:
    """Add Gaussian noise to a base value, keeping result positive."""
    return max(1.0, base + random.gauss(0, noise / 2))


# ── REAL mode: Spark + Iceberg ────────────────────────────────────────────────
def _try_import_spark():
    """Attempt to import PySpark and Iceberg utilities. Returns (spark, manager) or raises."""
    from src.batch_layer.iceberg_utils import IcebergTableManager, get_spark_session
    spark = get_spark_session("LambdaLakehouse-BenchCompaction")
    manager = IcebergTableManager(spark)
    return spark, manager


def _read_latency_real(spark, table_full_name: str, symbol: str, runs: int) -> List[float]:
    """
    Run `runs` SELECT queries against the Bronze Iceberg table and return
    the list of latencies (ms).

    Args:
        spark: Active SparkSession.
        table_full_name: Full Iceberg table name, e.g. 'lakehouse_catalog.bronze.trade_events'.
        symbol: Coin symbol to filter (e.g. 'BTCUSDT').
        runs: Number of repeated reads.

    Returns:
        List of latency measurements in milliseconds.
    """
    latencies: List[float] = []
    query = (
        f"SELECT symbol, count(*) as cnt, avg(price) as avg_price "
        f"FROM {table_full_name} "
        f"WHERE symbol = '{symbol}' "
        f"GROUP BY symbol"
    )
    for _ in range(runs):
        t0 = time.perf_counter()
        spark.sql(query).collect()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        latencies.append(elapsed_ms)
    return latencies


def _file_count_real(spark, table_full_name: str) -> int:
    """Count current data files in Iceberg table via metadata query."""
    try:
        result = spark.sql(
            f"SELECT count(*) as cnt FROM {table_full_name}.files"
        ).collect()
        return int(result[0]["cnt"]) if result else 0
    except Exception as exc:
        logger.warning("Khong the dem file count tu Iceberg metadata: %s", exc)
        return -1


def run_real_benchmark(
    symbol: str,
    runs: int,
) -> List[Dict[str, Any]]:
    """
    Real benchmark using an active Spark + Iceberg cluster.

    Args:
        symbol: Coin symbol filter for the read queries.
        runs: Number of read repetitions before and after compaction.

    Returns:
        List of result rows.
    """
    from src.batch_layer.compaction import CompactionJob
    from src.batch_layer.iceberg_utils import BRONZE_FULL_NAME

    logger.info("Khoi dong Spark Session...")
    spark, manager = _try_import_spark()

    all_rows: List[Dict[str, Any]] = []
    ts = _now_utc().isoformat()

    # ── Phase BEFORE ─────────────────────────────────────────────────────────
    logger.info("Phase BEFORE: Do latency truoc compaction (symbol=%s, runs=%d)", symbol, runs)
    before_latencies = _read_latency_real(spark, BRONZE_FULL_NAME, symbol, runs)
    file_count_before = _file_count_real(spark, BRONZE_FULL_NAME)

    for i, lat in enumerate(before_latencies, start=1):
        all_rows.append({
            "run_id": i,
            "mode": "real",
            "phase": "before",
            "symbol_filter": symbol,
            "read_latency_ms": round(lat, 3),
            "file_count": file_count_before,
            "compaction_duration_s": 0.0,
            "speedup_ratio": None,
            "simulated": False,
            "ts": ts,
        })

    avg_before = sum(before_latencies) / len(before_latencies) if before_latencies else 0.0
    logger.info("  BEFORE: avg_latency=%.1fms, file_count=%d", avg_before, file_count_before)

    # ── Compaction ────────────────────────────────────────────────────────────
    logger.info("Bat dau Iceberg Bin-Packing Compaction...")
    job = CompactionJob(spark)
    t_compact_start = time.perf_counter()
    compaction_result = job.run_binpack_compaction()
    compaction_duration_s = time.perf_counter() - t_compact_start
    logger.info(
        "Compaction hoan tat trong %.1fs: %s",
        compaction_duration_s, compaction_result,
    )

    # ── Phase AFTER ──────────────────────────────────────────────────────────
    logger.info("Phase AFTER: Do latency sau compaction (symbol=%s, runs=%d)", symbol, runs)
    after_latencies = _read_latency_real(spark, BRONZE_FULL_NAME, symbol, runs)
    file_count_after = _file_count_real(spark, BRONZE_FULL_NAME)

    avg_after = sum(after_latencies) / len(after_latencies) if after_latencies else 1.0
    speedup = avg_before / avg_after if avg_after > 0 else 0.0

    for i, lat in enumerate(after_latencies, start=1):
        all_rows.append({
            "run_id": i,
            "mode": "real",
            "phase": "after",
            "symbol_filter": symbol,
            "read_latency_ms": round(lat, 3),
            "file_count": file_count_after,
            "compaction_duration_s": round(compaction_duration_s, 2),
            "speedup_ratio": round(speedup, 3),
            "simulated": False,
            "ts": ts,
        })

    logger.info(
        "  AFTER:  avg_latency=%.1fms, file_count=%d, speedup=%.2fx",
        avg_after, file_count_after, speedup,
    )

    return all_rows


# ── SIMULATION mode ───────────────────────────────────────────────────────────
def run_simulation_benchmark(
    symbol: str,
    runs: int,
) -> List[Dict[str, Any]]:
    """
    Simulate compaction benchmark without Spark.

    Sinh du lieu gia lap dua tren cac hang so thuc nghiem tu Iceberg documentation
    va nghien cuu ve Small File Problem. Ket qua duoc danh dau `simulated=True`.

    Args:
        symbol: Coin symbol filter label.
        runs: Number of simulated read repetitions.

    Returns:
        List of simulated result rows.
    """
    logger.warning(
        "Spark/Iceberg khong kha dung. Chuyen sang SIMULATION MODE. "
        "Ket qua se duoc danh dau simulated=True."
    )

    all_rows: List[Dict[str, Any]] = []
    ts = _now_utc().isoformat()

    # Simulate file counts
    file_count_before = random.randint(_SIM_BEFORE_FILE_COUNT_MIN, _SIM_BEFORE_FILE_COUNT_MAX)
    file_count_after = random.randint(_SIM_AFTER_FILE_COUNT_MIN, _SIM_AFTER_FILE_COUNT_MAX)

    # ── Phase BEFORE ─────────────────────────────────────────────────────────
    before_latencies: List[float] = []
    for i in range(1, runs + 1):
        lat = _noise(_SIM_BEFORE_LATENCY_BASE_MS, _SIM_BEFORE_LATENCY_NOISE_MS)
        before_latencies.append(lat)
        all_rows.append({
            "run_id": i,
            "mode": "simulation",
            "phase": "before",
            "symbol_filter": symbol,
            "read_latency_ms": round(lat, 3),
            "file_count": file_count_before,
            "compaction_duration_s": 0.0,
            "speedup_ratio": None,
            "simulated": True,
            "ts": ts,
        })

    avg_before = sum(before_latencies) / len(before_latencies)

    # Simulate compaction
    logger.info("Simulation: Dang chay Iceberg Bin-Packing Compaction (simulated)...")
    time.sleep(0.5)   # brief pause to simulate async work

    # ── Phase AFTER ──────────────────────────────────────────────────────────
    after_latencies: List[float] = []
    for i in range(1, runs + 1):
        lat = _noise(_SIM_AFTER_LATENCY_BASE_MS, _SIM_AFTER_LATENCY_NOISE_MS)
        after_latencies.append(lat)
        all_rows.append({
            "run_id": i,
            "mode": "simulation",
            "phase": "after",
            "symbol_filter": symbol,
            "read_latency_ms": round(lat, 3),
            "file_count": file_count_after,
            "compaction_duration_s": round(_SIM_COMPACTION_DURATION_S, 2),
            "speedup_ratio": round(avg_before / _SIM_AFTER_LATENCY_BASE_MS, 3),
            "simulated": True,
            "ts": ts,
        })

    avg_after = sum(after_latencies) / len(after_latencies)
    speedup = avg_before / avg_after if avg_after > 0 else 0.0

    logger.info(
        "Simulation ket qua: BEFORE avg=%.1fms (%d files) | AFTER avg=%.1fms (%d files) | speedup=%.2fx",
        avg_before, file_count_before,
        avg_after, file_count_after,
        speedup,
    )

    return all_rows


# ── orchestrator ──────────────────────────────────────────────────────────────
def run_benchmark(
    symbol: str,
    runs: int,
    force_simulate: bool,
) -> List[Dict[str, Any]]:
    """
    Run compaction benchmark in real or simulation mode.

    Args:
        symbol: Coin symbol filter for read queries.
        runs: Number of read iterations before and after compaction.
        force_simulate: If True, skip Spark and run simulation directly.

    Returns:
        List of result rows.
    """
    if force_simulate:
        return run_simulation_benchmark(symbol, runs)

    try:
        logger.info("Thu khoi dong Spark Session de chay Real Benchmark...")
        return run_real_benchmark(symbol, runs)
    except Exception as exc:
        logger.warning("Khong the khoi dong Spark (%s). Chuyen sang Simulation.", exc)
        return run_simulation_benchmark(symbol, runs)


# ── CSV + summary ─────────────────────────────────────────────────────────────
def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    """Append result rows to the CSV log file."""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerows(rows)
    logger.info("Da ghi %d dong vao %s", len(rows), path)


def print_summary(rows: List[Dict[str, Any]]) -> None:
    """Print before/after summary table to stdout."""
    before_rows = [r for r in rows if r["phase"] == "before"]
    after_rows  = [r for r in rows if r["phase"] == "after"]

    avg_before = (
        sum(r["read_latency_ms"] for r in before_rows) / len(before_rows)
        if before_rows else 0.0
    )
    avg_after = (
        sum(r["read_latency_ms"] for r in after_rows) / len(after_rows)
        if after_rows else 0.0
    )
    speedup = avg_before / avg_after if avg_after > 0 else 0.0
    simulated = any(r.get("simulated", False) for r in rows)
    file_before = before_rows[0]["file_count"] if before_rows else "N/A"
    file_after  = after_rows[0]["file_count"]  if after_rows  else "N/A"
    compact_s   = after_rows[0]["compaction_duration_s"] if after_rows else 0.0

    mode_label = "SIMULATION" if simulated else "REAL"

    print("\n" + "=" * 65)
    print(f"{'BENCHMARK 3 -- ICEBERG COMPACTION EFFICIENCY':^65}")
    print(f"{'[ ' + mode_label + ' MODE ]':^65}")
    print("=" * 65)
    print(f"  {'Metric':<35} {'Before':>12} {'After':>12}")
    print("-" * 65)
    print(f"  {'Avg Read Latency (ms)':<35} {avg_before:>11.1f}  {avg_after:>11.1f}")
    print(f"  {'Parquet File Count':<35} {str(file_before):>12} {str(file_after):>12}")
    print("-" * 65)
    print(f"  {'Speedup Ratio':<35} {speedup:>10.2f}x")
    print(f"  {'Compaction Duration (s)':<35} {compact_s:>12.1f}")
    if simulated:
        print("\n  NOTE: Ket qua tren la SIMULATION. Chay voi Spark that de co so lieu chinh xac.")
    print("=" * 65 + "\n")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark 3: Iceberg Compaction Efficiency")
    parser.add_argument(
        "--symbol", default="BTCUSDT",
        help="Coin symbol dung lam filter khi do latency. Mac dinh: BTCUSDT",
    )
    parser.add_argument(
        "--runs", type=int, default=20,
        help="So lan doc lap lai truoc va sau compaction. Mac dinh: 20",
    )
    parser.add_argument(
        "--simulate", action="store_true",
        help="Buoc chay Simulation Mode (khong can Spark).",
    )
    parser.add_argument(
        "--output", default=str(OUTPUT_CSV),
        help=f"Duong dan file CSV dau ra. Mac dinh: {OUTPUT_CSV}",
    )
    args = parser.parse_args()

    rows = run_benchmark(
        symbol=args.symbol,
        runs=args.runs,
        force_simulate=args.simulate,
    )

    if rows:
        output_path = Path(args.output)
        write_csv(rows, output_path)
        print_summary(rows)
    else:
        logger.warning("Khong co ket qua nao duoc ghi.")


if __name__ == "__main__":
    main()
