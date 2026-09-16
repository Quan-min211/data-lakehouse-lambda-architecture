"""
bench_latency.py
================
Benchmark 1: Query Latency Comparison

Muc tieu:
  So sanh do tre truy van (P50 / P95 / P99 ms) giua 3 chien luoc:
    A) Batch-only  -- chi doc lakehouse.batch_agg
    B) Speed-only  -- chi doc lakehouse.speed_agg
    C) Lambda      -- dung AutoCorrectingQueryMerger (ghep Batch + Speed)

Kich ban do luong:
  Q1 -- Historical query  : [now-48h, now-24h]  (toan bo nam trong Batch)
  Q2 -- Realtime  query   : [now-5m,  now]      (toan bo nam trong Speed)
  Q3 -- Hybrid    query   : [now-30m, now]      (cat qua Watermark)

Dau ra:
  results/logs/bench_latency.csv
  Cot: query_type, strategy, run_id, symbol, latency_ms, rows_returned, watermark, ts

Su dung:
  python scripts/benchmarks/bench_latency.py
  python scripts/benchmarks/bench_latency.py --runs 50 --symbols BTCUSDT ETHUSDT

Luu y:
  - Khi ClickHouse offline, script tu dong thoat voi warning va khong ghi CSV.
  - Yeu cau Docker Compose dang chay: clickhouse.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

# ── path setup ──────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.serving_layer.clickhouse_client import ClickHouseQueryClient
from src.serving_layer.query_merger import AutoCorrectingQueryMerger
from src.serving_layer.watermark_reader import WatermarkReader
from src.utils.logger import setup_logger

logger = setup_logger("bench_latency")

# ── constants ────────────────────────────────────────────────────────────────
RESULT_DIR = ROOT_DIR / "results" / "logs"
OUTPUT_CSV = RESULT_DIR / "bench_latency.csv"
FIELDNAMES = [
    "run_id",
    "query_type",
    "strategy",
    "symbol",
    "latency_ms",
    "rows_returned",
    "watermark",
    "ts",
]


# ── helpers ──────────────────────────────────────────────────────────────────
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _timed_call(fn, *args, **kwargs):
    """Execute fn(*args, **kwargs) and return (result, elapsed_ms)."""
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return result, elapsed_ms


def _rows_from_result(result) -> int:
    """Extract row count from either a list or a MarketDataResponse."""
    if isinstance(result, list):
        return len(result)
    if hasattr(result, "candles"):
        return len(result.candles)
    return 0


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = max(0, int(len(sorted_vals) * pct / 100) - 1)
    return round(sorted_vals[idx], 3)


def _check_clickhouse_alive(ch_client: ClickHouseQueryClient) -> bool:
    """Verify ClickHouse is reachable. Exit early if not."""
    client = ch_client.get_client()
    if client is None:
        logger.error(
            "Khong the ket noi toi ClickHouse. "
            "Hay kiem tra: docker compose up -d clickhouse"
        )
        return False
    return True


def build_time_ranges(now: datetime) -> Dict[str, tuple]:
    """Build the 3 canonical query windows relative to `now`."""
    return {
        "Q1_historical": (now - timedelta(hours=48), now - timedelta(hours=24)),
        "Q2_realtime":   (now - timedelta(minutes=5), now),
        "Q3_hybrid":     (now - timedelta(minutes=30), now),
    }


# ── core benchmark ───────────────────────────────────────────────────────────
def run_benchmark(
    symbols: List[str],
    runs: int,
    watermark_offset_hours: int,
) -> List[Dict[str, Any]]:
    """
    Execute all benchmark runs and return a list of raw result dicts.

    Args:
        symbols: List of coin symbols to benchmark.
        runs: Number of repetitions per (query_type, strategy, symbol).
        watermark_offset_hours: Hours before now to set the mock watermark.

    Returns:
        List of result dicts with keys matching FIELDNAMES.
    """
    now = _now_utc()
    mock_watermark = now - timedelta(hours=watermark_offset_hours)

    ch_client = ClickHouseQueryClient()
    if not _check_clickhouse_alive(ch_client):
        sys.exit(1)

    watermark_reader = WatermarkReader(mock_watermark=mock_watermark)
    merger = AutoCorrectingQueryMerger(
        watermark_reader=watermark_reader,
        ch_client=ch_client,
    )

    live_watermark = watermark_reader.get_watermark()
    time_ranges = build_time_ranges(now)

    logger.info(
        "Bat dau Benchmark 1 -- Latency | symbols=%s | runs=%d | watermark=%s",
        symbols, runs, live_watermark.isoformat(),
    )

    all_rows: List[Dict[str, Any]] = []

    for symbol in symbols:
        for q_name, (t_start, t_end) in time_ranges.items():
            latencies: Dict[str, List[float]] = {
                "batch_only": [],
                "speed_only": [],
                "lambda":     [],
            }

            for run_id in range(1, runs + 1):
                ts_str = _now_utc().isoformat()

                # ── Batch-only ──────────────────────────────────────────────
                _, ms_b = _timed_call(
                    ch_client.query_candles,
                    table="batch_agg",
                    symbol=symbol,
                    start_time=t_start,
                    end_time=t_end,
                )
                latencies["batch_only"].append(ms_b)
                all_rows.append({
                    "run_id": run_id,
                    "query_type": q_name,
                    "strategy": "batch_only",
                    "symbol": symbol,
                    "latency_ms": round(ms_b, 3),
                    "rows_returned": 0,
                    "watermark": live_watermark.isoformat(),
                    "ts": ts_str,
                })

                # ── Speed-only ──────────────────────────────────────────────
                _, ms_s = _timed_call(
                    ch_client.query_candles,
                    table="speed_agg",
                    symbol=symbol,
                    start_time=t_start,
                    end_time=t_end,
                )
                latencies["speed_only"].append(ms_s)
                all_rows.append({
                    "run_id": run_id,
                    "query_type": q_name,
                    "strategy": "speed_only",
                    "symbol": symbol,
                    "latency_ms": round(ms_s, 3),
                    "rows_returned": 0,
                    "watermark": live_watermark.isoformat(),
                    "ts": ts_str,
                })

                # ── Lambda Merger ───────────────────────────────────────────
                try:
                    result_l, ms_l = _timed_call(
                        merger.merge_query,
                        symbol=symbol,
                        start_time=t_start,
                        end_time=t_end,
                    )
                    rows_l = _rows_from_result(result_l)
                except Exception as exc:
                    logger.warning(
                        "Lambda query error (%s %s run %d): %s",
                        symbol, q_name, run_id, exc,
                    )
                    ms_l, rows_l = 0.0, 0
                latencies["lambda"].append(ms_l)
                all_rows.append({
                    "run_id": run_id,
                    "query_type": q_name,
                    "strategy": "lambda",
                    "symbol": symbol,
                    "latency_ms": round(ms_l, 3),
                    "rows_returned": rows_l,
                    "watermark": live_watermark.isoformat(),
                    "ts": ts_str,
                })

            # ── Print per-query summary ─────────────────────────────────────
            for strategy, lats in latencies.items():
                if lats:
                    logger.info(
                        "  %s | %s | %s -> P50=%.1fms  P95=%.1fms  P99=%.1fms",
                        symbol, q_name, strategy,
                        _percentile(lats, 50),
                        _percentile(lats, 95),
                        _percentile(lats, 99),
                    )

    return all_rows


# ── CSV + summary ─────────────────────────────────────────────────────────────
def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    """Append benchmark rows to the CSV log file."""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerows(rows)
    logger.info("Da ghi %d dong vao %s", len(rows), path)


def print_summary(rows: List[Dict[str, Any]]) -> None:
    """Print a human-readable P50/P95/P99 table to stdout."""
    groups: Dict[str, List[float]] = defaultdict(list)
    for r in rows:
        key = f"{r['query_type']:<20} | {r['strategy']:<12}"
        groups[key].append(r["latency_ms"])

    print("\n" + "=" * 65)
    print(f"{'BENCHMARK 1 -- QUERY LATENCY SUMMARY':^65}")
    print("=" * 65)
    print(f"{'Query Type + Strategy':<35} {'P50':>8} {'P95':>8} {'P99':>8}")
    print("-" * 65)
    for key, lats in sorted(groups.items()):
        print(
            f"{key:<35} {_percentile(lats, 50):>7.1f}ms"
            f" {_percentile(lats, 95):>7.1f}ms"
            f" {_percentile(lats, 99):>7.1f}ms"
        )
    print("=" * 65 + "\n")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark 1: Query Latency")
    parser.add_argument(
        "--runs", type=int, default=30,
        help="So lan lap lai moi (query_type, strategy, symbol). Mac dinh: 30",
    )
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="Danh sach coin symbols. Mac dinh: BTCUSDT ETHUSDT",
    )
    parser.add_argument(
        "--watermark-offset-hours", type=int, default=24,
        help="Gio truoc now() de dat mock watermark (dung cho Q3 Hybrid). Mac dinh: 24",
    )
    parser.add_argument(
        "--output", default=str(OUTPUT_CSV),
        help=f"Duong dan file CSV dau ra. Mac dinh: {OUTPUT_CSV}",
    )
    args = parser.parse_args()

    rows = run_benchmark(
        symbols=args.symbols,
        runs=args.runs,
        watermark_offset_hours=args.watermark_offset_hours,
    )

    if rows:
        output_path = Path(args.output)
        write_csv(rows, output_path)
        print_summary(rows)
    else:
        logger.warning("Khong co ket qua nao duoc ghi.")


if __name__ == "__main__":
    main()
