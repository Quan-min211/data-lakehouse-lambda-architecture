"""
bench_reprocess.py
==================
Benchmark 2: Reprocessing Correctness (Speed vs Batch)

Muc tieu:
  Sau khi Batch Layer chay lai (reprocess), so sanh ket qua Speed View (xap xi)
  voi Batch View (chinh xac) de do muc sai lech.

Chi so do:
  - MAE  (Mean Absolute Error)     trung binh |VWAP_speed - VWAP_batch|
  - MAPE (Mean Absolute Percentage Error)     |delta| / VWAP_batch * 100%
  - RMSE (Root Mean Squared Error)
  - Coverage (%) : ty le candle Speed co nguon Batch tuong ung de so sanh

Kich ban thuc nghiem:
  1. Lay tat ca cac nen tu batch_agg trong cua so [now-2h, now-1h]
     (dat rang buoc nay nam trong Watermark nen ca 2 bang deu co du lieu)
  2. Lay tuong ung tu speed_agg cung cua so do
  3. Join theo (symbol, window_start) va tinh cac chi so sai lech
  4. Viet ket qua ra results/logs/bench_reprocess.csv

Dau ra CSV:
  Cot: symbol, window_start, vwap_batch, vwap_speed, mae, mape_pct, rmse, ts

Su dung:
  python scripts/benchmarks/bench_reprocess.py
  python scripts/benchmarks/bench_reprocess.py --hours-back 2 --window-hours 1

Luu y:
  - Yeu cau ca batch_agg va speed_agg da co du lieu trong khoang thoi gian tuong ung.
  - Neu mot trong hai bang trong, script bao cao lo canh bao va ket thuc.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── path setup ──────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.serving_layer.clickhouse_client import ClickHouseQueryClient
from src.utils.logger import setup_logger

logger = setup_logger("bench_reprocess")

# ── constants ────────────────────────────────────────────────────────────────
RESULT_DIR = ROOT_DIR / "results" / "logs"
OUTPUT_CSV = RESULT_DIR / "bench_reprocess.csv"

CANDLE_FIELDNAMES = [
    "symbol",
    "window_start",
    "vwap_batch",
    "vwap_speed",
    "abs_error",
    "pct_error",
    "ts",
]

SUMMARY_FIELDNAMES = [
    "symbol",
    "query_start",
    "query_end",
    "matched_candles",
    "coverage_pct",
    "mae",
    "mape_pct",
    "rmse",
    "ts",
]


# ── helpers ──────────────────────────────────────────────────────────────────
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_pct(delta: float, reference: float) -> float:
    """Safe MAPE computation: returns 0 when reference is 0."""
    if reference == 0.0:
        return 0.0
    return abs(delta) / abs(reference) * 100.0


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


# ── core computation ─────────────────────────────────────────────────────────
def compute_accuracy_metrics(
    batch_candles: List[Dict[str, Any]],
    speed_candles: List[Dict[str, Any]],
    symbol: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Join batch and speed candles by window_start, compute per-candle errors
    and aggregate MAE / MAPE / RMSE / Coverage.

    Args:
        batch_candles: Candles from lakehouse.batch_agg.
        speed_candles: Candles from lakehouse.speed_agg.
        symbol: Coin symbol being compared.

    Returns:
        Tuple of (per_candle_rows, summary_dict).
    """
    ts_str = _now_utc().isoformat()

    # Index speed candles by window_start for O(1) join
    speed_index: Dict[datetime, Dict[str, Any]] = {}
    for sc in speed_candles:
        ws = sc["window_start"]
        if isinstance(ws, str):
            ws = datetime.fromisoformat(ws)
        if ws.tzinfo is None:
            ws = ws.replace(tzinfo=timezone.utc)
        speed_index[ws] = sc

    per_candle_rows: List[Dict[str, Any]] = []
    abs_errors: List[float] = []
    pct_errors: List[float] = []
    sq_errors: List[float] = []
    matched = 0

    for bc in batch_candles:
        ws = bc["window_start"]
        if isinstance(ws, str):
            ws = datetime.fromisoformat(ws)
        if ws.tzinfo is None:
            ws = ws.replace(tzinfo=timezone.utc)

        vwap_batch = float(bc.get("vwap", 0.0))

        if ws in speed_index:
            matched += 1
            vwap_speed = float(speed_index[ws].get("vwap", 0.0))
            delta = vwap_speed - vwap_batch
            abs_err = abs(delta)
            pct_err = _safe_pct(delta, vwap_batch)

            abs_errors.append(abs_err)
            pct_errors.append(pct_err)
            sq_errors.append(abs_err ** 2)

            per_candle_rows.append({
                "symbol": symbol,
                "window_start": ws.isoformat(),
                "vwap_batch": round(vwap_batch, 6),
                "vwap_speed": round(vwap_speed, 6),
                "abs_error": round(abs_err, 6),
                "pct_error": round(pct_err, 4),
                "ts": ts_str,
            })
        else:
            # Speed does not have this candle — no contribution to error metrics
            per_candle_rows.append({
                "symbol": symbol,
                "window_start": ws.isoformat(),
                "vwap_batch": round(vwap_batch, 6),
                "vwap_speed": None,
                "abs_error": None,
                "pct_error": None,
                "ts": ts_str,
            })

    total_batch = len(batch_candles)
    coverage = (matched / total_batch * 100.0) if total_batch > 0 else 0.0
    mae = (sum(abs_errors) / len(abs_errors)) if abs_errors else 0.0
    mape = (sum(pct_errors) / len(pct_errors)) if pct_errors else 0.0
    rmse = math.sqrt(sum(sq_errors) / len(sq_errors)) if sq_errors else 0.0

    summary = {
        "symbol": symbol,
        "matched_candles": matched,
        "total_batch_candles": total_batch,
        "coverage_pct": round(coverage, 2),
        "mae": round(mae, 6),
        "mape_pct": round(mape, 4),
        "rmse": round(rmse, 6),
    }

    return per_candle_rows, summary


# ── benchmark entry ───────────────────────────────────────────────────────────
def run_benchmark(
    symbols: List[str],
    hours_back: int,
    window_hours: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetch batch and speed candles for each symbol, compute accuracy metrics.

    Args:
        symbols: List of coin symbols to compare.
        hours_back: How many hours ago the comparison window starts.
        window_hours: Width of the comparison window in hours.

    Returns:
        Tuple of (all_candle_rows, all_summary_rows).
    """
    now = _now_utc()
    t_start = now - timedelta(hours=hours_back)
    t_end = t_start + timedelta(hours=window_hours)

    logger.info(
        "Benchmark 2 -- Reprocess Correctness | window=[%s, %s] | symbols=%s",
        t_start.isoformat(), t_end.isoformat(), symbols,
    )

    ch_client = ClickHouseQueryClient()
    if not _check_clickhouse_alive(ch_client):
        sys.exit(1)

    all_candle_rows: List[Dict[str, Any]] = []
    all_summary_rows: List[Dict[str, Any]] = []
    ts_str = _now_utc().isoformat()

    for symbol in symbols:
        logger.info("  Dang xu ly symbol: %s", symbol)

        batch_candles = ch_client.query_candles(
            table="batch_agg",
            symbol=symbol,
            start_time=t_start,
            end_time=t_end,
        )
        speed_candles = ch_client.query_candles(
            table="speed_agg",
            symbol=symbol,
            start_time=t_start,
            end_time=t_end,
        )

        if not batch_candles:
            logger.warning(
                "  [%s] batch_agg TRONG trong khoang [%s, %s]. "
                "Hay chay Batch pipeline truoc: python src/batch_layer/spark_batch_jobs.py",
                symbol, t_start.isoformat(), t_end.isoformat(),
            )
            continue

        if not speed_candles:
            logger.warning(
                "  [%s] speed_agg TRONG trong khoang [%s, %s]. "
                "Hay chay Speed layer truoc.",
                symbol, t_start.isoformat(), t_end.isoformat(),
            )

        candle_rows, summary = compute_accuracy_metrics(batch_candles, speed_candles, symbol)

        logger.info(
            "  [%s] Matched=%d/%d | Coverage=%.1f%% | MAE=%.6f | MAPE=%.4f%% | RMSE=%.6f",
            symbol,
            summary["matched_candles"],
            summary["total_batch_candles"],
            summary["coverage_pct"],
            summary["mae"],
            summary["mape_pct"],
            summary["rmse"],
        )

        all_candle_rows.extend(candle_rows)
        all_summary_rows.append({
            "symbol": symbol,
            "query_start": t_start.isoformat(),
            "query_end": t_end.isoformat(),
            "matched_candles": summary["matched_candles"],
            "coverage_pct": summary["coverage_pct"],
            "mae": summary["mae"],
            "mape_pct": summary["mape_pct"],
            "rmse": summary["rmse"],
            "ts": ts_str,
        })

    return all_candle_rows, all_summary_rows


# ── CSV writers ───────────────────────────────────────────────────────────────
def write_candle_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    """Write per-candle comparison rows to CSV."""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CANDLE_FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerows(rows)
    logger.info("Da ghi %d dong candle vao %s", len(rows), path)


def write_summary_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    """Write aggregate summary rows to CSV."""
    summary_path = path.parent / (path.stem + "_summary.csv")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not summary_path.exists()
    with summary_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerows(rows)
    logger.info("Da ghi %d dong summary vao %s", len(rows), summary_path)


def print_summary(summary_rows: List[Dict[str, Any]]) -> None:
    """Print accuracy summary table to stdout."""
    print("\n" + "=" * 75)
    print(f"{'BENCHMARK 2 -- REPROCESSING CORRECTNESS':^75}")
    print("=" * 75)
    print(f"{'Symbol':<12} {'Matched':>8} {'Coverage':>10} {'MAE':>12} {'MAPE%':>10} {'RMSE':>12}")
    print("-" * 75)
    for r in summary_rows:
        print(
            f"{r['symbol']:<12} {r['matched_candles']:>8} "
            f"{r['coverage_pct']:>9.1f}% "
            f"{r['mae']:>12.6f} "
            f"{r['mape_pct']:>9.4f}% "
            f"{r['rmse']:>12.6f}"
        )
    print("=" * 75 + "\n")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark 2: Reprocessing Correctness")
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="Danh sach coin symbols. Mac dinh: BTCUSDT ETHUSDT",
    )
    parser.add_argument(
        "--hours-back", type=int, default=2,
        help="Bat dau cua so so sanh tu bao nhieu gio truoc now. Mac dinh: 2",
    )
    parser.add_argument(
        "--window-hours", type=int, default=1,
        help="Do rong cua so so sanh (gio). Mac dinh: 1",
    )
    parser.add_argument(
        "--output", default=str(OUTPUT_CSV),
        help=f"Duong dan file CSV dau ra. Mac dinh: {OUTPUT_CSV}",
    )
    args = parser.parse_args()

    candle_rows, summary_rows = run_benchmark(
        symbols=args.symbols,
        hours_back=args.hours_back,
        window_hours=args.window_hours,
    )

    output_path = Path(args.output)
    if candle_rows:
        write_candle_csv(candle_rows, output_path)
    if summary_rows:
        write_summary_csv(summary_rows, output_path)
        print_summary(summary_rows)
    else:
        logger.warning("Khong co du lieu nao de so sanh. Kiem tra lai bang batch_agg va speed_agg.")


if __name__ == "__main__":
    main()
