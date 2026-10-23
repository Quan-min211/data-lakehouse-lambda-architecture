"""
e2e_full_pipeline.py
====================
Full Lambda Lakehouse smoke test.

This script runs the one-command pipeline with small defaults, then verifies:
  - ClickHouse batch_agg exists and is queryable.
  - ClickHouse speed_agg exists and is queryable.
  - system_watermark exists.
  - FastAPI /api/market responds.
  - Redis Speed hot-cache has at least one latest candle when available.

It writes a structured CSV log to results/logs/e2e_full_pipeline.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
LOG_FILE = ROOT_DIR / "results" / "logs" / "e2e_full_pipeline.csv"


def run_command(command: list[str], label: str) -> int:
    """Run a command from the repository root and return its exit code."""
    print(f"\n[RUN] {label}")
    print("      " + " ".join(command))
    completed = subprocess.run(command, cwd=ROOT_DIR)
    return int(completed.returncode)


def clickhouse_count(table: str) -> int:
    """Return count() for a ClickHouse table, or -1 when unavailable."""
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(host="localhost", port=8123, database="lakehouse")
        return int(client.query(f"SELECT count() FROM lakehouse.{table}").result_rows[0][0])
    except Exception as exc:
        print(f"[WARN] ClickHouse count failed for {table}: {exc}")
        return -1


def clickhouse_latest_watermark() -> str:
    """Return the most recent Batch watermark, or an empty string on failure."""
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(host="localhost", port=8123, database="lakehouse")
        result = client.query(
            "SELECT max(watermark_time) FROM lakehouse.system_watermark "
            "WHERE layer = 'batch_layer'"
        )
        return str(result.result_rows[0][0]) if result.result_rows else ""
    except Exception as exc:
        print(f"[WARN] ClickHouse watermark check failed: {exc}")
        return ""


def redis_latest_count(symbols: list[str]) -> int:
    """Count Redis latest speed-cache keys found for the given symbols."""
    try:
        import redis

        client = redis.Redis(host="localhost", port=6379, decode_responses=True, socket_connect_timeout=3)
        client.ping()
        found = 0
        for symbol in symbols:
            if client.get(f"speed:candle:{symbol.upper()}:latest"):
                found += 1
        return found
    except Exception as exc:
        print(f"[WARN] Redis latest-key check skipped: {exc}")
        return -1


def fastapi_market_ok(symbol: str) -> tuple[bool, int]:
    """Call /api/market and return (ok, candles_count)."""
    try:
        url = f"http://localhost:8000/api/market?symbol={symbol.upper()}"
        with urllib.request.urlopen(url, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return int(response.status) < 400, int(payload.get("candles_count", 0))
    except Exception as exc:
        print(f"[WARN] FastAPI market check failed: {exc}")
        return False, 0


def write_log(row: dict[str, Any]) -> None:
    """Append e2e result to CSV."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp",
        "status",
        "pipeline_exit_code",
        "batch_rows",
        "batch_delta",
        "speed_rows",
        "speed_delta",
        "watermark_rows",
        "watermark_before",
        "watermark_after",
        "redis_latest_keys",
        "api_ok",
        "api_candles",
        "elapsed_s",
    ]
    is_new = not LOG_FILE.exists()
    with LOG_FILE.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()
        writer.writerow({name: row.get(name, "") for name in fieldnames})
    print(f"[INFO] E2E log appended to {LOG_FILE}")


def parse_args() -> argparse.Namespace:
    """Parse CLI options."""
    parser = argparse.ArgumentParser(description="Run full local Lambda Lakehouse smoke test.")
    parser.add_argument("--top-n", type=int, default=2, help="Top Binance symbols to fetch.")
    parser.add_argument("--klines-limit", type=int, default=100, help="Klines per symbol for the isolated run.")
    parser.add_argument("--aggtrades-limit", type=int, default=50, help="AggTrades per symbol for the isolated run.")
    parser.add_argument("--mock-days", type=int, default=1, help="Mock days to generate.")
    parser.add_argument("--ticks-per-min", type=int, default=1, help="Mock ticks per minute.")
    parser.add_argument("--replay-limit", type=int, default=1000, help="Mock events to replay.")
    parser.add_argument("--fault-mode", choices=["full", "light", "none"], default="light")
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol for API validation.")
    parser.add_argument("--no-docker", action="store_true", help="Pass --no-docker to run_lambda_pipeline.py.")
    return parser.parse_args()


def main() -> None:
    """Run the full e2e smoke test."""
    args = parse_args()
    started = time.perf_counter()
    batch_before = clickhouse_count("batch_agg")
    speed_before = clickhouse_count("speed_agg")
    watermark_before = clickhouse_latest_watermark()

    command = [
        sys.executable,
        "scripts/run_lambda_pipeline.py",
        "--top-n",
        str(args.top_n),
        "--klines-limit",
        str(args.klines_limit),
        "--aggtrades-limit",
        str(args.aggtrades_limit),
        "--mock-days",
        str(args.mock_days),
        "--ticks-per-min",
        str(args.ticks_per_min),
        "--replay-limit",
        str(args.replay_limit),
        "--fault-mode",
        args.fault_mode,
        "--smoke-symbol",
        args.symbol,
    ]
    if args.no_docker:
        command.append("--no-docker")
    pipeline_exit = run_command(command, "run one-command Lambda pipeline")

    # Give async speed writer a short grace period after replay.
    time.sleep(3)

    batch_rows = clickhouse_count("batch_agg")
    speed_rows = clickhouse_count("speed_agg")
    watermark_rows = clickhouse_count("system_watermark")
    watermark_after = clickhouse_latest_watermark()
    redis_keys = redis_latest_count([args.symbol])
    batch_delta = batch_rows - batch_before if batch_before >= 0 and batch_rows >= 0 else -1
    speed_delta = speed_rows - speed_before if speed_before >= 0 and speed_rows >= 0 else -1
    api_ok, api_candles = fastapi_market_ok(args.symbol)

    passed = (
        pipeline_exit == 0
        and batch_delta > 0
        and speed_delta > 0
        and watermark_rows > 0
        and bool(watermark_after)
        and api_ok
        and redis_keys > 0
    )
    status = "PASS" if passed else "FAIL"
    elapsed = time.perf_counter() - started

    print("\n" + "=" * 72)
    print("FULL PIPELINE E2E SUMMARY")
    print("=" * 72)
    print(f"status              : {status}")
    print(f"pipeline_exit_code  : {pipeline_exit}")
    print(f"batch_agg rows      : {batch_rows}")
    print(f"batch_agg delta     : {batch_delta}")
    print(f"speed_agg rows      : {speed_rows}")
    print(f"speed_agg delta     : {speed_delta}")
    print(f"watermark rows      : {watermark_rows}")
    print(f"watermark           : {watermark_before} -> {watermark_after}")
    print(f"Redis latest keys   : {redis_keys}")
    print(f"FastAPI market ok   : {api_ok} (candles={api_candles})")
    print(f"elapsed             : {elapsed:.1f}s")
    print("=" * 72 + "\n")

    write_log({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "pipeline_exit_code": pipeline_exit,
        "batch_rows": batch_rows,
        "batch_delta": batch_delta,
        "speed_rows": speed_rows,
        "speed_delta": speed_delta,
        "watermark_rows": watermark_rows,
        "watermark_before": watermark_before,
        "watermark_after": watermark_after,
        "redis_latest_keys": redis_keys,
        "api_ok": api_ok,
        "api_candles": api_candles,
        "elapsed_s": round(elapsed, 2),
    })

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
