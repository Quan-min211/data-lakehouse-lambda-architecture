"""
run_lambda_pipeline.py
======================
One-command orchestrator for the local Lambda Lakehouse demo pipeline.

Default flow:
  1. Start the required Docker services.
  2. Fetch fresh Binance REST data into datasets/raw/.
  3. Generate mock/fault-injected data from the fresh Binance sample.
  4. Replay mock aggTrade events into Kafka.
  5. Run the Batch Layer container.
  6. Smoke-check the Serving API.
  7. Append a structured CSV run log into results/logs/.

Examples:
  python scripts/run_lambda_pipeline.py
  python scripts/run_lambda_pipeline.py --top-n 5 --mock-days 1 --replay-limit 5000
  python scripts/run_lambda_pipeline.py --skip-fetch --skip-mock --replay-limit 1000
  python scripts/run_lambda_pipeline.py --no-docker --skip-batch --replay-limit 1000
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
RESULT_LOG_DIR = ROOT_DIR / "results" / "logs"
PIPELINE_LOG = RESULT_LOG_DIR / "lambda_pipeline_runs.csv"
KAFKA_TOPIC = "crypto_trades_raw"


def now_utc() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def run_command(command: list[str], label: str, check: bool = True) -> int:
    """Run a subprocess command from the repository root."""
    print(f"\n[RUN] {label}")
    print("      " + " ".join(command))
    completed = subprocess.run(command, cwd=ROOT_DIR)
    if check and completed.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {completed.returncode}")
    return int(completed.returncode)


def http_ok(url: str, timeout: int = 5) -> bool:
    """Return True when an HTTP endpoint responds with status < 400."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return int(response.status) < 400
    except Exception:
        return False


def wait_for_http(url: str, label: str, timeout_seconds: int = 90) -> bool:
    """Wait until an HTTP endpoint is reachable or timeout expires."""
    print(f"[WAIT] {label}: {url}")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if http_ok(url, timeout=3):
            print(f"[ OK ] {label} is ready")
            return True
        time.sleep(3)
    print(f"[WARN] {label} did not become ready within {timeout_seconds}s")
    return False


def start_docker_services(include_ui: bool) -> None:
    """Start the core services needed by the full local pipeline."""
    core_services = [
        "kafka",
        "minio",
        "minio-setup",
        "iceberg-rest",
        "clickhouse",
        "redis",
        "fastapi",
        "speed-layer",
    ]
    if include_ui:
        core_services.extend(["streamlit", "dagster"])

    run_command(["docker", "compose", "up", "-d", *core_services], "start Docker services")
    wait_for_http("http://localhost:8123/ping", "ClickHouse")
    wait_for_http("http://localhost:8000/health", "FastAPI")


def iter_trade_files(source_dir: Path) -> list[Path]:
    """Return JSON files containing TradeEvent records, sorted for deterministic replay."""
    if not source_dir.exists():
        return []
    return sorted(source_dir.glob("*.json"))


def iter_events(files: Iterable[Path], limit: int | None) -> Iterable[dict[str, Any]]:
    """Yield TradeEvent dictionaries from a list of JSON files."""
    emitted = 0
    for path in files:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        records = payload.get("records", payload) if isinstance(payload, dict) else payload
        if not isinstance(records, list):
            continue

        for record in records:
            if not isinstance(record, dict):
                continue
            yield record
            emitted += 1
            if limit is not None and emitted >= limit:
                return


def replay_mock_to_kafka(
    source_dir: Path,
    bootstrap_servers: str,
    limit: int | None,
    linger_ms: int,
) -> int:
    """Replay mock TradeEvent JSON files into Kafka."""
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Missing kafka-python. Run: pip install -r requirements.txt") from exc

    files = iter_trade_files(source_dir)
    if not files:
        raise RuntimeError(
            f"No mock aggTrade JSON files found in {source_dir}. "
            "Run scripts/generate_mock_data.py first."
        )

    print(f"[INFO] Replaying mock events from {source_dir}")
    print(f"[INFO] Kafka bootstrap={bootstrap_servers} topic={KAFKA_TOPIC}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
        acks=1,
        retries=5,
        linger_ms=linger_ms,
        batch_size=65536,
    )

    sent = 0
    started = time.perf_counter()
    try:
        for event in iter_events(files, limit):
            key = str(event.get("symbol", "UNKNOWN"))
            producer.send(KAFKA_TOPIC, key=key, value=event)
            sent += 1
            if sent % 1000 == 0:
                producer.flush()
                print(f"[INFO] Replayed {sent:,} events...")
        producer.flush()
    finally:
        producer.close()

    elapsed = max(time.perf_counter() - started, 0.001)
    print(f"[ OK ] Replayed {sent:,} events in {elapsed:.1f}s ({sent / elapsed:.1f} events/s)")
    return sent


def smoke_check_api(symbol: str) -> dict[str, Any]:
    """Call key FastAPI endpoints and return a compact result map."""
    checks: dict[str, Any] = {
        "health_ok": False,
        "watermark_ok": False,
        "market_ok": False,
        "market_candles": 0,
    }

    if http_ok("http://localhost:8000/health"):
        checks["health_ok"] = True

    try:
        with urllib.request.urlopen("http://localhost:8000/api/watermark", timeout=8) as response:
            checks["watermark_ok"] = int(response.status) < 400
    except Exception:
        pass

    try:
        url = f"http://localhost:8000/api/market?symbol={symbol.upper()}"
        with urllib.request.urlopen(url, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
            checks["market_ok"] = int(response.status) < 400
            checks["market_candles"] = int(payload.get("candles_count", 0))
    except Exception as exc:
        checks["market_error"] = str(exc)

    return checks


def write_pipeline_log(row: dict[str, Any]) -> None:
    """Append a structured CSV log for pipeline orchestration runs."""
    RESULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "started_at",
        "completed_at",
        "top_n",
        "mock_days",
        "fault_mode",
        "replayed_events",
        "batch_exit_code",
        "health_ok",
        "watermark_ok",
        "market_ok",
        "market_candles",
        "status",
        "error",
    ]
    is_new = not PIPELINE_LOG.exists()
    with PIPELINE_LOG.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()
        writer.writerow({name: row.get(name, "") for name in fieldnames})
    print(f"[INFO] Pipeline run log appended to {PIPELINE_LOG}")


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(
        description="Run the local Binance -> mock -> Kafka -> Batch/Speed -> Serving pipeline."
    )
    parser.add_argument("--top-n", type=int, default=5, help="Number of top Binance symbols to fetch.")
    parser.add_argument("--klines-limit", type=int, default=1000, help="Klines per symbol.")
    parser.add_argument("--aggtrades-limit", type=int, default=500, help="Recent aggTrades per symbol.")
    parser.add_argument("--mock-days", type=int, default=1, help="Number of mock days to generate.")
    parser.add_argument("--ticks-per-min", type=int, default=8, help="Mock aggTrade ticks per minute.")
    parser.add_argument(
        "--fault-mode",
        choices=["full", "light", "none"],
        default="light",
        help="Fault injection mode for generated mock data.",
    )
    parser.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap servers.")
    parser.add_argument("--replay-limit", type=int, default=5000, help="Max mock events to replay.")
    parser.add_argument("--replay-linger-ms", type=int, default=50, help="Kafka producer linger.ms.")
    parser.add_argument("--smoke-symbol", default="BTCUSDT", help="Symbol used for API smoke check.")
    parser.add_argument("--include-ui", action="store_true", help="Also start Streamlit and Dagster.")
    parser.add_argument("--no-docker", action="store_true", help="Do not start Docker services.")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip Binance REST fetch.")
    parser.add_argument("--skip-mock", action="store_true", help="Skip mock data generation.")
    parser.add_argument("--skip-replay", action="store_true", help="Skip replaying mock data to Kafka.")
    parser.add_argument("--skip-batch", action="store_true", help="Skip Batch Layer container run.")
    parser.add_argument("--skip-smoke", action="store_true", help="Skip Serving API smoke check.")
    return parser.parse_args()


def main() -> None:
    """Run the one-command local pipeline."""
    args = parse_args()
    run_id = now_utc().strftime("pipeline_%Y%m%d_%H%M%S")
    run_dir = ROOT_DIR / "datasets" / "runs" / run_id
    raw_dir = run_dir / "raw"
    mock_dir = run_dir / "mock"
    started_at = now_utc().isoformat()
    batch_exit_code: int | str = ""
    replayed_events = 0
    smoke_result: dict[str, Any] = {}
    status = "success"
    error = ""

    try:
        if not args.no_docker:
            start_docker_services(include_ui=args.include_ui)

        if not args.skip_fetch:
            run_command(
                [
                    sys.executable,
                    "scripts/fetch_binance_data.py",
                    "--top-n",
                    str(args.top_n),
                    "--klines-limit",
                    str(args.klines_limit),
                    "--aggtrades-limit",
                    str(args.aggtrades_limit),
                    "--output-dir",
                    str(raw_dir),
                ],
                "fetch fresh Binance REST data",
            )

        if not args.skip_mock:
            run_command(
                [
                    sys.executable,
                    "scripts/generate_mock_data.py",
                    "--days",
                    str(args.mock_days),
                    "--ticks-per-min",
                    str(args.ticks_per_min),
                    "--fault-mode",
                    args.fault_mode,
                    "--klines-dir",
                    str(raw_dir / "klines"),
                    "--output-dir",
                    str(mock_dir),
                ],
                "generate mock data from latest Binance sample",
            )

        if not args.skip_replay:
            replayed_events = replay_mock_to_kafka(
                source_dir=mock_dir / "aggtrades",
                bootstrap_servers=args.bootstrap,
                limit=args.replay_limit,
                linger_ms=args.replay_linger_ms,
            )

        if not args.skip_batch:
            batch_exit_code = run_command(
                [
                    "docker", "compose", "run", "--rm", "batch-pipeline",
                    "python3", "-u", "src/batch_layer/spark_batch_jobs.py",
                    "--write-iceberg", "--iceberg-strict",
                    "--input", f"/app/datasets/runs/{run_id}/mock/aggtrades",
                    "--batch-run-id", run_id,
                ],
                "run Batch Layer container",
                check=True,
            )

        if not args.skip_smoke:
            smoke_result = smoke_check_api(args.smoke_symbol)
            print(f"[INFO] Smoke check: {smoke_result}")

    except Exception as exc:
        status = "failed"
        error = str(exc)
        print(f"[FAIL] {error}")
        raise
    finally:
        write_pipeline_log({
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": now_utc().isoformat(),
            "top_n": args.top_n,
            "mock_days": args.mock_days,
            "fault_mode": args.fault_mode,
            "replayed_events": replayed_events,
            "batch_exit_code": batch_exit_code,
            "health_ok": smoke_result.get("health_ok", ""),
            "watermark_ok": smoke_result.get("watermark_ok", ""),
            "market_ok": smoke_result.get("market_ok", ""),
            "market_candles": smoke_result.get("market_candles", ""),
            "status": status,
            "error": error,
        })


if __name__ == "__main__":
    main()
