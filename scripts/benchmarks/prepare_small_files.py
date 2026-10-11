"""
prepare_small_files.py
======================
Create controlled small files in Iceberg Bronze for Benchmark 3.

The script repeatedly appends small chunks of clean TradeEvent records to
iceberg_catalog.bronze.crypto_trades.  It is intended to be run before
bench_compaction.py real mode so the small-file problem is measurable.

Examples:
  python scripts/benchmarks/prepare_small_files.py --chunk-size 25 --max-records 500 --repeat 2
  docker compose run --rm batch-pipeline python3 -u scripts/benchmarks/prepare_small_files.py --chunk-size 20

Output:
  results/logs/small_file_preparation.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.batch_layer.spark_batch_jobs import (
    DEFAULT_INPUTS,
    load_records_from_path,
    write_bronze_to_iceberg,
)
from src.data_quality.dq_checks import clean_and_deduplicate
from src.utils.logger import setup_logger

logger = setup_logger("prepare_small_files")

RESULT_DIR = ROOT_DIR / "results" / "logs"
OUTPUT_CSV = RESULT_DIR / "small_file_preparation.csv"
FIELDNAMES = [
    "run_id",
    "chunk_id",
    "records_in_chunk",
    "snapshot_id",
    "elapsed_s",
    "status",
    "error",
    "ts",
]


def load_input_records(input_path: Optional[Path]) -> List[Dict[str, Any]]:
    """Load raw input records from explicit path or default dataset locations."""
    if input_path is not None:
        return load_records_from_path(input_path)

    all_records: List[Dict[str, Any]] = []
    for path in DEFAULT_INPUTS:
        all_records.extend(load_records_from_path(path))
    return all_records


def chunk_records(records: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    """Yield fixed-size chunks."""
    for idx in range(0, len(records), chunk_size):
        yield records[idx:idx + chunk_size]


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    """Append preparation result rows to CSV."""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


def run_preparation(
    input_path: Optional[Path],
    chunk_size: int,
    max_records: Optional[int],
    repeat: int,
    strict: bool,
    sleep_seconds: float,
) -> List[Dict[str, Any]]:
    """Append many small chunks to Iceberg Bronze and return CSV rows."""
    raw_records = load_input_records(input_path)
    if max_records is not None:
        raw_records = raw_records[:max_records]

    clean_records, rejected, quality = clean_and_deduplicate(raw_records)
    if not clean_records:
        raise RuntimeError(
            f"No valid records available for Iceberg append. "
            f"total={quality.total_records} rejected={quality.rejected_records}"
        )

    run_id = datetime.now(timezone.utc).strftime("small_files_%Y%m%d_%H%M%S")
    logger.info(
        "Preparing small files: run_id=%s valid=%d rejected=%d chunk_size=%d repeat=%d",
        run_id,
        len(clean_records),
        len(rejected),
        chunk_size,
        repeat,
    )

    result_rows: List[Dict[str, Any]] = []
    chunk_id = 0
    for round_idx in range(1, repeat + 1):
        for chunk in chunk_records(clean_records, chunk_size):
            chunk_id += 1
            batch_run_id = f"{run_id}_r{round_idx:02d}_c{chunk_id:04d}"
            started = time.perf_counter()
            snapshot_id = ""
            status = "success"
            error = ""
            try:
                result = write_bronze_to_iceberg(
                    chunk,
                    batch_run_id=batch_run_id,
                    strict=strict,
                )
                if result:
                    snapshot_id = str(result.get("snapshot_id") or "")
                elif strict:
                    raise RuntimeError("Iceberg write returned no result")
                else:
                    status = "skipped"
            except Exception as exc:
                status = "failed"
                error = str(exc)
                logger.error("Small-file append failed for %s: %s", batch_run_id, exc)
                if strict:
                    raise
            finally:
                elapsed = time.perf_counter() - started
                result_rows.append({
                    "run_id": run_id,
                    "chunk_id": chunk_id,
                    "records_in_chunk": len(chunk),
                    "snapshot_id": snapshot_id,
                    "elapsed_s": round(elapsed, 3),
                    "status": status,
                    "error": error,
                    "ts": datetime.now(timezone.utc).isoformat(),
                })

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    return result_rows


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Create controlled small Iceberg files for Benchmark 3 real mode."
    )
    parser.add_argument("--input", default=None, help="JSON/CSV file or directory to load.")
    parser.add_argument("--chunk-size", type=int, default=25, help="Records per Iceberg append.")
    parser.add_argument("--max-records", type=int, default=1000, help="Maximum valid records to use.")
    parser.add_argument("--repeat", type=int, default=1, help="Repeat the chunked appends N times.")
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between appends.")
    parser.add_argument(
        "--no-strict",
        action="store_true",
        help="Do not fail immediately when Iceberg services are unavailable.",
    )
    parser.add_argument("--output", default=str(OUTPUT_CSV), help="CSV log output path.")
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")
    if args.repeat <= 0:
        raise ValueError("--repeat must be positive")

    input_path = Path(args.input).resolve() if args.input else None
    rows = run_preparation(
        input_path=input_path,
        chunk_size=args.chunk_size,
        max_records=args.max_records,
        repeat=args.repeat,
        strict=not args.no_strict,
        sleep_seconds=args.sleep,
    )
    write_csv(rows, Path(args.output))

    success = sum(1 for row in rows if row["status"] == "success")
    failed = sum(1 for row in rows if row["status"] == "failed")
    skipped = sum(1 for row in rows if row["status"] == "skipped")
    print("\n" + "=" * 72)
    print("ICEBERG SMALL-FILE PREPARATION SUMMARY")
    print("=" * 72)
    print(f"chunks attempted : {len(rows)}")
    print(f"success          : {success}")
    print(f"skipped          : {skipped}")
    print(f"failed           : {failed}")
    print(f"csv              : {Path(args.output)}")
    print("=" * 72 + "\n")


if __name__ == "__main__":
    main()
