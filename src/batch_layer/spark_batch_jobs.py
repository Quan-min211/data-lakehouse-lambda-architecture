"""
spark_batch_jobs.py
===================
Batch pipeline entry point for the Lambda Lakehouse project.

This job implements a practical Bronze -> Silver -> Gold flow for local runs:
  1. Load TradeEvent records from JSON/CSV datasets.
  2. Apply data quality validation and deduplication.
  3. Aggregate reconciled OHLCV/VWAP candles.
  4. Sync Gold candles and the batch watermark to ClickHouse when available.
  5. Optionally write Bronze records to Iceberg when Spark/Iceberg services run.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.batch_layer.clickhouse_sync import ClickHouseBatchSync
from src.data_quality.dq_checks import QualityReport, clean_and_deduplicate
from src.speed_layer.spike_detector import SpikeDetector
from src.utils.logger import setup_logger

logger = setup_logger("spark_batch_jobs")

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_INPUTS = [
    ROOT_DIR / "datasets" / "mock" / "aggtrades",
    ROOT_DIR / "datasets" / "raw" / "aggtrades",
    ROOT_DIR / "datasets" / "sample",
]
RESULT_LOG_DIR = ROOT_DIR / "results" / "logs"


def parse_epoch_ms(value: int) -> datetime:
    """Convert epoch milliseconds to timezone-aware UTC datetime."""
    return datetime.fromtimestamp(int(value) / 1000.0, timezone.utc)


def floor_to_minute(dt: datetime) -> datetime:
    """Floor a datetime to the start of its minute."""
    return dt.replace(second=0, microsecond=0)


def load_records_from_path(path: Path) -> List[Dict[str, Any]]:
    """Load TradeEvent-like records from a JSON file, CSV file, or directory."""
    if not path.exists():
        return []

    if path.is_dir():
        records: List[Dict[str, Any]] = []
        for child in sorted(path.glob("*")):
            if child.suffix.lower() in {".json", ".csv"}:
                records.extend(load_records_from_path(child))
        return records

    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            return [dict(item) for item in payload]
        if isinstance(payload, dict) and "records" in payload:
            return [dict(item) for item in payload["records"]]
        return [dict(payload)]

    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    return []


def load_default_records() -> List[Dict[str, Any]]:
    """Load records from the first populated default dataset directory."""
    for path in DEFAULT_INPUTS:
        records = load_records_from_path(path)
        if records:
            logger.info("Loaded %s records from %s", len(records), path)
            return records
    logger.warning("No default dataset found; returning an empty input set")
    return []


def aggregate_gold_candles(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate clean trade records into one-minute OHLCV candles."""
    groups: Dict[tuple[str, datetime], List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        window_start = floor_to_minute(parse_epoch_ms(record["trade_time"]))
        groups[(record["symbol"], window_start)].append(record)

    spike_detector = SpikeDetector()
    candles: List[Dict[str, Any]] = []
    for (symbol, window_start), trades in groups.items():
        ordered = sorted(trades, key=lambda item: (item["trade_time"], item["trade_id"]))
        prices = [float(item["price"]) for item in ordered]
        quantities = [float(item["quantity"]) for item in ordered]
        volume = sum(quantities)
        vwap = sum(price * qty for price, qty in zip(prices, quantities)) / volume if volume else 0.0

        candle = {
            "symbol": symbol,
            "window_start": window_start,
            "window_end": window_start + timedelta(minutes=1),
            "open_price": round(prices[0], 4),
            "high_price": round(max(prices), 4),
            "low_price": round(min(prices), 4),
            "close_price": round(prices[-1], 4),
            "volume": round(volume, 6),
            "trade_count": len(ordered),
            "vwap": round(vwap, 4),
        }
        candle["is_spike"] = spike_detector.is_spike_python(candle)
        candles.append(candle)

    return sorted(candles, key=lambda row: (row["symbol"], row["window_start"]))


def write_batch_run_log(
    batch_run_id: str,
    quality: QualityReport,
    candles_count: int,
    clickhouse_rows: int,
    watermark: Optional[datetime],
) -> Path:
    """Append a structured CSV log for batch observability."""
    RESULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULT_LOG_DIR / "batch_pipeline_runs.csv"
    is_new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "batch_run_id",
                "run_at",
                "total_records",
                "valid_records",
                "rejected_records",
                "duplicates_removed",
                "dq_pass_rate",
                "candles_count",
                "clickhouse_rows",
                "watermark",
            ],
        )
        if is_new:
            writer.writeheader()
        writer.writerow({
            "batch_run_id": batch_run_id,
            "run_at": datetime.now(timezone.utc).isoformat(),
            "total_records": quality.total_records,
            "valid_records": quality.valid_records,
            "rejected_records": quality.rejected_records,
            "duplicates_removed": quality.duplicates_removed,
            "dq_pass_rate": quality.pass_rate,
            "candles_count": candles_count,
            "clickhouse_rows": clickhouse_rows,
            "watermark": watermark.isoformat() if watermark else "",
        })
    return path


def maybe_write_bronze_to_iceberg(records: List[Dict[str, Any]], batch_run_id: str) -> Optional[Dict[str, Any]]:
    """Write Bronze records to Iceberg when Spark and Iceberg services are configured."""
    if not records:
        return None

    try:
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )
        from src.batch_layer.iceberg_utils import IcebergTableManager, get_spark_session

        schema = StructType([
            StructField("trade_id", LongType(), False),
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("quantity", DoubleType(), False),
            StructField("trade_time", LongType(), False),
            StructField("is_buyer_maker", BooleanType(), True),
            StructField("ingestion_time", LongType(), True),
            StructField("is_injected", BooleanType(), True),
            StructField("fault_type", StringType(), True),
        ])
        spark = get_spark_session("LambdaLakehouse-BatchPipeline")
        df = spark.createDataFrame(records, schema=schema)
        manager = IcebergTableManager(spark)
        return manager.write_to_bronze(df, batch_run_id=batch_run_id)
    except Exception as exc:
        logger.warning("Skipping Iceberg Bronze write because Spark/Iceberg is unavailable: %s", exc)
        return None


def run_batch_pipeline(
    input_path: Optional[Path] = None,
    batch_run_id: Optional[str] = None,
    sync_clickhouse: bool = True,
    write_iceberg: bool = False,
) -> Dict[str, Any]:
    """Run the batch pipeline and return a structured summary."""
    batch_run_id = batch_run_id or f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    raw_records = load_records_from_path(input_path) if input_path else load_default_records()
    clean_records, rejected_records, quality = clean_and_deduplicate(raw_records)
    candles = aggregate_gold_candles(clean_records)
    watermark = max((row["window_end"] for row in candles), default=None)

    iceberg_result = maybe_write_bronze_to_iceberg(clean_records, batch_run_id) if write_iceberg else None

    clickhouse_rows = 0
    watermark_synced = False
    if sync_clickhouse:
        sync = ClickHouseBatchSync()
        clickhouse_rows = sync.insert_batch_aggregates(candles, batch_run_id=batch_run_id)
        if watermark is not None:
            watermark_synced = sync.update_watermark(watermark)

    log_path = write_batch_run_log(
        batch_run_id=batch_run_id,
        quality=quality,
        candles_count=len(candles),
        clickhouse_rows=clickhouse_rows,
        watermark=watermark,
    )

    summary = {
        "batch_run_id": batch_run_id,
        "total_records": quality.total_records,
        "valid_records": quality.valid_records,
        "rejected_records": quality.rejected_records,
        "duplicates_removed": quality.duplicates_removed,
        "dq_pass_rate": quality.pass_rate,
        "candles_count": len(candles),
        "clickhouse_rows": clickhouse_rows,
        "watermark": watermark.isoformat() if watermark else None,
        "watermark_synced": watermark_synced,
        "iceberg_result": iceberg_result,
        "rejected_sample": rejected_records[:5],
        "log_path": str(log_path),
    }
    logger.info("Batch pipeline completed: %s", summary)
    return summary


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Run the Batch Layer pipeline")
    parser.add_argument("--mode", default="full", choices=["full", "sample"], help="Execution mode")
    parser.add_argument("--input", default=None, help="JSON/CSV file or directory to load")
    parser.add_argument("--batch-run-id", default=None, help="Optional deterministic batch run id")
    parser.add_argument("--no-clickhouse", action="store_true", help="Skip ClickHouse sync")
    parser.add_argument("--write-iceberg", action="store_true", help="Also append clean Bronze records to Iceberg")
    args = parser.parse_args()

    input_path = Path(args.input).resolve() if args.input else None
    summary = run_batch_pipeline(
        input_path=input_path,
        batch_run_id=args.batch_run_id,
        sync_clickhouse=not args.no_clickhouse,
        write_iceberg=args.write_iceberg,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
