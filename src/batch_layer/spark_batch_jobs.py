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
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import urllib.request
except ImportError:
    pass

# Tu dong tim va them PySpark & Py4J vao sys.path neu chay trong Spark container (/opt/spark)
for _base in [os.environ.get("SPARK_HOME", "/opt/spark"), "/opt/spark", "/usr/local/spark"]:
    _p = Path(_base) / "python"
    if _p.exists():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        _lib = _p / "lib"
        if _lib.exists():
            for _z in sorted(_lib.glob("*.zip")):
                if str(_z) not in sys.path:
                    sys.path.insert(0, str(_z))
        break

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


def consume_kafka_records(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    max_records: int = 50000,
    poll_timeout_ms: int = 2000,
    offset_file: Optional[Path] = None,
    reset_offsets: bool = False,
) -> List[Dict[str, Any]]:
    """
    Doc incremental cac ban ghi TradeEvent truc tiep tu Kafka topic 'crypto_trades_raw'
    va luu checkpoint offset vao file JSON de cac chu ky Batch tu dong tiep tuc
    doc tu offset chua xu ly.
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        logger.warning("kafka-python chua duoc cai dat; bo qua consume_kafka_records.")
        return []

    from src.utils.config import get_kafka_config

    kafka_cfg = get_kafka_config()
    brokers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.bootstrap_servers)
    topic_name = topic or os.getenv("KAFKA_TOPIC_RAW", kafka_cfg.topic_raw)
    checkpoint_path = offset_file or (RESULT_LOG_DIR / "batch_kafka_offsets.json")

    saved_offsets: Dict[str, int] = {}
    if not reset_offsets and checkpoint_path.exists():
        try:
            saved_offsets = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Khong doc duoc file offset %s: %s", checkpoint_path, exc)

    records: List[Dict[str, Any]] = []
    new_offsets: Dict[str, int] = dict(saved_offsets)

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=brokers,
            enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode("utf-8", errors="ignore")),
            consumer_timeout_ms=poll_timeout_ms,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        partitions = consumer.partitions_for_topic(topic_name)
        if not partitions:
            consumer.close()
            return []

        tps = [TopicPartition(topic_name, p) for p in sorted(partitions)]
        consumer.assign(tps)
        beginning = consumer.beginning_offsets(tps)
        end_offsets = consumer.end_offsets(tps)

        has_unread = False
        for tp in tps:
            key = f"{tp.topic}:{tp.partition}"
            start_off = saved_offsets.get(key, beginning.get(tp, 0))
            # Neu Kafka retention da xoa offset cu hoac topic duoc tao lai
            if start_off < beginning.get(tp, 0) or start_off > end_offsets.get(tp, 0):
                start_off = beginning.get(tp, 0)
            consumer.seek(tp, start_off)
            if start_off < end_offsets.get(tp, 0):
                has_unread = True

        if not has_unread:
            consumer.close()
            return []

        while len(records) < max_records:
            batch = consumer.poll(timeout_ms=poll_timeout_ms, max_records=min(5000, max_records - len(records)))
            if not batch:
                break
            reached_end = True
            for tp, messages in batch.items():
                key = f"{tp.topic}:{tp.partition}"
                for msg in messages:
                    if isinstance(msg.value, dict):
                        records.append(msg.value)
                    new_offsets[key] = msg.offset + 1
                if new_offsets.get(key, 0) < end_offsets.get(tp, 0):
                    reached_end = False
            if reached_end:
                break

        consumer.close()

        if records:
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            checkpoint_path.write_text(json.dumps(new_offsets, indent=2), encoding="utf-8")
            logger.info(
                "Consumed %d raw records from Kafka topic '%s' (saved offsets to %s)",
                len(records),
                topic_name,
                checkpoint_path,
            )
    except Exception as exc:
        logger.warning("Khong the doc du lieu tu Kafka (%s): %s", brokers, exc)

    return records


def load_default_records(from_kafka: bool = False) -> List[Dict[str, Any]]:
    """Load records from Kafka (when enabled) or the first populated default dataset directory."""
    use_kafka = from_kafka or (os.getenv("BATCH_CONSUME_KAFKA", "false").lower() in {"true", "1", "yes"})
    if use_kafka:
        kafka_records = consume_kafka_records()
        if kafka_records:
            return kafka_records
        logger.info("Khong co ban ghi moi trong Kafka; fallback sang default datasets.")

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


def check_iceberg_services_reachable() -> Dict[str, bool]:
    """
    Pre-flight check: ping MinIO S3 endpoint va Iceberg REST Catalog truoc khi
    khoi dong Spark. Giup phat hien loi ket noi som hon thay vi sau khi Spark
    da mat nhieu phut khoi dong.

    Returns:
        dict voi keys 'minio' va 'iceberg_rest', gia tri True/False.
    """
    import urllib.request
    import urllib.error

    minio_url    = os.getenv("MINIO_ENDPOINT",    "http://localhost:9000") + "/minio/health/live"
    iceberg_url  = os.getenv("ICEBERG_REST_URI",  "http://localhost:8181") + "/v1/config"

    result = {"minio": False, "iceberg_rest": False}
    for key, url in [("minio", minio_url), ("iceberg_rest", iceberg_url)]:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                result[key] = resp.status < 400
        except Exception as exc:
            logger.warning("Pre-flight check FAILED (%s -> %s): %s", key, url, exc)
    return result


def write_bronze_to_iceberg(
    records: List[Dict[str, Any]],
    batch_run_id: str,
    strict: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Ghi Bronze records vao Iceberg table.

    Khi chay voi flag --write-iceberg, ham nay se RAISE loi thay vi im lang bo qua
    (strict=True). Dieu nay dam bao nguoi dung biet ro rang Iceberg ghi that bai.

    Args:
        records:      Danh sach TradeEvent records da clean.
        batch_run_id: ID cua batch job hien tai.
        strict:       Neu True, raise RuntimeError thay vi tra ve None khi that bai.

    Returns:
        dict ket qua ghi Iceberg, hoac None neu khong kha dung (va strict=False).

    Raises:
        RuntimeError: Khi strict=True va Spark/Iceberg khong kha dung.
    """
    if not records:
        logger.warning("write_bronze_to_iceberg: khong co records nao de ghi.")
        return None

    # Pre-flight check truoc khi ton nhieu thoi gian khoi dong Spark
    service_status = check_iceberg_services_reachable()
    missing = [svc for svc, ok in service_status.items() if not ok]
    if missing:
        msg = (
            f"Iceberg write that bai — cac service sau khong phan hoi: {missing}.\n"
            "Hay dam bao Docker dang chay:\n"
            "  docker compose up -d minio iceberg-rest\n"
            "Sau do chay lai:"
            "  python src/batch_layer/spark_batch_jobs.py --write-iceberg"
        )
        if strict:
            raise RuntimeError(msg)
        logger.warning(msg)
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
            StructField("trade_id",      LongType(),   False),
            StructField("symbol",        StringType(), False),
            StructField("price",         DoubleType(), False),
            StructField("quantity",      DoubleType(), False),
            StructField("trade_time",    LongType(),   False),
            StructField("is_buyer_maker", BooleanType(), True),
            StructField("ingestion_time", LongType(),   True),
            StructField("is_injected",   BooleanType(), True),
            StructField("fault_type",    StringType(),  True),
        ])
        spark = get_spark_session("LambdaLakehouse-BatchPipeline")
        df = spark.createDataFrame(records, schema=schema)
        manager = IcebergTableManager(spark)
        result = manager.write_to_bronze(df, batch_run_id=batch_run_id)
        logger.info(
            "Iceberg Bronze write thanh cong: %d records | snapshot_id=%s",
            result.get("record_count", 0),
            result.get("snapshot_id"),
        )
        return result

    except Exception as exc:
        msg = f"Iceberg Bronze write that bai: {exc}"
        if strict:
            raise RuntimeError(msg) from exc
        logger.warning(
            "%s\nDe thu lai voi Iceberg that su:\n"
            "  docker compose up -d minio iceberg-rest\n"
            "  python src/batch_layer/spark_batch_jobs.py --write-iceberg",
            msg,
        )
        return None


def maybe_write_bronze_to_iceberg(
    records: List[Dict[str, Any]],
    batch_run_id: str,
    strict: bool = False,
) -> Optional[Dict[str, Any]]:
    """Backward-compatible wrapper around write_bronze_to_iceberg."""
    return write_bronze_to_iceberg(records, batch_run_id, strict=strict)


def sync_reconciliation_audit(
    sync: ClickHouseBatchSync,
    candles: List[Dict[str, Any]],
    quality: QualityReport,
    batch_run_id: str,
    duration_s: float,
) -> int:
    """
    Tu dong doi soat nen Batch (Ground Truth) voi nen Speed (Provisional) trong ClickHouse
    va luu chi so vao bang lakehouse.reconciliation_audit_metrics.
    """
    if not candles:
        return 0
    try:
        from src.serving_layer.clickhouse_client import ClickHouseQueryClient

        ch_query = ClickHouseQueryClient(
            host=sync.host,
            port=sync.port,
            user=sync.user,
            password=sync.password,
            database=sync.database,
        )
        by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for c in candles:
            by_symbol[c["symbol"]].append(c)

        dup_ratio = (
            round(quality.duplicates_removed / quality.total_records * 100.0, 4)
            if quality.total_records > 0
            else 0.0
        )
        audit_rows: List[Dict[str, Any]] = []

        for sym, sym_candles in by_symbol.items():
            min_ws = min(c["window_start"] for c in sym_candles)
            max_we = max(c["window_end"] for c in sym_candles)
            speed_rows = ch_query.query_candles(
                table="speed_agg",
                symbol=sym,
                start_time=min_ws,
                end_time=max_we,
            )
            speed_by_ws = {s["window_start"]: s for s in speed_rows}

            for b in sym_candles:
                ws = b["window_start"]
                s = speed_by_ws.get(ws)
                b_vol = float(b["volume"])
                s_vol = float(s["volume"]) if s else 0.0
                b_vwap = float(b["vwap"])
                s_vwap = float(s["vwap"]) if s else 0.0
                vol_drift = round(abs(s_vol - b_vol) / b_vol * 100.0, 4) if (s and b_vol > 0) else 0.0
                vwap_err = round(abs(s_vwap - b_vwap), 4) if s else 0.0

                if s is None:
                    status = "BATCH_ONLY"
                elif vol_drift < 1e-4 and vwap_err < 1e-4:
                    status = "MATCHED"
                else:
                    status = "RECONCILED_AND_HEALED"

                audit_rows.append({
                    "run_id": batch_run_id,
                    "symbol": sym,
                    "window_start": ws,
                    "window_end": b["window_end"],
                    "speed_count": int(s["trade_count"]) if s else 0,
                    "batch_count": int(b["trade_count"]),
                    "speed_volume": s_vol,
                    "batch_volume": b_vol,
                    "speed_vwap": s_vwap,
                    "batch_vwap": b_vwap,
                    "volume_drift_pct": vol_drift,
                    "vwap_error": vwap_err,
                    "sequence_gap_count": 0,
                    "sequence_gap_rate": 0.0,
                    "duplicate_count": quality.duplicates_removed,
                    "duplication_ratio": dup_ratio,
                    "convergence_latency_seconds": round(duration_s, 3),
                    "reconciliation_status": status,
                })

        if audit_rows:
            return sync.insert_reconciliation_audit_metrics(audit_rows, run_id=batch_run_id)
    except Exception as exc:
        logger.warning("Khong the dong bo reconciliation_audit_metrics: %s", exc)
    return 0


def run_batch_pipeline(
    input_path: Optional[Path] = None,
    batch_run_id: Optional[str] = None,
    sync_clickhouse: bool = True,
    write_iceberg: bool = False,
    iceberg_strict: bool = False,
    from_kafka: bool = False,
) -> Dict[str, Any]:
    """
    Run the batch pipeline and return a structured summary.

    Args:
        input_path:     Path to JSON/CSV file or directory. None = load default datasets.
        batch_run_id:   Optional deterministic ID for this run.
        sync_clickhouse: If True, push Gold candles and watermark to ClickHouse.
        write_iceberg:  If True, append clean Bronze records to Iceberg table.
        iceberg_strict: If True (and write_iceberg=True), raise RuntimeError on Iceberg
                        failure instead of silently skipping. Use this in Docker pipelines.
        from_kafka:     If True, consume new TradeEvent records directly from Kafka first.
    """
    t0 = time.perf_counter()
    batch_run_id = batch_run_id or f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    raw_records = (
        load_records_from_path(input_path)
        if input_path
        else load_default_records(from_kafka=from_kafka)
    )
    clean_records, rejected_records, quality = clean_and_deduplicate(raw_records)
    candles = aggregate_gold_candles(clean_records)
    watermark = max((row["window_end"] for row in candles), default=None)

    iceberg_result = (
        write_bronze_to_iceberg(clean_records, batch_run_id, strict=iceberg_strict)
        if write_iceberg else None
    )

    clickhouse_rows = 0
    quarantine_rows = 0
    reconciled_audit_rows = 0
    watermark_synced = False
    if sync_clickhouse:
        sync = ClickHouseBatchSync()
        quarantine_rows = sync.insert_quarantine_records(
            rejected_records,
            batch_run_id=batch_run_id,
        )
        clickhouse_rows = sync.insert_batch_aggregates(candles, batch_run_id=batch_run_id)
        if watermark is not None:
            watermark_synced = sync.update_watermark(watermark)
        elapsed_s = time.perf_counter() - t0
        reconciled_audit_rows = sync_reconciliation_audit(
            sync=sync,
            candles=candles,
            quality=quality,
            batch_run_id=batch_run_id,
            duration_s=elapsed_s,
        )

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
        "quarantine_rows": quarantine_rows,
        "reconciled_audit_rows": reconciled_audit_rows,
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
    parser = argparse.ArgumentParser(
        description="Batch Layer Pipeline — Bronze -> Silver -> Gold -> ClickHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Vi du:\n"
            "  # Chay pipeline thong thuong (chi ClickHouse):\n"
            "  python src/batch_layer/spark_batch_jobs.py\n"
            "\n"
            "  # Chay pipeline doc truc tiep tu Kafka va tu dong lap moi 60s:\n"
            "  python src/batch_layer/spark_batch_jobs.py --from-kafka --daemon --interval 60\n"
            "\n"
            "  # Chay pipeline + ghi Bronze vao Iceberg (can MinIO + Iceberg REST dang chay):\n"
            "  docker compose up -d minio iceberg-rest\n"
            "  python src/batch_layer/spark_batch_jobs.py --write-iceberg\n"
            "\n"
            "  # Chay ben trong Docker (iceberg-strict de bao loi ro rang):\n"
            "  docker compose up -d batch-pipeline"
        ),
    )
    parser.add_argument("--mode", default="full", choices=["full", "sample"],
                        help="Execution mode")
    parser.add_argument("--input", default=None,
                        help="JSON/CSV file or directory to load")
    parser.add_argument("--from-kafka", action="store_true",
                        help="Consume incremental raw TradeEvent records directly from Kafka topic")
    parser.add_argument("--daemon", action="store_true",
                        help="Run continuously in a loop every --interval seconds (Auto-Batch mode)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Seconds between Batch runs when --daemon is active (default: 60)")
    parser.add_argument("--batch-run-id", default=None,
                        help="Optional deterministic batch run id")
    parser.add_argument("--no-clickhouse", action="store_true",
                        help="Skip ClickHouse sync")
    parser.add_argument("--write-iceberg", action="store_true",
                        help=(
                            "Append clean Bronze records to Iceberg Bronze table. "
                            "Yeu cau: docker compose up -d minio iceberg-rest"
                        ))
    parser.add_argument("--iceberg-strict", action="store_true",
                        help=(
                            "Khi --write-iceberg duoc chi dinh, raise loi ngay lap tuc "
                            "thay vi im lang bo qua neu Iceberg khong kha dung."
                        ))
    args = parser.parse_args()

    # Pre-flight Iceberg check khi --write-iceberg duoc chi dinh
    if args.write_iceberg:
        logger.info("--write-iceberg duoc chi dinh. Dang kiem tra ket noi dich vu...")
        status = check_iceberg_services_reachable()
        all_ok = all(status.values())
        for svc, ok in status.items():
            icon = "OK" if ok else "FAIL"
            logger.info("  [%s] %s", icon, svc)
        if not all_ok:
            logger.error(
                "Mot so dich vu Iceberg chua san sang. Hay chay:\n"
                "  docker compose up -d minio iceberg-rest\n"
                "Sau do thu lai."
            )
            if args.iceberg_strict:
                sys.exit(1)

    input_path = Path(args.input).resolve() if args.input else None

    if not args.daemon:
        summary = run_batch_pipeline(
            input_path=input_path,
            batch_run_id=args.batch_run_id,
            sync_clickhouse=not args.no_clickhouse,
            write_iceberg=args.write_iceberg,
            iceberg_strict=args.iceberg_strict,
            from_kafka=args.from_kafka,
        )
        print(json.dumps(summary, indent=2, default=str))
        return

    logger.info(
        "Khoi dong Auto-Batch Daemon (interval=%ds, from_kafka=%s, write_iceberg=%s)...",
        args.interval,
        args.from_kafka,
        args.write_iceberg,
    )
    cycle = 0
    try:
        while True:
            cycle += 1
            run_id = (
                f"{args.batch_run_id}_{cycle}"
                if args.batch_run_id
                else f"auto_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            )
            summary = run_batch_pipeline(
                input_path=input_path,
                batch_run_id=run_id,
                sync_clickhouse=not args.no_clickhouse,
                write_iceberg=args.write_iceberg,
                iceberg_strict=args.iceberg_strict,
                from_kafka=args.from_kafka,
            )
            print(json.dumps(summary, indent=2, default=str), flush=True)
            time.sleep(max(5, args.interval))
    except KeyboardInterrupt:
        logger.info("Nhan Ctrl+C — Dung Auto-Batch Daemon.")


if __name__ == "__main__":
    main()
