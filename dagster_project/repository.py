"""
repository.py
=============
Dagster Software-Defined Assets for the Lambda Lakehouse batch pipeline.

The assets reuse the same production modules as the command-line batch job:
Bronze loads TradeEvent records, Silver applies data quality checks, Gold builds
reconciled OHLCV candles, ClickHouse sync publishes the serving batch view, and
Watermark sync advances the Query Merger boundary.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

from dagster import (
    AssetIn,
    Definitions,
    MetadataValue,
    Output,
    ScheduleDefinition,
    asset,
    define_asset_job,
    in_process_executor,
    repository,
)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.batch_layer.clickhouse_sync import ClickHouseBatchSync
from src.batch_layer.spark_batch_jobs import (
    aggregate_gold_candles,
    load_default_records,
    maybe_write_bronze_to_iceberg,
    write_batch_run_log,
)
from src.batch_layer.compaction import CompactionJob, get_spark_session
from src.data_quality.dq_checks import clean_and_deduplicate


def _batch_run_id() -> str:
    """Return a unique batch run identifier."""
    return f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


@asset(
    group_name="lakehouse_batch",
    description="Load raw TradeEvent records from local datasets and optionally append them to Iceberg Bronze.",
    metadata={
        "layer": "Bronze",
        "format": "TradeEvent JSON/CSV -> optional Apache Iceberg",
        "table": "iceberg_catalog.bronze.crypto_trades",
    },
)
def bronze_crypto_trades(context) -> Output[Dict[str, Any]]:
    """Load raw records for the current batch window."""
    batch_run_id = _batch_run_id()
    records = load_default_records()

    write_iceberg = os.getenv("BATCH_WRITE_ICEBERG", "false").lower() == "true"
    iceberg_strict = os.getenv("BATCH_ICEBERG_STRICT", "false").lower() == "true"
    iceberg_result = (
        maybe_write_bronze_to_iceberg(records, batch_run_id, strict=iceberg_strict)
        if write_iceberg else None
    )

    context.log.info("Loaded %s raw TradeEvent records for %s", len(records), batch_run_id)
    return Output(
        value={
            "batch_run_id": batch_run_id,
            "records": records,
            "iceberg_result": iceberg_result,
        },
        metadata={
            "batch_run_id": MetadataValue.text(batch_run_id),
            "records_loaded": MetadataValue.int(len(records)),
            "iceberg_write_enabled": MetadataValue.bool(write_iceberg),
            "iceberg_strict": MetadataValue.bool(iceberg_strict),
            "iceberg_snapshot": MetadataValue.text(str(iceberg_result.get("snapshot_id")) if iceberg_result else "skipped"),
        },
    )


@asset(
    ins={"bronze": AssetIn("bronze_crypto_trades")},
    group_name="lakehouse_batch",
    description="Apply data quality gates and deduplicate by (symbol, trade_id).",
    metadata={
        "layer": "Silver",
        "rules": "required fields, positive price/quantity, positive event time, latest duplicate wins",
    },
)
def silver_cleaned_trades(context, bronze: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """Clean and deduplicate Bronze records."""
    clean_records, rejected_records, report = clean_and_deduplicate(bronze["records"])
    context.log.info(
        "DQ pass rate %.2f%%: valid=%s rejected=%s duplicates=%s",
        report.pass_rate,
        report.valid_records,
        report.rejected_records,
        report.duplicates_removed,
    )

    return Output(
        value={
            "batch_run_id": bronze["batch_run_id"],
            "records": clean_records,
            "rejected_records": rejected_records,
            "quality": report,
        },
        metadata={
            "valid_records": MetadataValue.int(report.valid_records),
            "rejected_records": MetadataValue.int(report.rejected_records),
            "duplicates_removed": MetadataValue.int(report.duplicates_removed),
            "quality_pass_rate_pct": MetadataValue.float(report.pass_rate),
        },
    )


@asset(
    ins={"silver": AssetIn("silver_cleaned_trades")},
    group_name="lakehouse_batch",
    description="Aggregate clean trades into reconciled one-minute OHLCV/VWAP Gold candles.",
    metadata={
        "layer": "Gold",
        "granularity": "1 minute",
        "status": "Reconciled",
    },
)
def gold_market_aggregates(context, silver: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """Build reconciled Gold candles from Silver records."""
    candles = aggregate_gold_candles(silver["records"])
    watermark = max((row["window_end"] for row in candles), default=None)
    context.log.info("Aggregated %s Gold candles", len(candles))

    return Output(
        value={
            "batch_run_id": silver["batch_run_id"],
            "candles": candles,
            "quality": silver["quality"],
            "watermark": watermark,
        },
        metadata={
            "total_candles": MetadataValue.int(len(candles)),
            "watermark": MetadataValue.text(watermark.isoformat() if watermark else "none"),
        },
    )


@asset(
    ins={"silver": AssetIn("silver_cleaned_trades")},
    group_name="lakehouse_batch",
    description="Persist rejected Silver records into ClickHouse quarantine table for audit.",
    metadata={
        "target_db": "ClickHouse",
        "target_table": "lakehouse.dq_quarantine",
    },
)
def clickhouse_quarantine_sync(context, silver: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """Publish DQ rejected records to ClickHouse quarantine."""
    sync = ClickHouseBatchSync()
    rows_inserted = sync.insert_quarantine_records(
        silver["rejected_records"],
        batch_run_id=silver["batch_run_id"],
    )
    context.log.info("ClickHouse quarantine sync inserted %s rows", rows_inserted)

    return Output(
        value={
            "batch_run_id": silver["batch_run_id"],
            "rows_inserted": rows_inserted,
        },
        metadata={
            "rows_inserted": MetadataValue.int(rows_inserted),
            "clickhouse_table": MetadataValue.text("lakehouse.dq_quarantine"),
        },
    )


@asset(
    ins={"gold": AssetIn("gold_market_aggregates")},
    group_name="lakehouse_batch",
    description="Sync reconciled Gold candles into ClickHouse table lakehouse.batch_agg.",
    metadata={
        "target_db": "ClickHouse",
        "target_table": "lakehouse.batch_agg",
    },
)
def clickhouse_batch_sync(context, gold: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """Publish Gold candles to ClickHouse."""
    sync = ClickHouseBatchSync()
    rows_inserted = sync.insert_batch_aggregates(gold["candles"], batch_run_id=gold["batch_run_id"])
    write_batch_run_log(
        batch_run_id=gold["batch_run_id"],
        quality=gold["quality"],
        candles_count=len(gold["candles"]),
        clickhouse_rows=rows_inserted,
        watermark=gold["watermark"],
    )
    context.log.info("ClickHouse batch sync inserted %s rows", rows_inserted)

    return Output(
        value={
            "batch_run_id": gold["batch_run_id"],
            "rows_inserted": rows_inserted,
            "watermark": gold["watermark"],
            "candles_count": len(gold["candles"]),
        },
        metadata={
            "rows_inserted": MetadataValue.int(rows_inserted),
            "candles_count": MetadataValue.int(len(gold["candles"])),
            "clickhouse_table": MetadataValue.text("lakehouse.batch_agg"),
        },
    )


@asset(
    ins={"ch_sync": AssetIn("clickhouse_batch_sync")},
    group_name="lakehouse_batch",
    description="Update lakehouse.system_watermark for Query Merger routing.",
    metadata={
        "target_table": "lakehouse.system_watermark",
        "serving_impact": "Separates reconciled batch windows from provisional speed windows",
    },
)
def system_watermark_sync(context, ch_sync: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """Advance the batch watermark in ClickHouse."""
    watermark = ch_sync.get("watermark")
    synced = False
    if watermark is not None:
        synced = ClickHouseBatchSync().update_watermark(watermark)
    context.log.info("Watermark sync status=%s watermark=%s", synced, watermark)

    return Output(
        value={
            "batch_run_id": ch_sync["batch_run_id"],
            "watermark_timestamp": watermark.isoformat() if watermark else None,
            "synced": synced,
        },
        metadata={
            "new_watermark_utc": MetadataValue.text(watermark.isoformat() if watermark else "none"),
            "synced": MetadataValue.bool(synced),
        },
    )


@asset(
    group_name="lakehouse_maintenance",
    description="Run Iceberg full table maintenance (compaction, snapshot expiration, orphan removal) and ClickHouse OPTIMIZE FINAL.",
    metadata={
        "layer": "Maintenance",
        "target": "iceberg_catalog.bronze.crypto_trades & ClickHouse OLAP tables",
    },
)
def iceberg_small_files_compaction(context) -> Output[Dict[str, Any]]:
    """Run full table maintenance pipeline for Iceberg and ClickHouse."""
    try:
        from src.batch_layer.maintenance import run_table_maintenance
        spark = get_spark_session("Dagster-IcebergCompaction")
        maint_result = run_table_maintenance(
            spark=spark,
            target_file_size_mb=128,
            snapshot_expire_days=7,
            orphan_older_than_days=3,
            with_sort=False,
        )
        skipped = False
    except Exception as exc:
        maint_result = {"skipped": True, "reason": str(exc)}
        skipped = True
        context.log.warning("Iceberg Table Maintenance skipped: %s", exc)

    # ClickHouse physical part consolidation (OPTIMIZE TABLE FINAL)
    ch_optimized = False
    try:
        from src.utils.config import get_clickhouse_config
        import clickhouse_connect
        ch_cfg = get_clickhouse_config()
        ch_user = getattr(ch_cfg, "user", getattr(ch_cfg, "username", "default"))
        client = clickhouse_connect.get_client(
            host=ch_cfg.host,
            port=ch_cfg.port,
            username=ch_user,
            password=ch_cfg.password,
            database=ch_cfg.database,
            connect_timeout=5,
        )
        client.command("OPTIMIZE TABLE lakehouse.batch_agg FINAL")
        client.command("OPTIMIZE TABLE lakehouse.speed_agg FINAL")
        ch_optimized = True
        context.log.info("ClickHouse batch_agg & speed_agg successfully optimized.")
    except Exception as ch_exc:
        context.log.warning("ClickHouse OPTIMIZE skipped: %s", ch_exc)

    result = {
        "iceberg_maintenance": maint_result,
        "clickhouse_optimized": ch_optimized,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    return Output(
        value=result,
        metadata={
            "skipped": MetadataValue.bool(skipped),
            "clickhouse_optimized": MetadataValue.bool(ch_optimized),
            "result": MetadataValue.json(result),
        },
    )


batch_lakehouse_pipeline_job = define_asset_job(
    name="batch_lakehouse_pipeline_job",
    selection=[
        "bronze_crypto_trades",
        "silver_cleaned_trades",
        "clickhouse_quarantine_sync",
        "gold_market_aggregates",
        "clickhouse_batch_sync",
        "system_watermark_sync",
    ],
    description="Run Bronze -> Silver -> Gold -> ClickHouse -> Watermark for the Batch Layer.",
    executor_def=in_process_executor,
)

iceberg_compaction_job = define_asset_job(
    name="iceberg_compaction_job",
    selection=["iceberg_small_files_compaction"],
    description="Run Iceberg small-file compaction maintenance.",
    executor_def=in_process_executor,
)

batch_pipeline_schedule = ScheduleDefinition(
    job=batch_lakehouse_pipeline_job,
    cron_schedule="*/15 * * * *",
    description="Run Batch Lakehouse Pipeline every 15 minutes.",
)

compaction_schedule = ScheduleDefinition(
    job=iceberg_compaction_job,
    cron_schedule="0 */2 * * *",
    description="Run Iceberg compaction every 2 hours.",
)

all_assets = [
    bronze_crypto_trades,
    silver_cleaned_trades,
    clickhouse_quarantine_sync,
    gold_market_aggregates,
    clickhouse_batch_sync,
    system_watermark_sync,
    iceberg_small_files_compaction,
]

all_jobs = [
    batch_lakehouse_pipeline_job,
    iceberg_compaction_job,
]

all_schedules = [
    batch_pipeline_schedule,
    compaction_schedule,
]


@repository
def lambda_lakehouse_repo():
    """Register assets, jobs, and schedules into Dagster."""
    return [
        *all_assets,
        *all_jobs,
        *all_schedules,
    ]


defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
)
