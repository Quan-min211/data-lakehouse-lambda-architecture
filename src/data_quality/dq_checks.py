"""
dq_checks.py
============
Reusable data quality checks for TradeEvent records.

The checks are intentionally small and pure-Python so batch jobs, Dagster assets,
and unit tests can reuse the same validation rules without requiring Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple


Record = Dict[str, Any]


@dataclass(frozen=True)
class QualityReport:
    """Summary of a data quality pass."""

    total_records: int
    valid_records: int
    rejected_records: int
    duplicates_removed: int

    @property
    def pass_rate(self) -> float:
        """Return valid record ratio as a percentage."""
        if self.total_records == 0:
            return 0.0
        return round(self.valid_records / self.total_records * 100.0, 2)


def normalize_trade_record(raw: Record) -> Record:
    """Normalize a raw dict to the TradeEvent contract.

    Args:
        raw: Source record from JSON, CSV, Kafka dump, or generated mock data.

    Returns:
        A dict with the canonical TradeEvent fields and typed values.

    Raises:
        ValueError: If required fields are missing or cannot be converted.
    """
    required = ["trade_id", "symbol", "price", "quantity", "trade_time"]
    missing = [field for field in required if field not in raw or raw[field] in (None, "")]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    return {
        "trade_id": int(raw["trade_id"]),
        "symbol": str(raw["symbol"]).upper(),
        "price": float(raw["price"]),
        "quantity": float(raw["quantity"]),
        "trade_time": int(raw["trade_time"]),
        "is_buyer_maker": bool(raw.get("is_buyer_maker", False)),
        "ingestion_time": int(raw.get("ingestion_time") or raw["trade_time"]),
        "is_injected": bool(raw.get("is_injected", False)),
        "fault_type": raw.get("fault_type"),
    }


def validate_trade_record(record: Record) -> Tuple[bool, str | None]:
    """Validate one normalized TradeEvent record.

    Args:
        record: Normalized TradeEvent dict.

    Returns:
        Tuple of (is_valid, rejection_reason).
    """
    if record["trade_id"] < 0:
        return False, "trade_id_negative"
    if not record["symbol"]:
        return False, "symbol_empty"
    if record["price"] <= 0:
        return False, "price_non_positive"
    if record["quantity"] <= 0:
        return False, "quantity_non_positive"
    if record["trade_time"] <= 0:
        return False, "trade_time_non_positive"
    return True, None


def clean_and_deduplicate(records: Iterable[Record]) -> Tuple[List[Record], List[Record], QualityReport]:
    """Normalize, validate, and deduplicate records by (symbol, trade_id).

    Duplicate keys keep the record with the latest ingestion_time. Rejected
    records include a ``dq_error`` field so they can be quarantined or logged.

    Args:
        records: Raw records to process.

    Returns:
        Tuple of (clean_records, rejected_records, quality_report).
    """
    total = 0
    rejected: List[Record] = []
    by_key: Dict[Tuple[str, int], Record] = {}
    duplicate_count = 0

    for raw in records:
        total += 1
        try:
            record = normalize_trade_record(raw)
            is_valid, reason = validate_trade_record(record)
        except Exception as exc:
            invalid = dict(raw)
            invalid["dq_error"] = str(exc)
            rejected.append(invalid)
            continue

        if not is_valid:
            invalid = dict(record)
            invalid["dq_error"] = reason
            rejected.append(invalid)
            continue

        key = (record["symbol"], record["trade_id"])
        existing = by_key.get(key)
        if existing is not None:
            duplicate_count += 1
            if record.get("ingestion_time", 0) <= existing.get("ingestion_time", 0):
                continue
        by_key[key] = record

    clean = sorted(by_key.values(), key=lambda r: (r["symbol"], r["trade_time"], r["trade_id"]))
    report = QualityReport(
        total_records=total,
        valid_records=len(clean),
        rejected_records=len(rejected),
        duplicates_removed=duplicate_count,
    )
    return clean, rejected, report
