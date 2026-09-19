"""
trade_schema.py
===============
Dinh nghia Spark StructType schema cho TradeEvent — tach biet khoi window_aggregator.py
de co the import trong unit test ma KHONG can khoi dong PySpark/JVM.

Dung cho:
  - Unit test kiem tra cau truc schema (khong can Spark)
  - window_aggregator.py (import lai tu day)
  - bench_compaction.py (kiem tra schema Bronze)
"""

from __future__ import annotations

# Lazy import PySpark: chi import khi thuc su can (khi Spark JVM dang chay).
# Neu pyspark chua duoc cai dat, viec chi import module nay se KHONG gay loi.
_SCHEMA_CACHE = None


def get_trade_event_spark_schema():
    """
    Tra ve Spark StructType schema cho TradeEvent.
    PySpark chi duoc import khi ham nay duoc goi lan dau.

    Returns:
        pyspark.sql.types.StructType

    Raises:
        ImportError: neu pyspark chua duoc cai dat.
    """
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is None:
        from pyspark.sql.types import (
            StructType, StructField,
            StringType, DoubleType, LongType, BooleanType,
        )
        _SCHEMA_CACHE = StructType([
            StructField("trade_id",       StringType(),  False),
            StructField("symbol",         StringType(),  False),
            StructField("price",          DoubleType(),  False),
            StructField("quantity",       DoubleType(),  False),
            StructField("trade_time",     LongType(),    False),
            StructField("is_buyer_maker", BooleanType(), True),
            StructField("ingestion_time", LongType(),    True),
            StructField("is_injected",    BooleanType(), True),
            StructField("fault_type",     StringType(),  True),
        ])
    return _SCHEMA_CACHE


# Hang so TRADE_EVENT_FIELD_NAMES de dung trong unit test MA KHONG can PySpark
TRADE_EVENT_FIELD_NAMES = [
    "trade_id",
    "symbol",
    "price",
    "quantity",
    "trade_time",
    "is_buyer_maker",
    "ingestion_time",
    "is_injected",
    "fault_type",
]
