"""
quarantine.py
=============
Module quan ly va xu ly cac ban ghi bi cach ly (Quarantine) do vi pham
Data Quality Gates trong kien truc Data Lakehouse Lambda.

Chuc nang:
  1. Dinh tuyen va dong goi ban ghi loi (kem ma loi dq_error, timestamp, raw payload).
  2. Nap ban ghi cach ly vao ClickHouse table 'lakehouse.dq_quarantine'.
  3. Xuat du lieu cach ly ra JSON / CSV de audit va kiem toan du lieu.
  4. Tong hop thong ke cac loai vi pham Data Quality Gate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.utils.logger import setup_logger

logger = setup_logger("quarantine")

Record = Dict[str, Any]


@dataclass(frozen=True)
class QuarantineRecord:
    """Cau truc mot ban ghi bi cach ly."""

    trade_id: str
    symbol: str
    price: str
    quantity: str
    trade_time: str
    dq_error: str
    raw_record: str
    quarantined_at: str
    batch_run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class QuarantineManager:
    """Quan ly vong doi cua cac ban ghi loi bi cach ly (Quarantine Table Pattern)."""

    DDL_INIT_QUARANTINE = """
    CREATE TABLE IF NOT EXISTS lakehouse.dq_quarantine
    (
        batch_run_id    String,
        trade_id        String,
        symbol          String,
        price           String,
        quantity        String,
        trade_time      String,
        dq_error        String,
        raw_record      String,
        quarantined_at  DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(quarantined_at)
    ORDER BY (batch_run_id, quarantined_at);
    """

    def __init__(self, table_name: str = "lakehouse.dq_quarantine"):
        self.table_name = table_name

    @staticmethod
    def wrap_record(
        raw: Record,
        dq_error: str,
        batch_run_id: str = "",
        timestamp: Optional[datetime] = None,
    ) -> QuarantineRecord:
        """Dong goi mot ban ghi tho thanh QuarantineRecord chuan.

        Args:
            raw: Ban ghi goc bi tu choi.
            dq_error: Ma loi hoac ly do vi pham Data Quality.
            batch_run_id: ID phien batch hoac stream pipeline.
            timestamp: Thoi diem cach ly (UTC).

        Returns:
            QuarantineRecord da duoc chuan hoa.
        """
        now_dt = timestamp or datetime.now(timezone.utc)
        return QuarantineRecord(
            trade_id=str(raw.get("trade_id", "")),
            symbol=str(raw.get("symbol", "")).upper(),
            price=str(raw.get("price", "")),
            quantity=str(raw.get("quantity", "")),
            trade_time=str(raw.get("trade_time", "")),
            dq_error=str(dq_error or raw.get("dq_error", "unknown_error")),
            raw_record=json.dumps(raw, default=str),
            quarantined_at=now_dt.isoformat(),
            batch_run_id=str(batch_run_id),
        )

    def format_for_clickhouse(
        self,
        rejected_records: Iterable[Record],
        batch_run_id: str = "",
    ) -> List[List[Any]]:
        """Chuyen danh sach rejected_records thanh danh sach row de insert ClickHouse.

        Args:
            rejected_records: Cac ban ghi loi tu clean_and_deduplicate hoac DQ gate.
            batch_run_id: ID dot chay.

        Returns:
            List row tuong ung schema: [batch_run_id, trade_id, symbol, price, quantity,
                                       trade_time, dq_error, raw_record, quarantined_at].
        """
        now_dt = datetime.now(timezone.utc)
        rows: List[List[Any]] = []

        for r in rejected_records:
            qr = self.wrap_record(r, dq_error=r.get("dq_error", "unknown_error"), batch_run_id=batch_run_id, timestamp=now_dt)
            rows.append([
                qr.batch_run_id,
                qr.trade_id,
                qr.symbol,
                qr.price,
                qr.quantity,
                qr.trade_time,
                qr.dq_error,
                qr.raw_record,
                now_dt,
            ])
        return rows

    def save_to_clickhouse(
        self,
        client: Any,
        rejected_records: List[Record],
        batch_run_id: str = "",
    ) -> int:
        """Luu danh sach ban ghi cach ly truc tiep vao ClickHouse lakehouse.dq_quarantine.

        Args:
            client: ClickHouse native client (clickhouse_connect).
            rejected_records: Danh sach ban ghi bi reject.
            batch_run_id: ID phien batch.

        Returns:
            So luong ban ghi da insert.
        """
        if not rejected_records:
            return 0

        # Khoi tao bang neu chua co
        try:
            client.command(self.DDL_INIT_QUARANTINE)
        except Exception as exc:
            logger.warning("Khong the verify/tao DDL quarantine ClickHouse: %s", exc)

        rows = self.format_for_clickhouse(rejected_records, batch_run_id=batch_run_id)
        columns = [
            "batch_run_id",
            "trade_id",
            "symbol",
            "price",
            "quantity",
            "trade_time",
            "dq_error",
            "raw_record",
            "quarantined_at",
        ]

        client.insert(self.table_name, rows, column_names=columns)
        logger.info(
            "Da cach ly %d ban ghi vao %s (batch_run_id=%s)",
            len(rows),
            self.table_name,
            batch_run_id,
        )
        return len(rows)

    @staticmethod
    def save_to_json(rejected_records: List[Record], output_path: str | Path) -> None:
        """Luu danh sach ban ghi cach ly ra file JSON de audit offline."""
        p = Path(output_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(rejected_records, f, indent=2, default=str)
        logger.info("Da luu %d ban ghi cach ly vao file: %s", len(rejected_records), p)

    @staticmethod
    def get_error_summary(rejected_records: List[Record]) -> Dict[str, Any]:
        """Tong hop so luong ban ghi theo loai loi va theo coin symbol.

        Returns:
            Dict chua error_counts, symbol_counts va total_quarantined.
        """
        error_counts: Dict[str, int] = {}
        symbol_counts: Dict[str, int] = {}

        for r in rejected_records:
            err = str(r.get("dq_error", "unknown_error"))
            sym = str(r.get("symbol", "UNKNOWN")).upper() or "UNKNOWN"

            error_counts[err] = error_counts.get(err, 0) + 1
            symbol_counts[sym] = symbol_counts.get(sym, 0) + 1

        return {
            "total_quarantined": len(rejected_records),
            "by_error": dict(sorted(error_counts.items(), key=lambda x: -x[1])),
            "by_symbol": dict(sorted(symbol_counts.items(), key=lambda x: -x[1])),
        }
