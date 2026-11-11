"""
silver_store.py
===============
Kho Silver tich luy (append-only) cho che do Batch doc Kafka theo tung dot.

Van de can giai quyet:
  Khi Batch chi doc phan offset MOI cua Kafka, mot cua so 1 phut co the bi chia
  sang hai lan chay (hoac nhan su kien tre sau khi da tinh). Neu chi gop nen tu
  phan du lieu moi roi ghi vao ClickHouse ReplacingMergeTree, nen moi (chi chua
  mot phan giao dich) se THAY THE nen day du cu.

Cach giai quyet:
  Moi ban ghi sach duoc luu them vao kho nay (JSONL, phan vung theo
  symbol / ngay / gio cua trade_time). Moi lan chay, Batch xac dinh cac cua so
  bi anh huong boi ban ghi moi, doc lai TOAN BO ban ghi cua cac cua so do tu kho,
  khu trung theo (symbol, trade_id) roi tinh lai nen. Vi vay nen ghi vao
  batch_agg luon duoc tinh tu day du giao dich da biet.

Tinh chat:
  - Idempotent: chay lai cung mot lo (vi du sau khi ClickHouse loi va offset
    chua duoc commit) khong ghi trung vao kho.
  - Quy tac khu trung giong `clean_and_deduplicate`: ban ghi co
    ingestion_time moi hon thang; bang nhau thi giu ban ghi den truoc.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

from src.utils.logger import setup_logger

logger = setup_logger("silver_store")

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_SILVER_STORE_DIR = ROOT_DIR / "datasets" / "processed" / "silver_store"

Record = Dict[str, Any]
WindowKey = Tuple[str, datetime]
PartitionKey = Tuple[str, str, str]  # (symbol, YYYY-MM-DD, HH)


def _window_start(trade_time_ms: int) -> datetime:
    dt = datetime.fromtimestamp(int(trade_time_ms) / 1000.0, timezone.utc)
    return dt.replace(second=0, microsecond=0)


def _partition_of(record: Record) -> PartitionKey:
    dt = datetime.fromtimestamp(int(record["trade_time"]) / 1000.0, timezone.utc)
    return record["symbol"], dt.strftime("%Y-%m-%d"), dt.strftime("%H")


@dataclass
class SilverUpsertResult:
    """Ket qua mot lan nap ban ghi moi vao kho Silver."""

    affected_windows: Set[WindowKey] = field(default_factory=set)
    appended_records: int = 0
    cross_run_duplicates: int = 0  # ban ghi moi trung khoa voi ban ghi da co tu lan chay truoc


class SilverTradeStore:
    """Kho Silver dang file JSONL, phan vung theo symbol/ngay/gio."""

    def __init__(self, root: Path | str | None = None):
        self.root = Path(root or os.getenv("BATCH_SILVER_STORE_DIR", DEFAULT_SILVER_STORE_DIR))

    # ------------------------------------------------------------------ I/O
    def _path(self, partition: PartitionKey) -> Path:
        symbol, day, hour = partition
        return self.root / symbol / day / f"{hour}.jsonl"

    def _read_partition(self, partition: PartitionKey) -> List[Record]:
        path = self._path(partition)
        if not path.exists():
            return []
        rows: List[Record] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    @staticmethod
    def _dedupe(records: Iterable[Record]) -> Dict[Tuple[str, int], Record]:
        by_key: Dict[Tuple[str, int], Record] = {}
        for rec in records:
            key = (rec["symbol"], int(rec["trade_id"]))
            existing = by_key.get(key)
            if existing is not None and rec.get("ingestion_time", 0) <= existing.get("ingestion_time", 0):
                continue
            by_key[key] = rec
        return by_key

    # ------------------------------------------------------------ public API
    def upsert(self, clean_records: Iterable[Record]) -> SilverUpsertResult:
        """Nap ban ghi sach moi vao kho va tra ve cac cua so bi anh huong."""
        result = SilverUpsertResult()
        by_partition: Dict[PartitionKey, List[Record]] = defaultdict(list)
        for rec in clean_records:
            by_partition[_partition_of(rec)].append(rec)
            result.affected_windows.add((rec["symbol"], _window_start(rec["trade_time"])))

        for partition, new_rows in by_partition.items():
            existing = self._dedupe(self._read_partition(partition))
            to_append: List[Record] = []
            for rec in new_rows:
                key = (rec["symbol"], int(rec["trade_id"]))
                old = existing.get(key)
                if old is not None:
                    result.cross_run_duplicates += 1
                    if rec.get("ingestion_time", 0) <= old.get("ingestion_time", 0):
                        continue  # da co ban ghi bang hoac moi hon -> khong ghi lai (idempotent)
                existing[key] = rec
                to_append.append(rec)

            if to_append:
                path = self._path(partition)
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    for rec in to_append:
                        handle.write(json.dumps(rec, separators=(",", ":")) + "\n")
                result.appended_records += len(to_append)

        logger.info(
            "Silver store upsert: %d ban ghi moi, %d trung voi lan chay truoc, %d cua so bi anh huong",
            result.appended_records,
            result.cross_run_duplicates,
            len(result.affected_windows),
        )
        return result

    def load_windows(self, windows: Iterable[WindowKey]) -> List[Record]:
        """Doc lai toan bo ban ghi (da khu trung) thuoc cac cua so chi dinh."""
        wanted: Dict[PartitionKey, Set[datetime]] = defaultdict(set)
        for symbol, ws in windows:
            wanted[(symbol, ws.strftime("%Y-%m-%d"), ws.strftime("%H"))].add(ws)

        out: List[Record] = []
        for partition, starts in wanted.items():
            for rec in self._dedupe(self._read_partition(partition)).values():
                if _window_start(rec["trade_time"]) in starts:
                    out.append(rec)
        return out
