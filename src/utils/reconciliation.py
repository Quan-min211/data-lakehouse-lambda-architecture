"""
reconciliation.py
=================
Quy tac phan loai doi soat Batch vs Speed dung CHUNG cho:
  - Bang kiem toan lakehouse.reconciliation_audit_metrics (Batch Layer)
  - Endpoint GET /api/reconciliation (Serving Layer)

Truoc day hai noi dung hai tieu chi MATCHED khac nhau (do lech khoi luong tuong
doi < 1e-4 % o Batch, do lech tuyet doi < 1e-6 o API), nen cung mot cua so co the
nhan hai trang thai khac nhau.
"""

from __future__ import annotations

from typing import Optional

# VWAP duoc lam tron 4 chu so thap phan o ca hai tang -> sai so tuyet doi 1e-4 (don vi gia)
VWAP_MATCH_TOLERANCE = 1e-4
# Do lech khoi luong tuong doi (%), khong phu thuoc thang gia cua tung cap coin
VOLUME_MATCH_TOLERANCE_PCT = 1e-4

STATUS_MATCHED = "MATCHED"
STATUS_HEALED = "RECONCILED_AND_HEALED"
STATUS_BATCH_ONLY = "BATCH_ONLY"
STATUS_PROVISIONAL_ONLY = "PROVISIONAL_ONLY"


def volume_drift_pct(batch_volume: Optional[float], speed_volume: Optional[float]) -> Optional[float]:
    """Delta V% = |V_speed - V_batch| / V_batch * 100. None neu khong xac dinh."""
    if batch_volume is None or speed_volume is None:
        return None
    if batch_volume > 0:
        return round(abs(speed_volume - batch_volume) / batch_volume * 100.0, 4)
    return 0.0 if speed_volume == 0 else None


def classify_window(
    has_batch: bool,
    has_speed: bool,
    vwap_delta: Optional[float],
    drift_pct: Optional[float],
) -> str:
    """Tra ve trang thai doi soat cua mot cua so."""
    if has_batch and has_speed:
        matched = (
            vwap_delta is not None and vwap_delta < VWAP_MATCH_TOLERANCE
            and drift_pct is not None and drift_pct < VOLUME_MATCH_TOLERANCE_PCT
        )
        return STATUS_MATCHED if matched else STATUS_HEALED
    if has_batch:
        return STATUS_BATCH_ONLY
    return STATUS_PROVISIONAL_ONLY
