"""
dq_metrics.py
=============
Module thu thap, tinh toan va xuat cac chi so chat luong du lieu (Data Quality Metrics)
trong kien truc Data Lakehouse Lambda.

Chuc nang:
  1. Tinh toan ty le Pass Rate, Rejection Rate, Duplicate Rate cho tung dot chay.
  2. Phan tich chi tiet phan bo cac loai loi Data Quality Gate.
  3. Danh gia hieu qua bo loc Quarantine khi doi chieu voi Fault Injector:
     - Precision (Do chinh xac)
     - Recall    (Do phu bat loi)
     - F1-Score  (Chi so F1 can bang)
  4. Ghi log ket qua ra CSV 'results/logs/dq_metrics.csv' va ho tro bang 'sys_dq_metrics'.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.data_quality.dq_checks import QualityReport
from src.utils.logger import setup_logger

logger = setup_logger("dq_metrics")

Record = Dict[str, Any]
ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_METRICS_CSV = ROOT_DIR / "results" / "logs" / "dq_metrics.csv"


@dataclass
class DQMetricsSummary:
    """Tong hop toan bo chi so chat luong du lieu cua dot xu ly."""

    batch_run_id: str
    total_records: int
    valid_records: int
    rejected_records: int
    duplicates_removed: int
    pass_rate: float
    rejection_rate: float
    duplicate_rate: float
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    injected_count: int = 0
    injected_detected: int = 0
    injected_leaked: int = 0
    timestamp: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DQMetricsCollector:
    """Bo thu thap va tinh toan cac chi so Data Quality."""

    CSV_FIELDNAMES = [
        "batch_run_id",
        "total_records",
        "valid_records",
        "rejected_records",
        "duplicates_removed",
        "pass_rate",
        "rejection_rate",
        "duplicate_rate",
        "precision",
        "recall",
        "f1_score",
        "injected_count",
        "injected_detected",
        "injected_leaked",
        "timestamp",
    ]

    @staticmethod
    def compute_basic_metrics(
        report: QualityReport,
        batch_run_id: str = "",
        timestamp: Optional[datetime] = None,
    ) -> DQMetricsSummary:
        """Tinh toan cac chi so co ban tu QualityReport.

        Args:
            report: Doi tuong QualityReport tu clean_and_deduplicate.
            batch_run_id: ID phien chay.
            timestamp: Moc thoi gian ghi nhan.

        Returns:
            DQMetricsSummary chua cac ty le phan tram co ban.
        """
        now_dt = timestamp or datetime.now(timezone.utc)
        total = report.total_records
        pass_rate = report.pass_rate
        rej_rate = round(report.rejected_records / total * 100.0, 2) if total > 0 else 0.0
        dup_rate = round(report.duplicates_removed / total * 100.0, 2) if total > 0 else 0.0

        return DQMetricsSummary(
            batch_run_id=batch_run_id,
            total_records=total,
            valid_records=report.valid_records,
            rejected_records=report.rejected_records,
            duplicates_removed=report.duplicates_removed,
            pass_rate=pass_rate,
            rejection_rate=rej_rate,
            duplicate_rate=dup_rate,
            timestamp=now_dt.isoformat(),
        )

    @classmethod
    def evaluate_fault_injection(
        cls,
        clean_records: List[Record],
        rejected_records: List[Record],
        batch_run_id: str = "",
        timestamp: Optional[datetime] = None,
    ) -> DQMetricsSummary:
        """Danh gia nang luc phat hien va cach ly loi khi co FaultInjector.

        So sanh Ground Truth (is_injected) voi ket qua phan loai cua DQ Gate:
          - TP (True Positive) : Ban ghi bi bom loi va bi Gate bat dung (nam trong rejected)
          - FP (False Positive): Ban ghi sach nhung bi Gate bat nham (nam trong rejected)
          - FN (False Negative): Ban ghi bi bom loi nhung lot qua Gate (nam trong clean)
          - TN (True Negative) : Ban ghi sach duoc Gate cho qua hop le (nam trong clean)

        Returns:
            DQMetricsSummary co kem Precision, Recall va F1-Score.
        """
        now_dt = timestamp or datetime.now(timezone.utc)
        total = len(clean_records) + len(rejected_records)

        # 1. Dem trong rejected_records
        tp = sum(1 for r in rejected_records if bool(r.get("is_injected", False)))
        fp = sum(1 for r in rejected_records if not bool(r.get("is_injected", False)))

        # 2. Dem trong clean_records
        fn = sum(1 for r in clean_records if bool(r.get("is_injected", False)))
        tn = sum(1 for r in clean_records if not bool(r.get("is_injected", False)))

        injected_total = tp + fn
        precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else 1.0
        recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else 1.0
        f1 = (
            round(2.0 * precision * recall / (precision + recall), 4)
            if (precision + recall) > 0
            else 0.0
        )

        pass_rate = round(len(clean_records) / total * 100.0, 2) if total > 0 else 0.0
        rej_rate = round(len(rejected_records) / total * 100.0, 2) if total > 0 else 0.0

        return DQMetricsSummary(
            batch_run_id=batch_run_id,
            total_records=total,
            valid_records=len(clean_records),
            rejected_records=len(rejected_records),
            duplicates_removed=0,
            pass_rate=pass_rate,
            rejection_rate=rej_rate,
            duplicate_rate=0.0,
            precision=precision,
            recall=recall,
            f1_score=f1,
            injected_count=injected_total,
            injected_detected=tp,
            injected_leaked=fn,
            timestamp=now_dt.isoformat(),
        )

    @classmethod
    def log_to_csv(
        cls,
        summary: DQMetricsSummary,
        output_csv: Path | str = DEFAULT_METRICS_CSV,
    ) -> None:
        """Ghi nhat ky DQ Metrics vao file CSV."""
        path = Path(output_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not path.exists()

        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cls.CSV_FIELDNAMES)
            if is_new:
                writer.writeheader()
            writer.writerow(summary.to_dict())

        logger.info("Da ghi nhat ky DQ Metrics vao: %s", path)

    @staticmethod
    def format_markdown_report(summary: DQMetricsSummary) -> str:
        """Xuat bao cao DQ dang Markdown phuc vu bao cao luan van."""
        lines = [
            "### Báo Cáo Đo Lường Chất Lượng Dữ Liệu (Data Quality Gate)",
            f"- **Batch Run ID:** `{summary.batch_run_id or 'N/A'}`",
            f"- **Thời điểm kiểm tra:** `{summary.timestamp}`",
            "",
            "| Chỉ số | Giá trị | Ý nghĩa |",
            "|:---|:---:|:---|",
            f"| **Tổng số bản ghi** | {summary.total_records:,} | Quy mô mẫu kiểm định |",
            f"| **Hợp lệ (Pass)** | {summary.valid_records:,} ({summary.pass_rate}%) | Đủ chuẩn nạp vào Tầng Silver |",
            f"| **Bị từ chối (Quarantine)** | {summary.rejected_records:,} ({summary.rejection_rate}%) | Cách ly điều tra lỗi schema/giá âm |",
            f"| **Trùng lặp (Deduplicated)** | {summary.duplicates_removed:,} ({summary.duplicate_rate}%) | Loại bỏ bản ghi trùng key (trade_id) |",
        ]

        if summary.injected_count > 0:
            lines.extend([
                "",
                "#### Đánh Giá Khả Năng Bắt Lỗi Với Fault Injection",
                "| Metric | Kết quả | Chuẩn đánh giá |",
                "|:---|:---:|:---|",
                f"| **Số lỗi chủ động bơm** | {summary.injected_count:,} events | Ground Truth từ FaultInjector |",
                f"| **Phát hiện & Cách ly** | {summary.injected_detected:,} events | Số lỗi bị Quarantine bắt giữ |",
                f"| **Bỏ lọt (Leaked)** | {summary.injected_leaked:,} events | Số lỗi lọt qua Gate |",
                f"| **Precision (Độ chuẩn xác)** | **{summary.precision * 100:.2f}%** | Tránh bắt nhầm bản ghi sạch |",
                f"| **Recall (Độ nhạy bắt lỗi)** | **{summary.recall * 100:.2f}%** | Tỷ lệ lỗi được phát hiện |",
                f"| **F1-Score** | **{summary.f1_score:.4f}** | Độ tin cậy tổng thể của DQ Gate |",
            ])

        return "\n".join(lines)
