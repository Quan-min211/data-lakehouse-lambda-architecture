"""
maintenance.py
==============
Scheduled table maintenance pipeline for Apache Iceberg in the Batch Layer.
Implements:
  1. Expire Old Snapshots (Snapshot retention policy: > 7 days)
  2. Remove Orphan Files (Unreferenced Parquet data files on MinIO S3)
  3. Rewrite Manifests (Metadata compaction)

Matches requirement in:
  - docs/plan/plan_quan.md (Week 7: Maintenance pipeline)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.batch_layer.compaction import CompactionJob, get_spark_session
from src.utils.logger import setup_logger

logger = setup_logger("iceberg_maintenance")


def run_table_maintenance(
    spark=None,
    expire_older_than_days: int = 7,
    retain_last_n_snapshots: int = 3,
    orphan_older_than_days: int = 3,
    rewrite_manifests: bool = True,
) -> Dict[str, Any]:
    """
    Thực thi chu trình bảo trì bảng Iceberg định kỳ.

    Args:
        spark: SparkSession đã kết nối Iceberg Catalog (hoặc None để tự khởi tạo).
        expire_older_than_days: Số ngày lưu snapshot tối đa trước khi hết hạn.
        retain_last_n_snapshots: Số snapshot tối thiểu bắt buộc giữ lại.
        orphan_older_than_days: Ngưỡng ngày để dọn dẹp orphan files.
        rewrite_manifests: Có thực hiện gộp manifest files hay không.

    Returns:
        dict: Kết quả bảo trì chi tiết từng bước.
    """
    if spark is None:
        spark = get_spark_session("IcebergTableMaintenance")

    job = CompactionJob(spark)
    run_id = f"maint_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    logger.info("=" * 65)
    logger.info(f"Bắt đầu chu trình bảo trì bảng Iceberg: {run_id}")
    logger.info("=" * 65)

    t_start = time.time()
    results: Dict[str, Any] = {"run_id": run_id, "steps": {}}

    # 1. Dọn dẹp Snapshots cũ
    logger.info("[1/3] Expire Old Snapshots...")
    try:
        results["steps"]["expire_snapshots"] = job.expire_old_snapshots(
            older_than_days=expire_older_than_days,
            retain_last_n=retain_last_n_snapshots,
        )
    except Exception as exc:
        logger.warning(f"Expire Snapshots bỏ qua hoặc lỗi: {exc}")
        results["steps"]["expire_snapshots"] = {"skipped": True, "reason": str(exc)}

    # 2. Xóa các tệp mồ côi (Orphan files)
    logger.info("[2/3] Remove Orphan Files...")
    try:
        results["steps"]["remove_orphan_files"] = job.remove_orphan_files(
            older_than_days=orphan_older_than_days
        )
    except Exception as exc:
        logger.warning(f"Remove Orphan Files bỏ qua hoặc lỗi: {exc}")
        results["steps"]["remove_orphan_files"] = {"skipped": True, "reason": str(exc)}

    # 3. Gộp và tối ưu Manifest files
    if rewrite_manifests:
        logger.info("[3/3] Rewrite Manifests...")
        try:
            results["steps"]["rewrite_manifests"] = job.rewrite_manifests()
        except Exception as exc:
            logger.warning(f"Rewrite Manifests bỏ qua hoặc lỗi: {exc}")
            results["steps"]["rewrite_manifests"] = {"skipped": True, "reason": str(exc)}

    total_elapsed = round(time.time() - t_start, 2)
    results["total_elapsed_s"] = total_elapsed
    results["completed_at"] = datetime.now(timezone.utc).isoformat()

    logger.info("=" * 65)
    logger.info(f"✓ Chu trình bảo trì bảng Iceberg hoàn tất trong {total_elapsed}s")
    logger.info("=" * 65)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Iceberg Table Maintenance Pipeline")
    parser.add_argument("--expire-days", type=int, default=7, help="Expire snapshots older than N days")
    parser.add_argument("--retain-snapshots", type=int, default=3, help="Minimum snapshots to retain")
    parser.add_argument("--orphan-days", type=int, default=3, help="Remove orphan files older than N days")
    parser.add_argument("--skip-manifest", action="store_true", help="Skip manifest rewrite")
    args = parser.parse_args()

    results = run_table_maintenance(
        expire_older_than_days=args.expire_days,
        retain_last_n_snapshots=args.retain_snapshots,
        orphan_older_than_days=args.orphan_days,
        rewrite_manifests=not args.skip_manifest,
    )
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
