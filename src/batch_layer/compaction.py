"""
compaction.py
=============
Iceberg Compaction Job cho Batch Layer — giải quyết Small File Problem.

Vấn đề:
  Sau nhiều lần Spark Streaming ghi micro-batch vào Bronze Iceberg table,
  xuất hiện hàng nghìn file Parquet nhỏ (vài KB mỗi file).
  Điều này làm chậm đáng kể read performance do phải mở/đọc nhiều file.

Giải pháp — Iceberg Native Compaction:
  1. Bin-Packing: Ghép các file nhỏ thành file lớn hơn (target 128MB)
  2. Sort Compaction: Sắp xếp lại data trong file theo clustering key
  3. Manifest Rewrite: Tối ưu lại metadata manifests

Dùng cho Benchmark 3:
  Đo hiệu quả compaction: so sánh query latency TRƯỚC và SAU compaction.

Sử dụng:
  from src.batch_layer.compaction import CompactionJob, get_spark_session
  from src.batch_layer.iceberg_utils import IcebergTableManager

  spark = get_spark_session("Compaction")
  job   = CompactionJob(spark)
  job.run_full_compaction()
"""

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from pyspark.sql import SparkSession

from .iceberg_utils import (
    IcebergTableManager,
    CATALOG_NAME,
    BRONZE_NAMESPACE,
    BRONZE_TABLE,
    BRONZE_FULL_NAME,
    get_spark_session,
)
from ..utils.logger import setup_logger

logger = setup_logger("compaction")

# ── Compaction Config ──────────────────────────────────────────────────────────
TARGET_FILE_SIZE_BYTES = 134_217_728    # 128 MB — Iceberg default target
MIN_FILE_SIZE_BYTES    = 67_108_864     # 64 MB  — file nhỏ hơn này cần compact
MAX_CONCURRENT_WRITES  = 2             # Số luồng ghi song song
SNAPSHOT_RETENTION_MS  = 7 * 24 * 3600 * 1000   # 7 ngày


class CompactionJob:
    """
    Thực thi các chiến lược Compaction cho Iceberg Bronze table.

    Chiến lược:
    1. Bin-Packing Compaction (Benchmark 3 — chủ đạo)
    2. Sort Compaction (tùy chọn — cải thiện query selective)
    3. Manifest Rewrite (dọn dẹp metadata)
    4. Expire Snapshots (giải phóng dung lượng MinIO)
    """

    def __init__(self, spark: SparkSession):
        """
        Args:
            spark: SparkSession đã cấu hình Iceberg + MinIO.
        """
        self.spark      = spark
        self.table_mgr  = IcebergTableManager(spark)
        self.table_name = BRONZE_FULL_NAME
        logger.info(f"CompactionJob khởi tạo — target table: {self.table_name}")

    # ── Diagnostic ────────────────────────────────────────────────────────────

    def get_file_stats(self, label: str = "") -> dict:
        """
        Lấy thống kê file của Bronze table.
        Dùng để so sánh TRƯỚC/SAU compaction cho Benchmark 3.

        Args:
            label: Nhãn để phân biệt khi log (ví dụ: 'BEFORE', 'AFTER')

        Returns:
            dict: file_count, total_size_mb, avg_size_mb, min_size_mb, max_size_mb
        """
        stats = self.table_mgr.get_file_stats()
        if label:
            logger.info(f"[{label}] File stats: {stats}")
        return stats

    def diagnose_small_files(self, threshold_mb: float = 64.0) -> dict:
        """
        Chẩn đoán mức độ Small File Problem.

        Args:
            threshold_mb: Ngưỡng kích thước (MB). File nhỏ hơn ngưỡng này là 'small file'.

        Returns:
            dict: small_file_count, large_file_count, small_file_pct, recommendation
        """
        try:
            result = self.spark.sql(
                f"SELECT "
                f"  SUM(CASE WHEN file_size_in_bytes < {int(threshold_mb * 1048576)} THEN 1 ELSE 0 END) as small_files, "
                f"  SUM(CASE WHEN file_size_in_bytes >= {int(threshold_mb * 1048576)} THEN 1 ELSE 0 END) as large_files, "
                f"  COUNT(*) as total_files "
                f"FROM {CATALOG_NAME}.{BRONZE_NAMESPACE}.{BRONZE_TABLE}.files"
            ).collect()[0]

            total     = int(result["total_files"] or 0)
            small     = int(result["small_files"] or 0)
            large     = int(result["large_files"] or 0)
            small_pct = round(small / total * 100, 1) if total > 0 else 0

            recommendation = "OK"
            if small_pct > 50:
                recommendation = "CRITICAL — Nên chạy Bin-Packing Compaction ngay"
            elif small_pct > 20:
                recommendation = "WARNING — Lên lịch Compaction trong 24h"
            elif small_pct > 5:
                recommendation = "INFO — Bình thường, có thể compact theo tuần"

            diag = {
                "total_files":    total,
                "small_files":    small,
                "large_files":    large,
                "small_file_pct": small_pct,
                "threshold_mb":   threshold_mb,
                "recommendation": recommendation,
            }
            logger.info(f"Small File Diagnosis: {diag}")
            return diag

        except Exception as e:
            logger.error(f"Lỗi chẩn đoán small files: {e}")
            return {}

    # ── Compaction Strategies ─────────────────────────────────────────────────

    def run_bin_packing_compaction(
        self,
        target_file_size_bytes: int = TARGET_FILE_SIZE_BYTES,
        strategy: str = "binpack",
        min_file_size_bytes: int = MIN_FILE_SIZE_BYTES,
    ) -> dict:
        """
        Chạy Bin-Packing Compaction (chiến lược chính cho Benchmark 3).

        Bin-Packing gộp nhiều file nhỏ thành file lớn hơn theo target size.
        Không thay đổi thứ tự dữ liệu (khác với Sort Compaction).

        Dùng Iceberg Spark SQL: CALL iceberg_catalog.system.rewrite_data_files()

        Args:
            target_file_size_bytes: Kích thước file mục tiêu sau compact (default 128MB).
            strategy:               'binpack' (ghép file) hoặc 'sort' (sắp xếp).
            min_file_size_bytes:    Chỉ compact file nhỏ hơn ngưỡng này.

        Returns:
            dict: rewritten_files_count, added_files_count, elapsed_s
        """
        logger.info(
            f"Bắt đầu Bin-Packing Compaction: "
            f"target={target_file_size_bytes // 1048576}MB, "
            f"strategy={strategy}, "
            f"min_size={min_file_size_bytes // 1048576}MB"
        )
        t_start = time.time()
        stats_before = self.get_file_stats("BEFORE")

        try:
            result = self.spark.sql(f"""
                CALL {CATALOG_NAME}.system.rewrite_data_files(
                    table           => '{BRONZE_NAMESPACE}.{BRONZE_TABLE}',
                    strategy        => '{strategy}',
                    options         => map(
                        'target-file-size-bytes',    '{target_file_size_bytes}',
                        'min-file-size-bytes',       '{min_file_size_bytes}',
                        'max-concurrent-file-group-rewrites', '{MAX_CONCURRENT_WRITES}'
                    )
                )
            """).collect()[0]

            elapsed = time.time() - t_start
            stats_after = self.get_file_stats("AFTER")

            rewritten = int(result["rewritten_files_count"])
            added     = int(result["added_files_count"])

            logger.info(
                f"✓ Bin-Packing hoàn tất: "
                f"rewritten={rewritten} files → {added} files | "
                f"files: {stats_before.get('file_count')} → {stats_after.get('file_count')} | "
                f"avg_size: {stats_before.get('avg_size_mb')}MB → {stats_after.get('avg_size_mb')}MB | "
                f"{elapsed:.1f}s"
            )

            return {
                "strategy":           strategy,
                "rewritten_files":    rewritten,
                "added_files":        added,
                "elapsed_s":          round(elapsed, 2),
                "files_before":       stats_before.get("file_count"),
                "files_after":        stats_after.get("file_count"),
                "avg_size_mb_before": stats_before.get("avg_size_mb"),
                "avg_size_mb_after":  stats_after.get("avg_size_mb"),
                "completed_at":       datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Lỗi Bin-Packing Compaction: {e}")
            raise

    def run_sort_compaction(
        self,
        sort_order: str = "symbol ASC, trade_time_ts ASC",
        target_file_size_bytes: int = TARGET_FILE_SIZE_BYTES,
    ) -> dict:
        """
        Chạy Sort Compaction — sắp xếp lại dữ liệu trong file theo clustering key.

        Sort Compaction tốt hơn Bin-Packing khi truy vấn thường lọc theo symbol hoặc time range,
        vì dữ liệu trong file được sắp xếp giúp Iceberg skip partition nhanh hơn.

        Args:
            sort_order:             Thứ tự sắp xếp (default: symbol ASC, trade_time_ts ASC).
            target_file_size_bytes: Kích thước file mục tiêu.

        Returns:
            dict: rewritten_files_count, added_files_count, elapsed_s
        """
        logger.info(f"Bắt đầu Sort Compaction: sort_order='{sort_order}'")
        t_start = time.time()
        stats_before = self.get_file_stats("BEFORE_SORT")

        try:
            result = self.spark.sql(f"""
                CALL {CATALOG_NAME}.system.rewrite_data_files(
                    table           => '{BRONZE_NAMESPACE}.{BRONZE_TABLE}',
                    strategy        => 'sort',
                    sort_order      => '{sort_order}',
                    options         => map(
                        'target-file-size-bytes', '{target_file_size_bytes}'
                    )
                )
            """).collect()[0]

            elapsed = time.time() - t_start
            stats_after = self.get_file_stats("AFTER_SORT")

            logger.info(
                f"✓ Sort Compaction hoàn tất: "
                f"rewritten={result['rewritten_files_count']} files | "
                f"{elapsed:.1f}s"
            )

            return {
                "strategy":           "sort",
                "sort_order":         sort_order,
                "rewritten_files":    int(result["rewritten_files_count"]),
                "added_files":        int(result["added_files_count"]),
                "elapsed_s":          round(elapsed, 2),
                "files_before":       stats_before.get("file_count"),
                "files_after":        stats_after.get("file_count"),
                "completed_at":       datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Lỗi Sort Compaction: {e}")
            raise

    def rewrite_manifests(self) -> dict:
        """
        Rewrite Manifests — tối ưu metadata layer của Iceberg.

        Sau nhiều lần Append, Iceberg tích lũy nhiều manifest files nhỏ.
        Rewrite gộp chúng lại, giảm metadata scan overhead.

        Returns:
            dict: rewritten_manifests_count, added_manifests_count, elapsed_s
        """
        logger.info("Bắt đầu Manifest Rewrite...")
        t_start = time.time()

        try:
            result = self.spark.sql(f"""
                CALL {CATALOG_NAME}.system.rewrite_manifests(
                    table => '{BRONZE_NAMESPACE}.{BRONZE_TABLE}'
                )
            """).collect()[0]

            elapsed = time.time() - t_start
            logger.info(
                f"✓ Manifest Rewrite hoàn tất: "
                f"rewritten={result['rewritten_manifests_count']} | "
                f"added={result['added_manifests_count']} | {elapsed:.1f}s"
            )

            return {
                "rewritten_manifests": int(result["rewritten_manifests_count"]),
                "added_manifests":     int(result["added_manifests_count"]),
                "elapsed_s":           round(elapsed, 2),
                "completed_at":        datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Lỗi Manifest Rewrite: {e}")
            raise

    def expire_old_snapshots(
        self,
        older_than_days: int = 7,
        retain_last_n: int = 3,
    ) -> dict:
        """
        Xóa các Snapshot cũ để giải phóng dung lượng trên MinIO S3.

        Iceberg giữ snapshots để hỗ trợ Time Travel. Sau khi đủ số snapshot,
        có thể expire để xóa files không còn referenced.

        Args:
            older_than_days: Xóa snapshots cũ hơn N ngày.
            retain_last_n:   Luôn giữ lại ít nhất N snapshots gần nhất.

        Returns:
            dict: deleted_data_files_count, deleted_position_delete_files_count, elapsed_s
        """
        cutoff_ts = int(
            (datetime.now(timezone.utc) - timedelta(days=older_than_days)).timestamp() * 1000
        )
        logger.info(
            f"Expire Snapshots cũ hơn {older_than_days} ngày "
            f"(retain_last={retain_last_n})..."
        )
        t_start = time.time()

        try:
            result = self.spark.sql(f"""
                CALL {CATALOG_NAME}.system.expire_snapshots(
                    table            => '{BRONZE_NAMESPACE}.{BRONZE_TABLE}',
                    older_than       => TIMESTAMP '{datetime.fromtimestamp(cutoff_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}',
                    retain_last      => {retain_last_n}
                )
            """).collect()[0]

            elapsed = time.time() - t_start
            deleted_data  = int(result.get("deleted_data_files_count", 0))
            deleted_manifests = int(result.get("deleted_manifest_files_count", 0))

            logger.info(
                f"✓ Expire Snapshots hoàn tất: "
                f"deleted_data_files={deleted_data} | "
                f"deleted_manifests={deleted_manifests} | "
                f"{elapsed:.1f}s"
            )

            return {
                "deleted_data_files":     deleted_data,
                "deleted_manifest_files": deleted_manifests,
                "older_than_days":        older_than_days,
                "retain_last_n":          retain_last_n,
                "elapsed_s":              round(elapsed, 2),
                "completed_at":           datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Lỗi Expire Snapshots: {e}")
            raise

    # ── Full Compaction Pipeline ──────────────────────────────────────────────

    def run_full_compaction(
        self,
        target_file_size_mb: int = 128,
        expire_older_than_days: int = 7,
        retain_last_n_snapshots: int = 3,
        skip_sort: bool = True,
    ) -> dict:
        """
        Chạy toàn bộ quy trình Compaction (dùng cho Benchmark 3).

        Thứ tự thực thi:
        1. Chẩn đoán Small File Problem
        2. Bin-Packing Compaction (hoặc Sort nếu skip_sort=False)
        3. Manifest Rewrite
        4. Expire Old Snapshots

        Args:
            target_file_size_mb:     Kích thước file mục tiêu (MB).
            expire_older_than_days:  Xóa snapshot cũ hơn N ngày.
            retain_last_n_snapshots: Giữ ít nhất N snapshot gần nhất.
            skip_sort:               True = chỉ chạy Bin-Packing (nhanh hơn).

        Returns:
            dict: Tổng kết kết quả toàn bộ pipeline.
        """
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        logger.info("=" * 60)
        logger.info(f"Full Compaction Pipeline — Run ID: {run_id}")
        logger.info("=" * 60)

        t_total_start = time.time()
        results = {"run_id": run_id, "steps": {}}

        # Bước 0: Chẩn đoán
        logger.info("\n[1/4] Chẩn đoán Small File Problem...")
        results["steps"]["diagnosis"] = self.diagnose_small_files()

        # Bước 1: Bin-Packing Compaction
        logger.info("\n[2/4] Bin-Packing Compaction...")
        try:
            results["steps"]["bin_packing"] = self.run_bin_packing_compaction(
                target_file_size_bytes=target_file_size_mb * 1048576
            )
        except Exception as e:
            logger.warning(f"Bin-Packing bỏ qua (không có file để compact hoặc lỗi): {e}")
            results["steps"]["bin_packing"] = {"skipped": True, "reason": str(e)}

        # Bước 2: Sort Compaction (tùy chọn)
        if not skip_sort:
            logger.info("\n[2b/4] Sort Compaction...")
            try:
                results["steps"]["sort_compaction"] = self.run_sort_compaction()
            except Exception as e:
                logger.warning(f"Sort Compaction bỏ qua: {e}")
                results["steps"]["sort_compaction"] = {"skipped": True, "reason": str(e)}

        # Bước 3: Manifest Rewrite
        logger.info("\n[3/4] Manifest Rewrite...")
        try:
            results["steps"]["manifest_rewrite"] = self.rewrite_manifests()
        except Exception as e:
            logger.warning(f"Manifest Rewrite bỏ qua: {e}")
            results["steps"]["manifest_rewrite"] = {"skipped": True, "reason": str(e)}

        # Bước 4: Expire Snapshots
        logger.info("\n[4/4] Expire Old Snapshots...")
        try:
            results["steps"]["expire_snapshots"] = self.expire_old_snapshots(
                older_than_days=expire_older_than_days,
                retain_last_n=retain_last_n_snapshots,
            )
        except Exception as e:
            logger.warning(f"Expire Snapshots bỏ qua: {e}")
            results["steps"]["expire_snapshots"] = {"skipped": True, "reason": str(e)}

        total_elapsed = time.time() - t_total_start
        results["total_elapsed_s"]  = round(total_elapsed, 2)
        results["completed_at"]     = datetime.now(timezone.utc).isoformat()

        logger.info("=" * 60)
        logger.info(f"✅ Full Compaction hoàn tất: {total_elapsed:.1f}s")
        logger.info("=" * 60)

        return results


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    """Entry point để chạy Compaction Job độc lập từ command line."""
    import argparse
    parser = argparse.ArgumentParser(description="Iceberg Compaction Job — Lambda Lakehouse")
    parser.add_argument("--mode",    default="full",
                        choices=["full", "binpack", "sort", "manifest", "expire"],
                        help="Chế độ compaction")
    parser.add_argument("--target-mb",   type=int, default=128, help="Target file size (MB)")
    parser.add_argument("--expire-days", type=int, default=7,   help="Expire snapshots older than N days")
    parser.add_argument("--with-sort",   action="store_true",   help="Include Sort Compaction in full mode")
    args = parser.parse_args()

    spark = get_spark_session("CompactionJob")
    job   = CompactionJob(spark)

    if args.mode == "full":
        result = job.run_full_compaction(
            target_file_size_mb=args.target_mb,
            expire_older_than_days=args.expire_days,
            skip_sort=not args.with_sort,
        )
    elif args.mode == "binpack":
        result = job.run_bin_packing_compaction(
            target_file_size_bytes=args.target_mb * 1048576
        )
    elif args.mode == "sort":
        result = job.run_sort_compaction()
    elif args.mode == "manifest":
        result = job.rewrite_manifests()
    elif args.mode == "expire":
        result = job.expire_old_snapshots(older_than_days=args.expire_days)

    import json
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
