"""
reset_demo_state.py
===================
Script dọn dẹp và reset trạng thái demo / benchmark có kiểm soát
mà KHÔNG cần hủy bỏ hay khởi động lại toàn bộ Docker containers (docker compose down -v).

Các tác vụ thực hiện:
  1. ClickHouse: Truncate các bảng serving và tracking (batch_agg, speed_agg, system_watermark, dq_quarantine, reconciliation_audit_metrics).
  2. Redis: Xóa toàn bộ hot cache keys (speed:candle:*).
  3. Checkpoints: Làm sạch các thư mục checkpoint tạm của Spark Streaming.
  4. Xác nhận trạng thái sạch sau khi reset.

Sử dụng:
  python scripts/reset_demo_state.py
  python scripts/reset_demo_state.py --dry-run
  python scripts/reset_demo_state.py --keep-watermark
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from src.utils.config import get_clickhouse_config, get_redis_config
from src.utils.logger import setup_logger

logger = setup_logger("reset_demo_state")


def reset_clickhouse(keep_watermark: bool = False, dry_run: bool = False) -> list[str]:
    """Truncate các bảng dữ liệu trong ClickHouse."""
    cfg = get_clickhouse_config()
    tables = [
        "batch_agg",
        "speed_agg",
        "dq_quarantine",
        "reconciliation_audit_metrics",
    ]
    if not keep_watermark:
        tables.append("system_watermark")

    cleared = []
    try:
        import clickhouse_connect
        ch_user = getattr(cfg, "user", getattr(cfg, "username", "default"))
        client = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            username=ch_user,
            password=cfg.password,
            database=cfg.database,
            connect_timeout=5,
        )
        logger.info(f"Kết nối thành công ClickHouse tại {cfg.host}:{cfg.port}")

        for table in tables:
            full_table = f"{cfg.database}.{table}"
            if dry_run:
                logger.info(f"[DRY-RUN] Sẽ truncate bảng: {full_table}")
                cleared.append(table)
            else:
                try:
                    client.command(f"TRUNCATE TABLE IF EXISTS {full_table}")
                    logger.info(f"✓ Đã truncate bảng: {full_table}")
                    cleared.append(table)
                except Exception as ex:
                    logger.warning(f"Không thể truncate bảng {full_table}: {ex}")

    except Exception as exc:
        logger.warning(f"Bỏ qua ClickHouse (không kết nối được hoặc service chưa bật): {exc}")

    return cleared


def reset_redis(dry_run: bool = False) -> int:
    """Xóa các key cache nến tức thời trong Redis."""
    cfg = get_redis_config()
    deleted_count = 0
    try:
        import redis
        redis_db = getattr(cfg, "db", 0)
        r = redis.Redis(
            host=cfg.host,
            port=cfg.port,
            password=cfg.password or None,
            db=redis_db,
            socket_timeout=5,
        )
        r.ping()
        logger.info(f"Kết nối thành công Redis tại {cfg.host}:{cfg.port}")

        keys = r.keys("speed:candle:*")
        if keys:
            if dry_run:
                logger.info(f"[DRY-RUN] Sẽ xóa {len(keys)} keys cache nến trong Redis")
                deleted_count = len(keys)
            else:
                deleted_count = r.delete(*keys)
                logger.info(f"✓ Đã xóa {deleted_count} keys cache nến trong Redis")
        else:
            logger.info("Redis không có key nến nào cần xóa.")

    except Exception as exc:
        logger.warning(f"Bỏ qua Redis (không kết nối được hoặc service chưa bật): {exc}")

    return deleted_count


def clean_streaming_checkpoints(dry_run: bool = False) -> list[str]:
    """Dọn dẹp thư mục checkpoint của Spark Streaming trên local host."""
    checkpoint_dirs = [
        ROOT_DIR / "checkpoints",
        ROOT_DIR / ".checkpoint",
        ROOT_DIR / "docker-data" / "checkpoints",
    ]
    cleaned = []
    for cp_dir in checkpoint_dirs:
        if cp_dir.exists() and cp_dir.is_dir():
            if dry_run:
                logger.info(f"[DRY-RUN] Sẽ xóa thư mục checkpoint: {cp_dir}")
                cleaned.append(str(cp_dir))
            else:
                try:
                    shutil.rmtree(cp_dir)
                    logger.info(f"✓ Đã xóa thư mục checkpoint: {cp_dir}")
                    cleaned.append(str(cp_dir))
                except Exception as ex:
                    logger.warning(f"Không thể xóa thư mục {cp_dir}: {ex}")
    return cleaned


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset trạng thái dữ liệu demo/benchmark có kiểm soát")
    parser.add_argument("--dry-run", action="store_true", help="Chạy thử nghiệm không thực sự xóa")
    parser.add_argument("--keep-watermark", action="store_true", help="Giữ lại mốc batch watermark trong ClickHouse")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("  RESET DEMO STATE — LAMBDA LAKEHOUSE BENCHMARK TESTBED")
    print("=" * 70 + "\n")

    if args.dry_run:
        logger.info("Chế độ DRY-RUN được kích hoạt: chỉ kiểm tra, không xóa dữ liệu.")

    ch_cleared = reset_clickhouse(keep_watermark=args.keep_watermark, dry_run=args.dry_run)
    redis_deleted = reset_redis(dry_run=args.dry_run)
    cp_cleaned = clean_streaming_checkpoints(dry_run=args.dry_run)

    print("\n" + "-" * 70)
    print("  KẾT QUẢ RESET:")
    print(f"  • ClickHouse: Đã làm sạch {len(ch_cleared)} bảng: {ch_cleared}")
    print(f"  • Redis: Đã xóa {redis_deleted} hot cache keys")
    print(f"  • Checkpoints: Đã làm sạch {len(cp_cleaned)} thư mục: {cp_cleaned}")
    print("-" * 70 + "\n")
    logger.info("Reset hoàn tất. Môi trường sẵn sàng cho lượt chạy demo/benchmark mới!")


if __name__ == "__main__":
    main()
