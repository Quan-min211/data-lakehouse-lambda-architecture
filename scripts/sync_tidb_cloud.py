"""
sync_tidb_cloud.py
==================
Đồng bộ dữ liệu giao dịch từ máy nhà lên TiDB Cloud Serverless và kích hoạt dbt:
  1. Tự động đánh thức cụm TiDB Cloud Serverless (Scale-to-Zero) qua kết nối TLS.
  2. Khởi tạo schema `lakehouse_dbt` và bảng nguồn `raw_crypto_trades`.
  3. Nạp dữ liệu giao dịch từ máy nhà (datasets/mock/ hoặc Silver store) lên TiDB Cloud.
  4. (Tùy chọn `--run-dbt`) Chạy `dbt run --target tidb` và `dbt test --target tidb`
     để xây dựng đầy đủ 3 tầng Bronze -> Silver -> Gold ngay trên TiDB Cloud.
  5. Khi script kết thúc và đóng kết nối, TiDB Cloud Serverless tự động quay về
     trạng thái ngủ đông (Scale-to-Zero, chi phí $0).

Cách dùng:
    pip install pymysql certifi dbt-tidb
    python scripts/sync_tidb_cloud.py --max-rows 2000 --run-dbt
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Thêm root vào sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT_DIR, ".env"))
except ImportError:
    pass


def _resolve_ssl_ca() -> str:
    """Tự động tìm chứng chỉ gốc CA trên Windows/Linux cho TiDB Cloud Serverless TLS."""
    explicit_ca = os.getenv("TIDB_SSL_CA", "").strip()
    if explicit_ca and os.path.exists(explicit_ca):
        return explicit_ca
    try:
        import certifi
        return certifi.where()
    except ImportError:
        return ""


def _load_local_trades(max_rows: int = 2000) -> List[Dict[str, Any]]:
    """Đọc dữ liệu giao dịch sẵn có ở máy nhà (ưu tiên CSV mẫu hoặc tạo mẫu chuẩn)."""
    candidates = sorted(glob.glob(os.path.join(ROOT_DIR, "datasets", "mock", "*.csv")))
    rows: List[Dict[str, Any]] = []

    for csv_path in candidates:
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows.append(r)
                    if len(rows) >= max_rows:
                        return rows
        except Exception:
            continue

    if rows:
        return rows

    # Nếu máy nhà chưa chạy seed_data.py, tạo lô mẫu sạch + vài bản ghi trùng để dbt khử trùng
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    for i in range(min(max_rows, 300)):
        sym = "BTCUSDT" if i % 2 == 0 else "ETHUSDT"
        base_p = 82450.0 if sym == "BTCUSDT" else 3195.0
        t_ms = now_ms - (300 - i) * 1000
        rows.append({
            "trade_id": 9000000 + i,
            "symbol": sym,
            "price": round(base_p + (i % 15) * 1.25, 4),
            "quantity": round(0.05 + (i % 7) * 0.03, 4),
            "trade_time": t_ms,
            "is_buyer_maker": i % 2 == 0,
            "ingestion_time": t_ms + 40,
            "is_injected": False,
            "fault_type": "",
            "batch_run_id": "tidb_sync_seed",
        })
    return rows


def sync_to_tidb(max_rows: int = 2000, truncate_first: bool = False) -> bool:
    """Kết nối tới TiDB Cloud Serverless và đồng bộ bảng raw_crypto_trades."""
    host = os.getenv("TIDB_HOST", "").strip()
    port = int(os.getenv("TIDB_PORT", "4000"))
    user = os.getenv("TIDB_USER", "").strip()
    password = os.getenv("TIDB_PASSWORD", "").strip()
    database = os.getenv("TIDB_DATABASE", "lakehouse_dbt").strip()

    if not host or not user or not password:
        print("\n[HIBERNATING / CHƯA CẤU HÌNH TIDB CLOUD]")
        print("Vui lòng điền thông tin kết nối TiDB Cloud Serverless vào file .env:")
        print("  TIDB_HOST=gateway01.ap-southeast-1.prod.aws.tidbcloud.com")
        print("  TIDB_PORT=4000")
        print("  TIDB_USER=<prefix>.root")
        print("  TIDB_PASSWORD=<mat_khau_tidb_cloud>")
        print("  TIDB_DATABASE=lakehouse_dbt\n")
        return False

    try:
        import pymysql
    except ImportError:
        print("[LỖI] Thiếu thư viện pymysql. Chạy lệnh: pip install pymysql certifi dbt-tidb")
        return False

    ca_path = _resolve_ssl_ca()
    ssl_cfg: Optional[Dict[str, Any]] = {"ca": ca_path} if ca_path else {"ssl": True}

    print(f"[1/3] Đang đánh thức & kết nối tới TiDB Cloud Serverless ({host}:{port})...")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        ssl=ssl_cfg,
        connect_timeout=15,
        autocommit=True,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`;")
            cur.execute(f"USE `{database}`;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_crypto_trades (
                    trade_id BIGINT NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    price DOUBLE NOT NULL,
                    quantity DOUBLE NOT NULL,
                    trade_time BIGINT NOT NULL,
                    trade_time_ts DATETIME(3) NOT NULL,
                    is_buyer_maker TINYINT(1) DEFAULT 0,
                    ingestion_time BIGINT NOT NULL,
                    is_injected TINYINT(1) DEFAULT 0,
                    fault_type VARCHAR(64),
                    batch_run_id VARCHAR(64),
                    bronze_written_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
                    KEY idx_symbol_time (symbol, trade_time_ts)
                );
                """
            )
            if truncate_first:
                cur.execute("TRUNCATE TABLE raw_crypto_trades;")

            raw_trades = _load_local_trades(max_rows=max_rows)
            print(f"[2/3] Đang đồng bộ {len(raw_trades)} bản ghi giao dịch lên `{database}.raw_crypto_trades`...")

            insert_sql = """
                INSERT INTO raw_crypto_trades (
                    trade_id, symbol, price, quantity, trade_time, trade_time_ts,
                    is_buyer_maker, ingestion_time, is_injected, fault_type, batch_run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            batch_params = []
            for r in raw_trades:
                try:
                    tid = int(float(r.get("trade_id", 0)))
                    sym = str(r.get("symbol", "BTCUSDT")).strip().upper()
                    price = float(r.get("price", 0.0))
                    qty = float(r.get("quantity", 0.0))
                    t_ms = int(float(r.get("trade_time", 0)))
                    if t_ms <= 0:
                        continue
                    t_dt = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    ing_ms = int(float(r.get("ingestion_time") or t_ms))
                    buyer = 1 if str(r.get("is_buyer_maker", "")).lower() in {"true", "1", "yes"} else 0
                    inj = 1 if str(r.get("is_injected", "")).lower() in {"true", "1", "yes"} else 0
                    ftype = str(r.get("fault_type") or "")[:64]
                    brun = str(r.get("batch_run_id") or "tidb_sync")[:64]
                    batch_params.append((tid, sym, price, qty, t_ms, t_dt, buyer, ing_ms, inj, ftype, brun))
                except (ValueError, TypeError):
                    continue

            if batch_params:
                cur.executemany(insert_sql, batch_params)
            print(f"      -> Đã nạp thành công {len(batch_params)} bản ghi lên TiDB Cloud Serverless!")
    finally:
        conn.close()

    return True


def run_dbt_tidb() -> int:
    """Chạy dbt run & dbt test nhắm tới target `tidb` trên TiDB Cloud Serverless."""
    env = os.environ.copy()
    ca_path = _resolve_ssl_ca()
    if ca_path and not env.get("TIDB_SSL_CA"):
        env["TIDB_SSL_CA"] = ca_path

    dbt_dir = os.path.join(ROOT_DIR, "dbt_project")
    for subcmd in ("run", "test"):
        cmd = [
            "dbt", subcmd,
            "--project-dir", dbt_dir,
            "--profiles-dir", dbt_dir,
            "--target", "tidb",
        ]
        print(f"[3/3] Đang thực thi: {' '.join(cmd)}")
        proc = subprocess.run(cmd, env=env)
        if proc.returncode != 0:
            return proc.returncode

    print("\n[HOÀN TẤT] Mô hình Bronze -> Silver -> Gold trên TiDB Cloud đã cập nhật xong!")
    print("Khi không còn truy vấn, cụm TiDB Serverless sẽ tự động ngủ đông (Scale-to-Zero, $0).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Đồng bộ dữ liệu lên TiDB Cloud Serverless & chạy dbt.")
    parser.add_argument("--max-rows", type=int, default=2000, help="Số dòng tối đa đồng bộ mỗi lần (mặc định: 2000)")
    parser.add_argument("--truncate", action="store_true", help="Làm sạch bảng raw_crypto_trades trước khi nạp")
    parser.add_argument("--run-dbt", action="store_true", help="Chạy luôn `dbt run` và `dbt test` trên TiDB Cloud")
    args = parser.parse_args()

    ok = sync_to_tidb(max_rows=args.max_rows, truncate_first=args.truncate)
    if ok and args.run_dbt:
        sys.exit(run_dbt_tidb())


if __name__ == "__main__":
    main()
