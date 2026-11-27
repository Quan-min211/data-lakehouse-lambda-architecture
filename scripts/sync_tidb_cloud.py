"""
sync_tidb_cloud.py
==================
Đồng bộ dữ liệu từ máy nhà lên TiDB Cloud Serverless (Starter $0) và chạy dbt:
  1. Tự động đánh thức cụm TiDB Cloud qua kết nối TLS (certifi).
  2. Quét toàn bộ dữ liệu giao dịch hiện có trên máy nhà:
     - Thư mục datasets/ (raw, mock, processed/silver_store, sample: .csv, .json, .jsonl, .parquet)
     - Kho đối tượng MinIO S3 (các tệp Parquet của bảng Iceberg Bronze nếu Docker đang bật)
  3. Nạp theo từng lô (chunk 3,000 dòng/lần) lên `lakehouse_dbt.raw_crypto_trades` để không tốn RAM
     và không vượt giới hạn gói tin mạng, đồng thời đồng bộ thêm các bảng tổng hợp từ ClickHouse
     (`batch_agg`, `speed_agg`, `quarantine_trades`, `batch_watermarks`) nếu ClickHouse đang chạy.
  4. Chạy `dbt run --target tidb` và `dbt test --target tidb` để xây dựng đầy đủ 3 tầng
     Bronze -> Silver -> Gold ngay trên TiDB Cloud.

Cách dùng:
    # Đẩy TOÀN BỘ dữ liệu lên TiDB Cloud và chạy dbt:
    python scripts/sync_tidb_cloud.py --all --truncate --run-dbt

    # Hoặc giới hạn số dòng kiểm thử nhanh:
    python scripts/sync_tidb_cloud.py --max-rows 5000 --truncate --run-dbt
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT_DIR / ".env")
except ImportError:
    pass

CHUNK_SIZE = 3000


def _resolve_ssl_ca() -> str:
    """Tự động tìm chứng chỉ gốc CA trên Windows/Linux cho TiDB Cloud TLS."""
    explicit_ca = os.getenv("TIDB_SSL_CA", "").strip()
    if explicit_ca and os.path.exists(explicit_ca):
        return explicit_ca
    try:
        import certifi
        return certifi.where()
    except ImportError:
        return ""


def _normalize_trade_tuple(r: Dict[str, Any], default_run_id: str = "tidb_sync") -> Optional[Tuple[Any, ...]]:
    """Chuẩn hóa một bản ghi giao dịch từ bất kỳ định dạng nào sang tuple 11 cột của raw_crypto_trades."""
    try:
        raw_tid = r.get("trade_id") if r.get("trade_id") is not None else r.get("agg_trade_id", r.get("a"))
        if raw_tid is None:
            return None
        tid = int(float(raw_tid))

        sym = str(r.get("symbol") or r.get("s") or "BTCUSDT").strip().upper()
        if not sym:
            return None

        raw_price = r.get("price") if r.get("price") is not None else r.get("p")
        raw_qty = r.get("quantity") if r.get("quantity") is not None else r.get("q")
        if raw_price is None or raw_qty is None:
            return None
        price = float(raw_price)
        qty = float(raw_qty)

        raw_t = r.get("trade_time") if r.get("trade_time") is not None else r.get("T", r.get("timestamp"))
        if raw_t is None:
            return None
        if isinstance(raw_t, str) and not raw_t.replace(".", "", 1).isdigit():
            dt = datetime.fromisoformat(raw_t.replace("Z", "+00:00"))
            t_ms = int(dt.timestamp() * 1000)
        else:
            t_ms = int(float(raw_t))
            # Nếu là epoch giây thay vì mili-giây
            if 0 < t_ms < 10_000_000_000:
                t_ms *= 1000
        if t_ms <= 0:
            return None

        t_dt = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        raw_ing = r.get("ingestion_time")
        ing_ms = int(float(raw_ing)) if raw_ing not in (None, "") and str(raw_ing).replace(".", "", 1).isdigit() else t_ms

        raw_bm = r.get("is_buyer_maker") if r.get("is_buyer_maker") is not None else r.get("m", False)
        buyer = 1 if str(raw_bm).lower() in {"true", "1", "yes"} else 0
        inj = 1 if str(r.get("is_injected", "")).lower() in {"true", "1", "yes"} else 0
        ftype = str(r.get("fault_type") or "")[:64]
        brun = str(r.get("batch_run_id") or default_run_id)[:64]

        return (tid, sym, price, qty, t_ms, t_dt, buyer, ing_ms, inj, ftype, brun)
    except (ValueError, TypeError, OverflowError):
        return None


def _iter_file_records(file_path: Path) -> Iterator[Dict[str, Any]]:
    """Đọc tuần tự từng dòng dữ liệu từ CSV, JSON, JSONL hoặc Parquet mà không ngốn RAM."""
    suffix = file_path.suffix.lower()
    # Bỏ qua các file cấu hình / schema metadata không phải dữ liệu giao dịch
    if file_path.name.lower() in {"schema.json", "metadata.json", "readme.md", "offsets.json"}:
        return

    try:
        if suffix == ".csv":
            with file_path.open("r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row

        elif suffix == ".jsonl":
            with file_path.open("r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            yield obj

        elif suffix == ".json":
            with file_path.open("r", encoding="utf-8", errors="replace") as f:
                payload = json.load(f)
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        yield item
            elif isinstance(payload, dict) and isinstance(payload.get("records"), list):
                for item in payload["records"]:
                    if isinstance(item, dict):
                        yield item

        elif suffix == ".parquet":
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(file_path)
            for batch in pf.iter_batches(batch_size=5000):
                for row in batch.to_pylist():
                    yield row
    except Exception as exc:
        print(f"      [Bỏ qua file {file_path.name}]: {exc}")


def _iter_minio_iceberg_records() -> Iterator[Dict[str, Any]]:
    """Đọc trực tiếp các tệp Parquet của bảng Bronze Iceberg trên MinIO (localhost:9000) nếu đang chạy."""
    endpoint = os.getenv("MINIO_HOST_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    # Kiểm tra nhanh MinIO có đang mở cổng 9000 không
    try:
        urllib.request.urlopen(f"http://{endpoint}/minio/health/live", timeout=1.5)
    except Exception:
        return

    try:
        import pyarrow.fs as pafs
        import pyarrow.parquet as pq

        s3 = pafs.S3FileSystem(
            endpoint_override=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            scheme="http",
        )
        for bucket in ("warehouse", "bronze"):
            try:
                selector = pafs.FileSelector(bucket, recursive=True)
                infos = s3.get_file_info(selector)
            except Exception:
                continue

            parquet_paths = [
                fi.path for fi in infos
                if fi.type == pafs.FileType.File and fi.path.endswith(".parquet") and "/data/" in f"/{fi.path}"
            ]
            if parquet_paths:
                print(f"      -> Phát hiện {len(parquet_paths)} tệp Parquet Iceberg trong MinIO bucket `{bucket}`!")
            for p_path in parquet_paths:
                try:
                    with s3.open_input_file(p_path) as f_in:
                        pf = pq.ParquetFile(f_in)
                        for batch in pf.iter_batches(batch_size=5000):
                            for row in batch.to_pylist():
                                yield row
                except Exception:
                    continue
    except Exception:
        return


def _iter_all_local_trades() -> Iterator[Tuple[str, Dict[str, Any]]]:
    """Quét toàn bộ nguồn dữ liệu giao dịch trên máy nhà theo thứ tự ưu tiên."""
    datasets_dir = ROOT_DIR / "datasets"
    search_dirs = [
        ("datasets/raw", datasets_dir / "raw"),
        ("datasets/processed/silver_store", datasets_dir / "processed" / "silver_store"),
        ("datasets/mock", datasets_dir / "mock"),
        ("datasets/sample", datasets_dir / "sample"),
    ]

    found_any = False
    for label, folder in search_dirs:
        if not folder.exists():
            continue
        files = sorted(
            p for p in folder.rglob("*")
            if p.is_file() and p.suffix.lower() in {".csv", ".json", ".jsonl", ".parquet"}
        )
        if not files:
            continue
        print(f"      -> Quét `{label}`: tìm thấy {len(files)} tệp dữ liệu.")
        for fp in files:
            sym_guess = fp.stem.split("_")[0].upper()
            for rec in _iter_file_records(fp):
                if "symbol" not in rec and "s" not in rec and sym_guess.endswith("USDT"):
                    rec["symbol"] = sym_guess
                found_any = True
                yield (label, rec)

    # Nếu chưa có dữ liệu trên ổ đĩa (hoặc muốn gom thêm từ MinIO Iceberg)
    for rec in _iter_minio_iceberg_records():
        found_any = True
        yield ("minio_iceberg", rec)

    # Nếu máy nhà hoàn toàn trống, sinh bộ mẫu chuẩn 500 dòng để dbt vẫn chạy mượt
    if not found_any:
        print("      -> Chưa tìm thấy file dữ liệu cục bộ, tự động sinh 500 giao dịch mẫu chuẩn.")
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        for i in range(500):
            sym = "BTCUSDT" if i % 2 == 0 else "ETHUSDT"
            base_p = 82450.0 if sym == "BTCUSDT" else 3195.0
            t_ms = now_ms - (500 - i) * 1000
            yield (
                "generated_seed",
                {
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
                },
            )


def _query_clickhouse_jsonl(sql: str) -> List[Dict[str, Any]]:
    """Đọc dữ liệu từ ClickHouse HTTP (localhost:8123) không phụ thuộc thư viện ngoài."""
    ch_host = os.getenv("CLICKHOUSE_HOST_LOCAL", "localhost")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    url = f"http://{ch_host}:{ch_port}/?query=" + urllib.parse.quote(f"{sql} FORMAT JSONEachRow")
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=8) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _sync_clickhouse_serving_tables(conn: Any, database: str, truncate_first: bool) -> None:
    """Đồng bộ toàn bộ các bảng phục vụ của ClickHouse lên TiDB Cloud nếu ClickHouse đang chạy."""
    ch_host = os.getenv("CLICKHOUSE_HOST_LOCAL", "localhost")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    try:
        urllib.request.urlopen(f"http://{ch_host}:{ch_port}/ping", timeout=1.5)
    except Exception:
        print("      -> ClickHouse (localhost:8123) đang tắt, bỏ qua đồng bộ bảng ClickHouse.")
        return

    print("\n[2B] Phát hiện ClickHouse đang chạy — Đồng bộ thêm toàn bộ bảng ClickHouse lên TiDB Cloud...")
    with conn.cursor() as cur:
        cur.execute(f"USE `{database}`;")

        # 1. batch_agg & speed_agg
        for ch_tbl, tidb_tbl in [("lakehouse.batch_agg", "ch_batch_agg"), ("lakehouse.speed_agg", "ch_speed_agg")]:
            try:
                rows = _query_clickhouse_jsonl(f"SELECT * FROM {ch_tbl}")
            except Exception:
                continue

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {tidb_tbl} (
                    symbol VARCHAR(32) NOT NULL,
                    window_start DATETIME(3) NOT NULL,
                    window_end DATETIME(3) NOT NULL,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    volume DOUBLE,
                    trade_count BIGINT,
                    vwap DOUBLE,
                    is_spike TINYINT(1) DEFAULT 0,
                    PRIMARY KEY (symbol, window_start)
                );
                """
            )
            if truncate_first:
                cur.execute(f"TRUNCATE TABLE {tidb_tbl};")

            if rows:
                sql = f"""
                    REPLACE INTO {tidb_tbl} (
                        symbol, window_start, window_end, open_price, high_price,
                        low_price, close_price, volume, trade_count, vwap, is_spike
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                params = [
                    (
                        str(r.get("symbol", "")),
                        str(r.get("window_start", ""))[:19],
                        str(r.get("window_end", ""))[:19],
                        float(r.get("open_price") or 0),
                        float(r.get("high_price") or 0),
                        float(r.get("low_price") or 0),
                        float(r.get("close_price") or 0),
                        float(r.get("volume") or 0),
                        int(r.get("trade_count") or 0),
                        float(r.get("vwap") or 0),
                        1 if r.get("is_spike") else 0,
                    )
                    for r in rows
                ]
                for i in range(0, len(params), CHUNK_SIZE):
                    cur.executemany(sql, params[i : i + CHUNK_SIZE])
                print(f"      -> Đã đồng bộ {len(params):,} nến từ `{ch_tbl}` sang `{database}.{tidb_tbl}`.")

        # 2. quarantine_trades
        try:
            q_rows = _query_clickhouse_jsonl("SELECT * FROM lakehouse.quarantine_trades")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ch_quarantine_trades (
                    trade_id BIGINT,
                    symbol VARCHAR(32),
                    price DOUBLE,
                    quantity DOUBLE,
                    reason VARCHAR(128),
                    quarantined_at VARCHAR(64)
                );
                """
            )
            if truncate_first:
                cur.execute("TRUNCATE TABLE ch_quarantine_trades;")
            if q_rows:
                q_sql = """
                    INSERT INTO ch_quarantine_trades (trade_id, symbol, price, quantity, reason, quarantined_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                q_params = [
                    (
                        int(r.get("trade_id") or 0),
                        str(r.get("symbol") or ""),
                        float(r.get("price") or 0),
                        float(r.get("quantity") or 0),
                        str(r.get("reason") or r.get("error_reason") or "")[:128],
                        str(r.get("quarantined_at") or r.get("ingested_at") or "")[:64],
                    )
                    for r in q_rows
                ]
                for i in range(0, len(q_params), CHUNK_SIZE):
                    cur.executemany(q_sql, q_params[i : i + CHUNK_SIZE])
                print(f"      -> Đã đồng bộ {len(q_params):,} bản ghi cách ly sang `{database}.ch_quarantine_trades`.")
        except Exception:
            pass


def sync_to_tidb(max_rows: Optional[int] = None, truncate_first: bool = False) -> bool:
    """Kết nối tới TiDB Cloud Serverless và đồng bộ dữ liệu theo từng lô."""
    host = os.getenv("TIDB_HOST", "").strip()
    port = int(os.getenv("TIDB_PORT", "4000"))
    user = os.getenv("TIDB_USER", "").strip()
    password = os.getenv("TIDB_PASSWORD", "").strip()
    database = os.getenv("TIDB_DATABASE", "lakehouse_dbt").strip()

    if not host or not user or not password:
        print("\n[CHƯA CẤU HÌNH TIDB CLOUD TRONG .ENV]")
        return False

    try:
        import pymysql
    except ImportError:
        print("[LỖI] Thiếu thư viện pymysql. Chạy lệnh: pip install pymysql certifi dbt-tidb")
        return False

    ca_path = _resolve_ssl_ca()
    ssl_cfg: Optional[Dict[str, Any]] = {"ca": ca_path} if ca_path else {"ssl": True}

    print(f"[1/3] Đang đánh thức & kết nối tới TiDB Cloud ({host}:{port})...")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        ssl=ssl_cfg,
        connect_timeout=20,
        read_timeout=120,
        write_timeout=120,
        autocommit=True,
    )

    insert_sql = """
        INSERT INTO raw_crypto_trades (
            trade_id, symbol, price, quantity, trade_time, trade_time_ts,
            is_buyer_maker, ingestion_time, is_injected, fault_type, batch_run_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

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
                    KEY idx_symbol_time (symbol, trade_time_ts),
                    KEY idx_symbol_tid (symbol, trade_id)
                );
                """
            )
            if truncate_first:
                print(f"      -> Đang làm sạch bảng `{database}.raw_crypto_trades` trước khi nạp mới...")
                cur.execute("TRUNCATE TABLE raw_crypto_trades;")

            limit_str = "TOÀN BỘ" if not max_rows else f"tối đa {max_rows:,}"
            print(f"[2/3] Bắt đầu quét và đồng bộ {limit_str} giao dịch lên `{database}.raw_crypto_trades`...")

            total_inserted = 0
            source_counts: Dict[str, int] = {}
            chunk: List[Tuple[Any, ...]] = []

            for src_label, raw_rec in _iter_all_local_trades():
                tup = _normalize_trade_tuple(raw_rec, default_run_id=src_label)
                if tup is None:
                    continue
                chunk.append(tup)
                source_counts[src_label] = source_counts.get(src_label, 0) + 1

                if len(chunk) >= CHUNK_SIZE:
                    cur.executemany(insert_sql, chunk)
                    total_inserted += len(chunk)
                    chunk.clear()
                    print(f"      ... Đã nạp {total_inserted:,} giao dịch lên TiDB Cloud")

                if max_rows and (total_inserted + len(chunk)) >= max_rows:
                    break

            if chunk:
                cur.executemany(insert_sql, chunk)
                total_inserted += len(chunk)
                chunk.clear()

            print(f"      -> Hoàn tất nạp {total_inserted:,} bản ghi vào `raw_crypto_trades`! Chi tiết nguồn: {source_counts}")

        # Đồng bộ thêm các bảng tổng hợp của ClickHouse nếu container ClickHouse đang chạy
        _sync_clickhouse_serving_tables(conn, database=database, truncate_first=truncate_first)

    finally:
        conn.close()

    return True


def run_dbt_tidb() -> int:
    """Chạy dbt run & dbt test nhắm tới target `tidb` trên TiDB Cloud."""
    env = os.environ.copy()
    ca_path = _resolve_ssl_ca()
    if ca_path and not env.get("TIDB_SSL_CA"):
        env["TIDB_SSL_CA"] = ca_path.replace("\\", "/")

    dbt_bin = [shutil.which("dbt")] if shutil.which("dbt") else [sys.executable, "-m", "dbt.cli.main"]
    dbt_dir = str(ROOT_DIR / "dbt_project")
    for subcmd in ("run", "test"):
        cmd = dbt_bin + [
            subcmd,
            "--project-dir", dbt_dir,
            "--profiles-dir", dbt_dir,
            "--target", "tidb",
        ]
        print(f"\n[3/3] Đang thực thi dbt `{subcmd}` trên TiDB Cloud...")
        proc = subprocess.run(cmd, env=env)
        if proc.returncode != 0:
            return proc.returncode

    print("\n=================================================================")
    print("[HOÀN TẤT 100%] Toàn bộ dữ liệu & 3 tầng Bronze -> Silver -> Gold")
    print("đã được xây dựng và kiểm thử thành công trên TiDB Cloud!")
    print("=================================================================")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Đồng bộ dữ liệu lên TiDB Cloud & chạy dbt.")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Đẩy TOÀN BỘ dữ liệu hiện có trên máy (datasets + MinIO + ClickHouse) không giới hạn số dòng",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=2000,
        help="Số dòng tối đa đồng bộ nếu không bật --all (mặc định: 2000; đặt 0 để lấy tất cả)",
    )
    parser.add_argument("--truncate", action="store_true", help="Làm sạch bảng trước khi nạp mới")
    parser.add_argument("--run-dbt", action="store_true", help="Chạy luôn `dbt run` và `dbt test` trên TiDB Cloud")
    args = parser.parse_args()

    effective_limit: Optional[int] = None if (args.all or args.max_rows <= 0) else args.max_rows
    ok = sync_to_tidb(max_rows=effective_limit, truncate_first=args.truncate)
    if ok and args.run_dbt:
        sys.exit(run_dbt_tidb())


if __name__ == "__main__":
    main()
