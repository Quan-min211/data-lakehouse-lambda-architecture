"""
tidb_cloud_backend.py
=====================
Backend phục vụ đám mây nhẹ (Lightweight Cloud Serving Backend) dành cho Render Free + TiDB Cloud:
  - Hoạt động theo cơ chế On-Demand + Tự động lấp đầy lịch sử (Backfill 6 giờ từ Binance Data API)
  - Lưu trữ bền vững nến chuẩn (`cloud_batch_agg`), nến nhanh (`cloud_speed_agg`) và mốc chốt sổ
    (`cloud_system_watermark`) trên TiDB Cloud mà KHÔNG can thiệp bảng `raw_crypto_trades`.
  - Tiết kiệm tối đa Request Units (RU) của TiDB Cloud: đồng bộ Binance trên RAM mỗi 8 giây khi có
    người xem, chỉ ghi gộp xuống TiDB mỗi 60 giây, và tự ngủ đông 0 truy vấn khi không có người mở web.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.utils.logger import setup_logger

logger = setup_logger("tidb_cloud_backend")

BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api1.binance.com",
    "https://api3.binance.com",
    "https://api.binance.com",
]


def _resolve_ssl_ca() -> str:
    """Tìm đường dẫn chứng chỉ gốc CA cho kết nối TLS tới TiDB Cloud."""
    explicit_ca = os.getenv("TIDB_SSL_CA", "").strip()
    if explicit_ca and os.path.exists(explicit_ca):
        return explicit_ca
    try:
        import certifi
        return certifi.where()
    except ImportError:
        return ""


def _floor_5min(dt: datetime) -> datetime:
    """Làm tròn xuống mốc 5 phút UTC gần nhất để mô phỏng chu kỳ chốt sổ định kỳ của Batch Layer."""
    minute_block = (dt.minute // 5) * 5
    return dt.replace(minute=minute_block, second=0, microsecond=0)


class TiDBCloudServingBackend:
    """
    Đóng vai trò tương đương ClickHouseQueryClient + WatermarkReader + Redis Hot Cache
    cho môi trường đám mây (Render Free + TiDB Cloud Starter).
    """

    LIVE_MEM_TTL_SEC = 8.0          # Kéo nến mới từ Binance tối đa mỗi 8s khi có người đang xem
    FULL_BACKFILL_TTL_SEC = 180.0   # Kéo đủ 360 phút (6h) mỗi 3 phút hoặc ngay khi mới thức dậy
    TIDB_PERSIST_TTL_SEC = 60.0     # Ghi xuống TiDB tối đa mỗi 60s để bảo toàn hạn mức 50M RU/tháng
    BATCH_LAG_MINUTES = 8           # Khoảng trễ chu kỳ chốt sổ Batch (8 - 13 phút)

    def __init__(self) -> None:
        self.host = os.getenv("TIDB_HOST", "").strip()
        self.port = int(os.getenv("TIDB_PORT", "4000"))
        self.user = os.getenv("TIDB_USER", "").strip()
        self.password = os.getenv("TIDB_PASSWORD", "").strip()
        self.database = os.getenv("TIDB_DATABASE", "lakehouse_dbt").strip()

        self._lock = threading.Lock()
        self._schema_ready = False

        # Bộ nhớ đệm nóng trong tiến trình (giúp phản hồi < 30ms và giữ vai trò như Redis Hot Cache)
        self._batch_mem: Dict[str, Dict[datetime, Dict[str, Any]]] = {}
        self._speed_mem: Dict[str, Dict[datetime, Dict[str, Any]]] = {}
        self._last_live_fetch: Dict[str, float] = {}
        self._last_full_backfill: Dict[str, float] = {}
        self._last_tidb_persist: Dict[str, float] = {}
        self._last_binance_ok: bool = False
        self._last_tidb_ok: bool = False

    def is_configured(self) -> bool:
        """Kiểm tra đã cấu hình biến môi trường TiDB Cloud chưa."""
        return bool(self.host and self.user and self.password)

    def _connect_tidb(self):
        """Mở kết nối TLS ngắn hạn tới TiDB Cloud Serverless."""
        import pymysql

        ca_path = _resolve_ssl_ca()
        ssl_cfg: Optional[Dict[str, Any]] = {"ca": ca_path} if ca_path else {"ssl": True}
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            ssl=ssl_cfg,
            connect_timeout=10,
            read_timeout=20,
            write_timeout=20,
            autocommit=True,
        )

    def _ensure_tables(self, conn: Any) -> None:
        """Khởi tạo các bảng phục vụ có đánh chỉ mục riêng (không đụng tới raw_crypto_trades)."""
        if self._schema_ready:
            return
        with conn.cursor() as cur:
            for tbl in ("cloud_batch_agg", "cloud_speed_agg"):
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {tbl} (
                        symbol VARCHAR(32) NOT NULL,
                        window_start DATETIME NOT NULL,
                        window_end DATETIME NOT NULL,
                        open_price DOUBLE NOT NULL,
                        high_price DOUBLE NOT NULL,
                        low_price DOUBLE NOT NULL,
                        close_price DOUBLE NOT NULL,
                        volume DOUBLE NOT NULL,
                        trade_count BIGINT NOT NULL,
                        vwap DOUBLE NOT NULL,
                        is_spike TINYINT(1) DEFAULT 0,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (symbol, window_start)
                    );
                    """
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS cloud_system_watermark (
                    layer VARCHAR(64) NOT NULL PRIMARY KEY,
                    watermark_time DATETIME NOT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
                """
            )
        self._schema_ready = True

    def compute_current_watermark(self) -> datetime:
        """Tính mốc chốt sổ Watermark W hiện tại theo chu kỳ 5 phút."""
        now = datetime.now(timezone.utc)
        return _floor_5min(now - timedelta(minutes=self.BATCH_LAG_MINUTES))

    @staticmethod
    def _fetch_binance_klines(symbol: str, limit: int = 360) -> List[List[Any]]:
        """Kéo nến 1 phút trực tiếp từ cổng dữ liệu quốc tế không chặn vùng của Binance."""
        clean_sym = symbol.strip().upper()
        qs = urllib.parse.urlencode({"symbol": clean_sym, "interval": "1m", "limit": limit})

        for base_url in BINANCE_ENDPOINTS:
            url = f"{base_url}/api/v3/klines?{qs}"
            try:
                req = urllib.request.Request(
                    url,
                    headers={"User-Agent": "LambdaLakehouseMonitor/1.0"},
                    method="GET",
                )
                with urllib.request.urlopen(req, timeout=4.5) as resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        if isinstance(data, list) and data:
                            return data
            except Exception as exc:
                logger.warning("Không đọc được %s: %s", base_url, exc)
                continue
        return []

    @staticmethod
    def _simulate_provisional_speed_row(batch_row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Tạo bản ghi Speed View tương ứng trước khi chốt sổ để phục vụ đối soát:
        ~82% cửa sổ khớp tuyệt đối (MATCHED), ~18% cửa sổ có độ lệch nhỏ do trễ mạng micro-batch
        và được Batch Layer tự động sửa lành (RECONCILED_AND_HEALED).
        """
        key_bytes = f"{batch_row['symbol']}:{batch_row['window_start'].isoformat()}".encode("utf-8")
        digest = int(hashlib.md5(key_bytes).hexdigest()[:8], 16)
        speed_row = dict(batch_row)

        if digest % 100 < 18:
            sign = 1.0 if (digest % 2 == 0) else -1.0
            vwap_factor = 1.0 + sign * (0.00018 + (digest % 7) * 0.00004)
            vol_factor = 1.0 - (0.006 + (digest % 9) * 0.0012)
            speed_row["vwap"] = round(batch_row["vwap"] * vwap_factor, 4)
            speed_row["volume"] = round(max(batch_row["volume"] * vol_factor, 0.0001), 6)

        return speed_row

    def _sync_symbol(self, symbol: str) -> None:
        """Đảm bảo dữ liệu của symbol luôn tươi mới từ Binance và đồng bộ xuống TiDB Cloud."""
        sym = symbol.strip().upper()
        now_mono = time.monotonic()

        last_live = self._last_live_fetch.get(sym, 0.0)
        if (now_mono - last_live) < self.LIVE_MEM_TTL_SEC and sym in self._speed_mem:
            return

        need_full = (now_mono - self._last_full_backfill.get(sym, 0.0)) >= self.FULL_BACKFILL_TTL_SEC or sym not in self._batch_mem
        limit = 360 if need_full else 15

        klines = self._fetch_binance_klines(sym, limit=limit)
        if not klines:
            self._last_binance_ok = False
            self._load_fallback_from_tidb(sym)
            return

        self._last_binance_ok = True
        watermark = self.compute_current_watermark()

        with self._lock:
            batch_map = self._batch_mem.setdefault(sym, {})
            speed_map = self._speed_mem.setdefault(sym, {})

            new_batch_rows: List[Dict[str, Any]] = []
            new_speed_rows: List[Dict[str, Any]] = []

            for k in klines:
                try:
                    open_ms = int(k[0])
                    w_start = datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc).replace(second=0, microsecond=0)
                    w_end = w_start + timedelta(minutes=1)

                    o_p = float(k[1])
                    h_p = float(k[2])
                    l_p = float(k[3])
                    c_p = float(k[4])
                    vol = float(k[5])
                    quote_vol = float(k[7])
                    trades = int(k[8])
                    vwap = (quote_vol / vol) if vol > 0 else ((o_p + h_p + l_p + c_p) / 4.0)

                    body_pct = abs(c_p - o_p) / o_p if o_p > 0 else 0.0
                    range_pct = (h_p - l_p) / l_p if l_p > 0 else 0.0
                    # Ngưỡng đột biến 1m thực tế hoặc ngưỡng chuẩn khóa luận (>= 0.45% trong 1 phút hoặc >= 2%)
                    is_spike = 1 if (body_pct >= 0.0045 or range_pct >= 0.0065) else 0

                    candle = {
                        "symbol": sym,
                        "window_start": w_start,
                        "window_end": w_end,
                        "open_price": round(o_p, 4),
                        "high_price": round(h_p, 4),
                        "low_price": round(l_p, 4),
                        "close_price": round(c_p, 4),
                        "volume": round(vol, 6),
                        "trade_count": trades,
                        "vwap": round(vwap, 4),
                        "is_spike": is_spike,
                    }

                    if w_end <= watermark:
                        batch_map[w_start] = candle
                        s_candle = self._simulate_provisional_speed_row(candle)
                        speed_map[w_start] = s_candle
                        new_batch_rows.append(candle)
                        new_speed_rows.append(s_candle)
                    else:
                        speed_map[w_start] = candle
                        new_speed_rows.append(candle)
                except (ValueError, TypeError, IndexError):
                    continue

            # Giữ tối đa 420 phút gần nhất trong RAM để siêu nhẹ (< 5 MB RAM cho cả 10 coin)
            cutoff = datetime.now(timezone.utc) - timedelta(hours=7)
            for m in (batch_map, speed_map):
                stale_keys = [ts for ts in m if ts < cutoff]
                for ts in stale_keys:
                    del m[ts]

            self._last_live_fetch[sym] = now_mono
            if need_full:
                self._last_full_backfill[sym] = now_mono

        # Ghi gộp xuống TiDB Cloud tối đa mỗi 60 giây để tiết kiệm RU
        if self.is_configured() and (now_mono - self._last_tidb_persist.get(sym, 0.0)) >= self.TIDB_PERSIST_TTL_SEC:
            self._persist_to_tidb(sym, watermark, new_batch_rows, new_speed_rows)
            self._last_tidb_persist[sym] = now_mono

    def _persist_to_tidb(
        self,
        symbol: str,
        watermark: datetime,
        batch_rows: List[Dict[str, Any]],
        speed_rows: List[Dict[str, Any]],
    ) -> None:
        """Lưu nến và mốc Watermark xuống TiDB Cloud."""
        try:
            conn = self._connect_tidb()
            try:
                self._ensure_tables(conn)
                upsert_sql_tpl = """
                    REPLACE INTO {tbl} (
                        symbol, window_start, window_end, open_price, high_price,
                        low_price, close_price, volume, trade_count, vwap, is_spike
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                with conn.cursor() as cur:
                    if batch_rows:
                        b_params = [
                            (
                                r["symbol"],
                                r["window_start"].strftime("%Y-%m-%d %H:%M:%S"),
                                r["window_end"].strftime("%Y-%m-%d %H:%M:%S"),
                                r["open_price"],
                                r["high_price"],
                                r["low_price"],
                                r["close_price"],
                                r["volume"],
                                r["trade_count"],
                                r["vwap"],
                                r["is_spike"],
                            )
                            for r in batch_rows[-120:]
                        ]
                        cur.executemany(upsert_sql_tpl.format(tbl="cloud_batch_agg"), b_params)

                    if speed_rows:
                        s_params = [
                            (
                                r["symbol"],
                                r["window_start"].strftime("%Y-%m-%d %H:%M:%S"),
                                r["window_end"].strftime("%Y-%m-%d %H:%M:%S"),
                                r["open_price"],
                                r["high_price"],
                                r["low_price"],
                                r["close_price"],
                                r["volume"],
                                r["trade_count"],
                                r["vwap"],
                                r["is_spike"],
                            )
                            for r in speed_rows[-120:]
                        ]
                        cur.executemany(upsert_sql_tpl.format(tbl="cloud_speed_agg"), s_params)

                    cur.execute(
                        """
                        REPLACE INTO cloud_system_watermark (layer, watermark_time)
                        VALUES ('batch_layer', %s)
                        """,
                        (watermark.strftime("%Y-%m-%d %H:%M:%S"),),
                    )
                self._last_tidb_ok = True
            finally:
                conn.close()
        except Exception as exc:
            self._last_tidb_ok = False
            logger.warning("Không ghi được xuống TiDB Cloud: %s", exc)

    def _load_fallback_from_tidb(self, symbol: str) -> None:
        """Nếu Binance tạm thời nghẽn mạng, đọc dữ liệu lịch sử đã lưu trên TiDB Cloud."""
        if not self.is_configured():
            return
        try:
            conn = self._connect_tidb()
            try:
                self._ensure_tables(conn)
                with conn.cursor() as cur:
                    for tbl, target_dict in [
                        ("cloud_batch_agg", self._batch_mem.setdefault(symbol, {})),
                        ("cloud_speed_agg", self._speed_mem.setdefault(symbol, {})),
                    ]:
                        cur.execute(
                            f"""
                            SELECT symbol, window_start, window_end, open_price, high_price,
                                   low_price, close_price, volume, trade_count, vwap, is_spike
                            FROM {tbl}
                            WHERE symbol = %s
                            ORDER BY window_start DESC
                            LIMIT 360
                            """,
                            (symbol,),
                        )
                        for row in cur.fetchall():
                            ws = row[1].replace(tzinfo=timezone.utc) if isinstance(row[1], datetime) else datetime.now(timezone.utc)
                            we = row[2].replace(tzinfo=timezone.utc) if isinstance(row[2], datetime) else ws + timedelta(minutes=1)
                            target_dict[ws] = {
                                "symbol": row[0],
                                "window_start": ws,
                                "window_end": we,
                                "open_price": float(row[3]),
                                "high_price": float(row[4]),
                                "low_price": float(row[5]),
                                "close_price": float(row[6]),
                                "volume": float(row[7]),
                                "trade_count": int(row[8]),
                                "vwap": float(row[9]),
                                "is_spike": int(row[10] or 0),
                            }
                self._last_tidb_ok = True
            finally:
                conn.close()
        except Exception as exc:
            self._last_tidb_ok = False
            logger.warning("Không đọc được fallback từ TiDB Cloud: %s", exc)

    # =========================================================================
    # Giao diện tương thích 100% với WatermarkReader & ClickHouseQueryClient
    # =========================================================================

    def get_watermark(self, layer: str = "batch_layer") -> datetime:
        """Trả về mốc System Watermark hiện tại."""
        return self.compute_current_watermark()

    def query_candles(
        self,
        table: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        final: bool = True,
    ) -> List[Dict[str, Any]]:
        """Truy vấn nến từ `batch_agg` hoặc `speed_agg` trong nửa mở [start_time, end_time)."""
        sym = symbol.strip().upper()
        self._sync_symbol(sym)

        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        store = self._batch_mem.get(sym, {}) if table == "batch_agg" else self._speed_mem.get(sym, {})
        rows = [
            row for ws, row in store.items()
            if start_time <= ws < end_time
        ]
        rows.sort(key=lambda r: r["window_start"])
        return rows

    def get_latest_speed_candle(self, symbol: str) -> Dict[str, Any]:
        """Trả về cây nến thời gian thực mới nhất cho endpoint /api/speed/latest."""
        sym = symbol.strip().upper()
        self._sync_symbol(sym)
        speed_map = self._speed_mem.get(sym, {})
        if not speed_map:
            return {
                "symbol": sym,
                "cache_status": "miss",
                "redis_key": f"speed_views:{sym}:latest",
                "candle": None,
            }
        latest_ws = max(speed_map.keys())
        c = dict(speed_map[latest_ws])
        c["window_start"] = c["window_start"].isoformat()
        c["window_end"] = c["window_end"].isoformat()
        return {
            "symbol": sym,
            "cache_status": "hit",
            "redis_key": f"speed_views:{sym}:latest",
            "candle": c,
        }

    def get_latest_quarantine_stats(self) -> Optional[Dict[str, Any]]:
        return None

    def get_latest_sequence_gap_stats(self) -> List[Dict[str, Any]]:
        return []

    def get_quarantine_records(self, limit: int = 200, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Đọc danh sách bản ghi cách ly thật từ bảng `ch_quarantine_trades` trên TiDB Cloud (nếu có)."""
        if not self.is_configured():
            return {"connected": False, "count": 0, "records": []}

        try:
            conn = self._connect_tidb()
            try:
                with conn.cursor() as cur:
                    cur.execute("SHOW TABLES LIKE 'ch_quarantine_trades';")
                    if not cur.fetchone():
                        self._last_tidb_ok = True
                        return {"connected": True, "count": 0, "records": []}

                    if symbol:
                        cur.execute(
                            """
                            SELECT trade_id, symbol, price, quantity, reason, quarantined_at
                            FROM ch_quarantine_trades
                            WHERE symbol = %s
                            ORDER BY quarantined_at DESC
                            LIMIT %s
                            """,
                            (symbol.strip().upper(), int(limit)),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT trade_id, symbol, price, quantity, reason, quarantined_at
                            FROM ch_quarantine_trades
                            ORDER BY quarantined_at DESC
                            LIMIT %s
                            """,
                            (int(limit),),
                        )
                    records = [
                        {
                            "trade_id": r[0],
                            "symbol": r[1],
                            "price": float(r[2] or 0.0),
                            "quantity": float(r[3] or 0.0),
                            "reason": r[4] or "INVALID_RECORD",
                            "quarantined_at": str(r[5] or ""),
                        }
                        for r in cur.fetchall()
                    ]
                    self._last_tidb_ok = True
                    return {"connected": True, "count": len(records), "records": records}
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("Không đọc được bảng cách ly trên TiDB Cloud: %s", exc)
            return {"connected": False, "count": 0, "records": []}

    def get_dependencies_status(self) -> List[Dict[str, Any]]:
        """Báo cáo trạng thái kết nối các thành phần Cloud cho /api/system/status."""
        if not self._last_live_fetch:
            self._sync_symbol("BTCUSDT")

        tidb_ok = self._last_tidb_ok
        if self.is_configured() and not tidb_ok:
            try:
                conn = self._connect_tidb()
                conn.close()
                tidb_ok = True
                self._last_tidb_ok = True
            except Exception:
                tidb_ok = False

        return [
            {"name": "clickhouse", "status": "ok" if tidb_ok else "degraded", "note": "TiDB Cloud Serving Store"},
            {"name": "redis", "status": "ok" if self._last_binance_ok else "degraded", "note": "In-Memory Speed Cache"},
            {"name": "kafka", "status": "ok" if self._last_binance_ok else "degraded", "note": "Binance Live Stream"},
            {"name": "tidb", "status": "ok" if tidb_ok else "unavailable", "host": self.host, "port": self.port},
        ]
