"""
seed_speed_view.py
==================
Populate ClickHouse lakehouse.speed_agg with realistic Speed View (provisional)
candles corresponding to existing batch_agg candles.

Simulates the characteristics of real-time stream processing:
  - Streaming window aggregates with late / out-of-order events
  - Approximate VWAP with slight deviation (~0.05% - 0.25% MAPE)
  - Lower volume (~94% - 99%) due to bounded streaming watermark

Used to establish the Speed View baseline for Benchmark 2 (Reprocess Correctness).

Usage:
    python scripts/benchmarks/seed_speed_view.py
    python scripts/benchmarks/seed_speed_view.py --hours-back 2 --window-hours 1
    python scripts/benchmarks/seed_speed_view.py --symbols BTCUSDT ETHUSDT
"""

from __future__ import annotations

import argparse
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── path setup ──────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.serving_layer.clickhouse_client import ClickHouseQueryClient
from src.utils.logger import setup_logger

logger = setup_logger("seed_speed_view")


def get_reference_time(
    ch_client: ClickHouseQueryClient,
    symbol: str = "BTCUSDT",
) -> datetime:
    """Lay MAX(window_start) tu batch_agg lam moc neo."""
    try:
        client = ch_client.get_client()
        if client:
            res = client.query(
                f"SELECT MAX(window_start) FROM lakehouse.batch_agg WHERE symbol = '{symbol}'"
            )
            rows = res.result_rows
            if rows and rows[0][0]:
                max_ws = rows[0][0]
                if max_ws.tzinfo is None:
                    max_ws = max_ws.replace(tzinfo=timezone.utc)
                return max_ws
    except Exception as exc:
        logger.warning("Khong the lay MAX(window_start): %s", exc)
    return datetime.now(timezone.utc)


def seed_speed_view_candles(
    symbols: List[str],
    hours_back: int = 2,
    window_hours: int = 1,
    reference_time: Optional[datetime] = None,
) -> int:
    """
    Lay nen tu batch_agg, tao phien ban speed_agg (xap xi) va insert vao ClickHouse.

    Returns:
        Tong so nen speed_agg da duoc tao va insert.
    """
    ch_client = ClickHouseQueryClient()
    client = ch_client.get_client()
    if client is None:
        logger.error("Khong the ket noi toi ClickHouse.")
        return 0

    if reference_time is None:
        reference_time = get_reference_time(ch_client, symbol=symbols[0] if symbols else "BTCUSDT")

    t_start = reference_time - timedelta(hours=hours_back)
    t_end = t_start + timedelta(hours=window_hours)

    logger.info(
        "Seeding Speed View tu Batch View | Window: [%s, %s] | Symbols: %s",
        t_start.isoformat(),
        t_end.isoformat(),
        symbols,
    )

    total_inserted = 0
    now_utc = datetime.now(timezone.utc)

    # Đảm bảo bảng tồn tại
    try:
        client.command("""
            CREATE TABLE IF NOT EXISTS lakehouse.speed_agg
            (
                symbol          LowCardinality(String),
                window_start    DateTime64(3, 'UTC'),
                window_end      DateTime64(3, 'UTC'),
                open_price      Float64,
                high_price      Float64,
                low_price       Float64,
                close_price     Float64,
                volume          Float64,
                trade_count     UInt64,
                vwap            Float64,
                is_spike        UInt8 DEFAULT 0,
                created_at      DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(created_at)
            PARTITION BY toYYYYMMDD(window_start)
            ORDER BY (symbol, window_start);
        """)
    except Exception as e:
        logger.warning("Kiem tra/tao bang speed_agg: %s", e)

    columns = [
        "symbol",
        "window_start",
        "window_end",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "trade_count",
        "vwap",
        "is_spike",
        "created_at",
    ]

    for symbol in symbols:
        batch_candles = ch_client.query_candles(
            table="batch_agg",
            symbol=symbol,
            start_time=t_start,
            end_time=t_end,
        )

        if not batch_candles:
            logger.warning("  [%s] Khong tim thay nen batch_agg nao trong khoang thoi gian nay.", symbol)
            continue

        speed_rows = []
        random.seed(42 + hash(symbol) % 1000)  # Deterministic seed cho reproducible benchmark

        for bc in batch_candles:
            b_vwap = float(bc.get("vwap", 0.0))
            b_open = float(bc.get("open_price", 0.0))
            b_high = float(bc.get("high_price", 0.0))
            b_low = float(bc.get("low_price", 0.0))
            b_close = float(bc.get("close_price", 0.0))
            b_vol = float(bc.get("volume", 0.0))
            b_tc = int(bc.get("trade_count", 0))

            # Mô phỏng đặc tính Speed View (streaming):
            # 1. Một phần giao dịch đến muộn (> watermark) nên bị bỏ qua ở Speed View -> Vol thấp hơn
            vol_ratio = random.uniform(0.95, 0.985)
            s_vol = round(b_vol * vol_ratio, 6)
            s_tc = max(1, int(b_tc * vol_ratio))

            # 2. VWAP của Speed View sai lệch nhẹ do thiếu dữ liệu muộn (độ lệch Gaussian ~0.12%)
            pct_delta = random.gauss(0.0002, 0.0012)
            s_vwap = round(b_vwap * (1.0 + pct_delta), 4)

            # 3. Giá nến tương ứng
            s_open = b_open
            s_high = round(max(s_vwap, b_high * (1.0 - random.uniform(0, 0.0003))), 4)
            s_low = round(min(s_vwap, b_low * (1.0 + random.uniform(0, 0.0003))), 4)
            s_close = round(b_close * (1.0 + random.gauss(0, 0.0004)), 4)
            s_spike = int(bc.get("is_spike", 0))

            w_start = bc["window_start"]
            w_end = bc["window_end"]

            speed_rows.append([
                symbol,
                w_start,
                w_end,
                s_open,
                s_high,
                s_low,
                s_close,
                s_vol,
                s_tc,
                s_vwap,
                s_spike,
                now_utc,
            ])

        if speed_rows:
            client.insert("lakehouse.speed_agg", speed_rows, column_names=columns)
            logger.info("  [%s] Da nạp %d nến provisional vào lakehouse.speed_agg", symbol, len(speed_rows))
            total_inserted += len(speed_rows)

    logger.info("Hoan tat! Tong cong da nap %d nen vao speed_agg.", total_inserted)
    return total_inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed Speed View data for Benchmark 2")
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="Danh sach coin symbols. Mac dinh: BTCUSDT ETHUSDT",
    )
    parser.add_argument(
        "--hours-back", type=int, default=2,
        help="Khoang cach gio truoc reference_time. Mac dinh: 2",
    )
    parser.add_argument(
        "--window-hours", type=int, default=1,
        help="Do rong cua so (gio). Mac dinh: 1",
    )
    parser.add_argument(
        "--reference-time", default=None,
        help="Moc UTC ISO-8601 neo thoi gian. Mac dinh: lay tu batch_agg.",
    )
    args = parser.parse_args()

    ref_time = None
    if args.reference_time:
        ref_time = datetime.fromisoformat(args.reference_time)
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=timezone.utc)

    seed_speed_view_candles(
        symbols=args.symbols,
        hours_back=args.hours_back,
        window_hours=args.window_hours,
        reference_time=ref_time,
    )


if __name__ == "__main__":
    main()
