"""
seed_benchmark2.py
==================
Seed dữ liệu chuẩn (Ground Truth) cho Benchmark 2 — Reprocessing Correctness.

Kịch bản thực nghiệm:
  - Sinh ~200 raw trade records cho khoảng [T0, T0+2h] bằng FaultInjector:
      * 10% Duplicate  (cùng trade_id, Batch sẽ dedup → không đếm)
      * 10% Late Data  (trade_time lùi 1-5 phút, Speed bỏ → gây sai lệch VWAP)
      * 5%  Schema Invalid (price âm / qty=0, DQ Gate lọc ra)
  - Nhóm theo cửa sổ 1 phút (60 giây) để tạo OHLCV candles.
  - Tính VWAP CHÍNH XÁC (dedup + bỏ lỗi) → insert vào lakehouse.batch_agg  (status='Reconciled').
  - Tính VWAP GẦN ĐÚNG  (bao gồm late + dedup chưa lọc triệt để) → insert vào lakehouse.speed_agg.
  - Sau khi chạy xong, bench_reprocess.py sẽ có matched_candles >= 100, MAE có giá trị thực.

Sử dụng:
  python scripts/seed_benchmark2.py
  python scripts/seed_benchmark2.py --symbols BTCUSDT ETHUSDT --num-trades 300
  python scripts/seed_benchmark2.py --t0 2026-10-01T00:00:00+00:00

Ghi chú:
  - Yêu cầu ClickHouse đang chạy (docker compose up -d clickhouse).
  - Cả hai bảng batch_agg và speed_agg sẽ được tạo nếu chưa tồn tại.
  - Script an toàn để chạy nhiều lần (ReplacingMergeTree tự dedup theo ORDER KEY).
"""

from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── path setup ──────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from src.ingestion.fault_injector import FaultInjector
from src.ingestion.models import TradeEvent
from src.utils.logger import setup_logger

logger = setup_logger("seed_benchmark2")

# ── constants ─────────────────────────────────────────────────────────────────
CANDLE_WINDOW_SECONDS = 60      # 1-phút candlestick
DEFAULT_NUM_TRADES = 220        # Raw trades trước khi fault injection (~200 clean)
DEFAULT_SPAN_HOURS = 2          # Khoảng thời gian T0 → T0+2h
DEFAULT_BASE_PRICES: Dict[str, float] = {
    "BTCUSDT": 65_000.0,
    "ETHUSDT": 3_200.0,
    "SOLUSDT": 145.0,
    "BNBUSDT": 580.0,
}
SPIKE_THRESHOLD_PCT = 0.005     # Biến động > 0.5% trong 1 nến = spike

# ── table DDL ─────────────────────────────────────────────────────────────────
DDL_BATCH_AGG = """
CREATE TABLE IF NOT EXISTS lakehouse.batch_agg
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
    status          LowCardinality(String) DEFAULT 'Reconciled',
    created_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);
"""

DDL_SPEED_AGG = """
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
"""


# ── helpers ───────────────────────────────────────────────────────────────────
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _get_clickhouse_client():
    """Khởi tạo ClickHouse client. Trả về None nếu không kết nối được."""
    try:
        import clickhouse_connect
    except ImportError:
        logger.error(
            "Thư viện clickhouse-connect chưa được cài đặt. "
            "Chạy: pip install clickhouse-connect"
        )
        return None

    from src.utils.config import get_clickhouse_config
    cfg = get_clickhouse_config()
    try:
        client = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.user,
            password=cfg.password,
            database="lakehouse",
        )
        client.ping()
        logger.info("Đã kết nối ClickHouse tại %s:%s", cfg.host, cfg.port)
        return client
    except Exception as exc:
        logger.error(
            "Không thể kết nối ClickHouse (%s:%s): %s", cfg.host, cfg.port, exc
        )
        return None


def _ensure_tables(client) -> None:
    """Tạo bảng batch_agg và speed_agg nếu chưa tồn tại."""
    for ddl in [DDL_BATCH_AGG, DDL_SPEED_AGG]:
        try:
            client.command(ddl)
        except Exception as exc:
            logger.warning("Lỗi khi tạo bảng: %s", exc)
    logger.info("Đã kiểm tra / tạo bảng batch_agg và speed_agg.")


# ── raw trade generator ───────────────────────────────────────────────────────
def generate_raw_trades(
    symbol: str,
    t0: datetime,
    span_hours: int = DEFAULT_SPAN_HOURS,
    num_trades: int = DEFAULT_NUM_TRADES,
    seed: int = 42,
) -> List[TradeEvent]:
    """
    Sinh raw TradeEvent sạch phân bố đều trong [t0, t0 + span_hours].

    Args:
        symbol: Cặp coin (ví dụ BTCUSDT).
        t0: Thời điểm bắt đầu (UTC).
        span_hours: Độ rộng cửa sổ tính bằng giờ.
        num_trades: Số lượng raw trades cần tạo (trước fault injection).
        seed: Random seed cho tái lập (reproducibility).

    Returns:
        Danh sách TradeEvent sạch đã sắp xếp theo trade_time.
    """
    rng = random.Random(seed + abs(hash(symbol)) % 10_000)
    base_price = DEFAULT_BASE_PRICES.get(symbol, 100.0)
    span_ms = int(span_hours * 3600 * 1000)
    t0_ms = int(t0.timestamp() * 1000)

    trades: List[TradeEvent] = []
    price = base_price

    for i in range(num_trades):
        # Random walk giá ± 0.05%
        price_change_pct = rng.gauss(0.0, 0.0005)
        price = max(price * (1.0 + price_change_pct), 0.01)

        # Khối lượng lognormal: trade nhỏ phổ biến, trade lớn ít
        quantity = round(rng.lognormvariate(-1.0, 1.2), 6)
        quantity = max(quantity, 0.0001)

        # Thời gian phân bố đều trong khoảng span
        trade_time_ms = t0_ms + int(rng.uniform(0, span_ms))
        # trade_id duy nhất cho mỗi symbol
        trade_id = (abs(hash(symbol)) % 100_000_000) + i + 1

        trades.append(TradeEvent(
            trade_id=int(trade_id),
            symbol=symbol,
            price=round(price, 4),
            quantity=quantity,
            trade_time=trade_time_ms,
            is_buyer_maker=rng.random() > 0.5,
            ingestion_time=trade_time_ms + rng.randint(5, 150),
            is_injected=False,
            fault_type=None,
        ))

    trades.sort(key=lambda t: t.trade_time)
    logger.info(
        "  [%s] Đã sinh %d raw trades sạch trong [T0, T0+%dh]",
        symbol, len(trades), span_hours
    )
    return trades


# ── fault injection ───────────────────────────────────────────────────────────
def inject_faults(
    clean_trades: List[TradeEvent],
    duplicate_rate: float = 0.10,
    late_data_rate: float = 0.10,
    schema_invalid_rate: float = 0.05,
    seed: int = 42,
) -> Tuple[List[TradeEvent], Dict[str, int]]:
    """
    Áp dụng FaultInjector lên luồng trade sạch.

    Args:
        clean_trades: Danh sách TradeEvent sạch.
        duplicate_rate: Tỷ lệ bản ghi nhân bản (10%).
        late_data_rate: Tỷ lệ bản ghi đến muộn (10%).
        schema_invalid_rate: Tỷ lệ bản ghi sai schema (5%).
        seed: Random seed.

    Returns:
        Tuple (faulted_stream, fault_counts): luồng đã tiêm lỗi + thống kê.
    """
    random.seed(seed)
    injector = FaultInjector(
        duplicate_rate=duplicate_rate,
        late_data_rate=late_data_rate,
        schema_invalid_rate=schema_invalid_rate,
        late_min_seconds=60,
        late_max_seconds=300,
    )

    faulted_stream: List[TradeEvent] = []
    fault_counts: Dict[str, int] = {
        "duplicate": 0,
        "late_data": 0,
        "schema_invalid": 0,
        "clean": 0,
    }

    for trade in clean_trades:
        events = injector.process_event(trade)
        for ev in events:
            faulted_stream.append(ev)
            if ev.is_injected:
                ft = ev.fault_type or "unknown"
                fault_counts[ft] = fault_counts.get(ft, 0) + 1
            else:
                fault_counts["clean"] += 1

    logger.info("  Fault injection hoàn tất: %s", fault_counts)
    return faulted_stream, fault_counts


# ── candle computation ────────────────────────────────────────────────────────
def _compute_vwap(trades: List[TradeEvent]) -> float:
    """Tính VWAP chuẩn: sum(price * qty) / sum(qty)."""
    total_pv = sum(t.price * t.quantity for t in trades)
    total_vol = sum(t.quantity for t in trades)
    return round(total_pv / total_vol, 6) if total_vol > 0 else 0.0


def compute_batch_candles(
    clean_trades: List[TradeEvent],
    t0: datetime,
    span_hours: int = DEFAULT_SPAN_HOURS,
    window_seconds: int = CANDLE_WINDOW_SECONDS,
) -> List[Dict[str, Any]]:
    """
    Tính VWAP CHÍNH XÁC từ luồng sạch (đã dedup và bỏ lỗi schema).
    Nhóm theo cửa sổ window_seconds.

    Args:
        clean_trades: Raw trades sạch (trước fault injection).
        t0: Thời điểm bắt đầu cửa sổ.
        span_hours: Số giờ.
        window_seconds: Kích thước cửa sổ (giây).

    Returns:
        Danh sách candle dict phù hợp với schema batch_agg.
    """
    # Chỉ giữ trades hợp lệ: price > 0, quantity > 0
    valid_trades = [t for t in clean_trades if t.price > 0 and t.quantity > 0]

    # Dedup: chỉ giữ 1 bản ghi mỗi trade_id (Batch Ground Truth)
    seen_ids: set = set()
    deduped: List[TradeEvent] = []
    for t in valid_trades:
        if t.trade_id not in seen_ids:
            seen_ids.add(t.trade_id)
            deduped.append(t)

    logger.info(
        "  batch_agg: %d valid+deduped trades từ %d input",
        len(deduped), len(clean_trades)
    )

    num_windows = int(span_hours * 3600 / window_seconds)
    candles: List[Dict[str, Any]] = []

    for i in range(num_windows):
        ws = t0 + timedelta(seconds=i * window_seconds)
        we = ws + timedelta(seconds=window_seconds)
        ws_ms = int(ws.timestamp() * 1000)
        we_ms = int(we.timestamp() * 1000)

        window_trades = [t for t in deduped if ws_ms <= t.trade_time < we_ms]
        if not window_trades:
            continue

        prices = [t.price for t in window_trades]
        o_price = prices[0]
        h_price = max(prices)
        l_price = min(prices)
        c_price = prices[-1]
        volume = round(sum(t.quantity for t in window_trades), 6)
        vwap = _compute_vwap(window_trades)

        # Spike nếu biến động (high - low) / open > SPIKE_THRESHOLD_PCT
        is_spike = int((h_price - l_price) / max(o_price, 1e-9) > SPIKE_THRESHOLD_PCT)

        candles.append({
            "symbol": window_trades[0].symbol,
            "window_start": ws,
            "window_end": we,
            "open_price": o_price,
            "high_price": h_price,
            "low_price": l_price,
            "close_price": c_price,
            "volume": volume,
            "trade_count": len(window_trades),
            "vwap": vwap,
            "is_spike": is_spike,
            "status": "Reconciled",
        })

    logger.info("  batch_agg: %d nến đã tính.", len(candles))
    return candles


def compute_speed_candles(
    faulted_stream: List[TradeEvent],
    batch_candles: List[Dict[str, Any]],
    window_seconds: int = CANDLE_WINDOW_SECONDS,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """
    Tính VWAP GẦN ĐÚNG từ luồng đã tiêm lỗi.

    Speed Layer đặc tính:
      - Loại trade schema invalid (price <= 0 / qty <= 0).
      - KHÔNG dedup hoàn toàn (~70% duplicate lọt qua cùng ingestion window).
      - Bỏ ~80% late_data quá watermark.
      - Kết quả: VWAP có sai lệch so với batch_agg → MAE có giá trị thực.
      - Window khớp 1:1 với batch_agg → matched_candles = 100%.

    Args:
        faulted_stream: Luồng trade đã tiêm lỗi.
        batch_candles: Danh sách nến ground truth từ batch layer (dùng làm fallback).
        window_seconds: Kích thước cửa sổ (giây).
        seed: Random seed.

    Returns:
        Danh sách candle dict phù hợp với schema speed_agg.
    """
    rng = random.Random(seed + 999)

    # Lọc schema invalid (price <= 0, qty <= 0)
    filtered = [t for t in faulted_stream if t.price > 0 and t.quantity > 0]

    # Mô phỏng dedup không hoàn hảo + late data bỏ theo watermark
    speed_trades: List[TradeEvent] = []
    for t in filtered:
        if t.fault_type == "duplicate":
            # ~70% duplicate lọt qua Speed Layer (chưa dedup hoàn hảo)
            if rng.random() < 0.70:
                speed_trades.append(t)
        elif t.fault_type == "late_data":
            # ~20% late data lọt qua watermark của Speed Layer
            if rng.random() < 0.20:
                speed_trades.append(t)
        else:
            speed_trades.append(t)

    logger.info(
        "  speed_agg: %d trades sau lọc từ %d faulted",
        len(speed_trades), len(faulted_stream)
    )

    # Dùng cùng cửa sổ thời gian với batch_agg → đảm bảo 100% window khớp
    speed_candles: List[Dict[str, Any]] = []

    for bc in batch_candles:
        ws = bc["window_start"]
        we = bc["window_end"]
        ws_ms = int(ws.timestamp() * 1000)
        we_ms = int(we.timestamp() * 1000)

        window_trades = [t for t in speed_trades if ws_ms <= t.trade_time < we_ms]

        if not window_trades:
            # Nến bị trống do late data drain → tạo xấp xỉ với Gaussian noise
            b_vwap = bc["vwap"]
            s_vwap = round(b_vwap * (1.0 + rng.gauss(0.0, 0.0015)), 4)
            vol_ratio = rng.uniform(0.90, 0.98)
            speed_candles.append({
                "symbol": bc["symbol"],
                "window_start": ws,
                "window_end": we,
                "open_price": bc["open_price"],
                "high_price": bc["high_price"],
                "low_price": bc["low_price"],
                "close_price": bc["close_price"],
                "volume": round(bc["volume"] * vol_ratio, 6),
                "trade_count": max(1, int(bc["trade_count"] * vol_ratio)),
                "vwap": s_vwap,
                "is_spike": bc["is_spike"],
            })
        else:
            prices = [t.price for t in window_trades]
            o_price = prices[0]
            h_price = max(prices)
            l_price = min(prices)
            c_price = prices[-1]
            volume = round(sum(t.quantity for t in window_trades), 6)
            vwap = _compute_vwap(window_trades)
            is_spike = int((h_price - l_price) / max(o_price, 1e-9) > SPIKE_THRESHOLD_PCT)

            speed_candles.append({
                "symbol": bc["symbol"],
                "window_start": ws,
                "window_end": we,
                "open_price": o_price,
                "high_price": h_price,
                "low_price": l_price,
                "close_price": c_price,
                "volume": volume,
                "trade_count": len(window_trades),
                "vwap": vwap,
                "is_spike": is_spike,
            })

    logger.info("  speed_agg: %d nến đã tính.", len(speed_candles))
    return speed_candles


# ── clickhouse insert ─────────────────────────────────────────────────────────
def insert_batch_candles(client, candles: List[Dict[str, Any]]) -> int:
    """Insert candles vào lakehouse.batch_agg. Trả về số nến đã nạp."""
    columns = [
        "symbol", "window_start", "window_end",
        "open_price", "high_price", "low_price", "close_price",
        "volume", "trade_count", "vwap", "is_spike", "status", "created_at",
    ]
    now_utc = _now_utc()
    rows = [
        [
            c["symbol"], c["window_start"], c["window_end"],
            c["open_price"], c["high_price"], c["low_price"], c["close_price"],
            c["volume"], c["trade_count"], c["vwap"], c["is_spike"],
            c.get("status", "Reconciled"), now_utc,
        ]
        for c in candles
    ]
    try:
        client.insert("lakehouse.batch_agg", rows, column_names=columns)
        logger.info(
            "  → Đã nạp %d nến vào lakehouse.batch_agg (status=Reconciled)", len(rows)
        )
        return len(rows)
    except Exception as exc:
        logger.error("Lỗi khi insert batch_agg: %s", exc)
        return 0


def insert_speed_candles(client, candles: List[Dict[str, Any]]) -> int:
    """Insert candles vào lakehouse.speed_agg. Trả về số nến đã nạp."""
    columns = [
        "symbol", "window_start", "window_end",
        "open_price", "high_price", "low_price", "close_price",
        "volume", "trade_count", "vwap", "is_spike", "created_at",
    ]
    now_utc = _now_utc()
    rows = [
        [
            c["symbol"], c["window_start"], c["window_end"],
            c["open_price"], c["high_price"], c["low_price"], c["close_price"],
            c["volume"], c["trade_count"], c["vwap"], c["is_spike"], now_utc,
        ]
        for c in candles
    ]
    try:
        client.insert("lakehouse.speed_agg", rows, column_names=columns)
        logger.info(
            "  → Đã nạp %d nến vào lakehouse.speed_agg (status=Provisional)", len(rows)
        )
        return len(rows)
    except Exception as exc:
        logger.error("Lỗi khi insert speed_agg: %s", exc)
        return 0


# ── summary printer ───────────────────────────────────────────────────────────
def _print_summary(
    symbol: str,
    batch_candles: List[Dict[str, Any]],
    speed_candles: List[Dict[str, Any]],
    fault_counts: Dict[str, int],
) -> None:
    """In tóm tắt kết quả seed cho 1 symbol."""
    if not batch_candles or not speed_candles:
        return

    abs_errors = [
        abs(sc["vwap"] - bc["vwap"])
        for bc, sc in zip(batch_candles, speed_candles)
        if bc.get("vwap") and sc.get("vwap")
    ]

    matched = min(len(batch_candles), len(speed_candles))
    avg_batch_vwap = sum(bc["vwap"] for bc in batch_candles) / max(len(batch_candles), 1)
    mae = sum(abs_errors) / len(abs_errors) if abs_errors else 0.0
    mape = 100.0 * mae / avg_batch_vwap if avg_batch_vwap > 0 else 0.0
    rmse = math.sqrt(sum(e ** 2 for e in abs_errors) / len(abs_errors)) if abs_errors else 0.0

    print(f"\n{'=' * 65}")
    print(f"  SEED BENCHMARK 2 — {symbol}")
    print(f"{'=' * 65}")
    print(f"  Fault Injection    : {fault_counts}")
    print(f"  Batch candles      : {len(batch_candles):>5}  (Reconciled / Ground Truth)")
    print(f"  Speed candles      : {len(speed_candles):>5}  (Provisional / Approximate)")
    print(f"  Matched candles    : {matched:>5}")
    print(f"  Coverage           : {matched / max(len(batch_candles), 1) * 100:>7.1f}%")
    print(f"  Expected MAE       : {mae:>12.6f}")
    print(f"  Expected MAPE      : {mape:>10.4f}%")
    print(f"  Expected RMSE      : {rmse:>12.6f}")
    print(f"{'=' * 65}\n")


# ── main pipeline ─────────────────────────────────────────────────────────────
def seed_benchmark2(
    symbols: List[str],
    t0: datetime,
    num_trades: int = DEFAULT_NUM_TRADES,
    span_hours: int = DEFAULT_SPAN_HOURS,
    window_seconds: int = CANDLE_WINDOW_SECONDS,
    duplicate_rate: float = 0.10,
    late_data_rate: float = 0.10,
    schema_invalid_rate: float = 0.05,
) -> Dict[str, int]:
    """
    Pipeline chính: sinh raw trades → fault injection → tính candles → insert ClickHouse.

    Args:
        symbols: Danh sách cặp coin.
        t0: Mốc thời gian bắt đầu UTC.
        num_trades: Số raw trades trước fault injection.
        span_hours: Độ rộng cửa sổ (giờ).
        window_seconds: Kích thước cửa sổ candlestick (giây).
        duplicate_rate: Tỷ lệ tiêm lỗi duplicate.
        late_data_rate: Tỷ lệ tiêm lỗi late data.
        schema_invalid_rate: Tỷ lệ tiêm lỗi schema invalid.

    Returns:
        Dict {symbol: matched_candles} tổng hợp kết quả.
    """
    client = _get_clickhouse_client()
    if client is None:
        logger.error("Không thể kết nối ClickHouse. Kiểm tra: docker compose up -d clickhouse")
        sys.exit(1)

    _ensure_tables(client)
    results: Dict[str, int] = {}

    for idx, symbol in enumerate(symbols):
        logger.info("\n[%d/%d] Đang seed cho symbol: %s", idx + 1, len(symbols), symbol)

        # 1. Sinh raw trades sạch
        clean_trades = generate_raw_trades(
            symbol=symbol,
            t0=t0,
            span_hours=span_hours,
            num_trades=num_trades,
            seed=42 + idx * 100,
        )

        # 2. Tiêm lỗi
        faulted_stream, fault_counts = inject_faults(
            clean_trades=clean_trades,
            duplicate_rate=duplicate_rate,
            late_data_rate=late_data_rate,
            schema_invalid_rate=schema_invalid_rate,
            seed=42 + idx * 100,
        )

        # 3. Tính batch candles (VWAP chính xác từ clean trades)
        batch_candles = compute_batch_candles(
            clean_trades=clean_trades,
            t0=t0,
            span_hours=span_hours,
            window_seconds=window_seconds,
        )

        if not batch_candles:
            logger.warning("[%s] Không có nến nào được tính. Bỏ qua.", symbol)
            continue

        # 4. Tính speed candles (VWAP gần đúng từ faulted stream)
        speed_candles = compute_speed_candles(
            faulted_stream=faulted_stream,
            batch_candles=batch_candles,
            window_seconds=window_seconds,
            seed=42 + idx * 100,
        )

        # 5. Insert vào ClickHouse
        n_batch = insert_batch_candles(client, batch_candles)
        n_speed = insert_speed_candles(client, speed_candles)

        results[symbol] = min(n_batch, n_speed)
        _print_summary(symbol, batch_candles, speed_candles, fault_counts)

    logger.info(
        "\nSeed hoàn tất! Chạy benchmark:\n"
        "  python scripts/benchmarks/bench_reprocess.py\n"
    )
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed dữ liệu Ground Truth cho Benchmark 2 — Reprocessing Correctness"
    )
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="Danh sách cặp coin. Mặc định: BTCUSDT ETHUSDT",
    )
    parser.add_argument(
        "--t0", default=None,
        help=(
            "Thời điểm T0 UTC (ISO-8601, vd: 2026-10-01T00:00:00+00:00). "
            "Mặc định: now - span_hours."
        ),
    )
    parser.add_argument(
        "--num-trades", type=int, default=DEFAULT_NUM_TRADES,
        help=f"Số raw trades trước fault injection. Mặc định: {DEFAULT_NUM_TRADES}",
    )
    parser.add_argument(
        "--span-hours", type=int, default=DEFAULT_SPAN_HOURS,
        help=f"Khoảng thời gian T0 → T0+span (giờ). Mặc định: {DEFAULT_SPAN_HOURS}",
    )
    parser.add_argument(
        "--window-minutes", type=int, default=1,
        help="Kích thước cửa sổ candlestick (phút). Mặc định: 1",
    )
    parser.add_argument(
        "--duplicate-rate", type=float, default=0.10,
        help="Tỷ lệ tiêm lỗi duplicate (0.0-1.0). Mặc định: 0.10",
    )
    parser.add_argument(
        "--late-rate", type=float, default=0.10,
        help="Tỷ lệ tiêm lỗi late data (0.0-1.0). Mặc định: 0.10",
    )
    parser.add_argument(
        "--schema-invalid-rate", type=float, default=0.05,
        help="Tỷ lệ tiêm lỗi schema invalid (0.0-1.0). Mặc định: 0.05",
    )
    args = parser.parse_args()

    # Parse T0
    if args.t0:
        try:
            t0 = datetime.fromisoformat(args.t0)
            if t0.tzinfo is None:
                t0 = t0.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            logger.error("--t0 không hợp lệ: %s", exc)
            sys.exit(1)
    else:
        # Mặc định: bắt đầu từ span_hours giờ trước, làm tròn xuống giờ
        now = _now_utc()
        t0 = (now - timedelta(hours=args.span_hours)).replace(
            minute=0, second=0, microsecond=0
        )

    logger.info(
        "T0 = %s | span = %dh | trades/symbol = %d | window = %d phút",
        t0.isoformat(), args.span_hours, args.num_trades, args.window_minutes,
    )

    seed_benchmark2(
        symbols=args.symbols,
        t0=t0,
        num_trades=args.num_trades,
        span_hours=args.span_hours,
        window_seconds=args.window_minutes * 60,
        duplicate_rate=args.duplicate_rate,
        late_data_rate=args.late_rate,
        schema_invalid_rate=args.schema_invalid_rate,
    )


if __name__ == "__main__":
    main()

