"""
generate_mock_data.py
=====================
Sinh dữ liệu giao dịch tiền mã hóa tổng hợp dựa trên phân phối thống kê
của dữ liệu Binance thật đã cào về trong datasets/raw/klines/.

Phương pháp: Geometric Brownian Motion (GBM) + Statistical Distribution Fitting

Output:
  datasets/mock/klines/        → Synthetic OHLCV klines (CSV)
  datasets/mock/aggtrades/     → Synthetic aggTrades có fault injection (JSON, TradeEvent format)
  datasets/mock/stats/         → Báo cáo thống kê của từng coin (JSON)
  datasets/mock/summary.json   → Tổng kết lần chạy

Usage:
  python scripts/generate_mock_data.py
  python scripts/generate_mock_data.py --days 30 --ticks-per-min 8 --fault-mode full
  python scripts/generate_mock_data.py --days 7 --no-faults
"""

import json
import csv
import math
import random
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from typing import Optional

import numpy as np

# ── Custom JSON Encoder (fix numpy int64/float64 not serializable) ─────────────
class NumpyEncoder(json.JSONEncoder):
    """Encoder tương thích numpy types với json.dump."""
    def default(self, obj):
        if isinstance(obj, np.integer):  return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray):  return obj.tolist()
        if isinstance(obj, np.bool_):    return bool(obj)
        return super().default(obj)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("mock_generator")

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT_DIR      = Path(__file__).parent.parent
KLINES_DIR    = ROOT_DIR / "datasets" / "raw" / "klines"
MOCK_DIR      = ROOT_DIR / "datasets" / "mock"

# ── Fault Injection Rates (cấu hình cho Benchmark) ────────────────────────────
DEFAULT_FAULT_RATES = {
    "duplicate":      0.10,   # 10% — Event trùng lặp
    "late_data":      0.10,   # 10% — Event đến muộn 1-5 phút
    "out_of_order":   0.05,   #  5% — Event bị đảo thứ tự
    "schema_invalid": 0.03,   #  3% — Event có giá trị phi lý (price < 0)
}


# ── Data Structures ────────────────────────────────────────────────────────────

@dataclass
class CoinStats:
    """Thống kê phân phối học được từ dữ liệu Binance thật."""
    symbol:             str
    last_price:         float    # Giá cuối trong dataset thật (seed price)
    mu_annual:          float    # Expected annual return (drift)
    sigma_annual:       float    # Annual volatility (log-return std)
    mu_per_min:         float    # Drift mỗi phút
    sigma_per_min:      float    # Volatility mỗi phút
    volume_mean:        float    # Volume trung bình mỗi nến 1m
    volume_std:         float    # Độ lệch chuẩn volume
    volume_min:         float
    volume_max:         float
    trade_count_mean:   float    # Số lệnh khớp trung bình mỗi nến
    trade_count_std:    float
    buyer_maker_ratio:  float    # Tỷ lệ is_buyer_maker=True
    candle_count_real:  int      # Số nến thật dùng để phân tích
    source_file:        str


@dataclass
class MockTradeEvent:
    """TradeEvent schema (khớp với src/ingestion/models.py)."""
    trade_id:       int
    symbol:         str
    price:          float
    quantity:       float
    trade_time:     int          # Epoch ms UTC
    is_buyer_maker: bool
    ingestion_time: int          # Epoch ms UTC
    is_injected:    bool
    fault_type:     Optional[str]


# ── Step 1: Phân Tích Thống Kê Từ Data Thật ───────────────────────────────────

def load_and_analyze_klines(csv_path: Path) -> CoinStats:
    """
    Đọc file klines CSV và tính các tham số thống kê dùng để fit GBM.

    Thuộc tính phân tích:
    - Log-returns: r_t = ln(close_t / close_{t-1})
      → μ (drift): mean(r_t) * 1440 * 365   (annualized)
      → σ (volatility): std(r_t) * sqrt(1440 * 365)  (annualized)
    - Volume: mean, std, min, max của cột volume mỗi nến 1m
    - trade_count: mean, std
    - buyer_maker_ratio: tỷ lệ nến có is_buyer_maker=True
    """
    symbol = csv_path.stem.replace("_klines_1m", "").upper()
    log.info(f"  Phân tích {symbol} từ {csv_path.name}...")

    closes, volumes, trade_counts, buyer_makers = [], [], [], []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                closes.append(float(row["close"]))
                volumes.append(float(row["volume"]))
                tc = row.get("trade_count", "0")
                trade_counts.append(float(tc) if tc else 10.0)
                buyer_makers.append(row.get("is_buyer_maker", "False") == "True")
            except (ValueError, KeyError):
                continue

    if len(closes) < 10:
        raise ValueError(f"Không đủ dữ liệu trong {csv_path} (cần ít nhất 10 nến)")

    closes = np.array(closes)
    volumes = np.array(volumes)
    trade_counts = np.array(trade_counts) if trade_counts else np.array([10.0])

    # Log-returns (1 phút = 1 bước)
    log_returns = np.log(closes[1:] / closes[:-1])
    mu_per_min    = float(np.mean(log_returns))
    sigma_per_min = float(np.std(log_returns))

    # Annualized (1440 phút/ngày × 365 ngày)
    MINS_PER_YEAR = 1440 * 365
    mu_annual    = mu_per_min * MINS_PER_YEAR
    sigma_annual = sigma_per_min * math.sqrt(MINS_PER_YEAR)

    return CoinStats(
        symbol            = symbol,
        last_price        = float(closes[-1]),
        mu_annual         = mu_annual,
        sigma_annual      = sigma_annual,
        mu_per_min        = mu_per_min,
        sigma_per_min     = sigma_per_min,
        volume_mean       = float(np.mean(volumes)),
        volume_std        = float(np.std(volumes)),
        volume_min        = float(np.min(volumes)),
        volume_max        = float(np.max(volumes)),
        trade_count_mean  = float(np.mean(trade_counts)),
        trade_count_std   = float(np.std(trade_counts)) if len(trade_counts) > 1 else 3.0,
        buyer_maker_ratio = float(np.mean(buyer_makers)) if buyer_makers else 0.5,
        candle_count_real = len(closes),
        source_file       = str(csv_path),
    )


# ── Step 2: Sinh Dữ Liệu Giá — Geometric Brownian Motion ─────────────────────

def simulate_gbm_prices(stats: CoinStats, n_minutes: int, seed: int = 42) -> np.ndarray:
    """
    Mô phỏng chuỗi giá bằng Geometric Brownian Motion (GBM).

    Công thức:
        S(t+dt) = S(t) × exp[(μ - σ²/2)×dt + σ×√dt×Z]
        Z ~ N(0, 1)   (Wiener process increment)

    Với dt = 1 phút:
        S(t+1) = S(t) × exp[(μ_min - σ_min²/2) + σ_min × Z_t]

    Tham số:
        μ_min = mu_per_min   (drift mỗi phút học từ real data)
        σ_min = sigma_per_min (volatility mỗi phút học từ real data)
    """
    rng = np.random.default_rng(seed)
    prices = np.zeros(n_minutes + 1)
    prices[0] = stats.last_price

    drift      = stats.mu_per_min - 0.5 * stats.sigma_per_min ** 2
    diffusion  = stats.sigma_per_min

    for i in range(1, n_minutes + 1):
        z = rng.standard_normal()
        prices[i] = prices[i - 1] * math.exp(drift + diffusion * z)

    return prices[1:]   # Bỏ seed price, trả về n_minutes giá


def simulate_volume(stats: CoinStats, n_minutes: int, seed: int = 43) -> np.ndarray:
    """
    Sinh volume mỗi nến theo phân phối Log-Normal (phù hợp với tài chính).
    Log-Normal vì volume luôn > 0 và có đuôi dài (heavy tail).
    """
    rng = np.random.default_rng(seed)
    # Ước lượng tham số Log-Normal từ mean/std của real data
    mean = max(stats.volume_mean, 0.001)
    std  = max(stats.volume_std, 0.0001)
    sigma2 = math.log(1 + (std / mean) ** 2)
    mu_ln  = math.log(mean) - sigma2 / 2

    volumes = rng.lognormal(mean=mu_ln, sigma=math.sqrt(sigma2), size=n_minutes)
    # Clip theo range thực tế (± 3σ)
    volumes = np.clip(volumes, stats.volume_min * 0.5, stats.volume_max * 2.0)
    return volumes


# ── Step 3: Sinh Klines và AggTrades ─────────────────────────────────────────

def prices_to_ohlcv(prices: np.ndarray, volumes: np.ndarray,
                    stats: CoinStats, start_time_ms: int, ticks_per_min: int = 5) -> list[dict]:
    """
    Chuyển mảng giá GBM → list klines OHLCV (giống format klines thật).
    Mỗi nến 1 phút sinh từ ticks_per_min ticks trong nến đó.
    """
    rng = np.random.default_rng(77)
    candles = []

    for i, (close_price, volume) in enumerate(zip(prices, volumes)):
        candle_start_ms = start_time_ms + i * 60_000
        candle_end_ms   = candle_start_ms + 59_999

        # Sinh micro-ticks bên trong nến để có OHLC thực tế
        micro_returns = rng.normal(0, stats.sigma_per_min / math.sqrt(ticks_per_min), ticks_per_min)
        micro_prices  = [close_price * math.exp(sum(micro_returns[:j+1]) - sum(micro_returns)) for j in range(ticks_per_min)]
        micro_prices.append(close_price)

        open_p  = micro_prices[0]
        high_p  = max(micro_prices)
        low_p   = min(micro_prices)
        close_p = close_price

        trade_count = max(1, int(rng.normal(stats.trade_count_mean, stats.trade_count_std)))

        candles.append({
            "trade_id":       candle_start_ms,
            "symbol":         stats.symbol,
            "price":          round(close_p, 8),
            "quantity":       round(volume, 8),
            "trade_time":     candle_start_ms,
            "is_buyer_maker": bool(rng.random() < stats.buyer_maker_ratio),
            "ingestion_time": candle_start_ms + random.randint(50, 500),
            "is_injected":    False,
            "fault_type":     None,
            "open":           round(open_p, 8),
            "high":           round(high_p, 8),
            "low":            round(low_p, 8),
            "close":          round(close_p, 8),
            "volume":         round(volume, 8),
            "close_time":     candle_end_ms,
            "trade_count":    trade_count,
        })

    return candles


def candles_to_aggtrades(candles: list[dict], stats: CoinStats, ticks_per_candle: int = 8) -> list[MockTradeEvent]:
    """
    Từ mỗi nến OHLCV, sinh ra ticks_per_candle aggTrade events bên trong nến đó.
    Đây là dữ liệu sẽ được dùng như TradeEvent trong Kafka pipeline.
    """
    rng = np.random.default_rng(88)
    events = []

    for candle in candles:
        candle_start = candle["trade_time"]
        candle_vol   = candle["volume"]
        close_p      = candle["close"]
        open_p       = candle["open"]

        # Chia volume ngẫu nhiên giữa các ticks
        vol_splits = rng.dirichlet(np.ones(ticks_per_candle)) * candle_vol

        for j in range(ticks_per_candle):
            # Giá tick nội suy linear từ open → close với chút noise
            t_frac  = j / ticks_per_candle
            base_p  = open_p + (close_p - open_p) * t_frac
            noise   = rng.normal(0, stats.sigma_per_min * 0.3)
            tick_p  = max(base_p * math.exp(noise), 0.000001)

            tick_time = candle_start + int(t_frac * 59_000) + rng.integers(0, 500)
            trade_id  = int(tick_time * 1000 + j)

            events.append(MockTradeEvent(
                trade_id       = int(trade_id),
                symbol         = stats.symbol,
                price          = round(float(tick_p), 8),
                quantity       = round(float(vol_splits[j]), 8),
                trade_time     = int(tick_time),
                is_buyer_maker = bool(rng.random() < stats.buyer_maker_ratio),
                ingestion_time = int(tick_time) + int(rng.integers(10, 200)),
                is_injected    = False,
                fault_type     = None,
            ))

    return events


# ── Step 4: Fault Injection ────────────────────────────────────────────────────

def inject_faults(events: list[MockTradeEvent],
                  rates: dict[str, float],
                  late_min_s: int = 60,
                  late_max_s: int = 300) -> list[MockTradeEvent]:
    """
    Tiêm lỗi có kiểm soát vào danh sách events để phục vụ benchmark.

    Loại lỗi:
    - duplicate:      Thêm bản sao của event (cùng trade_id, is_injected=True)
    - late_data:      Lùi trade_time về 1-5 phút (giả lập event đến muộn)
    - out_of_order:   Hoán đổi thứ tự 2 event liền kề
    - schema_invalid: Đặt price < 0 (vi phạm validation rule)
    """
    rng = random.Random(42)
    result = []
    i = 0

    while i < len(events):
        evt = events[i]
        result.append(evt)

        # Duplicate injection
        if rng.random() < rates.get("duplicate", 0):
            dup = MockTradeEvent(**asdict(evt))
            dup.ingestion_time += rng.randint(1, 5000)
            dup.is_injected = True
            dup.fault_type  = "duplicate"
            result.append(dup)

        # Schema invalid injection
        elif rng.random() < rates.get("schema_invalid", 0):
            bad = MockTradeEvent(**asdict(evt))
            bad.price       = round(-abs(bad.price), 8)   # Giá âm = schema invalid
            bad.quantity    = 0.0
            bad.is_injected = True
            bad.fault_type  = "schema_invalid"
            result.append(bad)

        # Late data injection
        elif rng.random() < rates.get("late_data", 0):
            late = MockTradeEvent(**asdict(evt))
            delay_ms        = rng.randint(late_min_s * 1000, late_max_s * 1000)
            late.trade_time -= delay_ms   # Đặt trade_time về quá khứ (đến muộn)
            late.is_injected = True
            late.fault_type  = "late_data"
            result.append(late)

        # Out-of-order injection (hoán đổi với event tiếp theo)
        if rng.random() < rates.get("out_of_order", 0) and i + 1 < len(events):
            # Swap vị trí event hiện tại với event kế tiếp
            next_evt = events[i + 1]
            result.pop()              # Bỏ event vừa thêm
            ooo_evt = MockTradeEvent(**asdict(next_evt))
            ooo_evt.is_injected = True
            ooo_evt.fault_type  = "out_of_order"
            result.append(ooo_evt)
            result.append(evt)        # Thêm event hiện tại sau event kế
            i += 2                    # Bỏ qua event kế tiếp
            continue

        i += 1

    return result


# ── Step 5: Lưu File ──────────────────────────────────────────────────────────

def save_mock_klines_csv(symbol: str, candles: list[dict], output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{symbol.lower()}_mock_klines_1m.csv"
    fieldnames = [
        "trade_id", "symbol", "price", "quantity", "trade_time",
        "is_buyer_maker", "ingestion_time", "is_injected", "fault_type",
        "open", "high", "low", "close", "volume", "close_time", "trade_count"
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(candles)
    log.info(f"  ✓ Klines: {len(candles):,} nến → {filepath.name}")
    return str(filepath)


def save_mock_aggtrades_json(symbol: str, events: list[MockTradeEvent], output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{symbol.lower()}_mock_aggtrades.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump([asdict(e) for e in events], f, cls=NumpyEncoder, indent=2, ensure_ascii=False)

    total        = len(events)
    injected     = sum(1 for e in events if e.is_injected)
    by_type: dict = {}
    for e in events:
        if e.fault_type:
            by_type[e.fault_type] = by_type.get(e.fault_type, 0) + 1

    log.info(f"  ✓ AggTrades: {total:,} records ({injected} faults: {by_type})")
    return str(filepath), total, injected, by_type


def save_stats_json(stats: CoinStats, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{stats.symbol.lower()}_stats.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(asdict(stats), f, cls=NumpyEncoder, indent=2, ensure_ascii=False)
    return str(filepath)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate mock crypto data for Lambda Lakehouse benchmark")
    parser.add_argument("--days",          type=int,   default=7,     help="Số ngày dữ liệu cần sinh (default: 7)")
    parser.add_argument("--ticks-per-min", type=int,   default=8,     help="Số aggTrade ticks mỗi nến 1m (default: 8)")
    parser.add_argument("--fault-mode",    type=str,   default="full",choices=["full", "light", "none"],
                        help="Chế độ fault injection: full/light/none (default: full)")
    parser.add_argument("--start-date",    type=str,   default=None,  help="Ngày bắt đầu ISO 8601 (default: hôm nay - days)")
    args = parser.parse_args()

    # Fault rates theo mode
    if args.fault_mode == "full":
        fault_rates = DEFAULT_FAULT_RATES
    elif args.fault_mode == "light":
        fault_rates = {k: v / 2 for k, v in DEFAULT_FAULT_RATES.items()}
    else:
        fault_rates = {k: 0.0 for k in DEFAULT_FAULT_RATES}

    n_minutes   = args.days * 24 * 60   # Số nến 1m cần sinh
    run_id      = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Thời điểm bắt đầu
    if args.start_date:
        start_dt = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=args.days)
    start_ms = int(start_dt.timestamp() * 1000)

    log.info("=" * 60)
    log.info("Lambda Lakehouse — Mock Data Generator")
    log.info(f"Run ID        : {run_id}")
    log.info(f"Days          : {args.days} ({n_minutes:,} nến 1m/symbol)")
    log.info(f"Ticks/minute  : {args.ticks_per_min}")
    log.info(f"Fault mode    : {args.fault_mode} {fault_rates}")
    log.info(f"Start time    : {start_dt.isoformat()}")
    log.info(f"Klines source : {KLINES_DIR}")
    log.info("=" * 60)

    klines_files = sorted(KLINES_DIR.glob("*_klines_1m.csv"))
    if not klines_files:
        log.error(f"Không tìm thấy file klines trong {KLINES_DIR}")
        log.error("Hãy chạy trước: python scripts/fetch_binance_data.py --top-n 10 --klines-limit 1000")
        return

    summary = {
        "run_id":             run_id,
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "config": {
            "days":           args.days,
            "n_minutes":      n_minutes,
            "ticks_per_min":  args.ticks_per_min,
            "fault_mode":     args.fault_mode,
            "fault_rates":    fault_rates,
            "start_time":     start_dt.isoformat(),
        },
        "symbols":            [],
        "total_kline_records":    0,
        "total_aggtrade_records": 0,
        "total_fault_records":    0,
        "fault_breakdown":        {},
    }

    klines_out    = MOCK_DIR / "klines"
    aggtrades_out = MOCK_DIR / "aggtrades"
    stats_out     = MOCK_DIR / "stats"

    for idx, klines_file in enumerate(klines_files, 1):
        symbol = klines_file.stem.replace("_klines_1m", "").upper()
        log.info(f"\n[{idx}/{len(klines_files)}] Generating mock data cho {symbol}...")

        try:
            # Bước 1: Phân tích thống kê từ data thật
            stats = load_and_analyze_klines(klines_file)
            save_stats_json(stats, stats_out)
            log.info(f"  Stats: σ/min={stats.sigma_per_min:.6f}, μ/min={stats.mu_per_min:.6f}, "
                     f"last_price={stats.last_price:.4f}")

            # Bước 2: Sinh chuỗi giá GBM
            prices  = simulate_gbm_prices(stats, n_minutes, seed=idx * 100)
            volumes = simulate_volume(stats, n_minutes, seed=idx * 100 + 1)

            # Bước 3: Chuyển thành OHLCV candles
            candles = prices_to_ohlcv(prices, volumes, stats, start_ms, args.ticks_per_min)
            save_mock_klines_csv(symbol, candles, klines_out)
            summary["total_kline_records"] += len(candles)

            # Bước 4: Sinh aggTrade events từ candles
            events = candles_to_aggtrades(candles, stats, args.ticks_per_min)

            # Bước 5: Inject faults
            if args.fault_mode != "none":
                events = inject_faults(events, fault_rates)

            # Bước 6: Lưu aggTrades
            _, total, injected, by_type = save_mock_aggtrades_json(symbol, events, aggtrades_out)
            summary["total_aggtrade_records"] += total
            summary["total_fault_records"]    += injected
            for ft, cnt in by_type.items():
                summary["fault_breakdown"][ft] = summary["fault_breakdown"].get(ft, 0) + cnt

            summary["symbols"].append(symbol)

        except Exception as e:
            log.error(f"  ✗ Lỗi xử lý {symbol}: {e}")
            continue

    # Lưu summary
    summary_path = MOCK_DIR / f"summary_{run_id}.json"
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log.info("\n" + "=" * 60)
    log.info("✅ Mock data generation hoàn tất!")
    log.info(f"  Symbols     : {len(summary['symbols'])} coins")
    log.info(f"  Klines      : {summary['total_kline_records']:,} nến (1m)")
    log.info(f"  AggTrades   : {summary['total_aggtrade_records']:,} records")
    log.info(f"  Faults      : {summary['total_fault_records']:,} injected records")
    log.info(f"  Breakdown   : {summary['fault_breakdown']}")
    log.info(f"  Output dir  : {MOCK_DIR.resolve()}")
    log.info(f"  Summary     : {summary_path}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
