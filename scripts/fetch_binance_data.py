"""
fetch_binance_data.py
=====================
Script thu thập dữ liệu thực từ Binance Public API.
Không cần API Key, không cần Docker/Kafka.

Output:
  datasets/raw/klines/         → OHLCV 1m klines (CSV) cho từng coin
  datasets/raw/tickers/        → 24hr ticker snapshot (JSON)
  datasets/raw/aggtrades/      → Recent aggTrades (JSON) cho từng coin
  datasets/raw/summary.json    → Metadata của lần fetch

Usage:
  python scripts/fetch_binance_data.py
  python scripts/fetch_binance_data.py --top-n 5 --klines-limit 500
"""

import json
import csv
import os
import time
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("fetch_binance")

# ── Config ─────────────────────────────────────────────────────────────────────
BINANCE_REST_URL = "https://api.binance.com"
OUTPUT_DIR       = Path(__file__).parent.parent / "datasets" / "raw"
SLEEP_BETWEEN    = 0.4   # giây giữa các request để tránh rate limit
WEIGHT_LIMIT     = 1100  # budget an toàn (Binance giới hạn 1200/phút)

# Loại trừ leveraged tokens và stablecoins (giá cố định ~1.0, không có biến động)
EXCLUDED_TOKENS  = ["UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT"]
STABLECOIN_KEYWORDS = ["USDC", "BUSD", "TUSD", "USDD", "FDUSD", "DAI",
                        "USD1", "RLUS", "PYUSD", "EURC", "USDP", "GUSD"]


# ── Helper Functions ────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "lambda-lakehouse-fetcher/1.0"})
    return session


def fetch_top_symbols(session: requests.Session, top_n: int = 10) -> list[str]:
    """Lấy Top-N cặp USDT có khối lượng giao dịch 24h lớn nhất."""
    log.info(f"Đang lấy danh sách Top-{top_n} coins từ Binance 24hr Ticker...")
    resp = session.get(f"{BINANCE_REST_URL}/api/v3/ticker/24hr", timeout=15)
    resp.raise_for_status()
    tickers = resp.json()

    usdt_pairs = [
        t for t in tickers
        if t["symbol"].endswith("USDT")
        and not any(excl in t["symbol"] for excl in EXCLUDED_TOKENS)
        and not any(stable in t["symbol"] for stable in STABLECOIN_KEYWORDS)
    ]
    usdt_pairs.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
    symbols = [t["symbol"] for t in usdt_pairs[:top_n]]
    log.info(f"Top {top_n} symbols (đã lọc stablecoin): {symbols}")
    return symbols, usdt_pairs[:top_n]


def fetch_klines(session: requests.Session, symbol: str, interval: str = "1m", limit: int = 1000) -> list:
    """Lấy lịch sử nến OHLCV cho 1 symbol."""
    url = f"{BINANCE_REST_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = session.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_recent_aggtrades(session: requests.Session, symbol: str, limit: int = 500) -> list:
    """Lấy recent aggregate trades cho 1 symbol."""
    url = f"{BINANCE_REST_URL}/api/v3/aggTrades"
    params = {"symbol": symbol, "limit": limit}
    resp = session.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def kline_to_trade_event(symbol: str, kline: list) -> dict:
    """
    Chuyển đổi kline row → TradeEvent schema (khớp với src/ingestion/models.py).
    Dùng close_price làm price, volume làm quantity.
    """
    open_time_ms = int(kline[0])
    return {
        "trade_id":       open_time_ms,                            # Synthetic ID từ open_time
        "symbol":         symbol.upper(),
        "price":          float(kline[4]),                         # close_price
        "quantity":       float(kline[5]),                         # volume
        "trade_time":     open_time_ms,
        "is_buyer_maker": False,
        "ingestion_time": int(time.time() * 1000),
        "is_injected":    False,
        "fault_type":     None,
        # Extra OHLCV fields (giữ để phân tích)
        "open":           float(kline[1]),
        "high":           float(kline[2]),
        "low":            float(kline[3]),
        "close":          float(kline[4]),
        "volume":         float(kline[5]),
        "close_time":     int(kline[6]),
        "trade_count":    int(kline[8]),
    }


def aggtrade_to_trade_event(raw: dict, symbol: str) -> dict:
    """
    Chuyển đổi Binance aggTrade payload → TradeEvent schema.
    Ánh xạ: a→trade_id, p→price, q→quantity, T→trade_time, m→is_buyer_maker
    NOTE: Binance /api/v3/aggTrades không trả field 's' trong response
          vì symbol đã được truyền qua query param → phải pass symbol thủ công.
    """
    return {
        "trade_id":       int(raw["a"]),
        "symbol":         symbol.upper(),
        "price":          float(raw["p"]),
        "quantity":       float(raw["q"]),
        "trade_time":     int(raw["T"]),
        "is_buyer_maker": bool(raw["m"]),
        "ingestion_time": int(time.time() * 1000),
        "is_injected":    False,
        "fault_type":     None,
    }


def save_klines_csv(symbol: str, klines: list, output_dir: Path):
    """Lưu klines data ra file CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{symbol.lower()}_klines_1m.csv"

    fieldnames = [
        "trade_id", "symbol", "price", "quantity", "trade_time",
        "is_buyer_maker", "ingestion_time", "is_injected", "fault_type",
        "open", "high", "low", "close", "volume", "close_time", "trade_count"
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for kline in klines:
            row = kline_to_trade_event(symbol, kline)
            writer.writerow(row)

    log.info(f"  ✓ Saved {len(klines)} klines → {filepath.name}")
    return str(filepath)


def save_aggtrades_json(symbol: str, aggtrades: list, output_dir: Path):
    """Lưu aggTrades data ra file JSON (TradeEvent format)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{symbol.lower()}_aggtrades.json"

    # Truyền symbol vào hàm vì Binance không trả 's' field trong aggTrades response
    events = [aggtrade_to_trade_event(t, symbol) for t in aggtrades]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)

    log.info(f"  ✓ Saved {len(events)} aggTrades → {filepath.name}")
    return str(filepath)


def save_tickers_json(tickers: list, output_dir: Path):
    """Lưu 24hr ticker snapshot."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "ticker_24hr_snapshot.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(tickers, f, indent=2, ensure_ascii=False)
    log.info(f"✓ Saved 24hr ticker snapshot → {filepath.name}")
    return str(filepath)


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch Binance data for Lambda Lakehouse project")
    parser.add_argument("--top-n",        type=int, default=10,   help="Số lượng top coins (default: 10)")
    parser.add_argument("--klines-limit", type=int, default=1000, help="Số nến klines mỗi symbol (default: 1000)")
    parser.add_argument("--aggtrades-limit", type=int, default=500, help="Số aggTrades mỗi symbol (default: 500)")
    parser.add_argument("--skip-aggtrades", action="store_true",  help="Bỏ qua bước fetch aggTrades")
    args = parser.parse_args()

    run_id    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    klines_dir    = OUTPUT_DIR / "klines"
    aggtrades_dir = OUTPUT_DIR / "aggtrades"
    tickers_dir   = OUTPUT_DIR / "tickers"

    log.info("=" * 60)
    log.info("Lambda Lakehouse — Binance Data Fetcher")
    log.info(f"Run ID      : {run_id}")
    log.info(f"Top-N       : {args.top_n}")
    log.info(f"Klines limit: {args.klines_limit} nến/symbol (~{args.klines_limit} phút lịch sử)")
    log.info(f"Output dir  : {OUTPUT_DIR.resolve()}")
    log.info("=" * 60)

    session = get_session()
    summary = {
        "run_id":    run_id,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "symbols":   [],
        "files":     [],
        "total_kline_records":    0,
        "total_aggtrade_records": 0,
    }

    # ── Bước 1: Lấy Top-N symbols & ticker snapshot ───────────────────────────
    symbols, ticker_data = fetch_top_symbols(session, args.top_n)
    summary["symbols"] = symbols
    ticker_file = save_tickers_json(ticker_data, tickers_dir)
    summary["files"].append(ticker_file)
    time.sleep(SLEEP_BETWEEN)

    # ── Bước 2: Fetch klines & aggTrades cho từng symbol ─────────────────────
    total_weight = 40  # weight của lệnh ticker/24hr vừa gọi

    for i, symbol in enumerate(symbols, 1):
        log.info(f"\n[{i}/{len(symbols)}] Fetching data cho {symbol}...")

        # 2a. Klines (OHLCV)
        try:
            klines = fetch_klines(session, symbol, limit=args.klines_limit)
            kline_file = save_klines_csv(symbol, klines, klines_dir)
            summary["files"].append(kline_file)
            summary["total_kline_records"] += len(klines)
            total_weight += 2
        except requests.HTTPError as e:
            log.error(f"  ✗ Klines lỗi {symbol}: {e}")

        time.sleep(SLEEP_BETWEEN)

        # 2b. AggTrades (recent)
        if not args.skip_aggtrades:
            try:
                aggtrades = fetch_recent_aggtrades(session, symbol, limit=args.aggtrades_limit)
                at_file = save_aggtrades_json(symbol, aggtrades, aggtrades_dir)
                summary["files"].append(at_file)
                summary["total_aggtrade_records"] += len(aggtrades)
                total_weight += 2
            except requests.HTTPError as e:
                log.error(f"  ✗ AggTrades lỗi {symbol}: {e}")

            time.sleep(SLEEP_BETWEEN)

        # Rate limit guard
        if total_weight >= WEIGHT_LIMIT:
            log.warning(f"Gần đạt rate limit ({total_weight} weight). Đợi 60s...")
            time.sleep(60)
            total_weight = 0

    # ── Bước 3: Lưu summary ───────────────────────────────────────────────────
    summary_path = OUTPUT_DIR / f"summary_{run_id}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log.info("\n" + "=" * 60)
    log.info("✅ Fetch hoàn tất!")
    log.info(f"  Symbols    : {len(symbols)} coins")
    log.info(f"  Klines     : {summary['total_kline_records']:,} records")
    log.info(f"  AggTrades  : {summary['total_aggtrade_records']:,} records")
    log.info(f"  Summary    : {summary_path}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
