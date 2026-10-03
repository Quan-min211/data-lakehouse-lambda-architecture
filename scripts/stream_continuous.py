"""
stream_continuous.py
====================
Stream liên tục dữ liệu giao dịch crypto vào Kafka (vòng lặp vô hạn).

Hoạt động:
  - Mỗi vòng lặp: sinh N trades (random walk giá), gửi vào topic Kafka.
  - Spark Streaming (Docker) tự xử lý mỗi 5 giây → ghi vào ClickHouse speed_agg.
  - In progress bar + thống kê tốc độ real-time.
  - Nhấn Ctrl+C để dừng sạch.

Sử dụng:
  python scripts/stream_continuous.py
  python scripts/stream_continuous.py --batch 500 --symbols BTCUSDT ETHUSDT SOLUSDT
  python scripts/stream_continuous.py --batch 200 --delay 2.0
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── path setup ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ── Color codes ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
DIM    = "\033[2m"

# ── Constants ────────────────────────────────────────────────────────────────
KAFKA_BROKERS = ["localhost:9094", "localhost:9092"]
KAFKA_TOPIC   = "crypto_trades_raw"

BASE_PRICES: dict[str, float] = {
    "BTCUSDT":  95_000.0,
    "ETHUSDT":   3_500.0,
    "SOLUSDT":     180.0,
    "BNBUSDT":     620.0,
    "XRPUSDT":       0.65,
    "DOGEUSDT":      0.18,
    "ADAUSDT":       0.55,
    "DOTUSDT":       9.50,
    "LINKUSDT":     15.20,
    "AVAXUSDT":     35.00,
}

# ── Global state ─────────────────────────────────────────────────────────────
_running = True
_prices: dict[str, float] = {}


def _signal_handler(sig, frame):
    """Xử lý Ctrl+C: dừng vòng lặp sạch."""
    global _running
    print(f"\n\n{YELLOW}{BOLD}⏹  Đã nhận Ctrl+C — đang dừng stream sạch...{RESET}")
    _running = False


# ── Kafka Producer ────────────────────────────────────────────────────────────
def _connect_kafka(brokers: list[str]):
    """Kết nối Kafka Producer. Raise exception nếu không kết nối được."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        raise RuntimeError("Thiếu thư viện: pip install kafka-python")

    last_err = None
    for broker in brokers:
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                retries=5,
                linger_ms=50,          # Gộp messages để tăng throughput
                batch_size=65536,       # 64KB batch
                request_timeout_ms=8000,
            )
            return producer, broker
        except Exception as exc:
            last_err = exc
            continue
    raise RuntimeError(f"Không kết nối được Kafka {brokers}: {last_err}")


# ── Trade generator ───────────────────────────────────────────────────────────
def _generate_batch(symbols: list[str], count: int) -> list[dict]:
    """Sinh `count` TradeEvents với giá random walk, rải đều trong 45 giây gần nhất."""
    global _prices
    now_ms   = int(time.time() * 1000)
    span_ms  = min(45_000, max(1, count * 100))
    start_ms = now_ms - span_ms
    step_ms  = max(1, span_ms // count)

    trades = []
    for i in range(count):
        sym = symbols[i % len(symbols)]

        # Random walk ±0.15%
        _prices[sym] *= (1.0 + random.gauss(0.0, 0.0015))
        price = round(max(_prices[sym], 0.0001), 6)

        # Lognormal qty: trade nhỏ phổ biến, trade lớn ít
        qty = round(random.lognormvariate(-1.0, 0.8), 6)
        qty = max(qty, 0.0001)

        ts = start_ms + i * step_ms

        trades.append({
            "trade_id":       random.randint(100_000_000, 999_999_999),
            "symbol":         sym,
            "price":          price,
            "quantity":       qty,
            "trade_time":     ts,
            "is_buyer_maker": random.random() > 0.5,
            "ingestion_time": now_ms,
            "is_injected":    False,
            "fault_type":     None,
        })

    return trades


# ── Progress bar ──────────────────────────────────────────────────────────────
def _print_status(
    loop: int,
    sent_total: int,
    batch_size: int,
    elapsed_loop: float,
    broker: str,
) -> None:
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    rate = batch_size / elapsed_loop if elapsed_loop > 0 else 0
    bar_len = 25
    bar_fill = "█" * (loop % (bar_len + 1))
    bar_empty = "░" * (bar_len - len(bar_fill))
    bar = f"{CYAN}{bar_fill}{DIM}{bar_empty}{RESET}"

    print(
        f"\r  {DIM}[{now}]{RESET} "
        f"Loop {BOLD}#{loop:,}{RESET}  │  "
        f"{bar}  │  "
        f"Batch {GREEN}{batch_size:,}{RESET}  │  "
        f"Total {BOLD}{sent_total:,}{RESET}  │  "
        f"{YELLOW}{rate:,.0f} msg/s{RESET}  │  "
        f"{DIM}Broker: {broker}{RESET}    ",
        end="",
        flush=True,
    )


# ── Main loop ─────────────────────────────────────────────────────────────────
def stream_continuous(
    symbols: list[str],
    batch_size: int,
    delay: float,
) -> None:
    """Vòng lặp stream liên tục cho đến khi Ctrl+C."""
    global _running, _prices

    # Khởi tạo giá ban đầu
    _prices = {s: BASE_PRICES.get(s, 100.0) for s in symbols}

    print(f"\n{BOLD}{'=' * 72}{RESET}")
    print(f"{BOLD}{'  ⚡ CONTINUOUS STREAM — DATA LAKEHOUSE LAMBDA ARCHITECTURE':^72}{RESET}")
    print(f"{BOLD}{'=' * 72}{RESET}")
    print(f"  Symbols     : {', '.join(symbols)}")
    print(f"  Batch size  : {batch_size:,} trades / vòng lặp")
    print(f"  Delay       : {delay:.1f}s giữa các batch")
    print(f"  Topic       : {KAFKA_TOPIC}")
    print(f"  Dừng stream : {BOLD}Ctrl+C{RESET}")
    print(f"{BOLD}{'=' * 72}{RESET}\n")

    # Kết nối Kafka
    print(f"  {CYAN}Đang kết nối Kafka...{RESET}", end="", flush=True)
    try:
        producer, broker = _connect_kafka(KAFKA_BROKERS)
        print(f"\r  {GREEN}[✓]{RESET} Kafka sẵn sàng tại {BOLD}{broker}{RESET}              ")
    except RuntimeError as exc:
        print(f"\r  {RED}[✗] Lỗi: {exc}{RESET}")
        print(f"  {YELLOW}Kiểm tra: docker compose ps kafka{RESET}")
        sys.exit(1)

    print(f"  {GREEN}[✓]{RESET} Bắt đầu stream... (Spark xử lý mỗi 5s → ClickHouse speed_agg)\n")

    loop = 0
    sent_total = 0
    start_time = time.time()

    while _running:
        loop += 1
        t_loop = time.time()

        # Sinh trades và gửi vào Kafka
        trades = _generate_batch(symbols, batch_size)

        try:
            for trade in trades:
                producer.send(KAFKA_TOPIC, value=trade)
            producer.flush()
        except Exception as exc:
            print(f"\n  {RED}[!] Lỗi Kafka: {exc} — thử kết nối lại...{RESET}")
            try:
                producer.close()
                producer, broker = _connect_kafka(KAFKA_BROKERS)
                print(f"  {GREEN}[✓]{RESET} Đã kết nối lại: {broker}")
            except RuntimeError as reconn_err:
                print(f"  {RED}[✗] Không kết nối lại được: {reconn_err}{RESET}")
                time.sleep(5)
                continue

        sent_total += batch_size
        elapsed_loop = time.time() - t_loop

        _print_status(loop, sent_total, batch_size, elapsed_loop, broker)

        # Delay giữa các batch (kiểm tra _running mỗi 0.5s để Ctrl+C phản hồi nhanh)
        remaining = delay
        while remaining > 0 and _running:
            sleep_chunk = min(0.5, remaining)
            time.sleep(sleep_chunk)
            remaining -= sleep_chunk

    # ── Cleanup ────────────────────────────────────────────────────────────────
    try:
        producer.flush()
        producer.close()
    except Exception:
        pass

    total_elapsed = time.time() - start_time
    avg_rate = sent_total / total_elapsed if total_elapsed > 0 else 0

    print(f"\n\n{BOLD}{'─' * 72}{RESET}")
    print(f"  {GREEN}Stream kết thúc sau {total_elapsed:.0f}s{RESET}")
    print(f"  Tổng số trades đã gửi  : {BOLD}{sent_total:,}{RESET}")
    print(f"  Tổng vòng lặp          : {BOLD}{loop:,}{RESET}")
    print(f"  Tốc độ trung bình      : {BOLD}{avg_rate:,.0f} msg/s{RESET}")
    print(f"{BOLD}{'─' * 72}{RESET}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    signal.signal(signal.SIGINT, _signal_handler)

    parser = argparse.ArgumentParser(
        description="Stream liên tục dữ liệu crypto vào Kafka (Ctrl+C để dừng)"
    )
    parser.add_argument(
        "--symbols", nargs="+",
        default=["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"],
        help="Danh sách cặp coin. Mặc định: BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT",
    )
    parser.add_argument(
        "--batch", type=int, default=300,
        help="Số trades mỗi vòng lặp. Mặc định: 300",
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Giây nghỉ giữa các batch (0 = full speed). Mặc định: 1.0",
    )
    args = parser.parse_args()

    stream_continuous(
        symbols=args.symbols,
        batch_size=args.batch,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()
