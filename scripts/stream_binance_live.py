"""
stream_binance_live.py
======================
Stream liên tục dữ liệu giao dịch THẬT từ sàn Binance vào Kafka (vô hạn).

Hoạt động:
  - Kết nối Binance WebSocket API (wss://stream.binance.com:9443)
  - Tự động lấy Top N coin thanh khoản lớn nhất từ Binance REST API
  - Mỗi giao dịch thật được chuẩn hóa → đẩy vào Kafka topic crypto_trades_raw
  - Spark Streaming (Docker) xử lý mỗi 5 giây → ghi vào ClickHouse speed_agg
  - Tự động kết nối lại (auto-reconnect) khi mất kết nối
  - Nhấn Ctrl+C để dừng sạch

Sử dụng:
  python scripts/stream_binance_live.py
  python scripts/stream_binance_live.py --top 5
  python scripts/stream_binance_live.py --top 10 --bootstrap localhost:9094
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ── Color codes ───────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
DIM    = "\033[2m"

# ── Globals ───────────────────────────────────────────────────────────────────
_running = True
_start_time: float = 0.0


def _signal_handler(sig, frame):
    """Xu ly Ctrl+C: dung vong lap sach."""
    global _running
    print(f"\n\n{YELLOW}{BOLD}Nhan Ctrl+C — dang dung stream sach...{RESET}")
    _running = False


def _print_header(top_n: int, broker: str) -> None:
    print(f"\n{BOLD}{'=' * 72}{RESET}")
    print(f"{BOLD}{'  BINANCE LIVE STREAM — DATA LAKEHOUSE LAMBDA ARCHITECTURE':^72}{RESET}")
    print(f"{BOLD}{'=' * 72}{RESET}")
    print(f"  Nguon du lieu  : {GREEN}Binance WebSocket THAT (wss://stream.binance.com){RESET}")
    print(f"  Top N coin     : {BOLD}{top_n}{RESET} coin co thanh khoan lon nhat 24h")
    print(f"  Kafka Broker   : {BLUE}{broker}{RESET}")
    print(f"  Bat dau luc    : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Nhan {YELLOW}Ctrl+C{RESET} de dung sach.")
    print(f"{BOLD}{'=' * 72}{RESET}\n")


def _print_stats(ws_client) -> None:
    """In thong ke tien trinh ra man hinh."""
    elapsed = time.time() - _start_time
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    rate = ws_client.total_events_published / elapsed if elapsed > 0 else 0
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")

    print(
        f"\r  {DIM}[{now_str} UTC]{RESET}  "
        f"Nhan tu Binance: {GREEN}{BOLD}{ws_client.total_messages_received:,}{RESET} ticks  |  "
        f"Vao Kafka: {CYAN}{BOLD}{ws_client.total_events_published:,}{RESET} events  |  "
        f"Toc do: {YELLOW}{rate:.1f} ev/s{RESET}  |  "
        f"Thoi gian: {BOLD}{h:02d}:{m:02d}:{s:02d}{RESET}    ",
        end="",
        flush=True,
    )


def stream_binance_live(top_n: int, bootstrap_servers: str) -> None:
    """Vong lap stream lien tuc du lieu that tu Binance cho den khi Ctrl+C."""
    global _running, _start_time

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        from src.ingestion.binance_ws import BinanceWebSocketProducer
        from src.ingestion.kafka_producer import ResilientKafkaProducer
    except ImportError as exc:
        print(f"{RED}[FAIL] Khong import duoc module ingestion: {exc}{RESET}")
        sys.exit(1)

    _print_header(top_n, bootstrap_servers)

    # ── Ket noi Kafka Producer ────────────────────────────────────────────────
    print(f"  {DIM}[INIT]{RESET} Dang ket noi Kafka Producer...")
    try:
        producer = ResilientKafkaProducer(bootstrap_servers=bootstrap_servers)
    except Exception as exc:
        print(f"{RED}[FAIL] Khong ket noi duoc Kafka ({exc}){RESET}")
        sys.exit(1)
    print(f"  {GREEN}[PASS]{RESET} Kafka Producer san sang!\n")

    # ── Khoi tao Binance WebSocket Client ────────────────────────────────────
    ws_client = BinanceWebSocketProducer(kafka_producer=producer, top_n=top_n)

    # In thong ke sau moi 50 ticks
    _original_on_message = ws_client.on_message

    def _patched_on_message(ws, message: str):
        _original_on_message(ws, message)
        if ws_client.total_messages_received % 50 == 0:
            _print_stats(ws_client)

    ws_client.on_message = _patched_on_message

    # ── Vong lap chinh voi auto-reconnect ─────────────────────────────────────
    _start_time = time.time()
    retry_count = 0

    while _running:
        try:
            print(f"\n  {CYAN}[CONNECT]{RESET} Dang ket noi Binance WebSocket (lan #{retry_count + 1})...")
            ws_client.start_streaming()
            break

        except KeyboardInterrupt:
            _running = False
            break

        except Exception as exc:
            if not _running:
                break
            retry_count += 1
            wait_sec = min(2 ** retry_count, 60)
            print(f"\n  {RED}[RETRY]{RESET} Mat ket noi ({exc}). Thu lai sau {wait_sec}s...")
            for _ in range(wait_sec):
                if not _running:
                    break
                time.sleep(1)

    # ── Cleanup ───────────────────────────────────────────────────────────────
    elapsed = time.time() - _start_time
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)

    print(f"\n\n{BOLD}{'=' * 72}{RESET}")
    print(f"{BOLD}{'  TONG KET PHIEN BINANCE LIVE STREAM':^72}{RESET}")
    print(f"{BOLD}{'=' * 72}{RESET}")
    print(f"  Tong so tick nhan tu Binance : {GREEN}{BOLD}{ws_client.total_messages_received:,}{RESET}")
    print(f"  Tong so event vao Kafka      : {CYAN}{BOLD}{ws_client.total_events_published:,}{RESET}")
    print(f"  Cac cap coin da stream       : {', '.join(ws_client.symbols)}")
    print(f"  Thoi gian chay               : {BOLD}{h:02d}:{m:02d}:{s:02d}{RESET}")
    if elapsed > 0:
        print(f"  Toc do trung binh            : {YELLOW}{ws_client.total_events_published / elapsed:.2f} events/s{RESET}")
    print(f"{BOLD}{'=' * 72}{RESET}")

    try:
        producer.flush()
        producer.close()
    except Exception:
        pass
    print(f"\n{GREEN}Stream dung sach. Du lieu da ghi vao Kafka -> ClickHouse.{RESET}\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream du lieu giao dich THAT tu Binance vao Kafka lien tuc."
    )
    parser.add_argument(
        "--top", type=int, default=10,
        help="So luong coin thanh khoan lon nhat can stream (mac dinh: 10)"
    )
    parser.add_argument(
        "--bootstrap", type=str, default="localhost:9094",
        help="Kafka bootstrap servers (mac dinh: localhost:9094)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    stream_binance_live(top_n=args.top, bootstrap_servers=args.bootstrap)
