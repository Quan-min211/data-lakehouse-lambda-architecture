"""
auto_pipeline_daemon.py
=======================
Trình điều phối tự động toàn diện (Autonomous End-to-End Lambda Pipeline Daemon).

Giải quyết bài toán vận hành tự động 100% (Zero-Touch Automation) cho kiến trúc Lambda:
  1. [INGESTION STREAM]: Tự động chạy ngầm luồng thu thập dữ liệu (Binance WebSocket thật
     hoặc luồng giả lập GBM + FaultInjector) đẩy liên tục vào Kafka 'crypto_trades_raw'.
  2. [SPEED LAYER]: Đảm bảo container 'speed-layer' (Spark Structured Streaming) đang chạy
     để xử lý micro-batch mỗi 5 giây -> ClickHouse 'speed_agg' + Redis Hot Cache.
  3. [AUTO-BATCH & RECONCILIATION]: Định kỳ mỗi `--batch-interval` giây (mặc định 30s),
     tự động tiêu thụ incremental các message mới từ Kafka -> lọc Silver DQ Gate &
     Quarantine -> tính nến Gold -> ghi Iceberg Bronze (tùy chọn) -> đồng bộ ClickHouse
     'batch_agg' -> nâng mốc 'system_watermark' -> tự động đối soát với 'speed_agg' và
     ghi kết quả vào 'lakehouse.reconciliation_audit_metrics'.
  4. [AUTO-MAINTENANCE]: Định kỳ mỗi `--optimize-interval` giây (mặc định 300s), tự động
     chạy `OPTIMIZE TABLE ... FINAL` trên ClickHouse để hợp nhất các part vật lý.

Cách sử dụng:
  # 1. Chạy tự động hoàn toàn với dữ liệu giả lập liên tục + tự động tiêm lỗi & đối soát:
  python scripts/auto_pipeline_daemon.py --mode simulated --batch-interval 20

  # 2. Chạy tự động hoàn toàn với dữ liệu thật từ Binance WebSocket:
  python scripts/auto_pipeline_daemon.py --mode live --top-n 5 --batch-interval 30

  # 3. Chạy thử N chu kỳ rồi tự dừng (dùng cho demo hoặc kiểm thử tự động):
  python scripts/auto_pipeline_daemon.py --mode simulated --batch-interval 10 --max-cycles 3
"""

from __future__ import annotations

import argparse
import random
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.batch_layer.spark_batch_jobs import run_batch_pipeline
from src.ingestion.fault_injector import FaultInjector
from src.ingestion.kafka_producer import ResilientKafkaProducer
from src.ingestion.models import TradeEvent
from src.utils.config import get_clickhouse_config, get_kafka_config
from src.utils.logger import setup_logger

logger = setup_logger("auto_pipeline_daemon")

_STOP_EVENT = threading.Event()


def _handle_signal(signum, frame) -> None:
    logger.info("Nhận tín hiệu dừng (signal=%s). Đang dừng Auto-Pipeline Daemon an toàn...", signum)
    _STOP_EVENT.set()


def ensure_docker_services(services: Optional[List[str]] = None) -> bool:
    """Đảm bảo các dịch vụ Docker cốt lõi đang chạy."""
    target_services = services or [
        "kafka",
        "clickhouse",
        "redis",
        "minio",
        "iceberg-rest",
        "speed-layer",
        "fastapi",
        "streamlit",
    ]
    cmd = ["docker", "compose", "up", "-d", *target_services]
    logger.info("Kiểm tra & khởi động Docker services: %s", ", ".join(target_services))
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT_DIR), capture_output=True, text=True, timeout=180)
        if proc.returncode != 0:
            logger.warning("docker compose up -d trả về mã %d: %s", proc.returncode, proc.stderr.strip())
            return False
        return True
    except Exception as exc:
        logger.warning("Không thể gọi docker compose: %s", exc)
        return False


class BackgroundIngestionWorker:
    """Luồng chạy nền đẩy dữ liệu liên tục vào Kafka cho cả Speed Layer và Batch Layer."""

    BASE_PRICES: Dict[str, float] = {
        "BTCUSDT": 85000.0,
        "ETHUSDT": 3200.0,
        "SOLUSDT": 180.0,
        "BNBUSDT": 610.0,
        "XRPUSDT": 0.65,
    }

    def __init__(
        self,
        mode: str = "simulated",
        bootstrap_servers: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        top_n: int = 5,
        events_per_sec: int = 25,
        inject_faults: bool = True,
    ):
        self.mode = mode
        self.bootstrap_servers = bootstrap_servers or get_kafka_config().bootstrap_servers
        self.symbols = [s.upper() for s in (symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"])]
        self.top_n = top_n
        self.events_per_sec = max(1, events_per_sec)
        self.inject_faults = inject_faults
        self.total_published = 0
        self._thread: Optional[threading.Thread] = None
        self._ws_client: Optional[Any] = None

    def start(self) -> None:
        if self.mode == "external":
            logger.info("Chế độ 'external': Bỏ qua producer nội bộ, chờ dữ liệu từ nguồn Kafka bên ngoài.")
            return
        self._thread = threading.Thread(target=self._run, name="auto-ingestion-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._ws_client and getattr(self._ws_client, "ws", None):
            try:
                self._ws_client.ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

    def _run(self) -> None:
        if self.mode == "live":
            self._run_live_binance()
        else:
            self._run_simulated_stream()

    def _run_live_binance(self) -> None:
        from src.ingestion.binance_ws import BinanceWebSocketProducer

        try:
            producer = ResilientKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            fault_injector = (
                FaultInjector(
                    duplicate_rate=0.02,
                    late_data_rate=0.02,
                    schema_invalid_rate=0.01,
                )
                if self.inject_faults
                else FaultInjector(
                    duplicate_rate=0.0,
                    late_data_rate=0.0,
                    out_of_order_rate=0.0,
                    schema_invalid_rate=0.0,
                )
            )
            self._ws_client = BinanceWebSocketProducer(
                fault_injector=fault_injector,
                kafka_producer=producer,
                top_n=self.top_n,
                enable_auto_backfill=True,
            )
            logger.info("Bắt đầu luồng Binance Live WebSocket (top_n=%d)...", self.top_n)
            while not _STOP_EVENT.is_set():
                try:
                    self._ws_client.start_streaming()
                    break
                except Exception as exc:
                    if _STOP_EVENT.is_set():
                        break
                    logger.warning("Binance WS ngắt kết nối (%s), thử lại sau 3s...", exc)
                    time.sleep(3)
        except Exception as exc:
            logger.error("Không khởi tạo được Binance Live Producer (%s). Chuyển sang chế độ simulated!", exc)
            self._run_simulated_stream()

    def _run_simulated_stream(self) -> None:
        try:
            producer = ResilientKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        except Exception as exc:
            logger.error("Không kết nối được Kafka tại %s: %s", self.bootstrap_servers, exc)
            return

        injector = (
            FaultInjector(
                duplicate_rate=0.05,
                late_data_rate=0.05,
                out_of_order_rate=0.02,
                schema_invalid_rate=0.02,
            )
            if self.inject_faults
            else FaultInjector(
                duplicate_rate=0.0,
                late_data_rate=0.0,
                out_of_order_rate=0.0,
                schema_invalid_rate=0.0,
            )
        )
        prices = {sym: self.BASE_PRICES.get(sym, 100.0) for sym in self.symbols}
        seq_ids = {sym: int(time.time() * 1000) % 10_000_000 for sym in self.symbols}

        logger.info(
            "Bắt đầu luồng Simulated Stream (%s symbols, ~%d events/s, inject_faults=%s)...",
            self.symbols,
            self.events_per_sec,
            self.inject_faults,
        )
        while not _STOP_EVENT.is_set():
            t0 = time.perf_counter()
            now_ms = int(time.time() * 1000)
            for _ in range(self.events_per_sec):
                if _STOP_EVENT.is_set():
                    break
                sym = random.choice(self.symbols)
                # Bước nhảy giá GBM nhẹ
                drift = random.gauss(0.0, 0.0008)
                prices[sym] = max(0.0001, round(prices[sym] * (1.0 + drift), 4))
                seq_ids[sym] += 1

                ev = TradeEvent(
                    trade_id=seq_ids[sym],
                    symbol=sym,
                    price=prices[sym],
                    quantity=round(random.uniform(0.05, 2.5), 4),
                    trade_time=now_ms,
                    is_buyer_maker=random.random() < 0.5,
                    ingestion_time=now_ms,
                )
                for out_ev in injector.process_event(ev):
                    producer.send_trade(out_ev)
                    self.total_published += 1

            producer.flush()
            elapsed = time.perf_counter() - t0
            sleep_for = max(0.05, 1.0 - elapsed)
            _STOP_EVENT.wait(sleep_for)

        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


def optimize_clickhouse_tables() -> bool:
    """Chạy OPTIMIZE TABLE ... FINAL trên ClickHouse để dọn dẹp các part trùng lặp."""
    try:
        import clickhouse_connect

        cfg = get_clickhouse_config()
        client = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.user,
            password=cfg.password,
            database=cfg.database,
        )
        client.command("OPTIMIZE TABLE lakehouse.batch_agg FINAL")
        client.command("OPTIMIZE TABLE lakehouse.speed_agg FINAL")
        client.command("OPTIMIZE TABLE lakehouse.reconciliation_audit_metrics FINAL")
        logger.info("Đã tối ưu hóa (OPTIMIZE FINAL) các bảng ClickHouse thành công.")
        return True
    except Exception as exc:
        logger.warning("Bỏ qua ClickHouse OPTIMIZE FINAL: %s", exc)
        return False


def run_auto_daemon(args: argparse.Namespace) -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if args.ensure_docker:
        ensure_docker_services()

    worker = BackgroundIngestionWorker(
        mode=args.mode,
        bootstrap_servers=args.bootstrap,
        symbols=args.symbols,
        top_n=args.top_n,
        events_per_sec=args.rate,
        inject_faults=not args.no_faults,
    )
    worker.start()

    print("\n" + "=" * 88)
    print("  AUTONOMOUS LAMBDA LAKEHOUSE PIPELINE DAEMON (ZERO-TOUCH AUTO MODE)")
    print("=" * 88)
    print(f"  • Ingestion Mode     : {args.mode.upper()} (Fault Injection: {'ON' if not args.no_faults else 'OFF'})")
    print(f"  • Batch Interval     : Mỗi {args.batch_interval}s (Tự động đọc Kafka -> DQ -> Batch -> Watermark -> Audit)")
    print(f"  • Optimize Interval  : Mỗi {args.optimize_interval}s (ClickHouse OPTIMIZE FINAL)")
    print(f"  • Write Iceberg      : {'ON' if args.write_iceberg else 'OFF'}")
    print("  • Nhấn Ctrl+C để dừng tiến trình an toàn.")
    print("=" * 88 + "\n")

    # Đợi vài giây đầu để luồng Ingestion kịp bơm dữ liệu vào Kafka
    if args.mode != "external":
        _STOP_EVENT.wait(min(5.0, float(args.batch_interval)))

    cycle = 0
    last_optimize_ts = time.time()

    while not _STOP_EVENT.is_set():
        cycle += 1
        cycle_start = time.perf_counter()
        run_id = f"auto_cycle_{cycle}_{datetime.now(timezone.utc).strftime('%H%M%S')}"

        try:
            summary = run_batch_pipeline(
                input_path=None,
                batch_run_id=run_id,
                sync_clickhouse=True,
                write_iceberg=args.write_iceberg,
                iceberg_strict=False,
                from_kafka=True,
            )
            elapsed = round(time.perf_counter() - cycle_start, 2)
            print(
                f"[CYCLE #{cycle:03d}] "
                f"Kafka In: {summary['total_records']:5d} | "
                f"Valid: {summary['valid_records']:5d} | "
                f"Quarantined: {summary['quarantine_rows']:3d} | "
                f"Deduped: {summary['duplicates_removed']:4d} | "
                f"Batch Candles: {summary['clickhouse_rows']:4d} | "
                f"Audit Rows: {summary.get('reconciled_audit_rows', 0):4d} | "
                f"Watermark: {str(summary['watermark'] or 'N/A')[:19]} | "
                f"{elapsed:.2f}s",
                flush=True,
            )
        except Exception as exc:
            logger.error("Lỗi trong chu kỳ Auto-Batch #%d: %s", cycle, exc)

        if time.time() - last_optimize_ts >= args.optimize_interval:
            optimize_clickhouse_tables()
            last_optimize_ts = time.time()

        if args.max_cycles > 0 and cycle >= args.max_cycles:
            logger.info("Đã hoàn thành đủ %d chu kỳ (--max-cycles). Đang dừng...", args.max_cycles)
            _STOP_EVENT.set()
            break

        _STOP_EVENT.wait(max(2.0, float(args.batch_interval)))

    worker.stop()
    print("\n" + "=" * 88)
    print(f"  Đã dừng Auto-Pipeline Daemon an toàn sau {cycle} chu kỳ (Tổng events đã phát: {worker.total_published:,}).")
    print("=" * 88 + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Autonomous End-to-End Lambda Lakehouse Pipeline Daemon"
    )
    parser.add_argument(
        "--mode",
        choices=["simulated", "live", "external"],
        default="simulated",
        help="Chế độ thu thập dữ liệu: 'simulated' (GBM + Faults), 'live' (Binance WS), 'external' (chỉ chạy Auto-Batch)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        help="Danh sách cặp coin cho chế độ simulated",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Số lượng coin thanh khoản cao nhất cho chế độ live Binance WS",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=25,
        help="Số sự kiện giao dịch sinh mỗi giây (chế độ simulated, mặc định: 25)",
    )
    parser.add_argument(
        "--batch-interval",
        type=int,
        default=30,
        help="Chu kỳ tự động chạy Batch & Reconciliation tính bằng giây (mặc định: 30s)",
    )
    parser.add_argument(
        "--optimize-interval",
        type=int,
        default=300,
        help="Chu kỳ tự động chạy ClickHouse OPTIMIZE FINAL tính bằng giây (mặc định: 300s)",
    )
    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Kafka bootstrap servers (mặc định: lấy từ cấu hình / localhost:9094)",
    )
    parser.add_argument(
        "--write-iceberg",
        action="store_true",
        help="Bật ghi đồng thời vào bảng Apache Iceberg Bronze trong mỗi chu kỳ Batch",
    )
    parser.add_argument(
        "--no-faults",
        action="store_true",
        help="Tắt tiêm lỗi tự động (chỉ phát dữ liệu sạch 100%%)",
    )
    parser.add_argument(
        "--ensure-docker",
        action="store_true",
        help="Tự động kiểm tra và bật Docker Compose services trước khi chạy",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Số chu kỳ Batch tối đa trước khi tự thoát (0 = chạy vô hạn 24/7)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_auto_daemon(parse_args())

