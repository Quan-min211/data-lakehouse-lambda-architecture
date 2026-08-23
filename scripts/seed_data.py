"""
seed_data.py
============
Script sinh tập dữ liệu giả lập thị trường tiền mã hóa (Seed Data Generator).
Tạo ra chuỗi nến và khớp lệnh ngẫu nhiên có quy luật (Geometric Brownian Motion)
phục vụ chạy test offline và các bài toán Benchmark.

Usage:
    python scripts/seed_data.py --sample --count 1000
    python scripts/seed_data.py --kafka --count 5000 --symbol BTCUSDT
"""

import os
import sys
import json
import time
import random
import argparse
from datetime import datetime, timedelta

# Thêm root dir vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.ingestion.models import TradeEvent
from src.ingestion.fault_injector import FaultInjector
from src.ingestion.kafka_producer import ResilientKafkaProducer
from src.utils.logger import setup_logger

logger = setup_logger("seed_data")


def generate_mock_trades(
    symbol: str = "BTCUSDT",
    count: int = 1000,
    base_price: float = 65000.0,
    volatility: float = 0.002,
    start_time_ms: int = None
) -> list:
    """Sinh chuỗi giao dịch khớp lệnh giả lập theo mô hình biến động giá thị trường.

    Args:
        symbol (str): Mã cặp coin (BTCUSDT, ETHUSDT).
        count (int): Số lượng giao dịch cần sinh.
        base_price (float): Mức giá ban đầu.
        volatility (float): Độ biến động giá giữa các giao dịch.
        start_time_ms (int, optional): Mốc thời gian bắt đầu (epoch ms).

    Returns:
        list[TradeEvent]: Danh sách các đối tượng TradeEvent.
    """
    if start_time_ms is None:
        start_time_ms = int((time.time() - count * 0.5) * 1000)

    current_price = base_price
    current_time = start_time_ms
    start_trade_id = random.randint(100000000, 900000000)

    trades = []
    for i in range(count):
        # Bước giá ngẫu nhiên (Random Walk)
        price_change_pct = random.gauss(0, volatility)
        current_price = round(max(current_price * (1 + price_change_pct), 1.0), 2)

        # Khối lượng giao dịch phân phối log-normal
        qty = round(random.expovariate(1.5) + 0.001, 4)
        is_buyer_maker = random.choice([True, False])

        # Thời gian tăng dần 50ms - 800ms mỗi trade
        current_time += random.randint(50, 800)

        trade = TradeEvent(
            trade_id=start_trade_id + i,
            symbol=symbol,
            price=current_price,
            quantity=qty,
            trade_time=current_time,
            is_buyer_maker=is_buyer_maker,
            ingestion_time=current_time + random.randint(5, 50),
            is_injected=False
        )
        trades.append(trade)

    return trades


def save_to_file(trades: list, output_path: str):
    """Lưu danh sách giao dịch ra file JSON."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    data = [t.to_dict() for t in trades]
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Đã lưu {len(trades)} bản ghi mẫu vào: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Sinh dữ liệu mẫu và nạp vào hệ thống.")
    parser.add_argument("--count", type=int, default=1000, help="Số lượng bản ghi cần sinh.")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Mã cặp giao dịch.")
    parser.add_argument("--base-price", type=float, default=65000.0, help="Mức giá khởi điểm.")
    parser.add_argument("--sample", action="store_true", help="Lưu dữ liệu mẫu ra thư mục datasets/sample/.")
    parser.add_argument("--kafka", action="store_true", help="Gửi trực tiếp dữ liệu sinh ra vào Kafka.")
    parser.add_argument("--inject-faults", action="store_true", help="Kích hoạt tiêm lỗi (10% duplicate, 10% late).")

    args = parser.parse_args()

    logger.info(f"Đang sinh {args.count} giao dịch mẫu cho cặp {args.symbol}...")
    trades = generate_mock_trades(
        symbol=args.symbol,
        count=args.count,
        base_price=args.base_price
    )

    if args.inject_faults:
        injector = FaultInjector(duplicate_rate=0.10, late_data_rate=0.10)
        injected_trades = []
        for t in trades:
            injected_trades.extend(injector.process_event(t))
        trades = injected_trades
        logger.info(f"Sau khi tiêm lỗi: tổng cộng {len(trades)} sự kiện.")

    if args.sample or (not args.kafka and not args.sample):
        sample_path = os.path.join("datasets", "sample", f"{args.symbol.lower()}_trades_sample.json")
        save_to_file(trades, sample_path)

    if args.kafka:
        producer = ResilientKafkaProducer()
        sent = 0
        for t in trades:
            if producer.send_trade(t):
                sent += 1
        producer.flush()
        logger.info(f"Đã gửi thành công {sent}/{len(trades)} bản ghi vào Kafka!")


if __name__ == "__main__":
    main()
