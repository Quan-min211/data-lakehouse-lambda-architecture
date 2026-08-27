"""
stream_live_demo.py
===================
Script chạy thử nghiệm kết nối Binance WebSocket thật và đẩy vào Kafka.
Chạy trong 20 giây hoặc nhận 100 giao dịch thật rồi dừng để hiển thị kết quả.
"""

import os
import sys
import json
import time
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.ingestion.binance_ws import BinanceWebSocketProducer
from src.ingestion.kafka_producer import ResilientKafkaProducer
from src.utils.logger import setup_logger

logger = setup_logger("stream_demo")


def main():
    print("=" * 70)
    print("🚀 BẮT ĐẦU KẾT NỐI VÀ STREAM GIAO DỊCH THẬT TỪ SÀN BINANCE...")
    print("=" * 70)

    producer = ResilientKafkaProducer(bootstrap_servers="localhost:9094")
    ws_client = BinanceWebSocketProducer(kafka_producer=producer, top_n=10)

    # Chạy stream trong 1 thread riêng
    stream_thread = threading.Thread(target=ws_client.start_streaming, daemon=True)
    stream_thread.start()

    # Chờ 20 giây để thu thập dữ liệu thật
    start_time = time.time()
    while time.time() - start_time < 20:
        time.sleep(1)
        if ws_client.total_messages_received >= 100:
            break

    print("=" * 70)
    print(f"✅ KẾT QUẢ THU THẬP THẬT:")
    print(f" • Tổng số tin WebSocket nhận từ Binance: {ws_client.total_messages_received} ticks")
    print(f" • Tổng số sự kiện đã bắn vào Kafka topic 'crypto_trades_raw': {ws_client.total_events_published} events")
    print(f" • Các cặp coin đang stream: {ws_client.symbols}")
    print("=" * 70)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
