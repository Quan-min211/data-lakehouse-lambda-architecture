"""
verify_kafka_messages.py
========================
Đọc và hiển thị các bản ghi giao dịch thật vừa nhận được từ Kafka topic.
"""

import sys
import json
from datetime import datetime
from kafka import KafkaConsumer

# Bảo đảm UTF-8 trên Windows console
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def main():
    print("=" * 75)
    print(">>> DANG DOC CAC BAN GHI GIAO DICH THAT TU KAFKA TOPIC 'crypto_trades_raw'...")
    print("=" * 75)

    consumer = KafkaConsumer(
        "crypto_trades_raw",
        bootstrap_servers="localhost:9094",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    count = 0
    for msg in consumer:
        count += 1
        data = msg.value
        if count <= 8:
            trade_dt = datetime.fromtimestamp(data["trade_time"] / 1000.0).strftime("%H:%M:%S.%f")[:-3]
            side = "Taker SELL" if data.get("is_buyer_maker") else "Taker BUY "
            print(f" [{count:02d}] {trade_dt} | {data['symbol']:<8} | Gia: {data['price']:>12,.2f} USDT | Khoi luong: {data['quantity']:>10.4f} | {side} | ID: {data['trade_id']}")

    print("=" * 75)
    print(f"+++ TONG CONG DA CO {count} BAN GHI GIAO DICH THAT TRONG KAFKA TOPIC!")
    print("=" * 75)

    consumer.close()


if __name__ == "__main__":
    main()
