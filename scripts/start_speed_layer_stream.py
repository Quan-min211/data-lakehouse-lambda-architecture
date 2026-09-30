"""
start_speed_layer_stream.py
===========================
Chay tien trinh Apache Spark Structured Streaming cho Tang Toc Do (Speed Layer).
- Tu dong duy tri Spark Web UI truc quan tai http://localhost:4040
- Tinh toan micro-batch nen 1 phut va VWAP thoi gian thuc
"""

import os
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def start_speed_stream():
    print("=" * 70)
    print(">> DANG KHOI DONG APACHE SPARK STRUCTURED STREAMING (SPEED LAYER)...")
    print("=" * 70)

    spark = (
        SparkSession.builder
        .appName("CryptoLakehouse-SpeedLayer")
        .master("local[2]")
        .config("spark.ui.port", "4040")
        .config("spark.ui.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    ui_url = sc.uiWebUrl or "http://localhost:4040"
    print(f"\nSPARK WEB UI DA HOAT DONG TAI: {ui_url}")
    print("Mo trinh duyet tai: http://localhost:4040 de xem truc quan hoa Streaming!\n")

    # 2. Tao dong du lieu Trade Streaming thoi gian thuc (10 trades/giay)
    base_rate = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

    trade_stream = (
        base_rate
        .withColumn("symbol", F.lit("BTCUSDT"))
        .withColumn("trade_time", F.col("timestamp"))
        .withColumn(
            "price",
            F.round(
                F.lit(65000.0) + (F.rand(seed=42) * 200.0 - 100.0) + (F.sin(F.col("value").cast("double") / 10.0) * 150.0),
                2
            )
        )
        .withColumn(
            "quantity",
            F.round(F.rand(seed=100) * 2.5 + 0.05, 4)
        )
        .withColumn("is_buyer_maker", (F.rand() > 0.5).cast("boolean"))
        .withWatermark("trade_time", "1 minute")
    )

    # 3. Tinh toan nen 1 phut OHLCV & VWAP (Windowing & Aggregation)
    windowed_candles = (
        trade_stream
        .groupBy(
            F.window(F.col("trade_time"), "1 minute"),
            F.col("symbol")
        )
        .agg(
            F.first("price").alias("open_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.last("price").alias("close_price"),
            F.round(F.sum("quantity"), 4).alias("volume"),
            F.count("price").alias("trade_count"),
            F.round(F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity"), 2).alias("vwap")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )

    # 4. Xuat micro-batch dinh ky moi 5 giay
    query = (
        windowed_candles
        .writeStream
        .queryName("crypto_speed_layer_streaming")
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Spark Structured Streaming dang xu ly micro-batch moi 5 giay...")
    print("Moi ban mo http://localhost:4040 de xem do thi truc quan!")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nDang dung Spark Streaming...")
        query.stop()
        spark.stop()

if __name__ == "__main__":
    start_speed_stream()
