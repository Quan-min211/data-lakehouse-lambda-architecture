# TIEN TRINH 02: XAY DUNG TANG TOC DO (SPEED LAYER) VOI SPARK STRUCTURED STREAMING

* **Tac gia:** Nguyen Dang Quoc Anh (MSSV: 23133004)
* **Phoi hop:** Tuan thu schema bang ClickHouse da thong nhat voi Pham Minh Quan
* **Ngay hoan thanh:** 03/09/2026

---

## 1. Muc Tieu Ky Thuat Dat Duoc
Da hoan thanh thiet ke va hien thuc hoa toan dien Tang Toc Do (Speed Layer) trong kien truc Data Lakehouse Lambda:
1. **Engine Spark Structured Streaming (src/speed_layer/spark_streaming.py):**
   * Tieu thu dong du lieu su kien giao dich thoi gian thuc tu Kafka topic crypto_trades_raw.
   * Toi uu hoa vi xu ly (Micro-batching) voi Trigger interval 5 seconds dam bao do tre end-to-end SLA < 5s.
   * Cau hinh Checkpointing phuc hoi trang thai (Fault Tolerance).
2. **Co che Event-time & Watermarking (src/speed_layer/window_aggregator.py):**
   * Xu ly theo thoi gian phat sinh giao dich thuc te trade_time (Event-time ms tu Binance).
   * Thiet lap withWatermark('event_time', '1 minute') cho phep tiep nhan late data hop le trong nguong 1-2 phut va loai bo du lieu qua tre.
   * Cau hinh Tumbling Window 1 minute (va linh hoat 5 minutes).
3. **Bo Tinh Toan Chi So OHLCV & VWAP (src/speed_layer/metrics_calculator.py):**
   * Su dung ky thuat struct ordering struct(trade_time, price) de trich xuat chuan xac gia Open (giao dich dau tien) va gia Close (giao dich cuoi cung) trong moi cua so.
   * Tinh toan khoi luong giao dich volume va chi so VWAP theo cong thuc chuan: VWAP = sum(price * quantity) / sum(quantity).
4. **Phat Hien Bien Dong Bat Thuong (src/speed_layer/spike_detector.py):**
   * Thuat toan phat hien buoc nhay gia dot ngot |close - open| / open >= 2% hoac bien do dao dong nen (high - low) / low >= 3%.
   * Gan co nhi phan is_spike phuc vu canh bao truc tiep tren Dashboard.
5. **Ghi Du Lieu Tuc Thoi Vao ClickHouse Sink (src/speed_layer/clickhouse_writer.py):**
   * Ghi vi phan dong nen vao bang lakehouse.speed_agg voi co che ReplacingMergeTree(created_at) dam bao tinh Idempotent (chong trung lap khi retry).
   * Khop 100% DDL trong configs/clickhouse/init.sql da thong nhat voi Quan.

---

## 2. Ket Qua Kiem Thu (Verification)
* Toan bo Unit Tests dat chuan (17/17 passed trong tests/test_speed_layer.py va tests/test_ingestion.py).
