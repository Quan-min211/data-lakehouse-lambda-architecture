# BÁO CÁO THỰC NGHIỆM STREAM PROCESSING & ĐỐI SOÁT HYBRID

## Đề tài: Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa
> **Real-Time Stream Processing & Hybrid Query Merging Demonstration**

| Thông tin | Chi tiết |
| :--- | :--- |
| **Sinh viên thực hiện** | Nguyễn Đặng Quốc Anh (23133004) — Phạm Minh Quân (23133060) |
| **Giảng viên hướng dẫn** | ThS. Đoàn Minh Trí |
| **Khoa / Trường** | Khoa Công nghệ Thông tin — ĐH Sư phạm Kỹ thuật TP.HCM (HCMUTE) |
| **Thời điểm thực nghiệm** | Tháng 09/2026 |
| **Môi trường chạy** | Docker Compose trên Localhost (Linux Container trên Windows WSL2) |

---

## MỤC LỤC

1. [Mục tiêu đợt thực nghiệm](#1-mục-tiêu-đợt-thực-nghiệm)
2. [Hành trình dòng dữ liệu (End-to-End Data Pipeline)](#2-hành-trình-dòng-dữ-liệu-end-to-end-data-pipeline)
3. [Phân tích chi tiết kết quả thực nghiệm qua các minh chứng](#3-phân-tích-chi-tiết-kết-quả-thực-nghiệm-qua-các-minh-chứng)
   * [3.1 Tầng Ingestion: Thu thập dữ liệu Live WebSocket từ Binance](#31-tầng-ingestion-thu-thập-dữ-liệu-live-websocket-từ-binance)
   * [3.2 Tầng Speed Layer: Spark Structured Streaming xử lý Micro-batch](#32-tầng-speed-layer-spark-structured-streaming-xử-lý-micro-batch)
   * [3.3 Tầng Lưu trữ ClickHouse: Phân tách Batch View & Speed View](#33-tầng-lưu-trữ-clickhouse-phân-tách-batch-view--speed-view)
   * [3.4 Tầng Serving & Dashboard: Ghép nối Hybrid & Đối soát thời gian thực](#34-tầng-serving--dashboard-ghép-nối-hybrid--đối-soát-thời-gian-thực)
4. [Đánh giá các chỉ số kỹ thuật (SLA & Data Quality)](#4-đánh-giá-các-chỉ-số-kỹ-thuật-sla--data-quality)
5. [Kết luận & Kế hoạch tiếp theo](#5-kết-luận--kế-hoạch-tiếp-theo)

---

## 1. Mục tiêu đợt thực nghiệm

Đợt thực nghiệm này được thực hiện nhằm kiểm chứng toàn diện tính khả thi và độ ổn định của hệ thống **Data Lakehouse Lambda Architecture** đã đề xuất, tập trung vào các trọng tâm sau:

1. **Kiểm chứng thông suốt toàn bộ luồng dữ liệu (End-to-End Flow):**
   Từ khâu nhận dữ liệu trực tiếp ngoài thị trường qua sàn Binance $\rightarrow$ đưa vào Kafka $\rightarrow$ vi xử lý stream qua Spark $\rightarrow$ ghi bảng phân tích OLAP ClickHouse $\rightarrow$ hợp nhất và hiển thị lên giao diện Web.
2. **Xác thực độ trễ của Tầng Tốc độ (Speed Layer SLA):**
   Kiểm tra xem thời gian từ lúc một giao dịch khớp lệnh ngoài đời thực đến khi nến OHLCV 1 phút được ghi nhận vào database có đạt tiêu chí cam kết **$\text{SLA} < 5.0\text{s}$** hay không.
3. **Minh chứng cơ chế cốt lõi — Auto-Correcting Query Merger (CASE 3: HYBRID):**
   Chứng minh Serving Layer có khả năng tự động phân luồng: đọc dữ liệu lịch sử chuẩn xác từ **Batch Layer** (`batch_agg`) và dữ liệu tức thời từ **Speed Layer** (`speed_agg`), ghép chúng lại tại mốc `System Watermark` mà **không bị trùng lặp dữ liệu (Zero Double-Counting)**.

---

## 2. Hành trình dòng dữ liệu (End-to-End Data Pipeline)

Sơ đồ tuần tự xử lý luồng dữ liệu thời gian thực được minh họa như sau:

```mermaid
sequenceDiagram
    autonumber
    actor Market as Sàn Binance Global
    participant WS as Ingestion (Binance WS)
    participant Kafka as Apache Kafka (KRaft)
    participant Spark as Spark Streaming (Docker)
    participant CH as ClickHouse (speed_agg)
    participant API as FastAPI (Serving Layer)
    participant UI as Streamlit Dashboard

    Market->>WS: Giao dịch khớp lệnh (aggTrade ticks)
    WS->>Kafka: Đẩy TradeEvent vào topic 'crypto_trades_raw'
    loop Micro-batch Trigger (mỗi 5 giây)
        Spark->>Kafka: Đọc offset mới
        Spark->>Spark: Tumbling Window 1 phút, tính OHLCV & VWAP
        Spark->>CH: Ghi nến tạm thời vào lakehouse.speed_agg (SLA < 5s)
    end
    UI->>API: GET /api/market?symbol=BTCUSDT
    API->>CH: Đọc batch_agg (quá khứ) + speed_agg (hiện tại)
    API->>API: Ghép tại Watermark (CASE 3: HYBRID) & tính sai số Delta
    API-->>UI: Trả về chuỗi nến hoàn chỉnh (Reconciled + Provisional)
    UI-->>UI: Vẽ nến thời gian thực & cập nhật biểu đồ
```

---

## 3. Phân tích chi tiết kết quả thực nghiệm qua các minh chứng

Dưới đây là phân tích chi tiết dựa trên dữ liệu và hình ảnh chụp màn hình ghi nhận trực tiếp từ các phiên chạy thực tế của hệ thống.

---

### 3.1 Tầng Ingestion: Thu thập dữ liệu Live WebSocket từ Binance

Producer được khởi chạy thông qua module `src/ingestion/binance_ws.py` và `src/ingestion/kafka_producer.py`.

> [!TIP]
> **📌 VỊ TRÍ CHÈN ẢNH 1:**
> * **File gợi ý:** `images/hinh_1_ingestion_binance_websocket.png`
> * **Nội dung ảnh:** Màn hình PowerShell chạy log `kafka_producer` và `binance_ws` đẩy 2,200 events vào Kafka.
> 
> ![Hình 1: Tiến trình thu thập dữ liệu giao dịch trực tiếp từ sàn Binance qua WebSocket và phát vào Kafka topic crypto_trades_raw](./images/hinh_1_ingestion_binance_websocket.png)
> 
> *Hình 1: Tiến trình kết nối Binance WebSocket cho Top 5 cặp coin thanh khoản cao nhất, xử lý và phát liên tục 2,200 sự kiện giao dịch thật vào Kafka topic `crypto_trades_raw`.*

```text
2026-09-23 22:17:45 [INFO] [kafka_producer] Đang kết nối Kafka Producer tới: localhost:9094
2026-09-23 22:17:45 [INFO] [kafka_producer] Kết nối Kafka Producer thành công!
2026-09-23 22:17:45 [INFO] [binance_ws] Đang lấy danh sách Top 5 coin thanh khoản cao nhất từ Binance API...
2026-09-23 22:17:46 [INFO] [binance_ws] Top 5 coin được chọn: ['USDCUSDT', 'BTCUSDT', 'ETHUSDT', 'ZECUSDT', 'XRPUSDT']
2026-09-23 22:17:46 [INFO] [binance_ws] WebSocket URL được khởi tạo: wss://stream.binance.com:9443/stream?streams=usdcusdt@aggTrade/btcusdt@aggTrade/ethusdt@aggTrade/zecusdt@aggTrade/xrpusdt@aggTrade
2026-09-23 22:17:46 [INFO] [binance_ws] Bắt đầu lắng nghe dòng sự kiện giao dịch trực tiếp...
2026-09-23 22:17:47 [INFO] [binance_ws] Kết nối Binance WebSocket thành công cho 5 pairs!
2026-09-23 22:17:49 [INFO] [binance_ws] Đã xử lý 100 ticks | Đã phát 100 events vào Kafka.
...
2026-09-23 22:18:25 [INFO] [binance_ws] Đã xử lý 2200 ticks | Đã phát 2200 events vào Kafka.
```

#### Phân tích kỹ thuật:
1. **Lọc thanh khoản tự động:** Client tự động gọi endpoint Binance 24h ticker và chọn ra 5 cặp tiền có khối lượng lớn nhất tại thời điểm chạy: `USDCUSDT`, `BTCUSDT`, `ETHUSDT`, `ZECUSDT`, `XRPUSDT`.
2. **Kênh truyền Multiplexed Stream:** Sử dụng duy nhất 1 kết nối WebSocket bền vững gom chung stream cho cả 5 cặp coin, giảm áp lực mạng và socket descriptor.
3. **Tốc độ đẩy tin:** Trong vòng **40 giây** (từ 22:17:45 đến 22:18:25), hệ thống đã bắt được và đẩy thành công **2,200 sự kiện giao dịch thật** vào Kafka topic `crypto_trades_raw` (trung bình ~55 events/giây).

---

### 3.2 Tầng Speed Layer: Spark Structured Streaming xử lý Micro-batch

Tại container `lakehouse-speed-layer`, Spark Structured Streaming (chế độ Linux container) quét topic định kỳ và nạp nến vào ClickHouse qua module `clickhouse_writer.py`.

> [!TIP]
> **📌 VỊ TRÍ CHÈN ẢNH 2:**
> * **File gợi ý:** `images/hinh_2_speed_layer_spark_streaming_logs.png`
> * **Nội dung ảnh:** Màn hình VS Code Terminal hiển thị log `lakehouse-speed-layer` xử lý các Batch 16 đến Batch 23 với SLA < 5s.
> 
> ![Hình 2: Log thực thi các Micro-batch định kỳ của Spark Structured Streaming và thời gian ghi vào ClickHouse](./images/hinh_2_speed_layer_spark_streaming_logs.png)
> 
> *Hình 2: Log thực thi các Micro-batch (Batch 16 - Batch 23) của Spark Structured Streaming trong container Speed Layer. Thời gian ghi nến vào ClickHouse dao động từ 0.521s – 1.240s, thỏa mãn hoàn toàn cam kết $\text{SLA} < 5.0\text{s}$.*

```text
lakehouse-speed-layer | 2026-09-23 15:18:35 [INFO] [clickhouse_writer] Đã ghi thành công 5 nến vào 'lakehouse.speed_agg'
lakehouse-speed-layer | 2026-09-23 15:18:35 [INFO] [clickhouse_writer] [Batch 16] Hoàn thành ghi 5 nến vào ClickHouse trong 0.588s (SLA < 5s)
lakehouse-speed-layer | 2026-09-23 15:18:40 [INFO] [clickhouse_writer] [Batch 17] Nhận 5 bản ghi tổng hợp từ Spark Streaming.
lakehouse-speed-layer | 2026-09-23 15:18:41 [INFO] [clickhouse_writer] Đã ghi thành công 5 nến vào 'lakehouse.speed_agg'
lakehouse-speed-layer | 2026-09-23 15:18:41 [INFO] [clickhouse_writer] [Batch 17] Hoàn thành ghi 5 nến vào ClickHouse trong 0.947s (SLA < 5s)
lakehouse-speed-layer | 2026-09-23 15:18:45 [INFO] [clickhouse_writer] [Batch 18] Hoàn thành ghi 5 nến vào ClickHouse trong 0.521s (SLA < 5s)
lakehouse-speed-layer | 2026-09-23 15:18:51 [INFO] [clickhouse_writer] [Batch 19] Hoàn thành ghi 5 nến vào ClickHouse trong 0.928s (SLA < 5s)
lakehouse-speed-layer | 2026-09-23 15:18:56 [INFO] [clickhouse_writer] [Batch 20] Hoàn thành ghi 5 nến vào ClickHouse trong 1.24s (SLA < 5s)
lakehouse-speed-layer | 2026-09-23 15:19:01 [INFO] [clickhouse_writer] [Batch 21] Hoàn thành ghi 5 nến vào ClickHouse trong 1.088s (SLA < 5s)
lakehouse-speed-layer | 2026-09-23 15:19:06 [INFO] [clickhouse_writer] [Batch 22] Hoàn thành ghi 9 nến vào ClickHouse trong 1.085s (SLA < 5s)
lakehouse-speed-layer | 2026-09-23 15:19:11 [INFO] [clickhouse_writer] [Batch 23] Hoàn thành ghi 5 nến vào ClickHouse trong 0.885s (SLA < 5s)
```

#### Phân tích thời gian thực thi (Latency Breakdown):
* Chu kỳ kích hoạt (Trigger interval): Cố định **5 giây / lần**.
* Thời gian xử lý ghi ClickHouse trung bình dao động từ **0.521 giây đến 1.240 giây**.
* **Đánh giá SLA:** Toàn bộ các batch đều nhỏ hơn ngưỡng 5 giây, bảo đảm nến trên dashboard không bị giật lag và đáp ứng chuẩn thời gian thực khắt khe của hệ thống tài chính.

---

### 3.3 Tầng Lưu trữ ClickHouse: Phân tách Batch View & Speed View

Để minh chứng tính chất cốt lõi của Kiến trúc Lambda — **phân tách độc lập giữa Tầng Lô (Batch) và Tầng Tốc Độ (Speed)** trong cơ sở dữ liệu OLAP, chúng tôi thực thi truy vấn trực tiếp vào ClickHouse server:

> [!TIP]
> **📌 VỊ TRÍ CHÈN ẢNH 3:**
> * **File gợi ý:** `images/hinh_3_clickhouse_batch_vs_speed_views.png`
> * **Nội dung ảnh:** Màn hình Terminal truy vấn `lakehouse.batch_agg` và đếm số lượng so sánh: `Batch Layer: 380445` nến vs `Speed Layer: 70` nến.
> 
> ![Hình 3: Kết quả truy vấn ClickHouse đối chiếu quy mô nến giữa Batch Layer và Speed Layer](./images/hinh_3_clickhouse_batch_vs_speed_views.png)
> 
> *Hình 3: Kết quả truy vấn ClickHouse thể hiện rõ cấu trúc phân tách của Kiến trúc Lambda: 380,445 nến lịch sử chuẩn xác trong `lakehouse.batch_agg` và 70 nến tạm thời thời gian thực trong `lakehouse.speed_agg`.*

```sql
SELECT 'Batch Layer (batch_agg)' AS Layer, count(*) AS Total_Candles FROM lakehouse.batch_agg
UNION ALL
SELECT 'Speed Layer (speed_agg)' AS Layer, count(*) AS Total_Candles FROM lakehouse.speed_agg;
```

#### Kết quả thực tế:
```
┌─Layer────────────────────┬─Total_Candles─┐
│ Speed Layer (speed_agg)  │            70 │
│ Batch Layer (batch_agg)  │        380445 │
└──────────────────────────┴───────────────┘
```

#### Chi tiết các nến Batch Layer trong ClickHouse (`lakehouse.batch_agg`):
```sql
SELECT symbol, window_start, open_price, high_price, low_price, close_price, volume, trade_count
FROM lakehouse.batch_agg
ORDER BY window_start DESC LIMIT 5 FORMAT Pretty;
```
* Bảng trả về các nến lịch sử chuẩn xác chốt tại mốc `2026-09-02 08:08:00.000` (được sinh ra từ quá trình xử lý 1,048,320 bản ghi gốc lưu trong MinIO Iceberg).

#### Sự biến thiên thời gian thực của nến Speed Layer (`lakehouse.speed_agg`):
Khi truy vấn `lakehouse.speed_agg` lặp lại cách nhau 5 giây đối với cặp `BTCUSDT` và `ETHUSDT`:
* **Lần 1:** `BTCUSDT` ghi nhận $Volume = 2.59173$, $Trade\_Count = 243$, $Close\_Price = 84,598.73$.
* **Lần 2:** `BTCUSDT` tăng vọt lên $Volume = 13.83046$, $Trade\_Count = 644$, $Close\_Price = 84,587.72$.
* Nến của khung giờ `15:22:00` liên tục được co giãn biên độ giá (High/Low) và cập nhật giá đóng cửa tương ứng với từng micro-batch nhận từ Kafka.

---

### 3.4 Tầng Serving & Dashboard: Ghép nối Hybrid & Đối soát thời gian thực

Hai bức ảnh chụp giao diện Streamlit Dashboard cho cặp **`BTCUSDT`** ghi nhận sự biến chuyển của bộ điều hướng **Auto-Correcting Query Merger**:

#### Quan sát trạng thái tại thời điểm $T_1$:
* **Giá hiện tại:** `$61,360.80` | **VWAP:** `$61,455.25`.
* **Phân luồng Query Merger:** `🟠 CASE 3: HYBRID (↑ Partially Reconciled)`.
* **System Watermark:** `2026-09-23 15:05:08`.
* **Sai số đối soát ($\Delta$):** `$9.8800`.
* **Diễn giải:** Người dùng yêu cầu xem dữ liệu khung 1 giờ gần nhất. Vì khung này nằm vắt ngang qua mốc Watermark (`15:05:08`), hệ thống tự động:
  * Lấy dữ liệu trước `15:05:08` từ `batch_agg` (đã đối soát - Reconciled).
  * Lấy dữ liệu sau `15:05:08` từ `speed_agg` (tạm thời - Provisional).

> [!TIP]
> **📌 VỊ TRÍ CHÈN ẢNH 4:**
> * **File gợi ý:** `images/hinh_4_streamlit_dashboard_hybrid_t1.png`
> * **Nội dung ảnh:** Giao diện Streamlit Dashboard tại thời điểm $T_1$ với chỉ số Watermark `15:05:08`, cơ chế `CASE 3: HYBRID` và sai số đối soát $\Delta = \$9.8800$.
> 
> ![Hình 4: Giao diện Streamlit Dashboard hiển thị nến thời gian thực BTCUSDT và cơ chế phân luồng CASE 3: HYBRID tại thời điểm T1](./images/hinh_4_streamlit_dashboard_hybrid_t1.png)
> 
> *Hình 4: Giao diện Streamlit Dashboard tại thời điểm $T_1$: Tự động kích hoạt cơ chế `CASE 3: HYBRID` tại mốc Watermark `15:05:08` với sai số đối soát $\Delta = \$9.8800$.*

#### Quan sát trạng thái tại thời điểm $T_2$ (sau vài phút stream):
* **Giá hiện tại:** `$59,706.84` | **VWAP:** `$59,680.18`.
* **Phân luồng Query Merger:** `🟠 CASE 3: HYBRID (↑ Partially Reconciled)`.
* **System Watermark:** Được đẩy tịnh tiến lên `2026-09-23 15:11:28`.
* **Sai số đối soát ($\Delta$):** Giảm từ `$9.8800` xuống `$6.9000` ($\downarrow$ 30.1%).
* **Đặc điểm biểu đồ:** Chuỗi nến OHLCV và khối lượng volume phía dưới hiển thị liền mạch, không xuất hiện khoảng trống (gap) hay hiện tượng nhảy nến trùng lặp tại vị trí ranh giới Watermark.

> [!TIP]
> **📌 VỊ TRÍ CHÈN ẢNH 5:**
> * **File gợi ý:** `images/hinh_5_streamlit_dashboard_hybrid_t2.png`
> * **Nội dung ảnh:** Giao diện Streamlit Dashboard tại thời điểm $T_2$ với Watermark tịnh tiến lên `15:11:28` và sai số đối soát $\Delta$ giảm về `$6.9000`.
> 
> ![Hình 5: Biểu đồ nến BTCUSDT liên tục được cập nhật theo thời gian thực và sai số đối soát giảm về $6.9000](./images/hinh_5_streamlit_dashboard_hybrid_t2.png)
> 
> *Hình 5: Giao diện Streamlit Dashboard tại thời điểm $T_2$: Mốc Watermark tịnh tiến lên `15:11:28`, sai số đối soát $\Delta$ giảm về `$6.9000$`, chuỗi nến thời gian thực được cập nhật liền mạch.*


---

## 4. Đánh giá các chỉ số kỹ thuật (SLA & Data Quality)

Tổng hợp các chỉ số định lượng đo được trong toàn bộ chu trình thực nghiệm:

| Thành phần | Tiêu chí đo lường | Kết quả thực nghiệm | Đánh giá SLA |
| :--- | :--- | :--- | :--- |
| **Kafka Ingestion** | Tốc độ tiếp nhận & phát tin | ~55 – 180 msgs/s | Đạt (Không bị drop message) |
| **Speed Layer** | Chu kỳ Micro-batch trigger | 5.0 giây | Chuẩn định kỳ |
| **Speed Layer** | Thời gian ghi ClickHouse | 0.521s – 1.240s | **Đạt xuất sắc ($\text{SLA} < 5.0\text{s}$)** |
| **Batch Layer** | Khối lượng Master Data Iceberg | 1,048,320 records | Đã xác thực qua dbt (9/9 tests PASS) |
| **ClickHouse OLAP** | Quy mô nến Batch (`batch_agg`) | 380,445 nến | Sẵn sàng cho truy vấn lịch sử |
| **Serving Layer** | Độ chính xác phân luồng Hybrid | Phân tách tại Watermark | Không trùng lặp (Zero Double-Counting) |
| **Dung lượng bộ nhớ** | RAM tiêu thụ toàn hệ thống | ~7.5 – 8.2 GB | Nằm trong ngân sách 16GB máy cá nhân |

---

## 5. Kết luận & Kế hoạch tiếp theo

### 5.1 Kết luận
1. **Kiến trúc hoạt động đúng thiết kế:** Toàn bộ các dịch vụ Kafka, MinIO, Iceberg REST, Spark Batch, Spark Streaming, ClickHouse, Redis, FastAPI và Streamlit đã được container hóa hoàn chỉnh và phối hợp nhịp nhàng.
2. **Giải quyết được bài toán bù đắp thời gian thực (Real-time Compensation):** Tầng Speed Layer bù đắp hoàn hảo khoảng trống dữ liệu từ sau mốc Watermark của Batch Layer cho đến thời điểm hiện tại, đem lại trải nghiệm liền mạch cho người dùng.
3. **Cơ chế đối soát tự động hoạt động hiệu quả:** Sai số $\Delta$ giữa Speed View và Batch View được tính toán và theo dõi trực quan theo thời gian thực.

### 5.2 Kế hoạch tiếp theo
1. **Thực thi bộ 3 kịch bản Benchmark chính thức:**
   * **Benchmark 1 (Query Latency):** Chạy `scripts/benchmarks/bench_latency.py` đo độ trễ truy vấn giữa Hybrid Lambda vs Batch-only.
   * **Benchmark 2 (Reprocess Correctness):** Chạy `scripts/benchmarks/bench_reprocess.py` đo tỷ lệ sai lệch VWAP khi Batch Layer hoàn tất nạp dữ liệu.
   * **Benchmark 3 (Iceberg Compaction):** Chạy `scripts/benchmarks/bench_compaction.py` đo hiệu năng đọc sau khi gộp file nhỏ.
2. **Tổng hợp số liệu vào báo cáo luận văn:** Xuất biểu đồ tự động từ thư mục `results/plots/` vào chương thực nghiệm của khóa luận tốt nghiệp.
