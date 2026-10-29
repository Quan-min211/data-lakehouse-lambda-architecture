# Báo Cáo Chuyên Sâu: Xử Lý Vấn Đề Tệp Nhỏ (Small File Problem) Trên Apache Iceberg

> **Đề tài:** Data Lakehouse Lambda Architecture for Cryptocurrency Streaming  
> **Cơ quan:** Khoa CNTT — Trường ĐH Sư phạm Kỹ thuật TP.HCM (HCMUTE)  
> **Tài liệu tham chiếu:** Trả lời câu hỏi nghiên cứu số 3 (Research Question 3 - RQ3)  
> **Tệp đích:** `docs/md/smallfile.md`

---

## 1. Bản Chất Vấn Đề Tệp Nhỏ (Small File Problem)

### 1.1. Small File trong Big Data & Data Lakehouse là gì?
Trong hệ sinh thái dữ liệu lớn (HDFS, S3, Data Lakehouse), các định dạng tệp cột phổ biến như **Apache Parquet** được thiết kế để lưu trữ khối lượng lớn dữ liệu trong một tệp duy nhất. Mỗi tệp Parquet bao gồm:
* **Header & Data Pages:** Chứa các giá trị nén và mã hóa (Snappy/GZIP, Dictionary Encoding, Run-Length Encoding).
* **Footer Metadata:** Chứa toàn bộ Schema, thống kê min/max của từng cột, offset các row group và từ điển dữ liệu.

Một tệp được coi là **"Small File" (Tệp nhỏ)** khi kích thước của nó nhỏ hơn rất nhiều so với kích thước khối xử lý tối ưu (thường là từ vài chục KB đến vài MB, so với chuẩn lý tưởng 128 MB – 512 MB).

Hậu quả nghiêm trọng của Small File Problem:
1. **Nghẽn I/O và chi phí gọi API Object Storage (MinIO/S3):** Để đọc 1 GB dữ liệu chia thành 1,000 tệp nhỏ (1 MB/tệp), hệ thống phải thực hiện 1,000 yêu cầu mạng HTTP GET kèm 1,000 lần mở/đóng kết nối socket, chi phí bắt tay mạng và đọc Header/Footer lấn át hoàn toàn thời gian đọc dữ liệu thực.
2. **Quá tải Spark Driver (Task Scheduling Overhead):** Mặc định, mỗi tệp nhỏ trên Data Lake sẽ tạo thành một Input Split tương ứng với một Spark Task. Việc lập lịch cho hàng nghìn tác vụ xử lý các tệp chỉ vài KB làm Spark Driver bị nghẽn CPU, tiêu tốn bộ nhớ quản lý task và triệt tiêu khả năng tính toán song song.
3. **Phình to Metadata Catalog:** Mỗi tệp nhỏ sinh ra các bản ghi manifest file và manifest list trên Iceberg REST Catalog, làm chậm đáng kể các thao tác lập kế hoạch truy vấn (Query Planning).

### 1.2. Small File trong Project này là gì?
Trong đồ án này, bảng tầng Bronze **`iceberg_catalog.bronze.crypto_trades`** lưu trữ các sự kiện giao dịch thô (`TradeEvent`) thu thập từ sàn Binance. Mỗi tệp Parquet nhỏ trong hồ lưu trữ MinIO (`s3://warehouse/bronze/crypto_trades/`) có kích thước chỉ từ **vài chục KB đến vài trăm KB**, chứa từ vài trăm đến vài nghìn dòng giao dịch.

### 1.3. Tại sao Small File lại xuất hiện trong Project này?
Sự xuất hiện của tệp nhỏ trong đồ án này là **hệ quả tất yếu của bài toán nghiệp vụ và kiến trúc**, xuất phát từ 3 nguyên nhân:
1. **Bản chất dòng dữ liệu Crypto Streaming:** Thị trường tiền mã hóa giao dịch liên tục 24/7/365 với tần suất cao (High-Frequency Trading).
2. **Yêu cầu độ trễ thấp (Low-Latency Streaming / Micro-Batch Ingestion):** Để bảo đảm dữ liệu mới xuất hiện tức thì trong hồ dữ liệu cho việc phân tích, các đợt nạp dữ liệu (micro-batch ingestion hoặc controlled batch appends) diễn ra liên tục theo chu kỳ ngắn.
3. **Cơ chế Snapshot Isolation & ACID của Apache Iceberg:** Mỗi lần Spark ghi một đợt dữ liệu vào Iceberg, Iceberg cam kết nguyên tử (ACID Commit) bằng cách tạo ra các file Parquet hoàn toàn mới và đóng gói thành một Snapshot mới chứ không ghi đè/nối đuôi vào file đang có. Kết quả là chỉ sau vài giờ stream, hàng trăm tệp Parquet nhỏ bị phân mảnh trong hồ chứa MinIO.

---

## 2. Quy Trình Kỹ Thuật Xử Lý Small File Trong Mã Nguồn

Để giải quyết bài toán trên mà không làm gián đoạn luồng ghi dữ liệu trực tiếp, hệ thống áp dụng chiến lược kết hợp giữa **Cấu hình tối ưu từ đầu (Proactive Configuration)** và **Pipeline bảo trì định kỳ bất đồng bộ (Asynchronous Table Maintenance Pipeline)**.

```text
               DÒNG DỮ LIỆU ĐẾN (Streaming / Batch Appends)
                                   │
                                   ▼
          ┌─────────────────────────────────────────────────┐
          │  iceberg_catalog.bronze.crypto_trades           │
          │  (Nhiều file Parquet nhỏ vài chục KB phát sinh) │
          └────────────────────────┬────────────────────────┘
                                   │
                ĐIỀU PHỐI BẢO TRÌ BỞI DAGSTER / CRON
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         ▼                         ▼                         ▼
   [THAO TÁC 1]              [THAO TÁC 2]              [THAO TÁC 3]
Bin-Packing Compaction     Expire Old Snapshots     Remove Orphan Files
(Gộp file nhỏ -> 128MB)   (Xóa snapshot > 7 ngày)   (Dọn file rác MinIO)
         │                         │                         │
         └─────────────────────────┼─────────────────────────┘
                                   │
                                   ▼
          ┌─────────────────────────────────────────────────┐
          │  BẢNG BRONZE SAU BẢO TRÌ ĐÃ ĐƯỢC TỐI ƯU         │
          │  (Số file giảm 45%, Tốc độ đọc tăng 2.10x)      │
          └─────────────────────────────────────────────────┘
```

---

### Thao Tác 1: Cấu Hình Bảng Iceberg Tối Ưu Từ Khi Khởi Tạo
* **Tệp mã nguồn:** [src/batch_layer/iceberg_utils.py](file:///c:/HCMUTE/nam4ki1/tlcn/src/batch_layer/iceberg_utils.py)
* **Vị trí code:** Dòng 68 – dòng 93
* **Bảng mục tiêu:** `iceberg_catalog.bronze.crypto_trades`
* **Vị trí lưu trữ dữ liệu:** MinIO S3 bucket `s3://warehouse/bronze/crypto_trades/`

```sql
CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.crypto_trades (
    trade_id         BIGINT        NOT NULL  COMMENT 'Aggregate Trade ID từ Binance — khóa dedup',
    symbol           STRING        NOT NULL  COMMENT 'Cặp giao dịch uppercase: BTCUSDT, ETHUSDT...',
    price            DOUBLE        NOT NULL  COMMENT 'Giá khớp lệnh',
    quantity         DOUBLE        NOT NULL  COMMENT 'Khối lượng aggregate trade',
    trade_time       BIGINT        NOT NULL  COMMENT 'Thời điểm khớp lệnh — epoch milliseconds UTC',
    trade_time_ts    TIMESTAMP     NOT NULL  COMMENT 'trade_time dạng Timestamp — dùng để partition',
    is_buyer_maker   BOOLEAN,
    ingestion_time   BIGINT,
    is_injected      BOOLEAN,
    fault_type       STRING,
    batch_run_id     STRING,
    bronze_written_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(trade_time_ts))
TBLPROPERTIES (
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes'    = '134217728',   -- 128 MB Target Size
    'write.distribution-mode'         = 'hash',
    'history.expire.max-snapshot-age-ms' = '604800000', -- 7 ngày retention
    'format-version'                  = '2'
)
```

**Tác dụng & Cơ chế:**
1. `PARTITIONED BY (days(trade_time_ts))`: Áp dụng **Hidden Partitioning** của Iceberg. Dữ liệu được tự động phân vào các thư mục theo từng ngày giao dịch (UTC). Khi truy vấn theo ngày, Spark chỉ đọc đúng partition tương ứng (Partition Pruning), loại bỏ việc quét toàn bộ Data Lake.
2. `write.target-file-size-bytes = 134217728` (128 MB): Thiết lập kích thước khối tệp Parquet lý tưởng. Đây là mốc cơ sở để trình nén Iceberg tính toán điểm cắt khi tiến hành gộp dữ liệu.
3. `format-version = 2`: Cho phép hỗ trợ Snapshot Isolation, Time Travel và cơ chế xóa/ghi lại dữ liệu ở cấp độ dòng (Row-level rewrite).

---

### Thao Tác 2: Chạy Bin-Packing Compaction (`rewrite_data_files`)
* **Tệp mã nguồn:** [src/batch_layer/compaction.py](file:///c:/HCMUTE/nam4ki1/tlcn/src/batch_layer/compaction.py)
* **Vị trí code:** Dòng 196 – dòng 206 trong hàm `run_bin_packing_compaction()`
* **Bảng mục tiêu:** `iceberg_catalog.bronze.crypto_trades`

```python
result = self.spark.sql(f"""
    CALL {CATALOG_NAME}.system.rewrite_data_files(
        table           => '{BRONZE_NAMESPACE}.{BRONZE_TABLE}',
        strategy        => 'binpack',
        options         => map(
            'target-file-size-bytes',             '{target_file_size_bytes}',   -- 134,217,728 (128 MB)
            'min-file-size-bytes',                '{min_file_size_bytes}',      -- 67,108,864 (64 MB)
            'max-concurrent-file-group-rewrites', '{MAX_CONCURRENT_WRITES}'     -- 2 nhóm tệp đồng thời
        )
    )
""").collect()[0]
```

**Tác dụng & Cơ chế:**
* Gọi thủ tục hệ thống của Iceberg (`CALL system.rewrite_data_files`).
* Thuật toán **Bin-Packing** quét qua từng partition trong ngày. Những file nhỏ hơn ngưỡng `min-file-size-bytes` (64 MB) sẽ được gom vào các "thùng chứa" (bins).
* Spark đọc song song các nhóm tệp nhỏ này lên bộ nhớ và ghi ra các tệp Parquet mới có kích thước đạt xấp xỉ 128 MB.
* Quá trình này tạo ra một Snapshot mới chứa danh sách các tệp 128 MB mới và đánh dấu xóa các tệp nhỏ cũ một cách nguyên tử (**Zero Downtime** — các truy vấn đọc tại thời điểm compaction vẫn hoạt động bình thường trên snapshot hiện tại).

---

### Thao Tác 3: Thu Hồi Dung Lượng Bằng Cách Hết Hạn Snapshot Cũ (`expire_snapshots`)
* **Tệp mã nguồn:** [src/batch_layer/compaction.py](file:///c:/HCMUTE/nam4ki1/tlcn/src/batch_layer/compaction.py)
* **Vị trí code:** Dòng 372 – dòng 378 trong hàm `expire_old_snapshots()`
* **Bảng mục tiêu:** `iceberg_catalog.bronze.crypto_trades`

```python
result = self.spark.sql(f"""
    CALL {CATALOG_NAME}.system.expire_snapshots(
        table            => '{BRONZE_NAMESPACE}.{BRONZE_TABLE}',
        older_than       => TIMESTAMP '{datetime.fromtimestamp(cutoff_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}',
        retain_last      => {retain_last_n}   -- Luôn giữ lại tối thiểu 3 snapshots gần nhất
    )
""").collect()[0]
```

**Tác dụng & Cơ chế:**
* Mặc dù các tệp nhỏ đã được gộp ở Thao tác 2, Iceberg vẫn lưu giữ chúng trên MinIO để phục vụ tính năng **Time Travel** (cho phép truy vấn dữ liệu tại thời điểm quá khứ).
* Thủ tục `expire_snapshots` định kỳ loại bỏ các Snapshot cũ hơn 7 ngày (`older_than_days=7`), đồng thời xóa hẳn các tệp Parquet nhỏ cũ không còn được tham chiếu bởi bất kỳ Snapshot hợp lệ nào, thu hồi dung lượng lưu trữ trên MinIO S3. Tham số `retain_last=3` bảo đảm không bao giờ xóa nhầm các snapshot mới tạo.

---

### Thao Tác 4: Dọn Dẹp Tệp Mồ Côi Không Được Quản Lý (`remove_orphan_files`)
* **Tệp mã nguồn:** [src/batch_layer/compaction.py](file:///c:/HCMUTE/nam4ki1/tlcn/src/batch_layer/compaction.py)
* **Vị trí code:** Dòng 423 – dòng 428 trong hàm `remove_orphan_files()`
* **Bảng mục tiêu:** `iceberg_catalog.bronze.crypto_trades`

```python
result = self.spark.sql(f"""
    CALL {CATALOG_NAME}.system.remove_orphan_files(
        table      => '{BRONZE_NAMESPACE}.{BRONZE_TABLE}',
        older_than => TIMESTAMP '{cutoff_str}'
    )
""").collect()
```

**Tác dụng & Cơ chế:**
* Trong môi trường phân tán 24/7, nếu một Spark batch job bị ngắt đột ngột (do OOM, container restart, lỗi mạng), các file Parquet có thể đã được ghi xuống MinIO nhưng chưa kịp thực hiện commit vào Iceberg Metadata. Các file này được gọi là **Tệp mồ côi (Orphan files)**.
* Thủ tục `remove_orphan_files` so sánh danh sách tệp thực tế nằm trong bucket MinIO với danh sách tệp được quản lý trong metadata snapshot của Iceberg. Những tệp nằm ngoài metadata và có thời gian tạo cũ hơn 3 ngày (`older_than_days=3`) sẽ bị xóa bỏ an toàn, tránh phình to dung lượng vô ích.

---

### Thao Tác 5: Tích Hợp Pipeline Bảo Trì Thống Nhất
* **Tệp mã nguồn:** [src/batch_layer/maintenance.py](file:///c:/HCMUTE/nam4ki1/tlcn/src/batch_layer/maintenance.py)
* **Vị trí code:** Dòng 65 – dòng 94 trong hàm `run_table_maintenance()`

```python
# 1. Dọn dẹp Snapshots cũ (> 7 ngày)
results["steps"]["expire_snapshots"] = job.expire_old_snapshots(
    older_than_days=expire_older_than_days,
    retain_last_n=retain_last_n_snapshots,
)

# 2. Xóa các tệp mồ côi (> 3 ngày)
results["steps"]["remove_orphan_files"] = job.remove_orphan_files(
    older_than_days=orphan_older_than_days
)

# 3. Gộp và tối ưu Metadata Manifests
if rewrite_manifests:
    results["steps"]["rewrite_manifests"] = job.rewrite_manifests()
```

**Tác dụng:**
* Đóng gói toàn bộ các tác vụ bảo trì riêng lẻ thành một lệnh thực thi duy nhất `run_table_maintenance()`, có ghi log thời gian thực thi, thống kê số file đã xóa và xử lý lỗi mềm (*graceful error handling*).

---

### Thao Tác 6: Điều Phối Tự Động Hóa Qua Dagster & Tối Ưu ClickHouse
* **Tệp mã nguồn:** [dagster_project/repository.py](file:///c:/HCMUTE/nam4ki1/tlcn/dagster_project/repository.py)
* **Vị trí code:** Dòng 252 – dòng 313 (Asset `iceberg_small_files_compaction`) và dòng 343 – dòng 347 (Schedule `compaction_schedule`)

```python
@asset(
    group_name="lakehouse_maintenance",
    description="Run Iceberg full table maintenance and ClickHouse OPTIMIZE FINAL.",
)
def iceberg_small_files_compaction(context) -> Output[Dict[str, Any]]:
    # 1. Chạy bảo trì Iceberg trên MinIO
    from src.batch_layer.maintenance import run_table_maintenance
    spark = get_spark_session("Dagster-IcebergCompaction")
    maint_result = run_table_maintenance(spark=spark, target_file_size_mb=128)

    # 2. Tối ưu hóa các part vật lý trên ClickHouse OLAP Serving
    client.command("OPTIMIZE TABLE lakehouse.batch_agg FINAL")
    client.command("OPTIMIZE TABLE lakehouse.speed_agg FINAL")
    ...

# Lập lịch chạy tự động mỗi 2 giờ
compaction_schedule = ScheduleDefinition(
    job=iceberg_compaction_job,
    cron_schedule="0 */2 * * *",
)
```

**Tác dụng:**
* Chuyển hóa toàn bộ quy trình thành một Software-Defined Asset (SDA) trong hệ thống Dagster Orchestration.
* Cơ chế **Bảo trì kép (Dual Maintenance):** Vừa gộp file Iceberg trên Data Lake (MinIO), vừa gửi lệnh `OPTIMIZE TABLE FINAL` sang ClickHouse để sáp nhập các physical parts phân mảnh, bảo đảm hiệu năng truy vấn cho cả tầng Lưu trữ lẫn tầng Phục vụ.

---

## 3. Benchmark 3: Đánh Giá Hiệu Quả Compaction Định Lượng (RQ3)

Để chứng minh tính hiệu quả của các giải pháp trên bằng số liệu khoa học, đồ án triển khai kịch bản thực nghiệm **Benchmark 3**.

### 3.1. Vị Trí Triển Khai
* **Mã nguồn thực thi:** [scripts/benchmarks/bench_compaction.py](file:///c:/HCMUTE/nam4ki1/tlcn/scripts/benchmarks/bench_compaction.py)
* **Log kết quả thô (CSV):** `results/logs/bench_compaction.csv`
* **Biểu đồ trực quan hóa:** `results/plots/benchmark3_compaction.png`

### 3.2. Phương Pháp Đo Lường Thực Nghiệm
Quy trình đo lường được thực thi tự động qua hàm `run_real_benchmark()` (dòng 153 – 236 của `bench_compaction.py`):
1. **Pha TRƯỚC Compaction (Phase BEFORE):**
   * Quét bảng metadata `iceberg_catalog.bronze.crypto_trades.files` để ghi nhận số lượng file Parquet hiện có.
   * Chạy lặp lại $N=20$ lần câu lệnh phân tích tổng hợp trên bảng Bronze:
     ```sql
     SELECT symbol, count(*) as cnt, avg(price) as avg_price 
     FROM iceberg_catalog.bronze.crypto_trades 
     WHERE symbol = 'BTCUSDT' 
     GROUP BY symbol;
     ```
   * Ghi nhận thời gian truy vấn trung bình $\text{Latency}_{\text{BEFORE}}$ (ms).
2. **Kích hoạt Compaction:**
   * Kích hoạt `CompactionJob(spark).run_bin_packing_compaction()`.
3. **Pha SAU Compaction (Phase AFTER):**
   * Chạy lại chính xác câu lệnh SELECT phân tích trên với cùng điều kiện lặp lại $N=20$ lần.
   * Ghi nhận thời gian truy vấn trung bình $\text{Latency}_{\text{AFTER}}$ (ms) và số lượng file sau gộp.
4. **Xác định tỷ số gia tốc (Speedup Ratio):**
   $$\text{Speedup Ratio} = \frac{\text{Latency}_{\text{BEFORE}}}{\text{Latency}_{\text{AFTER}}}$$

### 3.3. Kết Quả Thực Nghiệm Thực Tế (Mode Real)

Dữ liệu thực nghiệm thu được từ các lần chạy thực tế trên môi trường container:

| Chỉ số thực nghiệm | Trước Compaction (BEFORE) | Sau Compaction (AFTER) | Mức độ cải thiện |
| :--- | :---: | :---: | :---: |
| **Số lượng Data Files** | **20 tệp** (phân mảnh) | **11 tệp** (đã gộp) | **Giảm 45.0%** |
| **Kích thước file trung bình** | $0.15 \text{ MB}$ | $0.28 \text{ MB}$ | Tăng độ gom cụm |
| **Thời gian truy vấn đọc (ms)** | **$5,340 \text{ ms}$** | **$2,549 \text{ ms}$** | **Giảm 52.3% độ trễ** |
| **Hệ số gia tốc (Speedup Ratio)** | — | — | **$2.10\times$ (Nhanh hơn gấp 2 lần)** |

> [!IMPORTANT]
> **Kết luận cho RQ3:**  
> Việc ứng dụng chiến lược Bin-Packing Compaction trên Apache Iceberg giúp giải quyết triệt để vấn đề Small File phát sinh từ streaming, giảm **45%** số lượng tệp lưu trữ và mang lại tốc độ phản hồi truy vấn nhanh hơn gấp **$2.10$ lần**.

