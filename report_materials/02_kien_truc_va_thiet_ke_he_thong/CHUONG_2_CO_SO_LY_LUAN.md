# CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ NỀN TẢNG CÔNG NGHỆ CỐT LÕI

> **Tài liệu tham khảo học thuật chính thức**  
> **Đề tài:** Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa  
> *(Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring)*  
> **Chuyên ngành:** Kỹ thuật Dữ liệu (Data Engineering) — Khoa Công Nghệ Thông Tin (HCMUTE)

---

## 2.1. TIẾN HÓA CỦA KIẾN TRÚC DỮ LIỆU DOANH NGHIỆP

Trong lịch sử phát triển của ngành Kỹ thuật Dữ liệu (Data Engineering), các mô hình kiến trúc lưu trữ và xử lý thông tin đã trải qua ba làn sóng tiến hóa mang tính cách mạng:

```
┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│     DATA WAREHOUSE      │     │        DATA LAKE        │     │     DATA LAKEHOUSE      │
│     (Thế hệ thứ 1)      │ ──> │      (Thế hệ thứ 2)     │ ──> │      (Thế hệ thứ 3)     │
│  • Cấu trúc cứng nhắc   │     │  • Schema-on-Read       │     │  • ACID trên S3/Object  │
│  • Chi phí lưu trữ cao  │     │  • Giá rẻ nhưng "Swamp" │     │  • Metadata phân tầng   │
│  • Khó scale Unstructured│    │  • Thiếu ACID / Update  │     │  • Hỗ trợ cả Batch/Stream│
└─────────────────────────┘     └─────────────────────────┘     └─────────────────────────┘
```

### 2.1.1. Kho Dữ Liệu Truyền Thống (Data Warehouse — DWH)
Ra đời từ thập niên 1980–1990 (đại diện bởi Teradata, Oracle, sau này là Snowflake, Google BigQuery), Data Warehouse được thiết kế xoay quanh mô hình quan hệ có cấu trúc cao (Structured Data).
* **Đặc tính:** Áp dụng nguyên lý *Schema-on-Write* (dữ liệu phải được làm sạch, biến đổi qua các pipeline ETL trước khi nạp vào kho).
* **Ưu điểm:** Tối ưu hóa cực cao cho các truy vấn phân tích nghiệp vụ (BI/Reporting), đảm bảo tuyệt đối các tính chất giao dịch ACID (*Atomicity, Consistency, Isolation, Durability*).
* **Hạn chế:**
  1. *Chi phí mở rộng đắt đỏ:* Lưu trữ và tính toán bị gắn kết chặt chẽ (Coupled Storage & Compute).
  2. *Không linh hoạt:* Không thể lưu trữ dữ liệu phi cấu trúc hoặc bán cấu trúc (JSON, logs, hình ảnh, stream ticks).
  3. *Độ trễ cao:* Pipeline ETL theo lô (Batch ETL) mất từ vài giờ đến hàng ngày để làm mới dữ liệu.

### 2.1.2. Hồ Dữ Liệu (Data Lake)
Khoảng năm 2010, làn sóng Big Data (Apache Hadoop HDFS, AWS S3, MinIO) đã dẫn tới sự xuất hiện của Data Lake.
* **Đặc tính:** Áp dụng nguyên lý *Schema-on-Read*, phân tách hoàn toàn giữa lưu trữ giá rẻ (Object Storage) và tính toán phân tán (Apache Spark, Presto). Dữ liệu thô từ nhiều nguồn được đổ trực tiếp vào hồ dưới các định dạng file mở (CSV, JSON, Apache Parquet).
* **Hạn chế nghiêm trọng (Bài toán "Data Swamp"):**
  1. *Thiếu tính giao dịch ACID:* Khi có tiến trình đang ghi dữ liệu mà bị lỗi giữa chừng, hồ dữ liệu sẽ rơi vào trạng thái bẩn (Dirty/Partial Data), gây sai lệch kết quả phân tích.
  2. *Không thể cập nhật hoặc xoá có chọn lọc:* Việc sửa một bản ghi đòi hỏi phải quét và ghi đè lại toàn bộ partition hoặc thư mục lớn.
  3. *Vấn nạn tệp nhỏ (Small File Problem):* Luồng dữ liệu streaming micro-batch sinh ra hàng triệu tệp Parquet kích thước dưới vài megabyte, làm nghẽn Metadata NameNode hoặc S3 API Calls.

### 2.1.3. Mô hình Lai Hiện Đại: Data Lakehouse
Data Lakehouse là mô hình kiến trúc thế hệ thứ 3, kết hợp hoàn hảo những ưu điểm tốt nhất của Data Warehouse và Data Lake:
$$\text{Data Lakehouse} = \text{Data Lake (Lưu trữ giá rẻ, mở, linh hoạt)} + \text{Data Warehouse (Giao dịch ACID, Metadata quản trị)}$$

Nhờ các định dạng bảng mở tiên tiến (Open Table Formats) như **Apache Iceberg**, **Delta Lake**, Data Lakehouse triển khai một tầng quản lý siêu dữ liệu (Metadata Layer) phân tầng nằm trực tiếp trên nền Object Storage (MinIO, S3). Kiến trúc này cho phép:
* Đảm bảo tính nhất quán ACID mức Snapshot Isolation.
* Cho phép tiến trình ghi lô (Batch) và ghi luồng (Streaming) hoạt động đồng thời trên cùng một bảng dữ liệu mà không gây tranh chấp hoặc khoá chết (Zero Concurrency Conflicts).
* Cung cấp khả năng quay ngược thời gian (*Time Travel*) và tiến hóa lược đồ dữ liệu (*Schema Evolution*) an toàn tuyệt đối.

---

## 2.2. KIẾN TRÚC LAMBDA (LAMBDA ARCHITECTURE)

### 2.2.1. Nguyên Lý Thiết Kế Của Nathan Marz
Kiến trúc Lambda được kỹ sư Nathan Marz đề xuất năm 2011 nhằm giải quyết triệt để sự xung đột giữa **Độ trễ truy vấn thời gian thực (Low Latency)** và **Tính chính xác toàn vẹn của dữ liệu (Consistency & Fault-Tolerance)** trong các hệ thống Big Data quy mô cực lớn.

Theo Nathan Marz, bài toán tổng quát của một hệ thống xử lý dữ liệu được định nghĩa bằng một hàm toán học đơn giản:
$$\text{Query} = \text{Function}(\text{All Data})$$

Tuy nhiên, do khối lượng toàn bộ dữ liệu ($\text{All Data}$) quá lớn (hàng Terabyte đến Petabyte), việc chạy hàm này trên toàn bộ lịch sử trong thời gian thực là điều bất khả thi về mặt vật lý. Kiến trúc Lambda giải quyết bài toán này bằng cách phân rã thành ba tầng độc lập:

$$\text{Query} = \text{Merge}\Big(\text{Batch View}, \text{Speed View}\Big)$$

```
                               ┌──────────────────────────────────────────────────┐
                               │                  DATA SOURCES                    │
                               │        (Binance WebSocket Live Stream)           │
                               └──────────────┬──────────────────┬────────────────┘
                                              │                  │
                                   ┌──────────▼──────────┐  ┌────▼────────────────┐
                                   │     BATCH LAYER     │  │   SPEED LAYER       │
                                   │  (Immutable Master) │  │ (Real-Time Approx.) │
                                   │    Apache Spark     │  │  Spark Streaming    │
                                   │  + Apache Iceberg   │  │   + Redis / ClickH  │
                                   │   [Ground Truth]    │  │   [Provisional]     │
                                   └──────────┬──────────┘  └────┬────────────────┘
                                              │                  │
                                              │  Batch Watermark │
                                              │  (Mốc phân luồng)│
                                              ▼                  ▼
                               ┌──────────────────────────────────────────────────┐
                               │                  SERVING LAYER                   │
                               │        (Auto-Correcting Query Merger)            │
                               │                                                  │
                               │  • [T_start, Watermark] : Lấy từ Batch View      │
                               │  • (Watermark, T_end]   : Lấy từ Speed View      │
                               │  • Khử trùng lặp tại biên Watermark (Zero Dup)   │
                               │  • Tự động đối soát sai số: ΔV% và ε_VWAP        │
                               └──────────────────────┬───────────────────────────┘
                                                      │
                                                      ▼
                                       ┌───────────────────────────┐
                                       │        USERS / BI         │
                                       │   (Streamlit / REST API)  │
                                       └───────────────────────────┘
```

### 2.2.2. Chi Tiết Vai Trò Của Ba Tầng Trong Kiến Trúc

#### 1. Tầng Xử Lý Lô (Batch Layer)
* **Chức năng:** Quản lý kho lưu trữ dữ liệu gốc bất biến (Master Dataset). Dữ liệu một khi đã ghi vào Master Dataset là bất biến (*Append-only*), không bao giờ bị ghi đè trực tiếp hay chỉnh sửa.
* **Đặc tính:** Định kỳ thực thi các tác vụ xử lý hàng loạt trên toàn bộ dữ liệu lịch sử để sinh ra các **Batch Views**.
* **Độ tin cậy:** Batch Layer là nguồn chuẩn xác tối cao (**Ground Truth**). Mọi sai lệch, dữ liệu trễ, hoặc dữ liệu trùng lặp từ mạng đều được Batch Layer tính toán lại chuẩn xác 100%.

#### 2. Tầng Xử Lý Tốc Độ (Speed Layer)
* **Chức năng:** Bù đắp khoảng trễ thời gian (Latency Gap) giữa các chu kỳ chạy của Batch Layer. Trong khi Batch Layer đang xử lý dữ liệu của các giờ trước, Speed Layer sẽ tiêu thụ luồng sự kiện mới nhất (Delta Stream) phát sinh trong vài giây vừa qua.
* **Đặc tính:** Tối ưu hóa tối đa cho độ trễ xử lý ($\text{SLA} < 5$ giây). Để đạt được tốc độ này, Speed Layer chấp nhận các thuật toán gần đúng (Approximate Algorithms) hoặc có thể tạm thời thiếu dữ liệu trễ (Late Events).
* **Trạng thái:** Dữ liệu Speed Layer là tạm thời (**Provisional**). Khi chu kỳ Batch kế tiếp hoàn tất, các nến Provisional này sẽ bị thay thế hoàn toàn bởi nến Batch chuẩn xác.

#### 3. Tầng Phục Vụ (Serving Layer)
* **Chức năng:** Cung cấp giao diện truy vấn thống nhất cho người dùng cuối. 
* **Thuật toán Query Merger:** Nhận yêu cầu truy vấn trong khoảng thời gian $[T_{\text{start}}, T_{\text{end}}]$, đối chiếu với mốc thời gian hoàn tất gần nhất của Batch Layer (gọi là **Batch Watermark** $W$):
  * **Trường hợp 1 (Dữ liệu hoàn toàn lịch sử: $T_{\text{end}} \le W$):** Đọc 100% từ Batch View (trạng thái `RECONCILED`).
  * **Trường hợp 2 (Dữ liệu hoàn toàn tức thời: $T_{\text{start}} > W$):** Đọc 100% từ Speed View (trạng thái `PROVISIONAL`).
  * **Trường hợp 3 (Dữ liệu giao thoa vắt ngang mốc Watermark: $T_{\text{start}} \le W < T_{\text{end}}$):** Cắt đôi cửa sổ truy vấn, ghép phần quá khứ từ Batch View với phần tương lai từ Speed View, đồng thời khử trùng lặp (*Deduplication*) tại biên giao thoa để đảm bảo nguyên tắc không tính trùng khối lượng (**Zero Double-Counting**).

---

## 2.3. SO SÁNH ĐỐI SÁNH CÁC MÔ HÌNH KIẾN TRÚC DỮ LIỆU

Nhằm làm rõ cơ sở lựa chọn kiến trúc cho bài toán giám sát đối soát tài chính tiền mã hóa, bảng dưới đây đối sánh toàn diện giữa **Lambda Architecture**, **Kappa Architecture** (do Jay Kreps đề xuất) và **Medallion Lakehouse thuần**:

| Tiêu Chí So Sánh | Lambda Architecture (Đề tài lựa chọn) | Kappa Architecture | Medallion Lakehouse Thuần |
| :--- | :--- | :--- | :--- |
| **Triết lý cốt lõi** | Phân tách 2 luồng: Batch (Exact) + Speed (Approx) | Một luồng Stream duy nhất xử lý cho mọi bài toán | Pipeline tuần tự: Bronze $\to$ Silver $\to$ Gold |
| **Khả năng đối soát (Reconciliation)** | **Cực cao:** Cho phép đối sánh trực tiếp giữa Ground Truth và nến thời gian thực để đo độ lệch ($\Delta V_{\%}$, $\varepsilon_{\text{VWAP}}$) | **Kém:** Chỉ có một view duy nhất, không có chuẩn đối chiếu để biết luồng stream có bị lệch do rớt mạng hay không | **Trung bình:** Dựa trên các batch snapshot định kỳ, khó phục vụ truy vấn dưới 1 giây |
| **Độ phức tạp codebase** | Phải duy trì logic biến đổi ở 2 tầng (Batch & Streaming) | Một codebase duy nhất cho xử lý luồng | Một codebase SQL/dbt duy nhất |
| **Xử lý dữ liệu trễ (Late Data)** | Tự động "chữa lành" (Self-Healing) khi Batch quét lại | Phụ thuộc hoàn toàn vào Watermark buffer (dễ mất nếu trễ quá lâu) | Xử lý tốt trong các lần quét lịch sử |
| **Tái xử lý lịch sử (Reprocessing)** | Rất dễ dàng: Chạy lại Spark Batch trên Master Dataset (Iceberg) | Phức tạp: Phải replay lại toàn bộ Kafka topic từ offset 0 | Dễ dàng: Chạy lại dbt/Spark trên Bronze table |
| **Chi phí hạ tầng** | Cần tài nguyên cho cả Batch và Speed, nhưng tối ưu được chi phí lưu trữ nhờ Iceberg + MinIO | Chi phí retention Kafka rất lớn nếu muốn lưu lịch sử nhiều năm | Tiết kiệm tài nguyên nhất, nhưng đánh đổi độ trễ thời gian thực |
| **Use-case tối ưu** | **Giám sát giao dịch tài chính, phát hiện gian lận, đối soát sàn Crypto** | Xử lý log clickstream, telemetry IoT đơn giản | Báo cáo quản trị BI, phân tích kinh doanh cuối ngày |

> [!NOTE]
> **Kết luận lựa chọn:** Đối với thị trường tiền mã hóa, việc dữ liệu bị rớt mạng, mất kết nối WebSocket hoặc đến muộn là điều xảy ra thường xuyên. Nếu sử dụng Kappa thuần, các giao dịch đến trễ quá Watermark sẽ bị vứt bỏ vĩnh viễn và không bao giờ được bù đắp. Do đó, **Kiến trúc Lambda là lựa chọn khoa học và tối ưu nhất**, vì nó vừa đảm bảo được độ trễ tức thời cho Dashboard, vừa có chốt chặn Batch Layer để tự động phục hồi và kiểm toán chính xác 100%.

---

## 2.4. ĐỊNH DẠNG BẢNG MỞ (OPEN TABLE FORMATS) & APACHE ICEBERG

### 2.4.1. Sự Ra Đời Của Open Table Formats
Trước đây, hồ dữ liệu Hive (Hive Metastore) quản lý dữ liệu theo cấp độ thư mục (*Directory-level*). Khi một truy vấn thực thi, nó phải duyệt qua toàn bộ cây thư mục để tìm file Parquet. Điều này dẫn tới 3 điểm yếu chí mạng:
1. Không thể thực hiện cập nhật ở mức dòng (*Row-level ACID*).
2. Khi tiến trình streaming ghi tệp mới, người dùng truy vấn có thể đọc phải tệp ghi dở dang (*Dirty Read*).
3. Đổi tên thư mục partition trên Object Storage là tác vụ cực kỳ chậm chạp ($O(N)$ HTTP Requests).

Các định dạng bảng mở thế hệ mới (**Apache Iceberg**, **Delta Lake**, **Apache Hudi**) đã dịch chuyển cơ chế quản lý từ **cấp thư mục sang cấp tệp tin (File-level Metadata)** thông qua cấu trúc cây siêu dữ liệu phân tầng.

### 2.4.2. Kiến Trúc Cấu Trúc Siêu Dữ Liệu Phân Tầng Của Apache Iceberg
Apache Iceberg tổ chức bảng dữ liệu thành một cây phân cấp hình phễu 4 tầng chặt chẽ:

```
[Iceberg Catalog] (REST Catalog / MinIO)
       │
       ▼ trỏ tới file metadata mới nhất
┌──────────────────────────────────────────────┐
│       TABLE METADATA (v1.metadata.json)      │  <-- Lưu Schema, Partition Spec,
│                                              │      Danh sách Snapshot IDs
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│           MANIFEST LIST (snap-1.avro)        │  <-- Đại diện cho 1 Snapshot, lưu
│                                              │      danh sách Manifest Files
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│           MANIFEST FILE (manifest-1.avro)    │  <-- Lưu chi tiết từng Data File,
│                                              │      Partition bounds, Min/Max stats
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│         DATA FILES (.parquet on MinIO)       │  <-- Lưu trữ dữ liệu nhị phân cột,
│                                              │      kèm thống kê từ điển nội tại
└──────────────────────────────────────────────┘
```

1. **Iceberg Catalog:** Điểm liên lạc trung tâm (Single Source of Truth), quản lý con trỏ nguyên tử (*Atomic Pointer*) trỏ tới file `metadata.json` hiện hành.
2. **Table Metadata File (`*.metadata.json`):** Lưu trữ toàn bộ lược đồ bảng (*Schema*), chiến lược phân vùng (*Partition Spec*), và mảng ghi nhận lịch sử các Snapshot (*Snapshots Array*).
3. **Manifest List File (`*.avro`):** Đại diện cho một Snapshot cụ thể tại một thời điểm. Nó chứa danh sách các Manifest Files cấu thành nên Snapshot đó, kèm theo thông tin tổng hợp về phân vùng của từng manifest.
4. **Manifest File (`*.avro`):** Chứa danh sách cụ thể từng tệp dữ liệu (*Data File*). Với mỗi tệp, manifest lưu trữ siêu dữ liệu cực kỳ chi tiết: đường dẫn file, trạng thái (`ADDED`, `EXISTING`, `DELETED`), và đặc biệt là **chỉ số thống kê Min/Max của từng cột**. Nhờ đó, công cụ truy vấn có thể thực hiện kỹ thuật cắt tỉa tệp tin (**Data File Pruning**) mà không cần đọc nội dung file Parquet.

### 2.4.3. Các Tính Năng Nổi Bật Của Apache Iceberg

#### 1. Cách Ly Giao Dịch Bằng Snapshot (Snapshot Isolation & ACID)
Mỗi thao tác ghi (Append, Overwrite, Delete) trên bảng Iceberg đều sinh ra một **Snapshot mới** hoàn chỉnh một cách nguyên tử thông qua cơ chế *Optimistic Concurrency Control (OCC)*. Tiến trình đọc luôn đọc trên một Snapshot cố định, hoàn toàn độc lập với các tiến trình đang ghi song song.

#### 2. Phân Vùng Ẩn (Hidden Partitioning)
Trong Hive truyền thống, nếu bảng phân vùng theo ngày `dt=2026-10-27`, người dùng bắt buộc phải nhớ lọc theo cột `dt`. Nếu người dùng lọc theo cột thời gian gốc `WHERE trade_time >= '2026-10-27 00:00:00'`, Hive sẽ quét toàn bộ bảng!
Apache Iceberg giải quyết vấn đề này bằng **Hidden Partitioning**: Hệ thống tự động chuyển đổi giữa trường `trade_time` và phép ánh xạ phân vùng `days(trade_time)`. Người dùng chỉ cần truy vấn theo trường nghiệp vụ tự nhiên, Iceberg sẽ tự động cắt tỉa các phân vùng không liên quan.

#### 2.4.4. Vấn Nạn Tệp Nhỏ (Small File Problem) & Cơ Chế Compaction
* **Bản chất vấn đề:** Trong Speed Layer hoặc Streaming Ingestion nạp vào Bronze Iceberg, các micro-batch được commit sau mỗi vài giây/phút. Mỗi commit sinh ra các file Parquet nhỏ (kích thước chỉ vài chục Kilobyte đến vài Megabyte). Sau 1 ngày, bảng Bronze có thể tích tụ hàng chục nghìn file nhỏ. Khi truy vấn:
  * Chi phí gửi yêu cầu HTTP `GET` đến Object Storage (MinIO/S3) bùng nổ.
  * Chi phí đọc siêu dữ liệu và giải nén header/footer của từng file Parquet làm độ trễ truy vấn tăng vọt từ vài mili-giây lên hàng chục giây.
* **Giải pháp Compaction (Bin-Packing):**
  Apache Iceberg cung cấp thủ tục hệ thống `rewrite_data_files`:
  * Gom các tệp nhỏ có cùng phân vùng thành các nhóm có tổng kích thước đạt chuẩn (Target File Size = 128MB).
  * Đọc song song và ghi lại thành các tệp Parquet lớn được tối ưu hóa.
  * Tạo một Snapshot mới thay thế các tệp nhỏ bằng các tệp lớn một cách nguyên tử mà không làm gián đoạn các truy vấn đang chạy.

---

## 2.5. XỬ LÝ LUỒNG DỮ LIỆU (STREAM PROCESSING) VỚI SPARK STRUCTURED STREAMING

### 2.5.1. Mô Hình Xử Lý Micro-Batch
Apache Spark Structured Streaming xem luồng dữ liệu liên tục như một **Bảng Dữ Liệu Vô Hạn (Unbounded Table)** được nối thêm liên tục (*Continuously Appended*).

```
   Luồng sự kiện mới đến      ──>    │ t1 │ t2 │ t3 │ t4 │ t5 │ t6 │ ... (Unbounded Stream)
                                            │
                                            ▼ Micro-batch Engine (Trigger: 5s)
                                     ┌────────────────────────────────┐
                                     │  Spark DataFrame Transformation│
                                     └────────────────┬───────────────┘
                                                      │
                                                      ▼
   Ghi kết quả vào Sink       ──>    ClickHouse (speed_agg) & Redis Hot Cache
```

Cứ sau mỗi chu kỳ kích hoạt ($\text{Trigger Interval} = 5$ giây), bộ lập lịch của Spark sẽ cắt các sự kiện tích lũy trong 5 giây vừa qua thành một DataFrame nhỏ và đẩy qua đồ thị tính toán phân tán DAG. Mô hình này kết hợp được thông lượng cực cao (High Throughput) của engine Spark với độ trễ thấp (Sub-second Latency).

### 2.5.2. Cửa Sổ Thời Gian Sự Kiện (Event-Time Tumbling Window)
Để biểu diễn giá tiền mã hóa dưới dạng nến OHLCV, hệ thống áp dụng kỹ thuật cửa sổ thời gian không chồng chập (**Tumbling Window**) có độ rộng 1 phút:
$$\text{Window}_k = \Big[k \times 60.000\text{ ms}, (k+1) \times 60.000\text{ ms}\Big)$$

Các giao dịch được gom nhóm dựa trên thời điểm thực sự xảy ra trên sàn Binance (**`trade_time`**), chứ không dựa trên thời điểm máy chủ nhận được tin (**`processing_time`**).

### 2.5.3. Cơ Chế Quản Lý Dữ Liệu Đến Muộn (Watermarking)
Để ngăn chặn việc bộ nhớ RAM bị tràn khi phải chờ đợi các giao dịch đến muộn vô hạn trong quá khứ, Spark Structured Streaming sử dụng cơ chế **Watermark**:
$$\text{Watermark Time} = \max(\text{Event Time đã thấy}) - \text{Độ trễ cho phép (Delay Threshold)}$$

* Trong đề tài, ngưỡng trễ được cấu hình là **1 phút**.
* Nếu một sự kiện giao dịch có $\text{trade\_time} < \text{Watermark Time}$, Spark sẽ coi sự kiện này đã quá hạn và **chủ động loại bỏ (Drop)** khỏi bộ đệm cửa sổ để giải phóng RAM cho StateStore.
* Sự kiện bị loại bỏ này sau đó sẽ được Tầng Batch (Batch Layer) thu thập và bù đắp lại trong chu kỳ xử lý lô tiếp theo.

### 2.5.4. Quản Lý Trạng Thái Bền Vững Với RocksDB State Store
Khi chạy các phép toán tổng hợp trạng thái (Stateful Aggregations) 24/7, Spark mặc định lưu trữ state trong bộ nhớ Heap của Java Virtual Machine (JVM). Khi số lượng coin tăng lên (10 coin, hàng triệu giao dịch), bộ nhớ Heap sẽ bị phân mảnh và gây ra hiện tượng nghẽn do dọn rác (GC Pauses) dẫn tới crash container.
* **Giải pháp cải tiến:** Cấu hình **RocksDB State Store Provider** (`RocksDBStateStoreProvider`). RocksDB là một cơ sở dữ liệu nhúng dạng Key-Value được viết bằng C++, quản lý bộ nhớ ngoài Heap (*Off-heap memory*) và tự động ghi tràn state xuống đĩa cứng (volume checkpoint). Cấu hình này giúp Spark Streaming duy trì mức chiếm dụng RAM ổn định và vận hành bền bỉ 24/7.

---

## 2.6. HỆ QUẢN TRỊ CƠ SỞ DỮ LIỆU PHÂN TÍCH TỐC ĐỘ CAO (OLAP) — CLICKHOUSE

### 2.6.1. Lưu Trữ Dạng Cột (Column-Oriented Storage)
Khác với các cơ sở dữ liệu quan hệ truyền thống (PostgreSQL, MySQL) lưu trữ dữ liệu theo từng dòng (Row-oriented), ClickHouse tổ chức lưu trữ toàn bộ giá trị của cùng một cột nằm liên tục nhau trên đĩa cứng:

```
Row-Oriented (PostgreSQL):    [ID_1, Symbol_1, Price_1, Vol_1] [ID_2, Symbol_2, Price_2, Vol_2] ...
Column-Oriented (ClickHouse): [ID_1, ID_2, ...] [Symbol_1, Symbol_2, ...] [Price_1, Price_2, ...]
```

* **Ưu điểm vượt trội cho phân tích tài chính:** Khi cần tính tổng khối lượng giao dịch hoặc vẽ biểu đồ giá trung bình VWAP, ClickHouse chỉ cần đọc đúng cột `price` và `volume`, bỏ qua hoàn toàn các cột khác. Điều này giúp giảm lượng I/O đọc đĩa từ 5 đến 20 lần.
* **Tỷ lệ nén cực cao (Data Compression):** Vì các giá trị trong cùng một cột có cùng kiểu dữ liệu và thường có tính tương đồng cao, ClickHouse áp dụng các thuật toán nén chuyên dụng (LZ4, ZSTD, DoubleDelta) giúp dung lượng bảng giảm từ 3 đến 5 lần so với dữ liệu thô.

### 2.6.2. Động Cơ Bảng ReplacingMergeTree Và Từ Khóa `FINAL`
Trong Serving Layer của đề tài, bảng `lakehouse.batch_agg` và `lakehouse.speed_agg` được khai báo bằng động cơ **ReplacingMergeTree**:
```sql
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (symbol, window_start);
```

* **Nguyên lý hoạt động:** ReplacingMergeTree cho phép nạp dữ liệu liên tục. Khi ClickHouse tiến hành nén các phần dữ liệu vật lý (Data Parts) ngầm dưới nền, nếu phát hiện nhiều dòng có cùng khoá sắp xếp `(symbol, window_start)`, nó sẽ tự động chỉ giữ lại dòng có `created_at` lớn nhất và xoá bỏ các phiên bản cũ.
* **Truy vấn với từ khóa `FINAL`:** Để đảm bảo tính chính xác tức thì cho Serving API ngay cả khi tiến trình merge ngầm chưa diễn ra, câu lệnh truy vấn sử dụng mệnh đề `SELECT ... FROM table FINAL`. ClickHouse sẽ thực hiện việc hợp nhất các bản ghi trùng lặp ngay trong bộ nhớ trước khi trả về kết quả cho Query Merger.

---

## 2.7. QUẢN TRỊ CHẤT LƯỢNG DỮ LIỆU CHỦ ĐỘNG (DATA QUALITY & CHAOS ENGINEERING)

### 2.7.1. Hợp Đồng Dữ Liệu (Data Contracts)
Data Contract là một cam kết kỹ thuật chuẩn hóa giữa bên phát dữ liệu (Data Producer — Ingestion Layer) và bên tiêu thụ (Data Consumer — Speed/Batch Layer). Mọi sự kiện giao dịch phát sinh từ sàn Binance đều phải tuân thủ nghiêm ngặt theo JSON Schema chuẩn:
$$\mathcal{E} = \Big\langle \text{trade\_id} \in \mathbb{N}^+, \text{symbol} \in \mathbb{S}, \text{price} \in \mathbb{R}^+, \text{quantity} \in \mathbb{R}^+, \text{trade\_time} \in \mathbb{N}^+ \Big\rangle$$

### 2.7.2. Mô Hình Cách Ly Dữ Liệu Lỗi (Quarantine Pattern)
Trong các hệ sinh thái dữ liệu cũ, khi gặp một bản ghi bị hỏng định dạng (giá âm, rỗng trường), pipeline thường chọn một trong hai cách xử lý tiêu cực:
1. *Fail-Fast:* Ném Exception và làm sập toàn bộ luồng xử lý $\to$ Gây gián đoạn dịch vụ hệ thống.
2. *Silent Drop:* Vứt bỏ bản ghi trong im lặng $\to$ Kỹ sư dữ liệu không phát hiện được sự cố để khắc phục.

Đề tài áp dụng mô hình **Quarantine Pattern** (Cổng kiểm soát chất lượng dữ liệu — Data Quality Gate):
* Bản ghi hợp lệ tiếp tục đi vào tầng Silver $\to$ Gold.
* Bản ghi vi phạm ràng buộc (`price <= 0`, `quantity <= 0`, thiếu trường) được tách ra, đóng gói kèm nhãn lỗi `dq_error` và tự động ghi vào bảng cách ly độc lập **`lakehouse.dq_quarantine`**.
* Pipeline tiếp tục vận hành thông suốt 100% (**Zero-Crash Pipeline Guarantee**) trong khi đội ngũ vận hành vẫn có đầy đủ thông tin để truy vết lỗi.

### 2.7.3. Kỹ Thuật Tiêm Lỗi Có Kiểm Soát (Chaos Engineering & Fault Injection)
Để đánh giá khoa học độ bền vững và năng lực tự phục hồi (*Self-Healing*) của kiến trúc Lambda, đề tài thiết kế module **Fault Injector** chủ động tiêm 4 loại lỗi phân tán điển hình vào luồng dữ liệu trước khi đẩy vào Kafka topic:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       MODULE FAULT INJECTOR (Ground Truth)                  │
├──────────────────────┬───────────────────────┬──────────────────────────────┤
│ Loại Lỗi Tiêm        │ Hành Vi Kỹ Thuật      │ Bài Toán Thực Tế Mô Phỏng    │
├──────────────────────┼───────────────────────┼──────────────────────────────┤
│ 1. Duplicate         │ Nhân bản sự kiện      │ Retry mạng At-Least-Once     │
│                      │ giữ nguyên trade_id   │ hoặc giáp ranh WebSocket/REST│
├──────────────────────┼───────────────────────┼──────────────────────────────┤
│ 2. Late Data         │ Giảm trade_time lùi lại│ Mạng lag, lệnh đến sau khi  │
│                      │ từ 1 - 5 phút         │ streaming watermark đã chốt  │
├──────────────────────┼───────────────────────┼──────────────────────────────┤
│ 3. Out-of-Order      │ Giữ lại sự kiện trong │ Xử lý song song qua nhiều    │
│                      │ buffer và phát sau    │ partition Kafka không đều    │
├──────────────────────┼───────────────────────┼──────────────────────────────┤
│ 4. Schema Invalid    │ Gán price âm hoặc     │ Lỗi serializer, hỏng byte gói│
│                      │ khối lượng bằng 0     │ tin trên đường truyền        │
└──────────────────────┴───────────────────────┴──────────────────────────────┘
```

Mỗi bản ghi bị tiêm lỗi được đánh dấu cờ `is_injected = True` kèm tên lỗi `fault_type` để phục vụ làm **Ground Truth chuẩn** khi đánh giá Ma trận nhầm lẫn (Confusion Matrix).

---

## 2.8. CÁC CÔNG THỨC TOÁN HỌC ĐỐI SOÁT VÀ ĐÁNH GIÁ CHỈ SỐ

Để phục vụ các bài thực nghiệm định lượng khoa học trong Chương 4, đề tài thiết lập hệ thống công thức toán học chuẩn xác như sau:

### 2.8.1. Công Thức Giá Trung Bình Theo Trọng Số Khối Lượng (VWAP)
VWAP là chỉ số tài chính quan trọng nhất phản ánh mức giá trung bình thực tế mà thị trường đã giao dịch trong một khung nến:
$$\text{VWAP} = \frac{\sum_{i=1}^{n} \Big(P_i \times Q_i\Big)}{\sum_{i=1}^{n} Q_i}$$
* Trong đó $P_i$ là giá khớp của giao dịch thứ $i$, $Q_i$ là khối lượng tương ứng.

### 2.8.2. Các Chỉ Số Sai Số Đối Soát Giữa Speed View Và Batch View

#### 1. Sai số tuyệt đối của VWAP ($\varepsilon_{\text{VWAP}}$):
$$\varepsilon_{\text{VWAP}} = \Big|\text{VWAP}_{\text{Speed}} - \text{VWAP}_{\text{Batch}}\Big|$$

#### 2. Độ lệch tỷ lệ khối lượng giao dịch ($\Delta V_{\%}$):
$$\Delta V_{\%} = \frac{\Big|\sum V_{\text{Speed}} - \sum V_{\text{Batch}}\Big|}{\sum V_{\text{Batch}}} \times 100\%$$

#### 3. Tỷ lệ bản ghi trùng lặp ($R_{\text{dup}}$):
$$R_{\text{dup}} = \frac{N_{\text{raw}} - N_{\text{distinct}}}{N_{\text{raw}}} \times 100\%$$

#### 4. Tỷ lệ mất chuỗi định danh giao dịch ($R_{\text{gap}}$):
$$R_{\text{gap}} = \frac{\sum_{i=2}^{k} \max\big(0, \text{ID}_i - \text{ID}_{i-1} - 1\big)}{\text{ID}_{\max} - \text{ID}_{\min} + 1} \times 100\%$$

#### 5. Thời gian hội tụ đối soát ($T_{\text{conv}}$):
Khoảng thời gian tính bằng giây từ lúc chu kỳ Batch Layer bắt đầu quét dữ liệu cho đến khi sai số đối soát $\Delta V_{\%}$ và $\varepsilon_{\text{VWAP}}$ tại Serving Layer hội tụ triệt để về $0.0\%$.

### 2.8.3. Chỉ Số Đánh Giá Cổng Chất Lượng Dữ Liệu (Classification Metrics)
Đánh giá độ chính xác của Data Quality Gate dựa trên Ma trận nhầm lẫn (Confusion Matrix):
* **True Positive (TP):** Bản ghi có lỗi được phát hiện và cách ly chuẩn xác vào bảng `dq_quarantine`.
* **False Positive (FP):** Bản ghi sạch bị cách ly nhầm (Lỗi loại I).
* **False Negative (FN):** Bản ghi lỗi bị lọt qua cổng vào tầng Silver (Lỗi loại II).
* **True Negative (TN):** Bản ghi sạch được cho qua hợp lệ vào tầng Silver.

Từ đó xác định:
$$\text{Precision} = \frac{\text{TP}}{\text{TP} + \text{FP}}$$

$$\text{Recall} = \frac{\text{TP}}{\text{TP} + \text{FN}}$$

$$\text{F1-Score} = 2 \times \frac{\text{Precision} \times \text{Recall}}{\text{Precision} + \text{Recall}}$$

$$\text{Accuracy} = \frac{\text{TP} + \text{TN}}{\text{TP} + \text{TN} + \text{FP} + \text{FN}}$$

---

## 2.9. TỔNG QUAN HỆ SINH THÁI CÔNG NGHỆ TRIỂN KHAI

Toàn bộ hệ thống được container hóa 100% bằng **Docker Compose**, phân bổ tài nguyên tối ưu trong giới hạn 8.5 GB RAM cho môi trường máy trạm cá nhân 16 GB:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     HẠ TẦNG DOCKER COMPOSE TOÀN DIỆN                        │
├────────────────────┬────────────────────┬───────────┬───────────────────────┤
│ Dịch vụ Container  │ Hình ảnh Image     │ Cổng Port │ Vai trò phân hệ       │
├────────────────────┼────────────────────┼───────────┼───────────────────────┤
│ lakehouse-kafka    │ apache/kafka:3.7.0 │ 9092, 9094│ Message Broker KRaft  │
│ lakehouse-minio    │ minio/minio:latest │ 9000, 9001│ S3 Object Storage     │
│ lakehouse-iceberg  │ iceberg-rest:latest│ 8181      │ REST Catalog Metadata │
│ lakehouse-speed    │ Custom Spark 3.5.1 │ Internal  │ Streaming Ingestion   │
│ lakehouse-batch    │ Custom Spark Batch │ Internal  │ Medallion Transformer │
│ lakehouse-clickh   │ clickhouse-server  │ 8123, 9009│ OLAP Analytics Engine │
│ lakehouse-redis    │ redis:7.2-alpine   │ 6379      │ Hot Cache (< 5ms)     │
│ lakehouse-fastapi  │ Custom Serving API │ 8000      │ Query Merger Gateway  │
│ lakehouse-streaml  │ Custom Dashboard   │ 8501      │ Visual Monitoring UI  │
│ lakehouse-dagster  │ Custom Dagster UI  │ 3000      │ Batch Orchestration   │
└────────────────────┴────────────────────┴───────────┴───────────────────────┘
```

Hệ sinh thái này đảm bảo tính tương thích cao, mã nguồn mở hoàn toàn, không phụ thuộc vào bất kỳ dịch vụ đám mây thương mại độc quyền nào, và cho phép tái lập thử nghiệm dễ dàng trên mọi môi trường chuẩn POSIX / Windows Docker Desktop.
