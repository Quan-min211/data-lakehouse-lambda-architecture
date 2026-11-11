# GHI CHÚ SỬA LỖI LOGIC — BATCH, DATA QUALITY, SERVING, DAGSTER, DASHBOARD (2026-11-12)

> Tài liệu nội bộ phục vụ viết Chương 3 (Mục 3.4–3.7) và phần "Hạn chế & Bài học" của báo cáo.
> Mỗi mục gồm: **Hiện tượng → Nguyên nhân gốc → Công nghệ / khái niệm liên quan → Cách sửa → Kiểm chứng → Gợi ý đưa vào báo cáo**.
> Kiểm thử: `python scripts/run_tests.py` → **7/7 suite, 84 test, 0 lỗi** (thêm suite mới `tests/test_batch_layer.py`).

---

## Bảng tóm tắt

| # | Lỗi | Mức độ | Tệp chính | Mục báo cáo |
| :-: | :--- | :-: | :--- | :-: |
| A1 | Nến batch bị ghi đè bằng nến thiếu khi đọc Kafka theo đợt | Nghiêm trọng | `src/batch_layer/silver_store.py` (mới), `spark_batch_jobs.py` | 3.4 |
| A2 | Watermark có thể lùi lại; chốt cả phút chưa đủ dữ liệu | Nghiêm trọng | `clickhouse_sync.py`, `watermark_reader.py`, `spark_batch_jobs.py` | 3.4, 3.6 |
| A3 | Không có dữ liệu Kafka mới thì nạp lại toàn bộ dữ liệu mock | Nghiêm trọng | `spark_batch_jobs.py` | 3.4 |
| A4 | Offset Kafka commit trước khi ghi xong (at-most-once) | Nghiêm trọng | `spark_batch_jobs.py`, `dagster_project/repository.py` | 3.4, 3.5 |
| B5 | Thời gian có offset (+07:00) bị lệch 7 giờ trong SQL | Trung bình | `api_routes.py`, `query_merger.py`, `clickhouse_client.py` | 3.6 |
| B6 | DQ Gate đọc sai boolean từ CSV, cho `NaN`/`inf` lọt qua | Trung bình | `src/data_quality/dq_checks.py` | 3.4 |
| B7 | Hai tiêu chí `MATCHED` khác nhau giữa Batch audit và API | Trung bình | `src/utils/reconciliation.py` (mới) | 3.4, 3.6 |
| B11 | Dashboard trộn các đợt benchmark, chèn bảng đối soát bịa | Trung bình | `dashboard/components/benchmark_charts.py`, `dashboard/app.py` | 3.7 |

---

## A1. Nến batch bị ghi đè bằng nến thiếu dữ liệu

**Hiện tượng.** Ở chế độ `--from-kafka` (daemon hoặc Dagster với `BATCH_FROM_KAFKA=true`), mỗi lượt Batch chỉ đọc phần offset mới rồi gộp nến từ phần đó. Một phút có giao dịch rơi vào hai lượt chạy liên tiếp sẽ chỉ còn phần của lượt sau. Một sự kiện đến trễ tạo ra "nến" chỉ gồm vài giao dịch trễ và nến này đè lên nến đầy đủ. Điều này phá vỡ khẳng định "Batch View là ground truth".

**Nguyên nhân gốc.** `lakehouse.batch_agg` dùng `ReplacingMergeTree(created_at) ORDER BY (symbol, window_start)`. Engine này **thay thế cả dòng** có cùng khóa sắp xếp bằng phiên bản có `created_at` mới nhất (và `FINAL` khi đọc trả về phiên bản đó). Nó không cộng dồn. Phép gộp OHLCV/VWAP lại không có tính chất "cộng dồn từ phần dư" nếu không biết các giao dịch cũ (open/close cần thứ tự thời gian, VWAP cần tổng $\sum p \cdot q$, và khử trùng cần tập `trade_id` đã thấy).

**Công nghệ / khái niệm.**
- ClickHouse `ReplacingMergeTree`: khử trùng theo khóa khi merge nền, chọn bản có cột version lớn nhất; `SELECT … FINAL` buộc khử trùng lúc đọc.
- Mẫu **recompute affected partitions** (tính lại phân vùng bị ảnh hưởng) của kiến trúc Lambda / Medallion: lớp Silver lưu bản ghi sạch tích lũy, lớp Gold được tính lại *từ toàn bộ Silver* của các cửa sổ có dữ liệu mới, thay vì cộng phần chênh lệch. Trong hệ thống sản xuất, vai trò này thường do bảng Iceberg/Delta Silver với `MERGE INTO` đảm nhận.

**Cách sửa.**
- Thêm `SilverTradeStore` ([src/batch_layer/silver_store.py](../../src/batch_layer/silver_store.py)): kho JSONL **append-only**, phân vùng `symbol/YYYY-MM-DD/HH.jsonl` theo `trade_time`, đặt tại `datasets/processed/silver_store` (đổi bằng `BATCH_SILVER_STORE_DIR`, đã nằm trong `.gitignore`).
- `build_incremental_gold_candles()` trong `spark_batch_jobs.py`:
  1. `store.upsert(clean_records)` ghi thêm bản ghi mới và trả về tập cửa sổ `(symbol, window_start)` bị ảnh hưởng.
  2. `store.load_windows(...)` đọc lại **toàn bộ** bản ghi của các cửa sổ đó, khử trùng theo `(symbol, trade_id)` với cùng quy tắc của DQ Gate (bản `ingestion_time` mới hơn thắng).
  3. `aggregate_gold_candles()` tính lại nến đầy đủ, rồi mới ghi vào `batch_agg`.
- Tính **idempotent**: bản ghi trùng khóa và không mới hơn sẽ không được ghi lại. Vì vậy chạy lại cùng một lô (ví dụ sau khi ClickHouse lỗi và offset chưa commit) cho kết quả như cũ. Số bản trùng với lần chạy trước được báo riêng trong `cross_run_duplicates`.
- Chế độ đọc file (`datasets/mock`, `--input`) giữ nguyên cách cũ, vì mỗi lần chạy đã đọc tập dữ liệu đầy đủ.
- Phân vùng theo giờ để mỗi lần chạy chỉ đọc vài tệp nhỏ, không phải cả ngày.

**Kiểm chứng.** `TestIncrementalGoldCandles` gồm bốn test: cửa sổ bị chia qua hai lượt chạy cho đủ 3 giao dịch; sự kiện trễ chỉ tính lại đúng cửa sổ của nó; chạy lại cùng lô không thay đổi kết quả; bản ghi có `ingestion_time` mới hơn thay thế bản cũ.

**Gợi ý báo cáo (3.4).** Trình bày như một quyết định thiết kế: *"Vì `ReplacingMergeTree` thay thế chứ không cộng dồn, Batch Layer không thể gộp phần chênh lệch; thay vào đó hệ thống tính lại cửa sổ bị ảnh hưởng từ kho Silver tích lũy."* Có thể đưa vào một sơ đồ hoạt động: đọc offset → DQ → upsert Silver → xác định cửa sổ → tính lại → ghi ClickHouse → nâng W → commit offset.

---

## A2. Watermark lùi lại và chốt cả phút chưa đủ dữ liệu

**Hiện tượng.**
1. W được tính bằng `max(window_end)` của **riêng lượt chạy hiện tại**. Một lượt chỉ chứa sự kiện trễ hoặc dữ liệu cũ sẽ ghi W nhỏ hơn và kéo mốc lùi về quá khứ. Query Merger khi đó đẩy các truy vấn đã chốt về lại Speed View.
2. Ở chế độ Kafka, phút cuối của lô thường chưa nhận đủ giao dịch nhưng vẫn nằm dưới W, nên bị phục vụ như dữ liệu `RECONCILED`.

**Nguyên nhân gốc.** Không có ràng buộc đơn điệu khi ghi. Bảng `system_watermark` là `ReplacingMergeTree(updated_at) ORDER BY layer`, nên khi merge nền ClickHouse **chỉ giữ dòng có `updated_at` mới nhất**. Một mốc nhỏ hơn được ghi sau sẽ *xóa vĩnh viễn* mốc lớn hơn, nên sửa riêng phía đọc là không đủ.

**Công nghệ / khái niệm.**
- **Low watermark đơn điệu** (monotonic watermark) trong xử lý luồng (Akidau và cộng sự, *Streaming Systems*; Spark Structured Streaming): watermark chỉ tiến, và chỉ đi qua một cửa sổ khi không còn chờ sự kiện của nó.
- **Allowed lateness**: chốt W ở `max(event_time) − độ trễ cho phép`, tương tự `withWatermark("event_time", "1 minute")` của Speed Layer.

**Cách sửa.**
- `ClickHouseBatchSync.get_current_watermark()` đọc `max(watermark_time)`. `update_watermark()` **chỉ ghi khi ứng viên lớn hơn mốc hiện tại**, nếu không thì trả về `False` và ghi log.
- `compute_watermark_candidate()`:
  - Đọc file (tập đầy đủ): $W = \max(\text{window\_end})$.
  - Đọc Kafka theo đợt: $W = \lfloor \max(\text{trade\_time}) - L \rfloor_{\text{phút}}$, với $L$ = `BATCH_WATERMARK_LATENESS_SECONDS` (mặc định 60 s, bằng watermark của Speed Layer). Các nến của phút chưa chốt vẫn được ghi vào `batch_agg` (lượt sau sẽ tính lại từ kho Silver) nhưng nằm trên W, nên Query Merger vẫn lấy chúng từ Speed View.
- `WatermarkReader.get_watermark()` dùng `SELECT max(watermark_time), count() … WHERE layer = {layer:String}`. Đây là truy vấn có tham số bind, thay cho cách nối chuỗi f-string trước đây.
- Dagster (`gold_market_aggregates`, `system_watermark_sync`) dùng chung các hàm trên.

**Kiểm chứng.** `TestMonotonicWatermark` dùng ClickHouse giả lập: ghi mốc mới → ghi mốc cũ hơn bị từ chối → ghi lại mốc bằng bị từ chối → mốc lớn hơn được ghi. `TestWatermarkCandidate` kiểm tra W ở chế độ Kafka dừng ở phút đã đóng. Truy vấn đọc mới đã chạy thử trên ClickHouse thật và trả về `2026-10-17 08:15 UTC`, khớp dòng hiện có.

**Gợi ý báo cáo (3.4 và 3.6).** Nêu bất biến *"W không bao giờ giảm"* như một tính chất bảo đảm của Serving Layer. Phân biệt W của Batch (mốc phục vụ) với watermark của Spark (mốc dọn trạng thái).

---

## A3. Không có dữ liệu Kafka mới → nạp lại toàn bộ dữ liệu mock

**Hiện tượng.** `load_default_records(from_kafka=True)` fallback sang `datasets/mock` khi Kafka không có bản ghi mới. Ở chế độ daemon, mọi chu kỳ rảnh đều nạp lại khoảng 368 MB dữ liệu mock, ghi lại `batch_agg`, đặt W theo dữ liệu mock (dễ gây lùi W như A2), và nếu bật `--write-iceberg` thì nối thêm bản sao vào Bronze.

**Cách sửa.**
- Ở chế độ Kafka, danh sách rỗng nghĩa là chu kỳ không làm gì, **không fallback**.
- Thêm `kafka_mode_enabled()` để CLI và Dagster cùng chấp nhận `BATCH_FROM_KAFKA` và `BATCH_CONSUME_KAFKA`. Trước đây CLI chỉ đọc tên thứ hai, Dagster chỉ đọc tên thứ nhất.

**Kiểm chứng.** `test_empty_kafka_does_not_fall_back_to_mock` và `TestLoadDefaultRecordsNoFallback`: các test này cố ý làm hàm nạp file ném lỗi, và đều đạt.

**Gợi ý báo cáo.** Một câu trong 3.4: chế độ tăng dần và chế độ nạp lại toàn bộ được tách rõ; chu kỳ không có dữ liệu mới là *no-op*.

---

## A4. Offset Kafka được commit trước khi xử lý xong

**Hiện tượng.** File offset `results/logs/batch_kafka_offsets.json` được ghi ngay trong hàm đọc, trước bước DQ, ghi ClickHouse và ghi Iceberg. Nếu ClickHouse lỗi (hoặc `--iceberg-strict` báo lỗi), lô đó mất vĩnh viễn: ngữ nghĩa **at-most-once**. Ngoài ra, `ClickHouseBatchSync` *không ném lỗi* khi không kết nối được mà chỉ trả về 0, nên lỗi bị nuốt mất.

**Công nghệ / khái niệm.** Các mức bảo đảm giao nhận của Kafka consumer: *at-most-once* (commit trước khi xử lý), *at-least-once* (commit sau khi xử lý), *exactly-once* (giao dịch hoặc sink idempotent). Mẫu chuẩn là **at-least-once + sink idempotent**: ClickHouse `ReplacingMergeTree` cùng kho Silver khử trùng nên đọc lại không sinh sai lệch.

**Cách sửa.**
- Tách hàm thành `read_kafka_records()` (chỉ đọc, trả về `KafkaReadResult` gồm bản ghi và offset dự kiến) và `commit_kafka_offsets()` (ghi file bằng `tmp` + `os.replace` để ghi nguyên tử).
- `run_batch_pipeline()` chỉ commit khi `persisted` đúng, tức ClickHouse kết nối được và các lệnh insert/watermark không ném lỗi. Trường hợp `--no-clickhouse` coi là đã lưu.
- Dagster: `kafka_read` được chuyển qua các asset. Offset được commit ở asset cuối `system_watermark_sync`, sau khi watermark đã ghi.
- `consume_kafka_records(commit=True)` giữ lại để tương thích ngược.

**Kiểm chứng.** `TestKafkaOffsetCommit`: ClickHouse sống thì offset được ghi (`kafka_offsets_committed = True`); ClickHouse chết thì file offset không được tạo.

**Gợi ý báo cáo (3.4, 3.5).** Câu khẳng định: *"Batch Layer bảo đảm at-least-once; tính idempotent của sink (ReplacingMergeTree và kho Silver khử trùng) biến việc đọc lại thành vô hại."*

---

## B5. Thời gian có offset bị lệch múi giờ

**Hiện tượng.** `GET /api/market?start_time=2026-09-04T17:00:00+07:00` được phân tích thành datetime có offset +07:00. `query_candles()` gọi `strftime` (in giờ địa phương 17:00) rồi gắn nhãn `'UTC'`, nên truy vấn lệch 7 giờ. Phép so với W trong `merge_query` cũng có thể chọn sai Case.

**Công nghệ / khái niệm.** Python `datetime` *aware* và *naive*: `strftime` không quy đổi múi giờ, chỉ `astimezone(timezone.utc)` mới quy đổi. ClickHouse `toDateTime64(str, 3, 'UTC')` hiểu chuỗi là giờ UTC.

**Cách sửa.**
- `parse_datetime_param()` trả về `dt.astimezone(timezone.utc)`.
- `merge_query()` và `get_reconciliation_report()` chuẩn hóa qua `_to_utc()`.
- `query_candles()` quy đổi về UTC trước khi định dạng, và thêm phần mili-giây.

**Kiểm chứng.** Ba test mới trong `tests/test_query_merger.py`: SQL chứa `10:00:00.000` khi đầu vào là `17:00+07:00`; tham số API được chuẩn hóa về UTC; khoảng 18:58–19:00 (+07:00) được định tuyến đúng Case 1.

---

## B6. DQ Gate đọc sai boolean và cho NaN/inf lọt qua

**Hiện tượng.**
- `bool("False") == True` trong Python, nên `is_buyer_maker` và `is_injected` đọc từ CSV luôn là `True`.
- `float("nan") <= 0` là `False`, nên giá hoặc khối lượng `NaN`/`inf` vượt qua luật "phải dương". `json.loads` của Python chấp nhận cả `NaN` lẫn `Infinity`, nên lỗi này có thể đến từ Kafka.

**Cách sửa.**
- Hàm `_to_bool()` nhận biết `"true"/"1"/"yes"/"y"/"t"` (không phân biệt hoa thường).
- Kiểm tra `math.isfinite()` trước các luật dấu, với hai mã lỗi mới `price_not_finite` và `quantity_not_finite` (đã thêm vào `00_global_constants.md` §3.1).
- Chuỗi `fault_type` rỗng từ CSV được chuẩn hóa thành `None`.

**Kiểm chứng.** Hai test mới trong `tests/test_data_quality.py`.

**Gợi ý báo cáo (3.4).** Bổ sung hai mã lỗi vào bảng danh mục mã quarantine. Nêu lý do cần luật hữu hạn riêng: phép so sánh với NaN luôn trả về False.

---

## B7. Thống nhất tiêu chí `MATCHED`

**Hiện tượng.** Bảng audit của Batch dùng độ lệch khối lượng tương đối `< 1e-4 %`. Endpoint `/api/reconciliation` và `bench_reprocess.py` lại dùng độ lệch tuyệt đối `< 1e-6`. Cùng một cửa sổ vì vậy có thể là `MATCHED` ở chỗ này và `RECONCILED_AND_HEALED` ở chỗ kia. Ngưỡng tuyệt đối còn phụ thuộc thang khối lượng của từng cặp coin (BTC khoảng vài đơn vị, XRP khoảng hàng trăm nghìn).

**Cách sửa.** Module mới [src/utils/reconciliation.py](../../src/utils/reconciliation.py) gồm `volume_drift_pct()` và `classify_window()`, với hằng số `VWAP_MATCH_TOLERANCE = 1e-4` (đơn vị giá) và `VOLUME_MATCH_TOLERANCE_PCT = 1e-4` (%). Ba nơi dùng chung module này: `sync_reconciliation_audit`, `get_reconciliation_report` và `bench_reprocess.py`. Định dạng CSV không đổi.

**Ảnh hưởng tới số liệu.** MAE, MAPE và RMSE của Benchmark 2 không đổi. Chỉ cột trạng thái từng nến trong `bench_reprocess.csv` có thể khác ở các đợt chạy sau.

---

## B11. Dashboard trộn các đợt benchmark và chèn số liệu bịa

**Hiện tượng.**
- Ô tổng quan BM1/BM3 gộp **mọi** đợt trong CSV, và BM3 còn lẫn các dòng `simulated`. Điều này trái quy ước *"chỉ dòng `mode=real` được dùng để kết luận"*.
- Hệ số tăng tốc lấy trung bình gồm cả lần đọc lạnh.
- Khi API thật trả về danh sách đối soát rỗng, `app.py` tự sinh bảng đối soát minh họa (lệch VWAP cố định 2,5) mà không gắn nhãn demo.

**Cách sửa.**
- Hàm `_latest_latency_run()` giữ đợt BM1 mới nhất theo `run_id`. Tab BM1 có thêm ô "Chỉ đợt đo mới nhất".
- Hàm `_latest_real_compaction()` giữ đợt `mode=real`, `simulated=False` mới nhất. `_warm_median()` lấy trung vị sau khi bỏ lần đọc lạnh, nhất quán với §4.3 của `00_global_constants.md`.
- Bảng đối soát minh họa chỉ hiện khi `data_source == "demo"` và có chú thích rõ.

**Lưu ý.** Ngưỡng đạt BM3 vẫn là `≥ 2.0×`. Với đợt chính thức 2026-11-11 (trung vị khoảng 1,51×), dashboard sẽ hiển thị **"Không đạt"**. Nếu nhóm muốn đổi chỉ tiêu, cần quyết định và ghi rõ trong báo cáo, không tự hạ ngưỡng.

---

## Thay đổi phụ trợ

- `scripts/reset_demo_state.py`: thêm cờ `--clear-batch-state` để xóa **cùng lúc** kho Silver và file offset Kafka. Hai trạng thái này phải đi cùng nhau: xóa kho mà giữ offset thì cửa sổ cũ bị tính lại từ dữ liệu thiếu.
- `dagster_project/repository.py`: `convergence_latency_seconds` dùng thời gian đo thật thay cho hằng số `0.1` s.
- `scripts/run_tests.py`: đăng ký suite `tests.test_batch_layer`.

## Các lỗi đã phát hiện nhưng CHƯA sửa (ghi vào phần Hạn chế nếu cần)

1. Case 3 của Query Merger không lấy nến Speed bù vào các phút thiếu nến Batch trong `[start, W)`. `_dedupe_prefer_batch` vì vậy thực tế không khử bản trùng nào.
2. Asset Bronze của Dagster ghi bản ghi *thô* vào Iceberg, còn CLI ghi bản ghi *đã làm sạch*. Dagster chạy file mode mỗi 15 phút còn nối thêm bản trùng vào Bronze.
3. `duplicate_count` trong bảng audit là tổng của cả lượt chạy, bị lặp lại ở mọi dòng.
4. `DQMetricsCollector.evaluate_fault_injection` coi mọi bản ghi `is_injected` trong Silver là FN, kể cả `late_data` và `out_of_order` hợp lệ. `bench_fault.py` dùng logic riêng đúng, nên §4.4 không bị ảnh hưởng.
5. Khi mất ClickHouse, `WatermarkReader` trả về mốc 1970 mà không cảnh báo. `/api/watermark` trả `updated_at = now`.
6. Các endpoint `async def` gọi ClickHouse đồng bộ nên chặn event loop khi có tải đồng thời.
7. dbt (tài liệu tham khảo): Silver incremental bỏ qua sự kiện trễ của ngày cũ; Gold phá thế hòa open/close theo `price` thay vì `trade_id`.
8. Lỗi lặt vặt: `--mode sample` bị bỏ qua; dữ liệu demo dùng ngưỡng spike 1,5%; DQ panel kết nối ClickHouse mà không truyền user/password; ký hiệu khoảng trong docstring và tab Kiến trúc ghi `[T_start, W] + (W, T_end]`, trong khi code dùng `[start, W) + [W, end)` (luận văn nên dùng ký hiệu của code).

## Ảnh hưởng tới số liệu thực nghiệm đã công bố

- **Benchmark 1–4** trong `00_global_constants.md` Phần 4 **không bị ảnh hưởng** bởi A1–A4. Bằng chứng: cả 16 lượt chạy trong `results/logs/batch_pipeline_runs.csv` đều là `batch_*` ở chế độ file, mỗi lượt nạp đủ 1.275.230 bản ghi, chưa có lượt Kafka (`auto_batch_*`, `auto_cycle_*`) nào ghi vào `batch_agg`.
- Lưu ý: `tests/test_dagster_pipeline.py` chạy thật job Dagster và ghi vào ClickHouse đang chạy (các lượt `batch_20261111_1918xx`/`1919xx`). Dữ liệu ghi giống hệt nên không làm đổi số liệu, nhưng nên biết khi đối chiếu log.
- Benchmark 2 chỉ có thể đổi cột trạng thái từng nến (B7), không đổi MAE/MAPE/RMSE.
- Dashboard sẽ hiển thị BM3 khoảng **1,5×** (trung vị, bỏ lần đọc lạnh) thay vì con số trộn nhiều đợt.
