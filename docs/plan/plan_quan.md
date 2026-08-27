# KẾ HOẠCH CHI TIẾT 13 TUẦN — PHẠM MINH QUÂN
## MSSV: `23133060` | TLCN 2026-2027
### Đề tài: Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa

> **Phụ trách chính:** Apache Iceberg · MinIO S3 · Batch Layer (Bronze→Silver→Gold) · Data Quality Gate · ClickHouse Schema · Benchmark 3 & 4
> **Phối hợp:** Kiểm tra Ingestion schema, hỗ trợ Speed Layer ghi đúng ClickHouse

---

## 📅 TUẦN 1 — Chuẩn bị dữ liệu & thiết lập hạ tầng nền

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Đọc kỹ `TradeEvent` model từ Quốc Anh | `src/ingestion/models.py` | Nắm rõ fields: `trade_id`, `symbol`, `price`, `quantity`, `trade_time`, `is_buyer_maker`, `is_injected`, `ingested_at` |
| 2 | Khởi chạy và test MinIO container | `docker-compose.yml` | MinIO console mở được tại `http://localhost:9001` |
| 3 | Tạo các MinIO bucket cần thiết | `docker-compose.yml` (minio-setup) | Buckets `warehouse`, `bronze`, `silver`, `gold` tồn tại |
| 4 | Khởi chạy và test Iceberg REST Catalog | `docker-compose.yml` | Iceberg REST API phản hồi tại `http://localhost:8181` |
| 5 | Phác thảo Bronze Table schema | `configs/iceberg/tables.yaml` | Draft schema bảng Bronze với partition strategy |
| 6 | Thống nhất ClickHouse schema với Quốc Anh | `configs/clickhouse/init.sql` | Draft DDL 3 bảng: `speed_agg`, `batch_agg`, `system_watermark` |

**🎯 Milestone Tuần 1:** MinIO + Iceberg REST Catalog chạy ổn định. Draft Bronze Table schema xong.

---

## 📅 TUẦN 2 — Xây dựng nền tảng Apache Iceberg & giải quyết Small File Problem

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | ⚠️ **Chốt ClickHouse schema với Quốc Anh** | `configs/clickhouse/init.sql` | DDL finalized, cả 2 đồng ý — không đổi nữa |
| 2 | Tạo Bronze Table trên Iceberg REST Catalog | `src/batch_layer/iceberg_utils.py` | Bảng `bronze.crypto_trades` tồn tại trên MinIO với ACID Snapshot Isolation |
| 3 | Cấu hình Hidden Partition theo ngày | `src/batch_layer/iceberg_utils.py` | Partition `days(trade_time)` hoạt động đúng |
| 4 | Viết script kiểm tra Small File Problem | `scripts/benchmarks/gen_small_files.py` | Giả lập ghi 2000+ file Parquet < 2MB vào Bronze bucket |
| 5 | Test đọc/ghi Iceberg cơ bản bằng PySpark | `tests/test_iceberg_basic.py` | PySpark đọc/ghi được bảng Bronze qua Iceberg REST |
| 6 | Cấu hình Spark-Iceberg connector | `configs/spark/spark-defaults.conf` | `spark.sql.catalog.demo` trỏ đúng Iceberg REST + MinIO |

**🎯 Milestone Tuần 2:** Bronze Table tạo được, Iceberg+MinIO+Spark kết nối thành công. ClickHouse schema đã chốt.

---

## 📅 TUẦN 3 — Xây dựng Data Quality Gate

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tạo module DQ Rules & Checks | `src/data_quality/dq_checks.py` | 4 rules: giá âm, qty=0, thiếu `trade_id`, sai schema |
| 2 | Tạo Quarantine Table trên Iceberg | `src/data_quality/quarantine.py` | Bảng `quarantine.rejected_trades` ghi nhận bản ghi lỗi + `error_code` + `rejected_at` |
| 3 | Tạo DQ Metrics collector | `src/data_quality/dq_metrics.py` | Xuất tỷ lệ pass/fail theo batch ra log CSV |
| 4 | Test DQ Gate với data mẫu có lỗi | `tests/test_dq_checks.py` | DQ Gate lọc đúng, bản ghi lỗi vào Quarantine, bản ghi sạch qua Silver |
| 5 | Phối hợp với Quốc Anh: nhận data mẫu từ Fault Injector | — | Có dataset test với `is_injected=true` để chạy DQ Gate |

**🎯 Milestone Tuần 3:** DQ Gate chạy được trên data mẫu. Quarantine Table hoạt động.

---

## 📅 TUẦN 4 — Xây dựng Silver Processor (Deduplication)

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tạo Silver Processor | `src/batch_layer/silver_processor.py` | Đọc Bronze, dedup theo `trade_id`, lọc lỗi → Quarantine |
| 2 | Kiểm tra dedup chính xác | `tests/test_silver_processor.py` | Dataset 10,000 records có 10% duplicate → Silver chỉ còn ~9,000 unique |
| 3 | Ghi Silver Table lên Iceberg | `src/batch_layer/silver_processor.py` | Bảng `silver.crypto_trades_clean` tồn tại trên MinIO |
| 4 | Test end-to-end Bronze → Silver với DQ Gate | — | Luồng Bronze → DQ Check → Silver / Quarantine hoạt động |
| 5 | Hỗ trợ Quốc Anh kiểm tra ClickHouse schema | `configs/clickhouse/init.sql` | Giải đáp thắc mắc về schema nếu có |

**🎯 Milestone Tuần 4:** Silver Processor hoạt động, dedup đúng, Quarantine đầy đủ.

---

## 📅 TUẦN 5 — Xây dựng Gold Aggregator (OHLCV + VWAP chính xác)

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tạo Gold Aggregator | `src/batch_layer/gold_aggregator.py` | Tính OHLCV (1m, 5m) + VWAP + Total Volume + Return từ Silver |
| 2 | Công thức VWAP chính xác | `src/batch_layer/gold_aggregator.py` | `VWAP = SUM(price * quantity) / SUM(quantity)` theo window |
| 3 | Ghi Gold Table lên Iceberg | `src/batch_layer/gold_aggregator.py` | Bảng `gold.ohlcv_aggregated` tồn tại, có partition theo ngày |
| 4 | Test Gold Aggregator với dữ liệu thực | `tests/test_gold_aggregator.py` | Kết quả OHLCV khớp với tính tay trên tập nhỏ |
| 5 | Test DQ Metrics collector cuối pipeline | `src/data_quality/dq_metrics.py` | Xuất report tỷ lệ clean/quarantine |

**🎯 Milestone Tuần 5:** Gold Aggregator tính toán chính xác OHLCV và VWAP.

---

## 📅 TUẦN 6 — Hoàn thiện Batch Layer & đồng bộ ClickHouse

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Tạo Bronze Writer (Kafka → Iceberg) | `src/batch_layer/bronze_writer.py` | Đọc từ Kafka topic `crypto_trades_raw`, ghi bất biến vào Bronze Table |
| 2 | Tạo ClickHouse Sync (Gold → `batch_agg`) | `src/batch_layer/clickhouse_sync.py` | Ghi kết quả Gold vào bảng ClickHouse `batch_agg` với nhãn `status='Reconciled'` |
| 3 | Tạo Spark Batch Job entry point | `src/batch_layer/spark_batch_jobs.py` | `python spark_batch_jobs.py --mode full` chạy toàn bộ pipeline Kafka→Bronze→Silver→Gold→ClickHouse |
| 4 | Test pipeline Batch end-to-end | — | Chạy full mode thành công với data thực từ Kafka |
| 5 | ⚠️ Tạo mock data `batch_agg` cho Quốc Anh test | — | Insert ~100 dòng mẫu vào `batch_agg` để Quốc Anh test Query Merger |

**🎯 Milestone Tuần 6:** Batch Layer hoàn chỉnh end-to-end. `batch_agg` có dữ liệu, Quốc Anh có thể test Serving Layer.

---

## 📅 TUẦN 7 — Watermark Manager & Tự động hóa bảo trì Iceberg

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | ⚠️ **Tạo Watermark Manager** | `src/batch_layer/watermark_manager.py` | Sau mỗi Batch Job: ghi mốc `max(trade_time)` vào bảng `system_watermark` trong ClickHouse |
| 2 | Tích hợp Watermark vào Batch Job | `src/batch_layer/spark_batch_jobs.py` | Watermark tự động cập nhật sau mỗi lần chạy |
| 3 | Tạo Compaction Job | `src/batch_layer/compaction.py` | Chạy `OPTIMIZE` (Bin-packing) trên Bronze Table, gộp file nhỏ |
| 4 | Tạo Maintenance pipeline | `src/batch_layer/maintenance.py` | Expire Snapshots cũ (>7 ngày), Remove Orphan Files tự động |
| 5 | Test Compaction: đo hiệu quả | — | Đo số file trước/sau Compaction, tốc độ query (chuẩn bị số liệu Benchmark 4) |

**🎯 Milestone Tuần 7:** `system_watermark` cập nhật tự động. Compaction đã test. Quốc Anh có thể hoàn thiện Query Merger.

---

## 📅 TUẦN 8 — Hỗ trợ tích hợp & kiểm tra hệ thống tổng thể

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Test tích hợp Speed Layer (Quốc Anh) ghi vào `speed_agg` | — | Verify schema đúng, data ghi được |
| 2 | Test tích hợp Query Merger đọc `system_watermark` | — | Verify watermark trả đúng giá trị |
| 3 | Chạy pipeline đầy đủ lần đầu | — | Ingestion → Speed/Batch chạy song song → Serving Layer trả kết quả |
| 4 | Ghi nhận lỗi tích hợp và fix | — | Danh sách bugs đã fix sau lần chạy đầu |
| 5 | Chuẩn bị dataset Benchmark | `datasets/` | Prepare 5M+ records BTCUSDT cho Benchmark chạy ổn định |

**🎯 Milestone Tuần 8:** Hệ thống chạy E2E lần đầu thành công (dù còn lỗi nhỏ).

---

## 📅 TUẦN 9 — Dashboard Streamlit (phần của Quân)

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Component: Trạng thái Watermark | `dashboard/components/watermark_status.py` | Hiển thị real-time `batch_watermark` và nhãn `Provisional`/`Reconciled` |
| 2 | Component: DQ Metrics panel | `dashboard/components/dq_panel.py` | Hiển thị tỷ lệ dữ liệu sạch/lỗi/quarantine |
| 3 | Hỗ trợ Quốc Anh tích hợp các component | `dashboard/app.py` | Dashboard hiển thị được dữ liệu thực |

**🎯 Milestone Tuần 9:** Dashboard có panel của Quân hiển thị đúng.

---

## 📅 TUẦN 10 — Chuẩn bị & hỗ trợ Benchmark

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Chuẩn bị data cho Benchmark 3 | — | Có dataset với đủ 4 loại lỗi đã inject |
| 2 | Chuẩn bị data cho Benchmark 4 | — | Có Bronze table với 2000+ file nhỏ < 2MB |
| 3 | Hỗ trợ Quốc Anh chạy Benchmark 1 & 2 | — | Đảm bảo `system_watermark` và `batch_agg` có đủ dữ liệu |
| 4 | Viết script Benchmark 3 | `scripts/benchmarks/bench_fault.py` | Script tự động tiêm lỗi + đo Precision/Recall/F1 |

**🎯 Milestone Tuần 10:** Script Benchmark 3 hoàn thiện. Data Benchmark 4 sẵn sàng.

---

## 📅 TUẦN 11 — Thực thi Benchmark 3 & 4

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | **Chạy Benchmark 3** (Data Quality & Fault) | `scripts/benchmarks/bench_fault.py` | Confusion Matrix + Precision/Recall/F1 trên Quarantine Table |
| 2 | **Chạy Benchmark 4** (Compaction Efficiency) | `scripts/benchmarks/bench_compaction.py` | Số file trước/sau, Query Execution Time trước/sau, MB/s |
| 3 | Xuất kết quả log CSV | `results/logs/bench_fault_results.csv` | Dữ liệu thô Benchmark 3 |
| 4 | Xuất kết quả log CSV | `results/logs/bench_compaction_results.csv` | Dữ liệu thô Benchmark 4 |
| 5 | Chạy `run_all_benchmarks.py` cùng Quốc Anh | `scripts/benchmarks/run_all_benchmarks.py` | 4 Benchmark đều pass và xuất đủ log |

**🎯 Milestone Tuần 11:** Có đủ số liệu raw cho 4 Benchmark.

---

## 📅 TUẦN 12 — Phân tích số liệu & Viết báo cáo

| STT | Nhiệm vụ | File cần tạo/chỉnh | Kết quả cần đạt |
|:---:|:---|:---|:---|
| 1 | Vẽ biểu đồ Benchmark 3 | `results/plots/benchmark3_*.png` | Biểu đồ Precision/Recall/F1, Error Rate theo loại lỗi |
| 2 | Vẽ biểu đồ Benchmark 4 | `results/plots/benchmark4_*.png` | Biểu đồ số file/query time trước-sau Compaction |
| 3 | Viết Chương 1 — Tổng quan & Đặt vấn đề | `docs/bao_cao/chuong1.md` | Hoàn chỉnh mục 1.1 → 1.6 |
| 4 | Viết Chương 2 — Cơ sở lý thuyết | `docs/bao_cao/chuong2.md` | Iceberg, Medallion, Lambda Architecture, DQ |
| 5 | Phân tích kết quả Benchmark 3 & 4 | — | Đoạn phân tích Trade-offs cho Chương 4 |

**🎯 Milestone Tuần 12:** Chương 1 & 2 hoàn thảo. Biểu đồ Benchmark 3 & 4 hoàn chỉnh.

---

## 📅 TUẦN 13 — Hoàn thiện & Chuẩn bị bảo vệ

| STT | Nhiệm vụ | Kết quả cần đạt |
|:---:|:---|:---|
| 1 | Smoke test Batch Layer + DQ + Compaction | Chạy ổn định, không lỗi runtime |
| 2 | Rà soát toàn bộ báo cáo (Chương 1, 2, 4 — phần Quân) | Không còn lỗi chính tả, đủ nội dung |
| 3 | Hoàn thiện slide (phần Quân: Slide Batch + Iceberg + DQ) | Slide rõ ràng, có số liệu Benchmark |
| 4 | Chuẩn bị câu trả lời phản biện về Iceberg & DQ | Nắm vững cơ chế Compaction, Quarantine |
| 5 | Nộp báo cáo & bảo vệ đề tài | 🎓 Hoàn thành! |

---

## 📌 FILES QUÂN CHỊU TRÁCH NHIỆM

```
src/batch_layer/
├── iceberg_utils.py          ← Tuần 2
├── bronze_writer.py          ← Tuần 6
├── silver_processor.py       ← Tuần 4
├── gold_aggregator.py        ← Tuần 5
├── clickhouse_sync.py        ← Tuần 6
├── watermark_manager.py      ← Tuần 7  ⚠️ Critical
├── compaction.py             ← Tuần 7
├── maintenance.py            ← Tuần 7
└── spark_batch_jobs.py       ← Tuần 6

src/data_quality/
├── dq_checks.py              ← Tuần 3
├── quarantine.py             ← Tuần 3
└── dq_metrics.py            ← Tuần 3

configs/
├── iceberg/tables.yaml       ← Tuần 2
├── clickhouse/init.sql       ← Tuần 2 (cùng Quốc Anh chốt)
└── spark/spark-defaults.conf ← Tuần 2

scripts/benchmarks/
├── bench_fault.py            ← Tuần 10
└── bench_compaction.py       ← Tuần 10

dashboard/components/
├── watermark_status.py       ← Tuần 9
└── dq_panel.py               ← Tuần 9

results/logs/
├── bench_fault_results.csv
└── bench_compaction_results.csv

results/plots/
├── benchmark3_*.png
└── benchmark4_*.png
```
