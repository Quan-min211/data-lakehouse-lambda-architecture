# PHÂN CÔNG NHIỆM VỤ TỔNG QUAN — TLCN 2026-2027
## Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa

---

* **Quốc Anh** (SV1): Nguyễn Đặng Quốc Anh — MSSV: `23133004`
* **Quân** (SV2): Phạm Minh Quân — MSSV: `23133060`
* **GVHD:** ThS. Đoàn Minh Trí

---

## BẢNG PHÂN CÔNG TASK LỚN

| Phân hệ / Hạng mục | Người phụ trách chính (Lead) | Người phối hợp (Support) |
|:---|:---:|:---:|
| **Infrastructure & Docker Compose** | Cả hai | Cả hai |
| **Data Schema & ClickHouse DDL**  | Cả hai | Cả hai |
| **Ingestion Pipeline, Kafka & Fault Injector** | Quốc Anh | Quân |
| **Apache Iceberg, MinIO S3 & Compaction** | **Quân** | Quốc Anh |
| **Speed Layer & Spark Streaming** | Quốc Anh | Quân |
| **Batch Layer, Data Quality Gate & Quarantine** | **Quân** | Quốc Anh |
| **ClickHouse OLAP — Speed View (`speed_agg`)** | Quốc Anh ghi | Quân định nghĩa schema |
| **ClickHouse OLAP — Batch View (`batch_agg`, `system_watermark`)** | **Quân** ghi | Quốc Anh đọc |
| **Serving Layer & Auto-Correcting Query Merger** | Quốc Anh | Quân |
| **Benchmark 1 — Query Latency** | Quốc Anh | Quân |
| **Benchmark 2 — Reconciliation Correctness** | Quốc Anh | Quân |
| **Benchmark 3 — Data Quality & Fault Handling** | **Quân** | Quốc Anh |
| **Benchmark 4 — Compaction Efficiency** | **Quân** | Quốc Anh |
| **Dashboard Streamlit** | Cả hai | Cả hai |
| **Báo cáo TLCN (5 chương)** | Cả hai | Cả hai |

---

## ĐIỂM GIAO — INTERFACE CHUNG 

| Interface | Quốc Anh cần | Quân cung cấp | Deadline |
|:---|:---|:---|:---:|
| `TradeEvent` model schema | Thiết kế Bronze Table theo đúng fields | Finalize `models.py` | **Tuần 1** |
| ClickHouse schema DDL | Ghi `speed_agg` đúng cột | Định nghĩa toàn bộ DDL | **Tuần 2** |
| `system_watermark` table | Đọc mốc để phân luồng Query Merger | Ghi sau mỗi Batch Job | **Tuần 7** |
| `batch_agg` test data | Test phân luồng `Reconciled` | Có ít nhất vài dòng mock | **Tuần 6** |

---
