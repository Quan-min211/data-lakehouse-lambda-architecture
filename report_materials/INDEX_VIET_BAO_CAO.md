# MỤC LỤC TỔNG HỢP TÀI LIỆU & HƯỚNG DẪN VIẾT BÁO CÁO TIỂU LUẬN / KHÓA LUẬN

> **Đề tài:** Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa  
> *(Auto-Correcting Lambda Lakehouse for Real-Time Crypto Market Monitoring)*  
> **Thư mục tổng hợp:** `report_materials/` (Bao gồm **53 tệp** Markdown `.md` và HTML `.html`)

---

## 🗺️ BẢN ĐỒ ÁNH XẠ TÀI LIỆU VÀO 5 CHƯƠNG BÁO CÁO

Bảng dưới đây giúp bạn biết chính xác khi viết từng chương trong báo cáo Word / LaTeX, cần mở tệp nào trong `report_materials/` để lấy nội dung, công thức và số liệu:

| Chương Báo Cáo | Nội Dung Cần Viết | Tệp Tài Liệu Tương Ứng Trong `report_materials/` |
| :--- | :--- | :--- |
| **CHƯƠNG 1: TỔNG QUAN VÀ ĐẶT VẤN ĐỀ** | • Bối cảnh thị trường tiền mã hóa và thách thức dữ liệu lớn thời gian thực.<br>• Giới hạn của kiến trúc Batch-only và Streaming-only.<br>• Mục tiêu đề tài, đối tượng, phạm vi nghiên cứu.<br>• **3 Câu hỏi nghiên cứu cốt lõi (RQ1, RQ2, RQ3)**. | • [de_cuong_chi_tiet_tieu_luan.md](file:///01_de_cuong_va_ke_hoach/de_cuong_chi_tiet_tieu_luan.md) (Mục 1.1 - 1.4)<br>• [proposal_tlcn_lambda.md](file:///01_de_cuong_va_ke_hoach/proposal_tlcn_lambda.md)<br>• [plan.md](file:///01_de_cuong_va_ke_hoach/plan.md) |
| **CHƯƠNG 2: CƠ SỞ LÝ THUYẾT & KIẾN TRÚC TỔNG THỂ** | • Kiến trúc Lambda: Tầng Speed, Tầng Batch, Tầng Serving.<br>• So sánh Lambda vs Kappa vs Medallion Lakehouse.<br>• Công nghệ lưu trữ Apache Iceberg, định dạng Parquet, bài toán Small Files.<br>• Cơ chế At-Least-Once Kafka, OLAP ClickHouse ReplacingMergeTree, Redis Cache.<br>• Mô hình toán học đối soát: $\Delta V_{\%}$, $\varepsilon_{\text{VWAP}}$, $R_{\text{dup}}$, $R_{\text{gap}}$, $T_{\text{conv}}$. | • **[CHUONG_2_CO_SO_LY_LUAN.md](file:///02_kien_truc_va_thiet_ke_he_thong/CHUONG_2_CO_SO_LY_LUAN.md) (TÀI LIỆU CỐT LÕI CHƯƠNG 2)**<br>• [sys-arch.md](file:///02_kien_truc_va_thiet_ke_he_thong/sys-arch.md)<br>• [caitan_lambda_architecture.md](file:///02_kien_truc_va_thiet_ke_he_thong/caitan_lambda_architecture.md)<br>• [smallfile.md](file:///02_kien_truc_va_thiet_ke_he_thong/smallfile.md)<br>• [summary.md](file:///02_kien_truc_va_thiet_ke_he_thong/summary.md) |
| **CHƯƠNG 3: THIẾT KẾ VÀ HIỆN THỰC HỆ THỐNG** | • **Tầng Ingestion:** Data Contracts JSON Schema, Fault Injector, Auto-backfill WebSocket gap.<br>• **Tầng Speed:** Spark Structured Streaming, Tumbling Window, Watermark, RocksDB State Store.<br>• **Tầng Batch:** Medallion (Bronze/Silver/Gold), PyIceberg Compaction, Remove Orphan Files, Expire Snapshots, Dagster.<br>• **Tầng Serving:** Thuật toán Auto-Correcting Query Merger 3 Cases, Zero Double-Counting.<br>• **Dashboard:** Giao diện Streamlit và Proactive Alerting Gate. | • [01_ingestion_data_contract.md](file:///03_quy_trinh_trien_khai_5_tang/01_ingestion_data_contract.md)<br>• [02_speed_layer_spark_streaming.md](file:///03_quy_trinh_trien_khai_5_tang/02_speed_layer_spark_streaming.md)<br>• [03_serving_layer_query_merger.md](file:///03_quy_trinh_trien_khai_5_tang/03_serving_layer_query_merger.md)<br>• [04_dashboard_streamlit_ui.md](file:///03_quy_trinh_trien_khai_5_tang/04_dashboard_streamlit_ui.md)<br>• [05_batch_dagster_orchestration.md](file:///03_quy_trinh_trien_khai_5_tang/05_batch_dagster_orchestration.md)<br>• Thư mục `06_tai_lieu_ky_thuat_module/` |
| **CHƯƠNG 4: THỰC NGHIỆM, KẾT QUẢ VÀ ĐÁNH GIÁ** | • **Benchmark 1 (Query Latency):** P50, P95, P99, RPS giữa Batch-only vs Speed-only vs Lambda Merge.<br>• **Benchmark 2 (Reprocessing Accuracy):** MAE, MAPE, RMSE của Close/VWAP/Volume; thời gian tự lành $T_{\text{conv}}$.<br>• **Benchmark 3 (Compaction Efficiency):** Giảm file count (20 -> 11), Speedup 2.10x truy vấn Iceberg.<br>• **Benchmark 4 (Fault Gate):** Confusion matrix, Precision=1.0, Recall=1.0, F1=1.0, Throughput > 430k rps.<br>• **Chaos Smoke Tests (4 Test Cases):** WebSocket drop, Late 2x watermark, Duplicate seam, Flash crash burst. | • [SHORT_REPORT.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/SHORT_REPORT.md)<br>• [huong_dan_bao_ve_va_tai_lap.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/huong_dan_bao_ve_va_tai_lap.md)<br>• [SKILL.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/SKILL.md)<br>• [bao_cao_stream_lien_tuc_kafka_offset_checkpoint.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/bao_cao_stream_lien_tuc_kafka_offset_checkpoint.md) |
| **CHƯƠNG 5: KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN** | • Tổng kết các đóng góp chính của đề tài.<br>• Trả lời định lượng cho 3 câu hỏi nghiên cứu.<br>• Những giới hạn phần cứng và hạn chế còn tồn đọng.<br>• Hướng nghiên cứu và mở rộng tiếp theo. | • [de_cuong_chi_tiet_tieu_luan.md](file:///01_de_cuong_va_ke_hoach/de_cuong_chi_tiet_tieu_luan.md) (Chương 5)<br>• [huong_dan_bao_ve_va_tai_lap.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/huong_dan_bao_ve_va_tai_lap.md) (Mục 4) |
| **PHỤ LỤC & TRÌNH CHIẾU** | • Slide báo cáo thuyết trình tương tác.<br>• Biểu đồ tương tác luồng dữ liệu, Small Files, 4 loại lỗi.<br>• Kịch bản demo 5-10 phút trước hội đồng. | • Thư mục `05_slide_va_giao_dien_thuyet_trinh/`<br>*(Mở trực tiếp bằng trình duyệt: `slides.html`, `report.html`, `4errors.html`, `demo_stream.html`)* |

---

## 📁 CHI TIẾT CẤU TRÚC CÁC THƯ MỤC CON

### 📂 `01_de_cuong_va_ke_hoach/` (8 tệp)
Chứa các bản đề cương, kế hoạch 13 tuần, tiêu chí nghiệm thu 100% và phân công nhiệm vụ thành viên:
1. `de_cuong_chi_tiet_tieu_luan.md`: Đề cương chi tiết 5 Chương chuẩn của Trường/Khoa.
2. `proposal_tlcn_lambda.md`: Bản thuyết minh đề tài ban đầu.
3. `plan.md`: Kế hoạch tiến độ 13 tuần chi tiết.
4. `ke_hoach_hoan_thanh_100_phan_tram.md`: Check-list điều kiện tiên quyết hoàn thành đồ án.
5. `phan_cong_nhiem_vu.md`: Phân bổ trách nhiệm giữa 2 thành viên.
6. `plan_quan.md`: Kế hoạch công việc của Minh Quân (Batch / Iceberg / ClickHouse).
7. `plan_quoc_anh.md`: Kế hoạch công việc của Quốc Anh (Ingestion / Speed / Serving).
8. `sprint_1.md`: Kế hoạch chạy nước rút sprint ban đầu.

### 📂 `02_kien_truc_va_thiet_ke_he_thong/` (8 tệp)
Chứa các bản mô tả kiến trúc hệ thống, đánh giá công nghệ và phân tích bài toán kỹ thuật:
1. `CHUONG_2_CO_SO_LY_LUAN.md`: **Văn bản Cơ sở lý luận toàn diện Chương 2** (9 chương mục, công thức toán học, ma trận so sánh, ASCII diagrams).
2. `sys-arch.md`: Tài liệu kiến trúc hệ thống tổng thể Lambda Lakehouse.
3. `caitan_lambda_architecture.md`: Phân tích cải tiến kiến trúc Lambda với cơ chế Auto-Correction.
4. `smallfile.md`: Phân tích chuyên sâu về bài toán Small Files trên Data Lakehouse và giải pháp Compaction.
5. `summary.md`: Tóm tắt tổng quan kỹ thuật toàn bộ dự án.
6. `AGENTS.md`: Đặc tả quy chuẩn kỹ thuật và quy tắc lập trình.
7. `development.md`: Hướng dẫn thiết lập môi trường phát triển cục bộ.
8. `ROOT_PROJECT_README.md`: README tổng quan của repository.

### 📂 `03_quy_trinh_trien_khai_5_tang/` (7 tệp)
Tài liệu hướng dẫn triển khai thực tế từng tầng kiến trúc từ Ingestion đến Dashboard:
1. `01_ingestion_data_contract.md`: Thiết kế Hợp đồng dữ liệu (Data Contract) và Ingestion Pipeline.
2. `02_speed_layer_spark_streaming.md`: Cài đặt tầng xử lý tốc độ Spark Structured Streaming.
3. `03_serving_layer_query_merger.md`: Thiết kế tầng phục vụ và bộ ghép nối Auto-Correcting Query Merger.
4. `04_dashboard_streamlit_ui.md`: Xây dựng giao diện giám sát Streamlit Dashboard.
5. `05_batch_dagster_orchestration.md`: Điều phối luồng xử lý lô Medallion tự động với Dagster.
6. `data.md`: Đặc tả bộ dữ liệu thị trường tiền mã hóa.
7. `reportmock.md`: Thiết kế và sinh dữ liệu giả lập có kiểm soát.

### 📂 `04_bao_cao_thuc_nghiem_va_benchmark/` (6 tệp)
Chứa toàn bộ số liệu thực nghiệm, kết quả 4 Benchmark, kiểm toán đối soát và biên bản chạy thử nghiệm:
1. `SHORT_REPORT.md`: Báo cáo tiến độ và kết quả kiểm toán toàn diện nhất của dự án.
2. `huong_dan_bao_ve_va_tai_lap.md`: Hướng dẫn chuẩn bị bảo vệ, kịch bản demo và câu hỏi phản biện.
3. `bao_cao_stream_lien_tuc_kafka_offset_checkpoint.md`: Báo cáo đánh giá độ bền streaming 24/7 và phục hồi checkpoint.
4. `crypto_lambda_lakehouse_pre_benchmark_audit_skill.md`: Quy trình kiểm toán chất lượng đối soát tài chính Crypto.
5. `SKILL.md`: Đặc tả kỹ năng đối soát dữ liệu phân tán.
6. `giai_thich_thuat_toan_gbm.md`: Giải thích thuật toán sinh biến động giá chuyển động Brown hình học (GBM).

### 📂 `05_slide_va_giao_dien_thuyet_trinh/` (9 tệp)
Các slide trình chiếu tương tác (HTML/JS) và tài liệu phục vụ buổi thuyết trình bảo vệ:
1. `slides.html`: Bộ Slide trình chiếu chính thức (có thể mở trực tiếp bằng trình duyệt Chrome/Edge).
2. `report.html`: Báo cáo trình chiếu trực quan đa phương tiện dạng Web.
3. `report.md`: Bản ghi chép nội dung báo cáo trình chiếu.
4. `4errors.html` & `4erors.html`: Mô hình minh họa trực quan 4 loại lỗi phân tán và cơ chế tự sửa.
5. `demo_stream.html`: Giao diện mô phỏng luồng stream dữ liệu thời gian thực.
6. `smallfile_diagram.html`: Biểu đồ tương tác giải thích cơ chế nén file nhỏ Iceberg.
7. `huong_dan_thuyet_trinh_va_phan_bien.md`: Kịch bản phân chia thời gian thuyết trình 10-15 phút và trả lời phản biện.
8. `stream_demo.md`: Kịch bản hướng dẫn bấm lệnh demo trực tiếp.

### 📂 `06_tai_lieu_ky_thuat_module/` (15 tệp)
README kỹ thuật chi tiết của từng thư mục mã nguồn và thành phần hạ tầng (được đổi tên tiền tố để tránh trùng lặp):
* `src_batch_layer_README.md`, `src_data_quality_README.md`, `src_ingestion_README.md`
* `src_serving_layer_README.md`, `src_speed_layer_README.md`, `src_utils_README.md`
* `configs_README.md`, `dagster_project_README.md`, `dashboard_README.md`, `datasets_README.md`
* `dbt_project_README.md`, `references_README.md`, `results_README.md`, `scripts_README.md`, `docs_md_README.md`

---

## 🎯 CÁC BƯỚC BẮT ĐẦU VIẾT BÁO CÁO NHANH CHÓNG

1. **Khởi tạo khung bài viết:** Dùng mục lục từ [de_cuong_chi_tiet_tieu_luan.md](file:///01_de_cuong_va_ke_hoach/de_cuong_chi_tiet_tieu_luan.md) dán vào file Word / Google Docs của bạn.
2. **Lấy hình vẽ kiến trúc & sơ đồ:** Mở [sys-arch.md](file:///02_kien_truc_va_thiet_ke_he_thong/sys-arch.md) và các file HTML trong thư mục `05_slide_va_giao_dien_thuyet_trinh/` để chụp ảnh sơ đồ đưa vào Chương 2 và Chương 3.
3. **Lấy số liệu định lượng đưa vào Chương 4:**
   * Số liệu Benchmark 1 (Latency): Xem [SHORT_REPORT.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/SHORT_REPORT.md) và [huong_dan_bao_ve_va_tai_lap.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/huong_dan_bao_ve_va_tai_lap.md).
   * Số liệu Benchmark 2 (Sai số VWAP & Volume): Xem [SHORT_REPORT.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/SHORT_REPORT.md).
   * Số liệu Benchmark 3 (Compaction Speedup 2.10x): Xem [ke_hoach_hoan_thanh_100_phan_tram.md](file:///01_de_cuong_va_ke_hoach/ke_hoach_hoan_thanh_100_phan_tram.md).
   * Số liệu Benchmark 4 (DQ Gate F1=1.0): Xem bảng Confusion Matrix trong [SHORT_REPORT.md](file:///04_bao_cao_thuc_nghiem_va_benchmark/SHORT_REPORT.md).

