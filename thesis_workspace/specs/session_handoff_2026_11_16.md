# BÀN GIAO PHIÊN LÀM VIỆC 2026-11-16 — VIẾT CHƯƠNG 1, 2, 4, 5

> Đọc tệp này đầu tiên khi mở phiên chat mới về báo cáo. Nó tóm tắt trạng thái, cách dựng tệp và những phát hiện đã làm thay đổi nội dung.
> Nguồn số liệu chuẩn vẫn là `specs/00_global_constants.md` (đã thêm ghi chú hiệu chỉnh 2026-11-16 ở Mục 4.1, 4.5 và Phần 5).

## 1. Trạng thái bản thảo (`thesis_workspace/drafts/`)

| Chương | Tệp | Số trang | Ghi chú |
| :---: | :--- | :---: | :--- |
| 1 | `chuong_1.md` → `chuong_1.docx` | 17 | Viết mới, đã đánh số trích dẫn theo danh mục 22 mục |
| 2 | `chuong_2.md` → `chuong_2.docx` | 20 | Viết mới, 16 tiểu mục |
| 3 | `chuong_3.md`, `chuong_3.docx` | 64 | Có từ trước (pandoc); chỉ đổi số trích dẫn; **nội dung chưa rà theo các phát hiện ở Mục 4 dưới đây** |
| 4 | `chuong_4.md` → `chuong_4.docx` | 16 | Viết mới; hình `figures/hinh_4_1..3.png` |
| 5 | `chuong_5.md` → `chuong_5.docx` | 7 | Viết mới |

Tổng 124 trang, vượt mức tối đa 100 của Mẫu 5. Nhóm sẽ tự rút gọn. Nên sửa trên `.md` rồi dựng lại `.docx`, vì `.docx` bị ghi đè hoàn toàn mỗi lần dựng.

## 2. Cách dựng tệp

Máy không có pandoc, và không được cài thêm gói Python (lần thử cài `latex2mathml` bị chặn). Ba script trong `thesis_workspace/` thay thế:

```bash
cd thesis_workspace/drafts
# Dựng docx: dùng chuong_3.docx làm khuôn style (Times New Roman, lề 2/2/3/2, giãn 1.5)
python ../build_chapter_docx.py chuong_3.docx chuong_N.md chuong_N.docx
# Vẽ lại hình Chương 4 từ results/logs/*.csv
python ../make_ch4_figures.py <repo_root> figures
```

- `build_chapter_docx.py`: markdown con → docx. Hỗ trợ `#`, `##`, `###`, đoạn văn, bảng `|...|`, dòng `**Bảng x.y: ...**` (tên bảng, cỡ 11 đậm, trên bảng), dòng `*Nguồn: ...*` (cỡ 10 nghiêng), `![Hình x.y: ...](figures/...png)` (tên hình dưới hình), `$$...$$` (công thức OMML), danh sách `-` và `1.`, khối code, định dạng trong dòng `**đậm**`, `*nghiêng*`, `` `code` ``, `~chỉ số dưới~`, `^chỉ số trên^`. Ô bảng cỡ 12 căn giữa, giãn dòng đơn; đề mục 1.1.1 không nghiêng (đúng Phần 7.1, khác `chuong_3.docx`); số trang giữa chân trang.
- `tex2omml.py`: tập con LaTeX → MathML → OMML qua `C:\Program Files\Microsoft Office\root\Office16\MML2OMML.XSL` và `lxml` (cần Microsoft Office). Ký hiệu chưa hỗ trợ sẽ báo lỗi rõ tên ký hiệu; thêm vào bảng `SYMBOLS`/`GREEK`.
- `make_ch4_figures.py`: bảng màu đã kiểm tra bằng `validate_palette.js` của skill dataviz; số thập phân dấu phẩy.
- Xem trước: xuất PDF bằng Word COM (`Documents.Open` → `ExportAsFixedFormat(path, 17)`), rồi kết xuất trang bằng `pymupdf`. `chuong_N.docx` phải được đóng trong WPS/Word trước khi dựng lại (lỗi `PermissionError`).

## 3. Quy ước viết đã thống nhất với nhóm

- Viết theo mã nguồn và `results/logs/`, không theo `master_outline.md` (outline chứa nhiều số liệu cũ và mô tả sai). Được gộp tiểu mục miễn đủ ý.
- Trích dẫn kiểu IEEE `[n]` theo danh mục 22 mục ở `00_global_constants.md` Phần 6 (mở rộng từ 8 mục ngày 2026-11-16, đã kiểm tra thông tin xuất bản trên web). Sách ghi kèm chương: `[14, Ch. 12]`. Khẳng định lấy từ hệ thống thì ghi đường dẫn tệp. Mỗi bảng và hình có dòng *Nguồn*. Thêm tài liệu mới phải xếp lại A–Z và đánh số lại ở mọi chương (`.md` lẫn `.docx`) và `references.bib`.
- Không tạo thư mục mới. Chỉ dùng từ viết tắt trong danh sách Phần 7.2, định nghĩa ở lần dùng đầu.

## 4. Phát hiện quan trọng đã đưa vào báo cáo (cần giữ nhất quán)

1. **Thực nghiệm 1:** watermark giả lập = T_now − 24h nên Q3 "hybrid" là **Trường hợp 2**; Trường hợp 3 chưa từng được đo độ trễ (chỉ có kiểm thử đơn vị). RQ1 cho kết quả âm: `lambda` P50 62,84 ms so với `batch_only` 60,12 ms. Đợt 2026-11-12 bị loại (phần lớn truy vấn trả về 0 nến).
2. **Thực nghiệm 2:** Speed View sinh từ Batch View cộng nhiễu có hạt giống cố định; MAPE 0,0937% / 0,1020% khớp kỳ vọng nhiễu 0,0971% và lặp lại y hệt giữa các đợt → không phải sai số thật của Spark.
3. **Thực nghiệm 3:** hệ số tăng tốc so sánh được ≈ 1,5× (trung vị bỏ lần đọc lạnh); 1,81× (CSV) và ~2,1× (đợt 2026-10-23) bị thổi phồng bởi lần đọc lạnh.
4. **Kiểm thử hỗn loạn:** chạy trong một tiến trình Python, ClickHouse giả lập, tầng tốc độ mô phỏng bằng quy tắc. Phiên bản cũ (commit `c4ea6f2`) cộng hằng số vào T_conv; chỉ đợt **2026-11-08** là số đo thật. TC4 chỉ có một đợt (`max_offsets = 20000` > 5 000 sự kiện).
5. **Bảng kiểm toán:** `sequence_gap_count` luôn ghi 0; `convergence_latency_seconds` là thời lượng lượt chạy lô.
6. **Dữ liệu:** tầng lô dùng 1 275 230 bản ghi tổng hợp (14 cặp, GBM từ nến Binance thật), không phải "5 triệu bản ghi BTCUSDT thật".
7. Các hình trong `results/plots/` (2026-10-27) là từ đợt cũ, không dùng.

## 5. Việc còn mở

- Rút gọn báo cáo về ≤ 100 trang (nhóm tự làm; Chương 3 là chỗ dài nhất).
- Rà Chương 3 theo các phát hiện ở Mục 4 (đặc biệt mọi câu mô tả Q3 là truy vấn lai, kiểm thử hỗn loạn, T_conv).
- Xác nhận Bảng 4.1: thông số phần cứng được đọc từ máy chủ ngày 2026-11-16, giả định là máy đã chạy thực nghiệm.
- Phần tiền đề (bìa, tóm tắt, danh mục) và Tài liệu tham khảo, Phụ lục chưa viết. Lưu ý: tóm tắt trong outline vẫn ghi số cũ (P50 = 50,92 ms, Speedup 2,10×).
- Nếu còn thời gian chạy lại thực nghiệm: đo Trường hợp 3 (sửa `--watermark-offset-hours` nhận số thập phân), đo độ tươi dữ liệu, chạy cả hai tầng thật trên cùng một luồng có lỗi.
