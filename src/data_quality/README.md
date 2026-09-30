# Data Quality Module

Thư mục chứa source code cho Data Quality Gates, Quarantine Pattern và DQ Metrics.

| File | Mô tả |
|:---|:---|
| `dq_checks.py` | Kiểm tra ràng buộc hợp lệ (Validation rules), chuẩn hóa và khử trùng lặp (Bronze → Silver) |
| `quarantine.py` | Quản lý cách ly bản ghi vi phạm (Quarantine Pattern), lưu trữ vào ClickHouse `lakehouse.dq_quarantine` hoặc JSON |
| `dq_metrics.py` | Thu thập chỉ số chất lượng dữ liệu (Pass Rate, Rejection Rate) và đo lường Precision / Recall / F1-Score đối với Fault Injection |
