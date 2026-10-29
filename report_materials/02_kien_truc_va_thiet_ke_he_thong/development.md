# Hướng dẫn Phát triển (Development Guidelines)

Tài liệu này đóng vai trò là cẩm nang phát triển và tài liệu onboarding kỹ thuật cho dự án **Data Lakehouse with Lambda Architecture**. Nó chi tiết hóa thiết lập môi trường, chiến lược phân nhánh Git, quy ước lập trình, và quy trình đảm bảo chất lượng.

---

## 1. Công nghệ & Kiến trúc dự án (Project Stack & Architecture)

Hệ thống Data Lakehouse được xây dựng theo mô hình **Lambda Architecture** gồm 3 tầng:

*   **Batch Layer:** Apache Spark (PySpark) + Apache Iceberg + MinIO + ClickHouse
*   **Speed Layer:** Apache Kafka + Spark Structured Streaming + Redis
*   **Serving Layer:** FastAPI (Query Merger) + ClickHouse + Redis
*   **Orchestration:** Dagster (Software-Defined Assets)
*   **Transformation:** dbt (Spark adapter) — Bronze → Silver → Gold
*   **Infrastructure:** Docker Compose (all services)

Chi tiết kiến trúc xem tại [docs/sys-arch.md](docs/sys-arch.md).

---

## 2. Khởi động nhanh (Getting Started)

### 2.1 Điều kiện tiên quyết (Prerequisites)
*   Python 3.10+ (khuyến nghị sử dụng Conda hoặc venv).
*   Docker Desktop (Docker Compose v2).
*   Git.

### 2.2 Thiết lập môi trường cục bộ (Local Environment Setup)
```bash
# Clone kho lưu trữ
git clone <repo-url>
cd lakehouse-lambda-benchmark

# Copy biến môi trường
cp .env.example .env

# Khởi chạy toàn bộ hạ tầng
docker compose up -d

# Cài đặt thư viện Python (cho scripts & benchmarks)
pip install -r requirements.txt
```

### 2.3 Kiểm tra dịch vụ
```bash
# Kiểm tra tất cả container đang chạy
docker compose ps

# Kiểm tra MinIO UI:   http://localhost:9001
# Kiểm tra ClickHouse:  http://localhost:8123
# Kiểm tra Dagster UI:  http://localhost:3000
# Kiểm tra FastAPI:     http://localhost:8000/docs
```

### 2.4 Chạy Pipeline & Benchmark
```bash
# 1. Nạp dữ liệu mẫu
python scripts/seed_data.py --sample

# 2. Chạy Batch Pipeline
python src/batch_layer/spark_batch_jobs.py --mode full

# 3. Chạy Speed Layer
python src/speed_layer/spark_streaming.py

# 4. Chạy Serving API
uvicorn src.serving_layer.api_routes:app --host 0.0.0.0 --port 8000

# 5. Chạy Benchmark
python scripts/benchmarks/run_all_benchmarks.py
```

---

## 3. Quy trình Phát triển & Git-Flow

### 3.1 Chiến lược phân nhánh (Branching Strategy)
*   **Nhánh nguồn:** Tất cả nhánh tính năng đều phải phân nhánh từ `develop`.
*   **Quy ước đặt tên:**
    ```bash
    git checkout develop
    git pull origin develop
    git checkout -b feature/ten-tinh-nang-cua-ban
    ```
    *(Ví dụ: `feature/batch-pipeline-setup`, `feature/query-merger-api`, `feature/bench-compaction`)*

### 3.2 Chu kỳ Tính năng (Feature Lifecycle)
```
1. Nhận task → 2. Tạo nhánh feature/ từ develop
→ 3. Code & test cục bộ → 4. Commit (Conventional Commits)
→ 5. Push & mở PR vào develop → 6. Review & merge
→ 7. Cập nhật docs/plans/ & docs/process/
```

> [!IMPORTANT]
> **Quy trình hoàn thành Task bắt buộc:**
> 1. Đánh dấu hoàn thành trong `docs/plans/` (sprint plans).
> 2. Viết tài liệu tiến trình trong `docs/process/` (bằng tiếng Việt).
> 3. Cập nhật README.md của thư mục tương ứng nếu tạo/sửa file.

### 3.3 Hướng dẫn Commit (Conventional Commits)
*   `feat: <mô tả>` — Tính năng mới.
*   `fix: <mô tả>` — Sửa lỗi.
*   `docs: <mô tả>` — Thay đổi tài liệu.
*   `refactor: <mô tả>` — Tái cấu trúc, không thay đổi hành vi.
*   `infra: <mô tả>` — Thay đổi Docker/infrastructure.
*   `bench: <mô tả>` — Thay đổi liên quan benchmark.
*   `chore: <mô tả>` — Cập nhật dependencies, configs.

---

## 4. Phong cách lập trình & Quy ước (PEP 8)

### 4.1 Tiêu chuẩn Python
*   **Định dạng:** 4 khoảng trắng (spaces). **Không dùng tab.**
*   **Biến & Hàm:** `snake_case` (ví dụ: `batch_cutoff`, `get_speed_view`).
*   **Class:** `PascalCase` (ví dụ: `QueryMerger`, `IcebergCompactor`).
*   **Hằng số:** `UPPER_CASE` (ví dụ: `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_TTL_SECONDS`).

### 4.2 Docstrings
Mỗi class và hàm chính phải có docstring:
```python
def merge_views(batch_result: dict, speed_result: dict) -> dict:
    """Hợp nhất kết quả từ Batch View và Speed View.

    Args:
        batch_result (dict): Kết quả truy vấn từ ClickHouse (Batch View).
        speed_result (dict): Kết quả truy vấn từ Redis (Speed View).

    Returns:
        dict: Kết quả đã hợp nhất với metadata (batch_cutoff, freshness).
    """
    pass
```

### 4.3 SQL & dbt
*   SQL keywords viết thường (`select`, `from`, `where`).
*   dbt model names: `snake_case`, prefix theo layer (`bronze_raw_events`, `silver_cleaned_trips`, `gold_revenue_daily`).

---

## 5. Kiểm thử & Đảm bảo Chất lượng

### 5.1 Chạy Tests
```bash
# Unit tests
pytest tests/ -v

# Test riêng từng module
pytest tests/test_batch_layer.py -v
pytest tests/test_serving_layer.py -v
```

### 5.2 Smoke Test (Không cần full data)
```bash
python scripts/seed_data.py --sample --small
python src/batch_layer/spark_batch_jobs.py --mode sample
```

---

## 6. Tiêu chuẩn hoàn thành (Definition of Done)

Một task được công nhận hoàn thành khi:
- [ ] Code chạy thành công cục bộ, không crash.
- [ ] Docker services liên quan đang chạy và hoạt động đúng.
- [ ] Dependencies mới được khai báo trong `requirements.txt` với version chính xác.
- [ ] Connection errors được xử lý gracefully (`try...except`).
- [ ] Formatting tuân thủ PEP 8 (4 spaces, snake_case).
- [ ] Hàm chính có docstrings đầy đủ.
- [ ] PR được review bởi ít nhất 1 thành viên khác.
