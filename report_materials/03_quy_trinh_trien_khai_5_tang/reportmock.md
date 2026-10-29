# 📋 Báo Cáo Quy Trình Thu Thập & Sinh Dữ Liệu Tổng Hợp

> **Hệ thống:** Data Lakehouse theo kiến trúc Lambda — TLCN 2026-2027
> **Nhóm:** Phạm Minh Quân (23133060) & Nguyễn Đặng Quốc Anh (23133004)
> **GVHD:** ThS. Đoàn Minh Trí — HCMUTE, Khoa CNTT

---

## 1. Tổng Quan Chiến Lược Dữ Liệu

Dự án sử dụng **2 nguồn dữ liệu bổ trợ nhau** để đảm bảo pipeline được kiểm thử toàn diện:

```
┌─────────────────────────────────────────────────────────────┐
│  NGUỒN 1 — Real Data (Binance Public API)                   │
│  Phục vụ: Development, Unit Test, Seed Pipeline             │
│  Tool: scripts/fetch_binance_data.py                        │
├─────────────────────────────────────────────────────────────┤
│  NGUỒN 2 — Synthetic Data (GBM-based Mock Generator)        │
│  Phục vụ: Benchmark 1-4, DQ Gate Testing, Stress Test       │
│  Tool: scripts/generate_mock_data.py                        │
└─────────────────────────────────────────────────────────────┘
```

**Lý do dùng 2 nguồn:**
- Real data từ Binance chỉ có ~16.7 giờ lịch sử/coin với 1 lần fetch (giới hạn 1.000 nến/request)
- Benchmark cần **hàng trăm nghìn records** với các điều kiện đặc biệt (fault events, spike events) khó xuất hiện tự nhiên trong thời gian ngắn
- Synthetic data duy trì **tính thống kê chính xác** của thị trường thật nhờ học từ real data

---

## 2. Quy Trình Thu Thập Dữ Liệu Thật (Real Data)

### 2.1. Script & Công Cụ

| Thành phần | Chi tiết |
|:---|:---|
| **Script** | `scripts/fetch_binance_data.py` |
| **API** | Binance Public REST API (miễn phí, không cần API Key) |
| **Thư viện** | `requests` (Python standard) |
| **Output** | `datasets/raw/klines/`, `datasets/raw/aggtrades/`, `datasets/raw/tickers/` |

### 2.2. Quy Trình Step-by-Step

```
Bước 1: Gọi GET /api/v3/ticker/24hr
        → Lấy toàn bộ ~2.000 cặp giao dịch
        → Lọc: chỉ giữ USDT pairs
        → Loại trừ: Leveraged tokens (UPUSDT, DOWNUSDT...)
        → Loại trừ: Stablecoins (USDC, BUSD, USD1, RLUSD...)
              ↓
Bước 2: Sắp xếp giảm dần theo quoteVolume (khối lượng USDT 24h)
        → Chọn Top-10 coins có thanh khoản cao nhất
              ↓
Bước 3: Với mỗi symbol trong Top-10:
        ├── GET /api/v3/klines?symbol=X&interval=1m&limit=1000
        │       → 1.000 nến 1-phút gần nhất (~16.7 giờ)
        │       → Lưu: datasets/raw/klines/{symbol}_klines_1m.csv
        │
        └── GET /api/v3/aggTrades?symbol=X&limit=500
                → 500 aggregate trades gần nhất
                → Chuẩn hóa về TradeEvent schema
                → Lưu: datasets/raw/aggtrades/{symbol}_aggtrades.json
              ↓
Bước 4: Lưu metadata
        → datasets/raw/summary_{run_id}.json
```

### 2.3. Kết Quả Lần Chạy Thực Tế (2026-09-01)

| Chỉ số | Lần 1 | Lần 2 (có filter stablecoin) |
|:---|:---:|:---:|
| **Symbols** | 5 | 10 |
| **Klines records** | 2,500 | 10,000 |
| **AggTrades records** | 2,500 | 5,000 |
| **Thời gian chạy** | ~13s | ~12s |
| **Stablecoins bị loại** | 2/5 | 3/10 |

**Top 10 coins (sau filter, 2026-09-01):**
`BTCUSDT · ETHUSDT · SOLUSDT · ZECUSDT · XRPUSDT · ENSOUSDT · UNIUSDT · DOGEUSDT · LINKUSDT · SUIUSDT`

### 2.4. Quản Lý Rate Limit Binance

| API endpoint | Weight | Giới hạn |
|:---|:---:|:---:|
| `GET /ticker/24hr` | 40 | 1.200/phút |
| `GET /klines` | 2 | 1.200/phút |
| `GET /aggTrades` | 2 | 1.200/phút |

**Chiến lược an toàn trong script:**
- Sleep 0.4 giây giữa mỗi request
- Budget: tối đa 1.100 weight (dự phòng 100 weight)
- Tự động pause 60 giây khi gần đạt ngưỡng

---

## 3. Chiến Lược Lưu Trữ Dữ Liệu

### 3.1. Kiến Trúc 3 Tầng Lưu Trữ

```
┌──────────────────────────────────────────────────────────────┐
│  TIER 1 — HOT STORAGE (Kafka + Redis)                        │
│  Mục đích  : Dữ liệu đang được xử lý bởi Speed Layer        │
│  Retention : Kafka 24h | Redis TTL 1-5 phút                  │
│  Truy cập  : Milliseconds                                    │
│  Tự động   : Producer chạy liên tục (Docker restart:always)  │
├──────────────────────────────────────────────────────────────┤
│  TIER 2 — WARM STORAGE (Apache Iceberg trên MinIO S3)        │
│  Mục đích  : Bronze / Silver / Gold tables của Batch Layer   │
│  Retention : Snapshot 7 ngày (sau expire_snapshots())        │
│  Truy cập  : Giây đến phút                                   │
│  Tự động   : Dagster scheduled jobs (Bronze: 5p, Silver: 1h) │
├──────────────────────────────────────────────────────────────┤
│  TIER 3 — COLD STORAGE (datasets/ — File System)             │
│  Mục đích  : Seed data, Benchmark data, Replay scenarios     │
│  Retention : Vĩnh viễn (không bị gitignore nếu < 1MB)        │
│  Truy cập  : Phút                                            │
│  Tự động   : scripts/fetch_binance_data.py chạy theo lịch    │
└──────────────────────────────────────────────────────────────┘
```

### 3.2. Cấu Trúc Thư Mục Lưu Trữ

```
datasets/
├── raw/                          ← Tier 3: Data thật từ Binance API
│   ├── klines/
│   │   ├── btcusdt_klines_1m.csv     (1.000 nến × 14 cột)
│   │   ├── ethusdt_klines_1m.csv
│   │   └── ... (10 files)
│   ├── aggtrades/
│   │   ├── btcusdt_aggtrades.json    (500 records, TradeEvent format)
│   │   └── ... (10 files)
│   ├── tickers/
│   │   └── ticker_24hr_snapshot.json
│   └── summary_YYYYMMDD_HHMMSS.json
│
├── mock/                         ← Tier 3: Data tổng hợp (GBM-based)
│   ├── klines/
│   │   ├── btcusdt_mock_klines_1m.csv    (10.080 nến/7 ngày)
│   │   └── ... (10 files)
│   ├── aggtrades/
│   │   ├── btcusdt_mock_aggtrades.json   (80.640 records với faults)
│   │   └── ... (10 files)
│   ├── stats/
│   │   ├── btcusdt_stats.json            (Thống kê học từ real data)
│   │   └── ... (10 files)
│   └── summary_YYYYMMDD_HHMMSS.json
│
├── schemas/
│   ├── trade_event_schema.json          ← JSON Schema chính thức
│   └── dlq_event_schema.json
└── sample/
    └── btcusdt_trades_sample.json       ← Sample nhỏ để test nhanh
```

### 3.3. Quy Tắc gitignore

```gitignore
datasets/raw/      ← KHÔNG commit (data lớn, tái tạo được)
datasets/mock/     ← KHÔNG commit (tái tạo được từ script)
/data/             ← KHÔNG commit (thư mục data ở root)
```

> **Lý do:** Data thô từ Binance có thể tái tạo bất kỳ lúc nào bằng `fetch_binance_data.py`. Mock data tái tạo bằng `generate_mock_data.py`. Chỉ commit **script**, không commit **data**.

---

## 4. Quy Trình Sinh Dữ Liệu Tổng Hợp (Mock Data)

### 4.1. Tại Sao Cần Mock Data?

| Vấn đề | Giải thích |
|:---|:---|
| **Không đủ volume** | 1 lần fetch từ Binance chỉ có ~16.7h dữ liệu. Benchmark cần 7-30 ngày |
| **Thiếu fault events** | Dữ liệu thật hiếm khi có duplicate/late/schema error một cách có kiểm soát |
| **Khó reproduce** | Thị trường crypto thay đổi liên tục, kết quả benchmark cần tái tạo được |
| **Market conditions** | Cần test cả trending, ranging, high-volatility scenarios |

### 4.2. Phương Pháp — Geometric Brownian Motion (GBM)

**GBM** là mô hình toán học tiêu chuẩn trong tài chính để mô phỏng giá tài sản, được dùng trong định giá quyền chọn (Black-Scholes model).

**Công thức:**

```
S(t + Δt) = S(t) × exp[(μ - σ²/2)×Δt + σ×√Δt×Z_t]

Trong đó:
  S(t)   = Giá tại thời điểm t
  μ      = Drift (xu hướng giá trung bình) — học từ real data
  σ      = Volatility (độ biến động) — học từ real data
  Δt     = 1 phút (1 bước thời gian)
  Z_t    = Biến ngẫu nhiên chuẩn N(0,1) — Wiener process increment
```

**Tại sao GBM phù hợp với crypto?**
- Giá luôn dương (S(t) > 0 mọi t) ✅
- Phần trăm thay đổi giá (returns) phân phối xấp xỉ Normal ✅
- Có cả drift (xu hướng dài hạn) và diffusion (ngẫu nhiên ngắn hạn) ✅
- Được chấp nhận rộng rãi trong nghiên cứu học thuật ✅

### 4.3. Các Thuộc Tính Thống Kê Được Học

| Thuộc tính | Công thức tính | Dùng để |
|:---|:---|:---|
| **Log-return** | `r_t = ln(close_t / close_{t-1})` | Cơ sở tính μ và σ |
| **μ (drift/phút)** | `mean(r_t)` | Xu hướng giá trung bình |
| **σ (volatility/phút)** | `std(r_t)` | Biên độ biến động |
| **μ (annualized)** | `μ_min × 1440 × 365` | Tham chiếu học thuật |
| **σ (annualized)** | `σ_min × √(1440 × 365)` | Tham chiếu học thuật |
| **Volume mean** | `mean(volume_per_candle)` | Phân phối khối lượng |
| **Volume σ** | `std(volume_per_candle)` | Độ lệch khối lượng |
| **buyer_maker_ratio** | `mean(is_buyer_maker)` | Tỷ lệ maker/taker |
| **trade_count mean/σ** | `mean/std(trade_count)` | Số lệnh/nến |

### 4.4. Sinh Volume — Log-Normal Distribution

Volume giao dịch có phân phối **Log-Normal** (không đối xứng, đuôi dài về phía phải) — đặc trưng của thị trường tài chính.

```
ln(Volume) ~ Normal(μ_ln, σ_ln²)

μ_ln = ln(mean_vol) - σ_ln²/2
σ_ln = √ln(1 + (std_vol/mean_vol)²)
```

### 4.5. Quy Trình Mock Generation (Step-by-Step)

```
Input: datasets/raw/klines/{symbol}_klines_1m.csv
                    ↓
┌─────────────────────────────────────────────┐
│  Bước 1: PHÂN TÍCH THỐNG KÊ (Analyze)      │
│  → Tính log-returns từ chuỗi close prices   │
│  → Fit μ, σ (GBM parameters)               │
│  → Fit volume distribution (Log-Normal)     │
│  → Tính buyer_maker_ratio, trade_count dist │
│  → Lưu: datasets/mock/stats/{symbol}.json   │
└─────────────┬───────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Bước 2: SINH GIÁ (GBM Simulation)         │
│  → Seed price = giá cuối của real data      │
│  → Chạy GBM n_minutes = days × 24 × 60     │
│  → Output: mảng giá close mỗi phút         │
└─────────────┬───────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Bước 3: SINH OHLCV (Candle Building)       │
│  → Mỗi phút: sinh ticks_per_min micro-ticks │
│  → open = giá đầu, close = giá cuối        │
│  → high = max(micro-ticks)                 │
│  → low  = min(micro-ticks)                 │
│  → volume ~ Log-Normal(μ_ln, σ_ln)         │
└─────────────┬───────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Bước 4: SINH AGGTRADES                     │
│  → Mỗi nến sinh 8 aggTrade ticks            │
│  → Giá nội suy linear từ open → close       │
│  → Volume phân chia theo Dirichlet dist     │
│  → is_buyer_maker theo buyer_maker_ratio    │
└─────────────┬───────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Bước 5: FAULT INJECTION                    │
│  → Duyệt từng event, xác suất inject fault  │
│  → Gắn nhãn is_injected=True, fault_type   │
│  (Chi tiết ở Mục 4.6)                      │
└─────────────┬───────────────────────────────┘
              ↓
Output: datasets/mock/klines/{symbol}_mock_klines_1m.csv
        datasets/mock/aggtrades/{symbol}_mock_aggtrades.json
```

### 4.6. Fault Injection — Chi Tiết

| Loại lỗi | Tỷ lệ (full mode) | Cách inject | Nhãn |
|:---|:---:|:---|:---|
| **duplicate** | 10% | Tạo bản sao event, tăng `ingestion_time` 1-5s | `fault_type="duplicate"` |
| **late_data** | 10% | Lùi `trade_time` về 1-5 phút trước | `fault_type="late_data"` |
| **out_of_order** | 5% | Hoán đổi thứ tự event hiện tại với event kế | `fault_type="out_of_order"` |
| **schema_invalid** | 3% | Đặt `price < 0`, `quantity = 0` | `fault_type="schema_invalid"` |

**Tất cả faults đều có `is_injected = True`** — đây là Ground Truth để:
- DQ Gate phát hiện và gửi về Quarantine ✅
- Benchmark 3 đo recall/precision của DQ system ✅
- Silver Layer dedup phát hiện duplicate ✅

### 4.7. Kết Quả Dự Kiến (7 ngày, 10 coins)

| Chỉ số | Giá trị |
|:---|:---|
| **Klines records** | 10 × 10.080 = **100,800 nến** |
| **AggTrades records** (trước fault) | 100,800 × 8 = **806,400 records** |
| **Fault records** (≈28% injected) | **~230,000 records** |
| **Tổng aggTrades** (sau fault) | **~1,036,000 records** |
| **Thời gian chạy** | ~30-60 giây |

---

## 5. So Sánh Real Data vs Mock Data

| Tiêu chí | Real Data (Binance) | Mock Data (GBM) |
|:---|:---:|:---:|
| **Độ chính xác giá** | ✅ Chính xác 100% | ⚠️ Xấp xỉ (cùng phân phối) |
| **Volume thực tế** | ✅ Có | ⚠️ Xấp xỉ Log-Normal |
| **Số lượng records** | ⚠️ Giới hạn ~16.7h/fetch | ✅ Không giới hạn (7-30 ngày) |
| **Fault events** | ❌ Không có | ✅ Có (có nhãn Ground Truth) |
| **Reproducibility** | ❌ Thay đổi theo ngày | ✅ Cố định (seeded random) |
| **Thị trường đặc biệt** | ❌ Khó kiểm soát | ✅ Có thể thiết kế |
| **Phù hợp benchmark** | ⚠️ Một phần | ✅ Tối ưu |

**Kết luận:** Dùng **Real Data cho development & validation** (đảm bảo pipeline xử lý đúng data thật), dùng **Mock Data cho benchmark** (kiểm soát điều kiện thử nghiệm).

---

## 6. Cách Sử Dụng

### 6.1. Thu Thập Data Thật

```powershell
# Cài thư viện (chỉ cần 1 lần)
pip install requests

# Fetch data (10 coins, 1000 nến/coin)
python scripts/fetch_binance_data.py --top-n 10 --klines-limit 1000

# Chỉ klines, bỏ qua aggTrades (nhanh hơn)
python scripts/fetch_binance_data.py --top-n 10 --klines-limit 1000 --skip-aggtrades
```

### 6.2. Sinh Mock Data

```powershell
# Cài thêm thư viện (numpy)
pip install numpy

# Sinh 7 ngày dữ liệu, fault injection đầy đủ (cho benchmark)
python scripts/generate_mock_data.py --days 7 --ticks-per-min 8 --fault-mode full

# Sinh 30 ngày, fault nhẹ (cho stress test)
python scripts/generate_mock_data.py --days 30 --ticks-per-min 5 --fault-mode light

# Sinh 3 ngày, không có fault (cho development)
python scripts/generate_mock_data.py --days 3 --no-faults
```

### 6.3. Chế Độ Fault Mode

| Mode | Tỷ lệ lỗi | Dùng cho |
|:---|:---|:---|
| `full` | duplicate 10%, late 10%, ooo 5%, invalid 3% | Benchmark 3 & 4 |
| `light` | Giảm 50% tất cả | Integration test |
| `none` | 0% | Development, unit test |

### 6.4. Kết Hợp với Pipeline

```powershell
# Bước 1: Fetch real data
python scripts/fetch_binance_data.py --top-n 10 --klines-limit 1000

# Bước 2: Sinh mock data cho benchmark
python scripts/generate_mock_data.py --days 7 --fault-mode full

# Bước 3: Seed dữ liệu vào Kafka (khi Docker đang chạy)
# [Script này sẽ được triển khai trong Tuần 5-6]
# python scripts/seed_kafka.py --source mock --symbol BTCUSDT

# Bước 4: Chạy benchmark
python scripts/benchmarks/run_all_benchmarks.py
```

---

## 7. Tham Chiếu Học Thuật

| Phương pháp | Tài liệu tham khảo |
|:---|:---|
| Geometric Brownian Motion | Black, F. & Scholes, M. (1973). *The Pricing of Options and Corporate Liabilities.* Journal of Political Economy |
| Log-Normal Distribution cho Volume | Ané, T. & Geman, H. (2000). *Order Flow, Transaction Clock, and Normality of Asset Returns.* Journal of Finance |
| Synthetic Data for ML/DQ Testing | Jordon, J. et al. (2022). *Synthetic Data — what, why and how?* The Royal Society |
| Fault Injection Testing | Hsueh, M.C. et al. (1997). *Fault Injection Techniques and Tools.* IEEE Computer |

---

*Tài liệu được tạo: 2026-09-02*
*Script thực thi: `scripts/fetch_binance_data.py`, `scripts/generate_mock_data.py`*
*Output data: `datasets/raw/`, `datasets/mock/`*
