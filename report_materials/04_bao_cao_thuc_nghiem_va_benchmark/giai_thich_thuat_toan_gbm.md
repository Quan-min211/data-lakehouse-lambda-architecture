# 📐 Giải Thích Chi Tiết Thuật Toán Geometric Brownian Motion (GBM)
## Trong Mô Phỏng Dữ Liệu Thị Trường Tiền Mã Hóa — Hệ Thống Lambda Lakehouse

> **Tài liệu tham khảo chuyên sâu phục vụ Thuyết trình & Phản biện Luận văn Tốt nghiệp**  
> **Đề tài:** Thiết kế và triển khai hệ thống Data Lakehouse theo kiến trúc Lambda hỗ trợ đối soát dữ liệu thời gian thực cho thị trường tiền mã hóa.  
> **Sinh viên thực hiện:** Nguyễn Đặng Quốc Anh (23133004) — Phạm Minh Quân (23133060)  
> **Giảng viên hướng dẫn:** ThS. Đoàn Minh Trí — Trường ĐH Sư phạm Kỹ thuật TP.HCM (HCMUTE)

---

## 1. 📌 Tổng Quan: Mô Hình GBM Là Gì?

**Geometric Brownian Motion (GBM)** — tiếng Việt gọi là **Chuyển động Brown hình học** — là mô hình quá trình ngẫu nhiên thời gian liên tục chuẩn mực nhất trong toán tài chính và kinh tế lượng. 

Mô hình này là nền tảng cốt lõi của công trình định giá quyền chọn nổi tiếng **Black–Scholes–Merton** (đoạt giải **Nobel Kinh tế năm 1973**).

Trong đề tài này, GBM được sử dụng để sinh ra **~1.5 triệu bản ghi giao dịch giả lập (7 ngày liên tục cho 13 cặp coin)** từ 1.000 nến thật cào từ sàn Binance, nhằm phục vụ kiểm thử tải và chạy các bài đo thực nghiệm (**Benchmark 1, 2, 3**).

---

## 2. 🧮 Cơ Sở Toán Học Của Mô Hình GBM

### 2.1. Phương trình vi phân ngẫu nhiên (SDE — Stochastic Differential Equation)

Trong lý thuyết xác suất và vi tích phân ngẫu nhiên (Itô Calculus), sự biến thiên của giá tài sản $S_t$ theo thời gian liên tục được mô tả bởi phương trình:

$$dS_t = \mu S_t dt + \sigma S_t dW_t$$

Hoặc viết dưới dạng tỷ suất sinh lợi tức thời:

$$\frac{dS_t}{S_t} = \mu dt + \sigma dW_t$$

Trong đó:
* $S_t$: Giá tài sản tại thời điểm $t$.
* $\mu$: Hệ số trôi dạt (**Drift**), đại diện cho xu hướng tăng/giảm giá trung bình trong dài hạn.
* $\sigma$: Hệ số khuếch tán hay độ biến động (**Volatility**), đại diện cho biên độ dao động rung lắc của giá.
* $dt$: Bước thời gian vô cùng bé.
* $dW_t$: Vi phân của quá trình Wiener chuẩn (**Standard Brownian Motion**), thỏa mãn:
  $$dW_t = \sqrt{dt} \cdot Z \quad \text{với} \quad Z \sim \mathcal{N}(0, 1)$$

---

### 2.2. Nghiệm giải tích chính xác qua Bổ đề Itô (Itô's Lemma)

Do số hạng nhiễu $dW_t$ là ngẫu nhiên và không khả vi theo giải tích thông thường, ta áp dụng **Bổ đề Itô (Itô's Lemma)** cho hàm số $f(S_t) = \ln(S_t)$:

$$d(\ln S_t) = \frac{\partial \ln S_t}{\partial S_t} dS_t + \frac{1}{2} \frac{\partial^2 \ln S_t}{\partial S_t^2} (dS_t)^2$$

Vì $\frac{\partial \ln S_t}{\partial S_t} = \frac{1}{S_t}$, $\frac{\partial^2 \ln S_t}{\partial S_t^2} = -\frac{1}{S_t^2}$, và theo quy tắc nhân Itô: $(dS_t)^2 = \sigma^2 S_t^2 dt$:

$$d(\ln S_t) = \frac{1}{S_t} (\mu S_t dt + \sigma S_t dW_t) - \frac{1}{2} \frac{1}{S_t^2} (\sigma^2 S_t^2 dt)$$

$$d(\ln S_t) = \left( \mu - \frac{\sigma^2}{2} \right) dt + \sigma dW_t$$

Lấy tích phân hai vế từ $0$ đến $t$:

$$\ln(S_t) - \ln(S_0) = \left( \mu - \frac{\sigma^2}{2} \right) t + \sigma W_t$$

Lấy hàm số mũ ($\exp$) hai vế, ta thu được **công thức nghiệm giải tích dạng liên tục**:

$$S_t = S_0 \times \exp\left[ \left( \mu - \frac{\sigma^2}{2} \right) t + \sigma W_t \right]$$

---

### 2.3. Dạng rời rạc hóa (Discrete Form — Cài đặt trực tiếp vào Python)

Để mô phỏng từng bước nhảy thời gian $\Delta t$ (ví dụ: mỗi bước là 1 phút), công thức tính giá $S_{t+1}$ từ giá $S_t$ của bước trước đó trở thành:

$$S_{t+1} = S_t \times \exp\left[ \underbrace{\left( \mu - \frac{\sigma^2}{2} \right) \Delta t}_{\text{Thành phần xu hướng (Drift)}} + \underbrace{\sigma \sqrt{\Delta t} \cdot Z_t}_{\text{Thành phần ngẫu nhiên (Diffusion)}} \right]$$

*Trong đó:* $Z_t \sim \mathcal{N}(0, 1)$ là biến ngẫu nhiên độc lập tuân theo phân phối chuẩn tắc (Mean = 0, Std = 1).

---

## 3. 🔍 Giải Thích Chi Tiết Từng Biến Số Trong Hệ Thống Của Đề Tài

Trong hệ thống mã nguồn của dự án (tại file `scripts/generate_mock_data.py`), các biến số được thiết lập và mang ý nghĩa cụ thể như sau:

```
μ = 0.0 (drift trung tính)  ·  σ = 0.02 (độ biến động 2%/phút)
dt = 1/525,600 (mỗi tick = 1 phút/năm)  ·  Z ~ N(0,1)
S₀ = Giá thực tế cuối cùng của từng coin từ Binance
```

---

### 3.1. $S_0$ — Giá khởi tạo thực tế (Seed Price)
* **Ý nghĩa:** Là mức giá xuất phát điểm tại thời điểm $t = 0$ của chuỗi mô phỏng.
* **Cách lấy trong đề tài:** Không lấy một con số ngẫu nhiên tùy tiện, mà lấy **chính xác giá đóng cửa (Close price) của cây nến thật cuối cùng** được cào từ Binance API (trong thư mục `datasets/raw/klines/`).
  * Ví dụ: `BTCUSDT` có $S_0 = 78,170.94\text{ USDT}$, `ETHUSDT` có $S_0 = 2,740.50\text{ USDT}$, `SOLUSDT` có $S_0 = 142.10\text{ USDT}$.
* **Mục đích:** Đảm bảo chuỗi dữ liệu giả lập nối tiếp mượt mà, liền mạch về mặt biên độ giá với dữ liệu lịch sử thật, không bị hiện tượng nhảy vọt giá (gap) vô lý.

---

### 3.2. $\mu = 0.0$ — Drift trung tính (Neutral Drift)
* **Ý nghĩa:** $\mu$ biểu thị tỷ suất sinh lợi kỳ vọng trung bình theo thời gian (kỳ vọng xu hướng thị trường).
* **Tại sao lại đặt $\mu = 0.0$ trong kịch bản chuẩn?**
  1. **Tính chất Martingale (Công bằng / Không thiên lệch):** Nếu đặt $\mu > 0$, giá coin sẽ có xu hướng tăng phi mã (bull run) sau 7 ngày; nếu đặt $\mu < 0$, giá sẽ tụt dốc không phanh (crash). Đặt $\mu = 0.0$ giúp dữ liệu mô phỏng trạng thái **thị trường dao động tự nhiên quanh mức cân bằng**, không bị thiên vị cho phe mua hay phe bán.
  2. **Tập trung vào kiểm thử hạ tầng (Data Engineering):** Mục tiêu của đồ án là đo đạc hiệu năng xử lý của Spark Streaming, Kafka, ClickHouse và Iceberg, chứ không phải đánh giá chiến lược đầu tư. Do đó, drift trung tính $\mu = 0.0$ là thiết lập chuẩn mực khoa học để kiểm thử tải ổn định.

---

### 3.3. $\sigma = 0.02$ — Độ biến động (Volatility = 2% mỗi phút)
* **Ý nghĩa:** $\sigma$ đại diện cho độ lệch chuẩn của tỷ suất sinh lợi logarit, tức là biên độ co giãn rung lắc của giá trong 1 đơn vị thời gian.
* **Tại sao con số 2%/phút ($\sigma = 0.02$) lại cực kỳ quan trọng với đề tài?**
  1. **Đặc thù thị trường Crypto:** Tiền mã hóa có tính biến động cao hơn cổ phiếu truyền thống từ 5 đến 10 lần. Mức dao động $\approx 2\%$/phút phản ánh đúng những thời điểm thị trường có khối lượng giao dịch sôi động.
  2. **Kích hoạt thuật toán phát hiện đột biến giá (Price Spike Detector):**
     * Trong Tầng Speed Layer (`src/speed_layer/spike_detector.py`), nhóm cài đặt thuật toán cảnh báo đột biến giá khi biên độ nến vượt ngưỡng:
       $$\text{Spike Threshold} \ge 2.0\%$$
     * Với $\sigma = 0.02$, phân phối chuẩn $Z \sim \mathcal{N}(0, 1)$ sẽ có khoảng **~5% số nến** rơi vào vùng ngoài $2\sigma$ hoặc $3\sigma$ (tương đương biên độ giật $\ge 2\%$).
     * Nhờ vậy, bộ tạo dữ liệu sẽ sinh ra các đợt giật giá tự nhiên để **kiểm chứng xem Spark Streaming có bật cờ `is_spike = 1` kịp thời trong vòng SLA < 5 giây hay không**.

---

### 3.4. $dt = \frac{1}{525,600}$ — Bước nhảy thời gian quy chuẩn theo năm
* **Ý nghĩa:** Là độ dài của một bước nhảy thời gian (time-step) được quy đổi theo đơn vị **Năm (Annualized Base)**.
* **Nguồn gốc con số 525,600:**
  * Thị trường Crypto giao dịch liên tục **24/7/365**, không có ngày nghỉ cuối tuần hay lễ tết:
    $$1 \text{ năm} = 365 \text{ ngày} \times 24 \text{ giờ/ngày} \times 60 \text{ phút/giờ} = \mathbf{525,600 \text{ phút}}$$
  * Mỗi bước mô phỏng là **1 phút**, do đó quy đổi sang năm:
    $$dt = \Delta t = \frac{1}{525,600}$$
* **Tại sao phải quy chuẩn theo năm?**
  Trong toán tài chính, các chỉ số độ biến động $\sigma$ và lợi suất $\mu$ thường được chuẩn hóa theo năm (annualized) để so sánh giữa các loại tài sản. Việc nhân với $\sqrt{dt} = \sqrt{\frac{1}{525,600}}$ giúp bảo đảm tính đồng nhất thứ nguyên (dimensional consistency) giữa mô hình toán học và mã nguồn máy tính.

---

### 3.5. $Z \sim \mathcal{N}(0, 1)$ — Biến ngẫu nhiên chuẩn (Wiener Increment)
* **Ý nghĩa:** Là một biến ngẫu nhiên tuân theo phân phối Gauss (phân phối chuẩn tắc) với kỳ vọng $\mathbb{E}[Z] = 0$ và phương sai $\text{Var}(Z) = 1$.
* **Vai trò trong công thức:**
  * Đại diện cho **sự bất định hoàn toàn (Shock/Innovation)** của thị trường: những tin tức kinh tế vĩ mô, phát ngôn của các nhân vật ảnh hưởng, hay các lệnh mua/bán cá voi diễn ra ngẫu nhiên từng phút.
  * Trong mã nguồn Python: được sinh bởi bộ sinh số ngẫu nhiên của NumPy:
    ```python
    z = rng.standard_normal()
    ```
  * Để đảm bảo **tính tái tạo khoa học (Reproducibility)**, nhóm cố định `seed = 42`. Bất kỳ ai chạy lại script trên máy tính khác cũng sẽ sinh ra chính xác 100% từng con số giống hệt nhau.

---

### 3.6. $-\frac{\sigma^2}{2}$ — Số hạng hiệu chỉnh Itô (Itô Correction Term)
* **Ý nghĩa toán học đặc biệt:** 
  Khi nhìn vào công thức, nhiều người hay thắc mắc: *"Tại sao không phải là $\mu$ mà lại là $\mu - \frac{\sigma^2}{2}$?"*.
* **Giải thích:** 
  Do tính chất phi tuyến của hàm số mũ, nếu một biến ngẫu nhiên $X \sim \mathcal{N}(m, s^2)$, thì kỳ vọng toán học của hàm mũ là:
  $$\mathbb{E}[e^X] = e^{m + \frac{1}{2}s^2}$$
  Áp dụng vào phương trình giá: nếu ta không trừ đi $\frac{\sigma^2}{2}$, thì giá kỳ vọng sau một bước sẽ bị phình to:
  $$\mathbb{E}[S_{t+1} \mid S_t] = S_t \cdot e^{\mu \Delta t + \frac{1}{2}\sigma^2 \Delta t} \quad (\text{Bị lệch so với xu hướng thật } \mu)$$
  Bằng cách đưa số hạng hiệu chỉnh $-\frac{\sigma^2}{2}$ vào hàm mũ, ta triệt tiêu được phần dư $\frac{1}{2}\sigma^2$:
  $$\mathbb{E}[S_{t+1} \mid S_t] = S_t \cdot e^{\mu \Delta t}$$
  👉 **Số hạng này giúp cho giá trị kỳ vọng của chuỗi giá giả lập hoàn toàn chuẩn xác về mặt toán học, không bị méo mó theo thời gian.**

---

## 4. 🔄 Quy Trình Sinh Dữ Liệu Thực Tế Trong Project: Từ Giá GBM Đến TradeEvent

Trong file `scripts/generate_mock_data.py`, quá trình sinh dữ liệu trải qua 4 bước khép kín:

```
[Binance Real Data] (1.000 nến thật)
        │
        ▼ (Học tham số μ, σ, Volume mean/std, BuyerMaker ratio)
┌─────────────────────────────────────────────────────────────┐
│ Bước 1: Sinh chuỗi giá đóng cửa (Close Prices) bằng GBM    │
│ S(t+1) = S(t) * exp((μ - σ²/2)*dt + σ*sqrt(dt)*Z)           │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Bước 2: Sinh khối lượng Volume theo phân phối Log-Normal    │
│ ln(Vol) ~ N(μ_vol, σ_vol²)  --> Volume luôn dương & đuôi dài │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Bước 3: Nội suy tạo nến OHLCV 1 phút (Micro-ticks)          │
│ Tạo 5-8 ticks bên trong mỗi phút: Open, High, Low, Close    │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Bước 4: Tiêm lỗi có kiểm soát (Fault Injection)             │
│ Cố tình tạo: Duplicate (10%), Late Data (10%),             │
│ Out-of-order (5%), Schema Invalid (3%)                      │
│ Đánh dấu: is_injected = True                                │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
[Output JSON & CSV] (1.26M aggTrades + 131K nến cho 13 coins)
```

---

## 5. 🎯 Tại Sao Mô Hình GBM Lại Phù Hợp Hoàn Hảo Với Đề Tài Luận Văn Này?

Khi báo cáo hoặc phản biện, hội đồng có thể hỏi: *"Tại sao em không dùng mô hình khác như Random Walk, ARIMA hay Deep Learning (GANs/LSTM)?"*

Dưới đây là bảng so sánh làm nổi bật lý do nhóm lựa chọn GBM:

| Tiêu chí | Random Walk cơ bản | Deep Learning (TimeGAN, LSTM) | Geometric Brownian Motion (GBM) |
| :--- | :--- | :--- | :--- |
| **Giá trị âm** | ❌ Dễ bị âm khi giá tụt dốc ($S_t < 0$) | ⚠️ Có thể âm nếu không chuẩn hóa khắt khe | ✅ **Chắc chắn luôn dương ($S_t > 0$) nhờ hàm $\exp$** |
| **Bản chất tài chính** | ❌ Không phản ánh đúng phân phối log-return | ⚠️ Mô hình hộp đen (Black-box), khó giải thích toán học | ✅ **Chuẩn mực học thuật quốc tế (Giải Nobel Kinh tế 1973)** |
| **Tốc độ sinh dữ liệu** | ✅ Nhanh | ❌ Rất chậm, tốn tài nguyên train GPU | ✅ **Cực nhanh: Sinh 1.5 triệu bản ghi chỉ mất ~30 giây trên CPU** |
| **Tính tái tạo (Reproducibility)** | ⚠️ Phụ thuộc random seed | ❌ Khó tái tạo chính xác 100% giữa các máy tính | ✅ **Tái tạo chính xác 100% nhờ Seeded Pseudo-Random** |
| **Độ phù hợp mục tiêu đề tài** | ❌ Quá thô sơ | ❌ Lạc đề (đề tài làm Data Lakehouse, không làm AI) | ✅ **Phù hợp tuyệt đối với mục tiêu thử tải Big Data** |

---

## 6. 🎤 Kịch Bản Trình Bày Ngắn Gọn (1 Phút) Cho Thầy / Hội Đồng

> *"Kính thưa Thầy và Hội đồng,  
> Để có đủ khối lượng dữ liệu lớn phục vụ đo đạc hiệu năng hệ thống Data Lakehouse mà không bị giới hạn bởi Rate Limit của sàn Binance, nhóm đã áp dụng mô hình toán học chuẩn mực **Geometric Brownian Motion (GBM)**.  
> 
> Công thức cốt lõi là:
> $$S_{t+1} = S_t \times \exp\left[\left(\mu - \frac{\sigma^2}{2}\right)\Delta t + \sigma \sqrt{\Delta t} Z_t\right]$$
> Trong đó:
> - $S_0$ lấy trực tiếp từ **giá đóng cửa thực tế** của từng coin từ Binance để bảo đảm tính liên tục.
> - $\mu = 0.0$ giữ cho **thị trường dao động trung tính**, phản ánh khách quan trạng thái vận hành.
> - $\sigma = 0.02$ mô phỏng **độ biến động 2%/phút**, phù hợp với đặc thù thị trường crypto và giúp kích hoạt bộ cảnh báo **Price Spike ($\ge 2\%$)** của tầng Speed Layer.
> - Bước thời gian $\Delta t = \frac{1}{525,600}$ chuẩn hóa theo tổng số phút của một năm giao dịch 24/7/365.
> - Thành phần ngẫu nhiên $Z_t \sim \mathcal{N}(0, 1)$ tạo ra các cú rung lắc tự nhiên của thị trường.
>
> Nhờ mô hình này, nhóm đã sinh ra **1.5 triệu bản ghi giao dịch chuẩn mực**, bảo đảm giá luôn dương, giữ nguyên đặc tính biến động của 13 đồng coin, và tạo tiền đề vững chắc để chạy các bài kiểm thử Benchmark 1, 2 và 3 trong đề tài."*

---
*Tài liệu được khởi tạo ngày 14/09/2026 phục vụ bảo vệ đề tài TLCN — Khóa K23 HCMUTE.*
