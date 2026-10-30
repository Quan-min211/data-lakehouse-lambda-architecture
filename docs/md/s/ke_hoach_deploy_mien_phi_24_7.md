# Kế hoạch deploy miễn phí và vận hành stream 24/7

> Cập nhật: 30/10/2026  
> Phạm vi: đưa đồ án Lambda Lakehouse lên web, nhận dữ liệu Binance liên tục, vẫn giữ được khả năng chứng minh Batch Layer, Speed Layer và Serving Layer.

## 1. Kết luận ngắn

**Có thể đưa đồ án lên web và chạy stream dài hạn, nhưng không thể cam kết “mãi mãi 24/7” bằng các free tier managed hiện tại.** Free tier có thể đổi quota, ngủ khi không có traffic, thu hồi tài nguyên, hoặc yêu cầu xác thực thanh toán. Vì vậy cần phân biệt hai mục tiêu:

| Mục tiêu | Có đạt miễn phí không? | Nhận định |
|---|---:|---|
| Demo công khai UI/API khi bảo vệ | Có | Vercel phù hợp cho frontend tĩnh; backend demo có thể dùng Render free với giới hạn ngủ/quota. |
| Stream Binance liên tục vài giờ/ngày để kiểm thử | Có thể | Chạy trên máy cá nhân hoặc VM Always Free, kèm giám sát và cơ chế tự khôi phục. |
| Stream dữ liệu liên tục 24/7 trong nhiều tháng | Có điều kiện | Cần một máy luôn bật (PC tại nhà hoặc VM Always Free còn quota), persistent volume, watchdog và quy trình backup. Không dùng Render free làm Speed Layer. |
| “Mãi mãi”, SLA và không mất dữ liệu | Không thể bảo đảm bằng free tier | Cần ngân sách cloud tối thiểu hoặc hạ tầng tự quản cùng backup ngoài máy. |

## 2. Đánh giá các dịch vụ đã chọn

| Dịch vụ | Dùng cho | Đánh giá | Quyết định |
|---|---|---|---|
| Vercel | Frontend | Tốt nếu chuyển dashboard thành Next.js/React tĩnh hoặc client-side. Dashboard hiện là Streamlit nên **không deploy nguyên trạng lên Vercel**. | Dùng Vercel cho FE mới; hoặc giữ Streamlit trên máy/VM. |
| Render free | FastAPI demo | Phù hợp API demo ngắn hạn, không phù hợp tiến trình WebSocket/Spark/Kafka chạy nền 24/7 do chính sách free instance, quota và khả năng ngủ. | Chỉ dùng cho API demo không-critical; không chạy Speed Layer, Kafka, Spark, ClickHouse. |
| TiDB Cloud Starter | Metadata/audit nhỏ | Có thể dùng cho user, cấu hình dashboard, audit deployment, bảng cảnh báo. Không phải thay thế drop-in cho ClickHouse. | Không dùng làm Serving OLAP chính ở pha đầu. |
| Oracle Cloud Always Free | Compute liên tục | Là lựa chọn cloud-free gần nhất với nhu cầu: chạy Docker Compose tối giản trên VM. Việc cấp tài nguyên phụ thuộc region/quota; cần thẻ để đăng ký và phải tự vận hành. | Phương án cloud ưu tiên cho data plane. |
| PC/laptop cá nhân + Cloudflare Tunnel | Compute liên tục miễn phí | Rẻ nhất về tiền cloud và đúng kiến trúc hiện tại, nhưng phụ thuộc điện, mạng và máy chủ của người làm đồ án. | Phương án dự phòng và tốt cho giai đoạn bảo vệ. |

Nguồn tham khảo chính sách cần kiểm tra lại ngay trước khi đăng ký: [Render Free](https://render.com/docs/free), [Vercel Fair Use](https://vercel.com/docs/limits/fair-use-guidelines), [TiDB Cloud Starter](https://www.pingcap.com/tidb-serverless-pricing-details/), [Oracle Cloud Free Tier](https://www.oracle.com/cloud/free/).

## 3. Khoảng cách giữa code hiện tại và cloud-free

| Hạng mục hiện tại | Trạng thái | Việc còn thiếu trước deploy |
|---|---|---|
| Kafka → Spark Structured Streaming → ClickHouse/Redis | Đã chạy local | Cần profile production tối giản, restart policy, healthcheck và persistent volumes trên host cloud. |
| Checkpoint và Kafka offset | Đã có fail-closed khi mất offset/checkpoint | Cần backup checkpoint/metadata, alert khi lag hoặc restart loop và runbook khôi phục từ Binance REST. |
| Iceberg/MinIO | Đã chạy local | Cần ổ đĩa persistent, lifecycle/retention, backup snapshot và kiểm tra dung lượng. Không được dùng filesystem ephemeral của Render. |
| Batch Layer | Đã chạy local | Không chạy Spark cluster 24/7. Chuyển thành job theo lịch (mỗi 15–60 phút hoặc nightly) trên cùng VM/PC. |
| ClickHouse Serving Layer | Đã là dependency chính | Giữ ClickHouse trong VM/PC. TiDB không tương thích trực tiếp với `ReplacingMergeTree`, `FINAL`, kiểu thời gian và truy vấn OLAP đang dùng. |
| Redis speed cache | Đã chạy local | Gắn volume nếu cần warm restart; chấp nhận cache có thể tái tạo từ ClickHouse. |
| FastAPI | Đã có | Bổ sung CORS allow-list, API key/rate limit, `/ready` kiểm tra dependency, cấu hình production và reverse proxy TLS. |
| Dashboard Streamlit | Đã có | Chọn giữ trên VM/PC hoặc viết FE React/Next.js để dùng Vercel. Không nên cố đưa Streamlit server lên Vercel. |
| dbt-spark | Chỉ là model tham khảo | Không đưa dbt lên production cloud ở pha đầu; runtime chuẩn là `spark_batch_jobs.py`, tránh hai source of truth. |
| Secrets | `.env` local | Tách `.env.production`, đưa secret vào provider secret manager/environment; tuyệt đối không commit. |
| Observability | Có log container | Cần health endpoint, metrics tối thiểu, cảnh báo process died/lag/disk-full và log retention. |

## 4. Kiến trúc triển khai khuyến nghị

### Phương án A — khuyến nghị: Oracle Always Free hoặc PC luôn bật

Đây là phương án giữ được Lambda Architecture thật nhất với chi phí cloud bằng 0 (không tính điện/internet máy cá nhân).

```text
Binance WebSocket / REST
          |
          v
  VM Always Free hoặc PC luôn bật
  ├─ Kafka + ingestion producer
  ├─ Spark speed worker ──> ClickHouse + Redis
  ├─ MinIO + Iceberg
  ├─ batch job theo lịch ──> ClickHouse exact view
  ├─ FastAPI
  └─ Streamlit (tạm thời) / reverse proxy TLS
          |
          +── Cloudflare Tunnel hoặc domain/reverse proxy

Vercel (Next.js/React UI) ──HTTPS──> FastAPI
TiDB Cloud Starter (tuỳ chọn) <── deployment audit, user/config/alerts
```

Nguyên tắc vận hành:

- Không expose Kafka, MinIO, Redis, ClickHouse ra Internet; chỉ expose reverse proxy cho API/UI.
- Chạy ingestion và speed worker độc lập với API. API restart không được làm dừng stream.
- Batch chạy theo lịch bằng cron/systemd timer/Dagster trên chính host, thay vì giữ Spark cluster rỗng 24/7.
- Bắt đầu chỉ với 1–3 symbol, retention có giới hạn và partitions nhỏ; tăng dần sau burn-in.

### Phương án B — chỉ demo web miễn phí

`Vercel FE + Render FastAPI + TiDB Cloud` chỉ phù hợp khi mục tiêu là demo giao diện/API. Nó **không thỏa điều kiện stream liên tục** vì không có nơi ổn định để chạy Kafka + Spark + ClickHouse + MinIO với volume lâu dài. Có thể dùng phương án này song song với Phương án A: Render làm staging API, còn API production và stream ở VM/PC.

## 5. Bảng kế hoạch thực hiện chi tiết

| Pha | Công việc | Deliverable | Tiêu chí hoàn thành | Ước lượng |
|---|---|---|---|---:|
| 0. Chốt phạm vi | Chọn Phương án A; chọn 1–3 symbol, retention Bronze/Kafka/Iceberg, RPO/RTO cho đồ án. | `docs/deployment/decision.md` | Có quyết định rõ host chính và host dự phòng. | 0.5 ngày |
| 1. Audit cloud-readiness | Lập inventory môi trường, port, volume, secret và RAM thực dùng; không đổi schema/Docker service. | Bảng inventory + sizing | Có baseline Docker local và giới hạn RAM/disk cho host. | 1 ngày |
| 2. Deployment profile | Tạo `compose.production.yml`/profile tối giản: data plane, restart policy, healthcheck, volume named/bind rõ ràng. | Profile deploy riêng | `docker compose ... up -d` sau reboot tự hồi phục. | 1–2 ngày |
| 3. Cứng hóa ingestion | Thêm reconnect exponential backoff, heartbeat, kiểm soát duplicate/out-of-order, Binance REST backfill khi gap, DLQ/quarantine và metrics lag. | Test fault-injection + runbook | Ngắt mạng/restart Kafka worker không tạo silent data loss; gap được ghi nhận và bù. | 2–3 ngày |
| 4. Lưu trữ và chi phí | Thiết lập retention Kafka, Iceberg snapshot expiry/orphan cleanup, compaction schedule, MinIO/ClickHouse backup và disk watermark. | Chính sách retention + script backup/restore | Disk không tăng vô hạn; restore mẫu thành công. | 2 ngày |
| 5. Batch scheduler | Đưa batch pipeline thành scheduled job; lock để tránh hai batch cùng chạy; alert khi job lỗi. | Cron/systemd/Dagster job | Batch watermark tiến đều và exact view cập nhật đúng. | 1–2 ngày |
| 6. API security | CORS cho domain FE, API key/rate limit, reverse proxy HTTPS, tắt port nội bộ public, `/live`/`/ready`. | Checklist bảo mật + smoke test | Chỉ API/UI public; secrets không nằm trong git/log. | 1–2 ngày |
| 7. Frontend | Quyết định: giữ Streamlit ở VM cho MVP hoặc làm Next.js/React để deploy Vercel. Không trộn hai frontend production. | UI public | UI hiển thị health, watermark, provisional/exact và thời gian cập nhật. | 2–5 ngày |
| 8. TiDB/dbt | Dùng TiDB chỉ cho metadata/audit nếu thật sự cần. Không chuyển ClickHouse hoặc chạy dbt-spark cloud trước khi có thiết kế migration riêng. | ADR database | Không có hai source of truth cho gold transform. | 0.5–1 ngày |
| 9. CI/CD | GitHub Actions: lint/test/build image; deploy host bằng SSH pull có kiểm soát; rollback image tag. | Pipeline CI/CD | Merge main qua test; deploy/restart/rollback được log. | 1–2 ngày |
| 10. Burn-in | Chạy liên tục 7 ngày, chủ động test restart/reboot/network loss/disk pressure; đo latency, lag, correctness và compaction. | Báo cáo burn-in CSV + biểu đồ | Không silent loss; có bằng chứng khôi phục và benchmark. | 7 ngày |

## 6. Thứ tự làm để không lãng phí công sức

1. Không deploy dbt/TiDB trước. Chốt host luôn bật và tạo production profile trước.
2. Hạ stack về cấu hình chạy được trên một host nhỏ: Kafka, MinIO/Iceberg, ClickHouse, Redis, speed worker, API; Spark master/worker/thrift chỉ bật khi cần batch hoặc benchmark.
3. Hoàn thiện reliability stream và backup trước frontend đẹp. Một UI trên Vercel không có ý nghĩa nếu worker có thể mất dữ liệu âm thầm.
4. Sau 7 ngày burn-in, mới public API/domain và quyết định có cần rewrite Streamlit sang Next.js không.

## 7. Điều kiện nghiệm thu “đã deploy 24/7 cho đồ án”

- [ ] Host tự khởi động lại được toàn bộ data plane sau reboot mà không cần thao tác tay.
- [ ] Stream nhận được trade mới, lưu Bronze, cập nhật speed view và trả qua API trong SLA đã công bố.
- [ ] Batch chạy theo lịch, có watermark và exact view đối soát được với speed view.
- [ ] Có kiểm thử mất mạng, restart Kafka/worker, restart host và backfill có log chứng minh.
- [ ] Có retention, compaction và backup/restore thử nghiệm thành công.
- [ ] Không public các cổng dữ liệu nội bộ; secret không có trong Git.
- [ ] Dashboard hiển thị trạng thái nguồn, lag, watermark, thời điểm dữ liệu cuối cùng và cảnh báo stale data.
- [ ] Burn-in tối thiểu 7 ngày có báo cáo benchmark/reconciliation.

## 8. Quyết định cần chủ đồ án chốt trước khi triển khai thật

1. Host chính: **Oracle Always Free VM** hay **PC tại nhà luôn bật**. Đây là quyết định bắt buộc vì nó thay đổi cách networking, volume và deploy.
2. UI: chấp nhận Streamlit trên host cho MVP hay đầu tư rewrite Next.js để dùng Vercel.
3. Mức mất dữ liệu tối đa chấp nhận được (RPO) và thời gian khôi phục (RTO), để chọn retention và backup.
4. TiDB Cloud có thật sự cần không. Nếu chỉ để “có dbt cloud”, chưa có lý do kỹ thuật: dbt project hiện là tham khảo và runtime production là Spark/Python.

## 9. Khuyến nghị cuối cùng

Đối với đồ án này, cấu hình hợp lý nhất là: **Oracle Always Free/PC always-on chạy data plane + FastAPI; Vercel chỉ chạy frontend React/Next.js; TiDB Cloud chỉ chứa metadata/audit nếu cần.** Không dùng Render free cho luồng 24/7, và không thay ClickHouse bằng TiDB khi chưa có migration design, benchmark và thay đổi schema được phê duyệt.

