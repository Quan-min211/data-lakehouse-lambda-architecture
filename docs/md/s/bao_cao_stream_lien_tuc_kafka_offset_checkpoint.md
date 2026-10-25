# Báo Cáo: Mất Kafka Offset Và Checkpoint Khi Stream Liên Tục

Ngày cập nhật: 2026-10-25

## Tình huống phát sinh

Ngoài Iceberg small files, một failure mode nghiêm trọng của stream 24/7 là
Kafka retention xóa segment chứa offset mà Spark Structured Streaming checkpoint
chưa xử lý, hoặc checkpoint bị mất/hỏng sau lỗi volume/Docker Desktop. Khi query
khởi động lại, Spark không còn đọc đúng điểm resume.

Rủi ro cao nhất trước khi sửa là `failOnDataLoss=false`: Spark có thể bỏ phần
offset không còn tồn tại và tiếp tục tính candle. Pipeline vẫn xanh nhưng
`speed_agg` bị thiếu volume/trade; đây là silent data loss, khó phát hiện trong
demo và đặc biệt không chấp nhận được cho kết luận benchmark.

## Fix đã áp dụng

| Thành phần | Thay đổi | Lý do |
|---|---|---|
| `src/speed_layer/spark_streaming.py` | Thêm `kafka_source_options()` và đặt `failOnDataLoss=true` mặc định | Query fail-closed thay vì bỏ offset im lặng |
| `src/speed_layer/spark_streaming.py` | Thêm `KAFKA_STARTING_OFFSETS`, `KAFKA_MAX_OFFSETS_PER_TRIGGER`, CLI `--starting-offsets`, `--allow-data-loss` | Recovery và backpressure phải là lựa chọn minh bạch |
| `docker-compose.yml` | Khai báo rõ các guardrail Kafka cho `speed-layer` | Runtime Docker dùng cùng chính sách integrity |
| `.env.example` | Bổ sung biến cấu hình | Clone mới có default an toàn |
| `src/speed_layer/README.md` | Thêm recovery runbook | Không xóa checkpoint trước khi lưu bằng chứng/điều tra |
| `tests/test_speed_layer.py` | Thêm 2 unit test cho Kafka source options | Ngăn regression về silent data loss |

## Hành vi sau sửa

1. Checkpoint hợp lệ: Spark resume theo checkpoint; `startingOffsets` không ghi
đè vị trí resume.
2. Checkpoint tham chiếu offset bị retention xóa: Spark dừng rõ lỗi thay vì
đưa candle thiếu vào `speed_agg`.
3. Operator giữ checkpoint cũ, chạy Batch để Batch View/Iceberg phục hồi
Ground Truth cho cửa sổ ảnh hưởng.
4. Nếu Kafka còn dữ liệu cần replay provisional, operator tạo **checkpoint mới**
và chọn `KAFKA_STARTING_OFFSETS=earliest`; `ReplacingMergeTree(created_at)` giữ
revision candle mới nhất.
5. `--allow-data-loss` là escape hatch có chủ đích, không phải default. Khi dùng
phải ghi nhận khoảng bị mất và để Serving ưu tiên Batch sau watermark.

## Kịch bản kiểm thử được khuyến nghị

| Case | Cách inject | Kết quả mong đợi |
|---|---|---|
| Kafka retention loss | Dùng checkpoint cũ với topic đã xóa offset cũ | Speed query fail rõ ràng, không ghi candle mới sai |
| Checkpoint volume loss | Đổi sang checkpoint path mới | `latest` chỉ đọc event mới; `earliest` replay backlog theo lựa chọn operator |
| Flash-volume spike | Publish vượt `KAFKA_MAX_OFFSETS_PER_TRIGGER` | Micro-batch bị rate-limit, không OOM; backlog được xử lý dần |
| Recovery reconciliation | Chạy Batch sau gap | `batch_agg`/watermark thay thế provisional Speed ở các window đã đối soát |

## Kiểm chứng thực hiện

```powershell
python -m pytest tests/test_speed_layer.py -q
python -m pytest tests/test_integration.py -q
docker compose up -d --build speed-layer
docker compose logs --tail 100 speed-layer
```

## Các rủi ro stream liên tục khác cần theo dõi

- WebSocket/API silent gap hoặc đổi sequence ID: cần collector/backfill REST có
rate-limit/backoff để đóng khoảng thiếu.
- Late/out-of-order event: Speed watermark cố ý có thể loại event quá trễ;
Batch phải reprocess từ Bronze để chữa số liệu cuối.
- Duplicate tại seam REST/WebSocket: Batch DQ deduplicate và ClickHouse revision
key vẫn phải được kiểm tra bằng reconciliation metrics.
- Flash crash/backpressure: rate limit đã có (`maxOffsetsPerTrigger=50000`), nhưng
cần burn-in 24 giờ trước khi khẳng định SLA production.

## Kết luận

Tình huống Kafka offset/checkpoint được xử lý theo nguyên tắc **fail closed,
recover explicitly, reconcile with Batch truth**. Điều này ưu tiên tính đúng
đắn hơn việc giữ dashboard tiếp tục hiển thị dữ liệu khi nguồn provisional đã
không còn đầy đủ.
