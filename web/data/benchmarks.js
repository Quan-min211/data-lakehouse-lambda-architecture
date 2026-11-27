/**
 * benchmarks.js
 * =============
 * Bộ dữ liệu thực nghiệm chính thức (Run 2026-11-11 & Chaos Audit 2026-11-08)
 * Đồng bộ 100% với Chương 4 Khóa luận & results/logs/*.csv.
 * Được phục vụ tĩnh trên Vercel để tra cứu tức thì ngay cả khi máy chủ ở nhà đang ngủ đông.
 */

window.OFFICIAL_BENCHMARKS = {
    meta: {
        officialRunDate: "2026-11-11 UTC",
        chaosRunDate: "2026-11-08 UTC",
        datasetTrades: "1,275,230 giao dịch",
        unitTests: "89/89 đạt (7 nhóm kiểm thử)"
    },

    // Benchmark 1: Query Latency (RQ1 — results/logs/bench_latency.csv, 2026-11-11, BTCUSDT, 30 runs/cell)
    bm1_latency: {
        scenarios: [
            {
                query: "Truy vấn lịch sử (24h)",
                range: "Từ 48 giờ trước đến 24 giờ trước",
                rows: 105,
                batch_only: { p50: 63.66, p95: 73.67, p99: 109.65, mean: 60.91 },
                speed_only: { p50: 60.09, p95: 75.80, p99: 112.84, mean: 62.08 },
                lambda:     { p50: 65.42, p95: 100.05, p99: 122.02, mean: 67.54 }
            },
            {
                query: "Truy vấn tức thời (5 phút)",
                range: "5 phút gần nhất",
                rows: 5,
                batch_only: { p50: 55.88, p95: 96.17, p99: 121.85, mean: 59.07 },
                speed_only: { p50: 58.74, p95: 96.70, p99: 102.78, mean: 59.69 },
                lambda:     { p50: 59.44, p95: 99.79, p99: 135.07, mean: 59.76 }
            },
            {
                query: "Truy vấn kết hợp (30 phút)",
                range: "30 phút gần nhất (cắt qua mốc chốt sổ)",
                rows: 30,
                batch_only: { p50: 60.28, p95: 79.51, p99: 96.30,  mean: 56.64 },
                speed_only: { p50: 58.73, p95: 96.21, p99: 109.58, mean: 62.91 },
                lambda:     { p50: 64.51, p95: 117.19, p99: 128.15, mean: 67.63 }
            }
        ],
        overall: {
            batch_only: { p50: 60.12, p95: 80.54,  p99: 124.24, mean: 58.87 },
            speed_only: { p50: 58.94, p95: 96.70,  p99: 112.18, mean: 61.56 },
            lambda:     { p50: 62.84, p95: 114.71, p99: 131.32, mean: 64.98 }
        },
        insight: "Độ trễ trung vị P50 khi hợp nhất hai luồng dữ liệu (62.84 ms) chỉ chênh ~2.7 ms so với truy vấn một luồng đơn lẻ (58.94 – 60.12 ms), đổi lại bảo đảm ghép nối liền mạch theo mốc chốt sổ mà không bị trùng lặp."
    },

    // Benchmark 2: Reprocessing Correctness (RQ2 — results/logs/bench_reprocess_summary.csv, 2026-11-11)
    bm2_reprocess: {
        runs: [
            {
                symbol: "BTCUSDT",
                window: "2026-10-17 07:59 → 08:59 UTC",
                matched_candles: 57,
                coverage_pct: 100.0,
                mae: 61.2726,
                mape_pct: 0.0937,
                rmse: 77.1853,
                post_batch_error: 0.0
            },
            {
                symbol: "ETHUSDT",
                window: "2026-10-17 07:59 → 08:59 UTC",
                matched_candles: 55,
                coverage_pct: 100.0,
                mae: 3.2056,
                mape_pct: 0.1020,
                rmse: 3.9567,
                post_batch_error: 0.0
            }
        ],
        insight: "Toàn bộ 112/112 cửa sổ nến 1 phút đạt độ bao phủ đối soát 100%. Sau khi luồng Lô hoàn tất tái xử lý và nâng mốc chốt sổ, mọi sai lệch tạm thời đều được thay thế hoàn toàn về 0.000%."
    },

    // Benchmark 3: Iceberg Small-File Compaction (RQ3 — results/logs/bench_compaction.csv, mode=real, 20 runs)
    bm3_compaction: {
        files_before: 118,
        files_after: 10,
        file_reduction_pct: 91.5,
        avg_mb_before: 0.97,
        avg_mb_after: 10.60,
        total_mb_before: 114.37,
        total_mb_after: 105.97,
        warm_median_before_ms: 1821.84,
        warm_median_after_ms: 1206.37,
        warm_speedup: 1.51,
        mean_before_ms: 2219.96,
        mean_after_ms: 1226.09,
        mean_speedup: 1.81,
        stddev_before_ms: 1553.07,
        stddev_after_ms: 55.48,
        compaction_duration_s: 17.54,
        series_before: [
            8740.48, 2116.82, 2028.14, 1985.40, 1912.66,
            1889.31, 1854.12, 1835.77, 1821.84, 1810.45,
            1798.20, 1842.19, 1865.03, 1804.88, 1779.64,
            1815.92, 1830.11, 1791.50, 1962.40, 1914.38
        ],
        series_after: [
            1245.12, 1318.44, 1290.15, 1254.80, 1212.33,
            1195.60, 1206.37, 1218.90, 1188.45, 1201.72,
            1362.10, 1240.55, 1192.08, 1209.64, 1224.18,
            1208.87, 1198.46, 1182.38, 1177.93, 1200.95
        ],
        insight: "Tối ưu hóa lưu trữ giúp gộp 118 tệp dữ liệu nhỏ xuống còn 10 tệp (-91.5%) chỉ trong 17.54 giây, tăng tốc độ truy vấn lên 1.51 lần và giảm độ dao động thời gian phản hồi tới 28 lần."
    },

    // Benchmark 4: Data Quality Gate (bm_fault_20261111_185324)
    bm4_dq_gate: {
        total_records: 16018,
        tp: 874,
        fp: 0,
        fn: 0,
        tn: 14126,
        duplicates_removed: 1018,
        precision: 1.0,
        recall: 1.0,
        f1_score: 1.0,
        mean_accuracy: 0.9365,
        median_duration_ms: 14.26,
        median_throughput_rps: 374895,
        peak_throughput_rps: 425100
    },

    // Chaos Smoke Test Protocol (4 Scenarios — 2026-11-08T13:45:47 UTC)
    chaos_tests: [
        {
            code: "Kịch bản 1",
            name: "Đứt kết nối luồng thu thập & mất chuỗi giao dịch",
            fault: "Gián đoạn kết nối tạm thời, thiếu hụt 50/200 giao dịch liên tiếp",
            pre_drift: "Thiếu 25.0% giao dịch · Lệch khối lượng 23.58%",
            post_drift: "Khôi phục đủ 100% · Độ lệch 0.0%",
            t_conv_s: 0.0035,
            status: "ĐẠT"
        },
        {
            code: "Kịch bản 2",
            name: "Giao dịch đến trễ vượt ngưỡng chờ thời gian thực",
            fault: "40/200 giao dịch đến trễ 120 giây do nghẽn mạng",
            pre_drift: "Lệch khối lượng tạm thời 21.15%",
            post_drift: "Bù đắp đầy đủ theo giờ khớp lệnh · Độ lệch 0.0%",
            t_conv_s: 0.0094,
            status: "ĐẠT"
        },
        {
            code: "Kịch bản 3",
            name: "Bản ghi trùng lặp hàng loạt khi kết nối lại",
            fault: "Phát lặp 1,000 giao dịch thành 2,000 bản ghi",
            pre_drift: "Trùng lặp 50.0% (nhân đôi khối lượng ảo)",
            post_drift: "Khử trùng hoàn toàn · Độ lệch 0.0%",
            t_conv_s: 0.0123,
            status: "ĐẠT"
        },
        {
            code: "Kịch bản 4",
            name: "Đột biến lưu lượng giao dịch gấp 10 lần",
            fault: "Dồn dập 5,000 giao dịch trong thời gian ngắn",
            pre_drift: "Áp lực tải cao gấp 10 lần bình thường",
            post_drift: "Xử lý trọn vẹn 5,000/5,000 · Hệ thống ổn định",
            t_conv_s: 0.0618,
            status: "ĐẠT"
        }
    ]
};
