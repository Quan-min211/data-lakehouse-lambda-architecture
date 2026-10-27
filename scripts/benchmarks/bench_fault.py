"""
bench_fault.py
==============
Benchmark 3: Data Quality Gate & Fault Handling Evaluation.
Evaluates the efficacy of the Data Quality Gate and Quarantine Table
when subjected to controlled fault injection:
  - Confusion Matrix: True Positive (TP), False Positive (FP), False Negative (FN), True Negative (TN)
  - Key Classification Metrics: Precision, Recall, F1-Score
  - Execution Overhead: Latency of Data Quality & Deduplication checking (ms)

Matches requirements in:
  - docs/md/de_cuong_chi_tiet_tieu_luan.md (Section 4.4)
  - docs/plan/plan_quan.md (Weeks 10-11)

Outputs:
  - results/logs/bench_fault_results.csv
  - results/logs/bench_fault_summary.csv
  - results/plots/benchmark_fault_matrix.png (optional plot)
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.data_quality.dq_checks import clean_and_deduplicate, QualityReport
from src.data_quality.dq_metrics import DQMetricsCollector, DQMetricsSummary
from src.ingestion.fault_injector import FaultInjector
from src.ingestion.models import TradeEvent
from src.utils.logger import setup_logger

logger = setup_logger("bench_fault")

RESULT_LOG_DIR = ROOT_DIR / "results" / "logs"
RESULT_PLOT_DIR = ROOT_DIR / "results" / "plots"
RAW_RESULTS_CSV = RESULT_LOG_DIR / "bench_fault_results.csv"
SUMMARY_CSV = RESULT_LOG_DIR / "bench_fault_summary.csv"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def generate_benchmark_dataset(
    count: int = 5000,
    symbol: str = "BTCUSDT",
    fault_rate: float = 0.20,
    seed: int = 42,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Sinh tập dữ liệu TradeEvent có kiểm soát lỗi chính xác:
      - Duplicate (trùng lặp ID)
      - Late data (lệch timestamp > 2h)
      - Schema invalid (thiếu field/sai type)
      - Negative/Zero price & quantity
      - Clean (chuẩn)
    """
    random.seed(seed)
    injector = FaultInjector(
        duplicate_rate=fault_rate * 0.35,
        late_data_rate=fault_rate * 0.25,
        schema_invalid_rate=fault_rate * 0.20,
        out_of_order_rate=fault_rate * 0.20,
        late_min_seconds=60,
        late_max_seconds=300,
    )

    t0_ms = int(datetime(2026, 10, 17, 8, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    raw_records: List[Dict[str, Any]] = []
    fault_counts: Dict[str, int] = {
        "clean": 0,
        "duplicate": 0,
        "late_data": 0,
        "schema_invalid": 0,
        "negative_price": 0,
    }

    base_price = 68000.0
    for i in range(count):
        # 1. Sinh event gốc
        event = TradeEvent(
            trade_id=1000000 + i,
            symbol=symbol,
            price=round(base_price + random.uniform(-100.0, 100.0), 2),
            quantity=round(random.uniform(0.01, 2.5), 4),
            trade_time=t0_ms + (i * 200),
            is_buyer_maker=random.random() > 0.5,
            ingestion_time=t0_ms + (i * 200) + 15,
            is_injected=False,
            fault_type=None,
        )

        # Chủ động tạo một số lỗi giá âm / số lượng 0
        if random.random() < (fault_rate * 0.10):
            event.price = -abs(event.price)
            event.is_injected = True
            event.fault_type = "negative_price"
            raw_records.append(event.to_dict())
            fault_counts["negative_price"] += 1
            continue

        # Chạy qua FaultInjector
        injected_events = injector.process_event(event)
        for ev in injected_events:
            rec = ev.to_dict()
            f_type = rec.get("fault_type")
            if rec.get("is_injected") and f_type:
                fault_counts[f_type] = fault_counts.get(f_type, 0) + 1
            else:
                fault_counts["clean"] += 1
            raw_records.append(rec)

    return raw_records, fault_counts


def run_single_benchmark_iteration(
    run_id: str,
    iteration: int,
    count: int = 5000,
    symbol: str = "BTCUSDT",
    fault_rate: float = 0.20,
    seed: int = 42,
) -> Dict[str, Any]:
    """Chạy 1 lượt kiểm thử độ chính xác và độ trễ của Data Quality Gate."""
    raw_dataset, fault_counts = generate_benchmark_dataset(
        count=count, symbol=symbol, fault_rate=fault_rate, seed=seed + iteration
    )
    total_records = len(raw_dataset)

    # Đo độ trễ xử lý của DQ Gate
    t_start = time.perf_counter()
    clean_records, rejected_records, report = clean_and_deduplicate(raw_dataset)
    duration_ms = round((time.perf_counter() - t_start) * 1000.0, 2)
    throughput_rps = round(total_records / (duration_ms / 1000.0), 0) if duration_ms > 0 else 0.0

    # Đánh giá ma trận nhầm lẫn cho Quarantine Gate (Lỗi Schema & Giá trị bất hợp lệ):
    # - TP: Bản ghi bị tiêm lỗi schema_invalid/negative_price và bị Gate đưa vào quarantine
    # - FP: Bản ghi sạch nhưng bị Gate đưa vào quarantine nhầm
    # - FN: Bản ghi lỗi schema/giá âm nhưng lọt qua Gate vào clean_records
    # - TN: Bản ghi sạch/hợp lệ được Gate cho qua
    quarantine_fault_types = {"schema_invalid", "negative_price"}
    tp = sum(1 for r in rejected_records if r.get("fault_type") in quarantine_fault_types or r.get("is_injected", False))
    fp = sum(1 for r in rejected_records if not r.get("is_injected", False))
    fn = sum(1 for r in clean_records if r.get("fault_type") in quarantine_fault_types)
    tn = sum(1 for r in clean_records if r.get("fault_type") not in quarantine_fault_types)

    precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else 1.0
    recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else 1.0
    f1 = round(2.0 * precision * recall / (precision + recall), 4) if (precision + recall) > 0 else 0.0
    accuracy = round((tp + tn) / total_records, 4) if total_records > 0 else 1.0

    # Đánh giá năng lực Deduplication Gate
    expected_dups = fault_counts.get("duplicate", 0)
    actual_dups = report.duplicates_removed
    dedup_elimination_rate = round(actual_dups / expected_dups * 100.0, 2) if expected_dups > 0 else 100.0

    pass_rate = round(len(clean_records) / total_records * 100.0, 2) if total_records > 0 else 0.0
    rejection_rate = round(len(rejected_records) / total_records * 100.0, 2) if total_records > 0 else 0.0

    return {
        "run_id": run_id,
        "iteration": iteration,
        "symbol": symbol,
        "total_records": total_records,
        "valid_records": len(clean_records),
        "rejected_records": len(rejected_records),
        "duplicates_removed": actual_dups,
        "dedup_elimination_rate_pct": dedup_elimination_rate,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "accuracy": accuracy,
        "pass_rate_pct": pass_rate,
        "rejection_rate_pct": rejection_rate,
        "duration_ms": duration_ms,
        "throughput_rps": throughput_rps,
        "timestamp": now_utc().isoformat(),
    }


def write_results_to_csv(results: List[Dict[str, Any]]) -> None:
    """Lưu kết quả chi tiết từng lượt chạy vào CSV."""
    RESULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(results[0].keys())
    is_new = not RAW_RESULTS_CSV.exists()
    with RAW_RESULTS_CSV.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()
        for row in results:
            writer.writerow(row)
    logger.info("Raw benchmark results saved to %s", RAW_RESULTS_CSV)


def compute_and_write_summary(results: List[Dict[str, Any]], run_id: str) -> Dict[str, Any]:
    """Tính toán giá trị trung vị & trung bình cho toàn bộ đợt kiểm thử."""
    import statistics

    n = len(results)
    summary = {
        "run_id": run_id,
        "runs_count": n,
        "total_records_sum": sum(r["total_records"] for r in results),
        "tp_sum": sum(r["tp"] for r in results),
        "fp_sum": sum(r["fp"] for r in results),
        "fn_sum": sum(r["fn"] for r in results),
        "tn_sum": sum(r["tn"] for r in results),
        "mean_precision": round(statistics.mean(r["precision"] for r in results), 4),
        "mean_recall": round(statistics.mean(r["recall"] for r in results), 4),
        "mean_f1_score": round(statistics.mean(r["f1_score"] for r in results), 4),
        "mean_accuracy": round(statistics.mean(r["accuracy"] for r in results), 4),
        "median_duration_ms": round(statistics.median(r["duration_ms"] for r in results), 2),
        "median_throughput_rps": round(statistics.median(r["throughput_rps"] for r in results), 0),
        "timestamp": now_utc().isoformat(),
    }

    is_new = not SUMMARY_CSV.exists()
    with SUMMARY_CSV.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary.keys()))
        if is_new:
            writer.writeheader()
        writer.writerow(summary)
    logger.info("Summary benchmark results saved to %s", SUMMARY_CSV)
    return summary


def plot_fault_matrix(summary: Dict[str, Any], output_path: Path) -> None:
    """Vẽ biểu đồ Confusion Matrix và các chỉ số Phân loại."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np

        RESULT_PLOT_DIR.mkdir(parents=True, exist_ok=True)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # 1. Confusion Matrix Heatmap
        cm = np.array([
            [summary["tp_sum"], summary["fn_sum"]],
            [summary["fp_sum"], summary["tn_sum"]],
        ])
        cax = ax1.matshow(cm, cmap="Blues", alpha=0.8)
        for i in range(2):
            for j in range(2):
                ax1.text(
                    j, i, f"{cm[i, j]:,}",
                    ha="center", va="center", color="black", fontsize=14, weight="bold"
                )

        ax1.set_xticks([0, 1])
        ax1.set_yticks([0, 1])
        ax1.set_xticklabels(["Fault (Positive)", "Clean (Negative)"])
        ax1.set_yticklabels(["Fault", "Clean"])
        ax1.set_xlabel("Predicted Label (DQ Gate Action)", labelpad=10)
        ax1.set_ylabel("True Label (Ground Truth)", labelpad=10)
        ax1.set_title("Confusion Matrix (DQ Quarantine Filter)", fontsize=12, pad=15)
        fig.colorbar(cax, ax=ax1, fraction=0.046, pad=0.04)

        # 2. Classification Metrics Bar Chart
        metrics = ["Precision", "Recall", "F1-Score", "Accuracy"]
        scores = [
            summary["mean_precision"],
            summary["mean_recall"],
            summary["mean_f1_score"],
            summary["mean_accuracy"],
        ]
        colors = ["#2b5c8f", "#348aa7", "#5dd39e", "#bce784"]
        bars = ax2.bar(metrics, scores, color=colors, width=0.55)
        ax2.set_ylim(0, 1.15)
        ax2.set_ylabel("Score (0.0 - 1.0)")
        ax2.set_title("DQ Gate Classification Efficacy", fontsize=12)
        ax2.grid(axis="y", linestyle="--", alpha=0.5)

        for bar in bars:
            yval = bar.get_height()
            ax2.text(
                bar.get_x() + bar.get_width() / 2.0,
                yval + 0.02,
                f"{yval:.4f}",
                ha="center", va="bottom", fontsize=11, weight="bold"
            )

        plt.suptitle("Benchmark 3: Data Quality Gate & Fault Handling Evaluation", fontsize=14, weight="bold")
        plt.tight_layout()
        plt.savefig(output_path, dpi=200)
        plt.close()
        logger.info("Saved Confusion Matrix plot to %s", output_path)
    except Exception as exc:
        logger.warning("Could not generate fault benchmark plot: %s", exc)


def print_markdown_report(summary: Dict[str, Any], results: List[Dict[str, Any]]) -> None:
    """In bảng báo cáo chuẩn Markdown cho kết quả Benchmark 3."""
    print("\n" + "=" * 80)
    print("  BENCHMARK 3: DATA QUALITY GATE & FAULT HANDLING REPORT")
    print("=" * 80 + "\n")

    print(f"• Run ID: `{summary['run_id']}` | Total Runs: `{summary['runs_count']}` | Processed: `{summary['total_records_sum']:,} records`")
    print(f"• Throughput: `{summary['median_throughput_rps']:,} rec/sec` | Median Latency: `{summary['median_duration_ms']} ms`\n")

    print("| Metric | Value | Description |")
    print("| :--- | :---: | :--- |")
    print(f"| **True Positives (TP)** | {summary['tp_sum']:,} | Lỗi được phát hiện và cách ly chuẩn xác |")
    print(f"| **False Positives (FP)** | {summary['fp_sum']:,} | Bản ghi sạch bị cách ly nhầm (Lỗi loại I) |")
    print(f"| **False Negatives (FN)** | {summary['fn_sum']:,} | Lỗi lọt qua Gate vào Silver (Lỗi loại II) |")
    print(f"| **True Negatives (TN)** | {summary['tn_sum']:,} | Bản ghi sạch được cho qua hợp lệ |")
    print(f"| **Precision** | **{summary['mean_precision']:.4f}** | Độ chuẩn xác khi đưa vào Quarantine |")
    print(f"| **Recall** | **{summary['mean_recall']:.4f}** | Độ phủ bắt lỗi của Data Quality Gate |")
    print(f"| **F1-Score** | **{summary['mean_f1_score']:.4f}** | Điểm cân bằng điều hòa giữa Precision và Recall |")
    print(f"| **Accuracy** | **{summary['mean_accuracy']:.4f}** | Độ chính xác phân loại tổng thể |")

    print("\n" + "-" * 80)
    if summary["mean_f1_score"] >= 0.95:
        print(" [EVALUATION] ==> PASSED: Data Quality Gate đạt tiêu chuẩn phân loại lỗi cao (F1 >= 0.95).")
    else:
        print(" [EVALUATION] ==> WARNING: Data Quality Gate có tỷ lệ lọt lỗi đáng kể.")
    print("-" * 80 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark 3: Data Quality & Fault Handling Evaluation")
    parser.add_argument("--runs", type=int, default=3, help="Số lượt kiểm thử độc lập (mặc định: 3)")
    parser.add_argument("--count", type=int, default=5000, help="Số lượng records mỗi lượt (mặc định: 5000)")
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol coin thử nghiệm (mặc định: BTCUSDT)")
    parser.add_argument("--fault-rate", type=float, default=0.20, help="Tỷ lệ tiêm lỗi (mặc định: 0.20 = 20%)")
    parser.add_argument("--no-plot", action="store_true", help="Không vẽ biểu đồ")
    args = parser.parse_args()

    run_id = f"bm_fault_{now_utc().strftime('%Y%m%d_%H%M%S')}"
    logger.info("Starting Benchmark 3 (runs=%d, count=%d, fault_rate=%.2f)...", args.runs, args.count, args.fault_rate)

    results: List[Dict[str, Any]] = []
    for i in range(args.runs):
        logger.info("Running iteration %d/%d...", i + 1, args.runs)
        res = run_single_benchmark_iteration(
            run_id=run_id,
            iteration=i + 1,
            count=args.count,
            symbol=args.symbol,
            fault_rate=args.fault_rate,
            seed=100 * (i + 1),
        )
        results.append(res)

    write_results_to_csv(results)
    summary = compute_and_write_summary(results, run_id=run_id)

    if not args.no_plot:
        plot_path = RESULT_PLOT_DIR / "benchmark_fault_matrix.png"
        plot_fault_matrix(summary, plot_path)

    print_markdown_report(summary, results)


if __name__ == "__main__":
    main()
