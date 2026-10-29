"""
aggregate_report_materials.py
==============================
Thu thập và phân loại toàn bộ tài liệu Markdown (.md) và HTML (.html)
trong kho mã nguồn vào thư mục 'report_materials/' để phục vụ viết báo cáo luận văn.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT_DIR = Path(__file__).resolve().parents[1]
TARGET_DIR = ROOT_DIR / "report_materials"

CATEGORIES = {
    "01_de_cuong_va_ke_hoach": [
        "docs/md/de_cuong_chi_tiet_tieu_luan.md",
        "docs/md/proposal_tlcn_lambda.md",
        "docs/md/plan.md",
        "docs/md/s/ke_hoach_hoan_thanh_100_phan_tram.md",
        "docs/plan/phan_cong_nhiem_vu.md",
        "docs/plan/plan_quan.md",
        "docs/plan/plan_quoc_anh.md",
        "docs/plans/sprint_1.md",
    ],
    "02_kien_truc_va_thiet_ke_he_thong": [
        "docs/md/sys-arch.md",
        "docs/md/caitan_lambda_architecture.md",
        "docs/md/smallfile.md",
        "docs/md/summary.md",
        "AGENTS.md",
        "development.md",
        "README.md",
    ],
    "03_quy_trinh_trien_khai_5_tang": [
        "docs/process/01_ingestion_data_contract.md",
        "docs/process/02_speed_layer_spark_streaming.md",
        "docs/process/03_serving_layer_query_merger.md",
        "docs/process/04_dashboard_streamlit_ui.md",
        "docs/process/05_batch_dagster_orchestration.md",
        "docs/data/data.md",
        "docs/data/reportmock.md",
    ],
    "04_bao_cao_thuc_nghiem_va_benchmark": [
        "SHORT_REPORT.md",
        "docs/md/s/bao_cao_stream_lien_tuc_kafka_offset_checkpoint.md",
        "docs/md/s/huong_dan_bao_ve_va_tai_lap.md",
        ".agents/skills/crypto-lambda-lakehouse-reconciliation-audit/crypto_lambda_lakehouse_pre_benchmark_audit_skill.md",
        ".agents/skills/crypto-lambda-lakehouse-reconciliation-audit/SKILL.md",
        "docs/presentation/giai_thich_thuat_toan_gbm.md",
    ],
    "05_slide_va_giao_dien_thuyet_trinh": [
        "docs/presentation/slides.html",
        "docs/presentation/report.html",
        "docs/presentation/report.md",
        "docs/presentation/4errors.html",
        "docs/presentation/4erors.html",
        "docs/presentation/demo_stream.html",
        "docs/presentation/smallfile_diagram.html",
        "docs/presentation/huong_dan_thuyet_trinh_va_phan_bien.md",
        "docs/presentation/stream_demo.md",
    ],
    "06_tai_lieu_ky_thuat_module": [
        ("src/batch_layer/README.md", "src_batch_layer_README.md"),
        ("src/data_quality/README.md", "src_data_quality_README.md"),
        ("src/ingestion/README.md", "src_ingestion_README.md"),
        ("src/serving_layer/README.md", "src_serving_layer_README.md"),
        ("src/speed_layer/README.md", "src_speed_layer_README.md"),
        ("src/utils/README.md", "src_utils_README.md"),
        ("configs/README.md", "configs_README.md"),
        ("dagster_project/README.md", "dagster_project_README.md"),
        ("dashboard/README.md", "dashboard_README.md"),
        ("datasets/README.md", "datasets_README.md"),
        ("dbt_project/README.md", "dbt_project_README.md"),
        ("references/README.md", "references_README.md"),
        ("results/README.md", "results_README.md"),
        ("scripts/README.md", "scripts_README.md"),
        ("docs/md/README.md", "docs_md_README.md"),
    ],
}


def main():
    if TARGET_DIR.exists():
        shutil.rmtree(TARGET_DIR)
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    copied_files = []
    skipped_files = []

    for category, file_list in CATEGORIES.items():
        cat_dir = TARGET_DIR / category
        cat_dir.mkdir(parents=True, exist_ok=True)

        for item in file_list:
            if isinstance(item, tuple):
                rel_src, target_name = item
            else:
                rel_src = item
                target_name = Path(rel_src).name
                if target_name == "README.md" and rel_src == "README.md":
                    target_name = "ROOT_PROJECT_README.md"

            src_path = ROOT_DIR / rel_src
            dst_path = cat_dir / target_name

            if src_path.exists():
                shutil.copy2(src_path, dst_path)
                copied_files.append((category, target_name, str(src_path)))
            else:
                skipped_files.append(rel_src)

    print(f"\n=======================================================")
    print(f"  DA TONG HOP {len(copied_files)} FILES VAO THU MUC:")
    print(f"  {TARGET_DIR}")
    print(f"=======================================================\n")

    for cat in CATEGORIES.keys():
        files_in_cat = [f for c, f, _ in copied_files if c == cat]
        print(f"[{cat}] ({len(files_in_cat)} tệp)")
        for f in files_in_cat:
            print(f"   • {f}")
        print()

    if skipped_files:
        print(f"[CANH BAO] Khong tim thay {len(skipped_files)} tệp: {skipped_files}")


if __name__ == "__main__":
    main()
