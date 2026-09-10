"""
test_dagster_pipeline.py
========================
Unit tests cho Tầng Điều phối Lô (Batch Layer Orchestration) với Dagster:
- Kiểm tra tính hợp lệ của Repository, Assets, Jobs, Schedules
- Kiểm tra quan hệ phụ thuộc Data Lineage DAG
- Kiểm tra in-memory Materialization toàn bộ 6 Software-Defined Assets
"""

import unittest
from dagster import materialize
from dagster_project.repository import (
    all_assets,
    all_jobs,
    all_schedules,
    lambda_lakehouse_repo,
    batch_pipeline_schedule,
    compaction_schedule,
)


class TestDagsterRepositoryDefinitions(unittest.TestCase):
    """Kiểm thử định nghĩa Dagster Repository."""

    def test_repository_asset_count(self):
        """Repository phải có đủ 6 Software-Defined Assets."""
        self.assertEqual(len(all_assets), 6)
        keys = [a.key.to_user_string() for a in all_assets]
        expected_keys = [
            "bronze_crypto_trades",
            "silver_cleaned_trades",
            "gold_market_aggregates",
            "clickhouse_batch_sync",
            "system_watermark_sync",
            "iceberg_small_files_compaction",
        ]
        for ek in expected_keys:
            self.assertIn(ek, keys)

    def test_repository_jobs(self):
        """Repository phải đăng ký đủ 2 Asset Jobs chính."""
        self.assertEqual(len(all_jobs), 2)
        job_names = [j.name for j in all_jobs]
        self.assertIn("batch_lakehouse_pipeline_job", job_names)
        self.assertIn("iceberg_compaction_job", job_names)

    def test_repository_schedules(self):
        """Repository phải cấu hình đúng lịch trình tự động."""
        self.assertEqual(len(all_schedules), 2)
        self.assertEqual(batch_pipeline_schedule.cron_schedule, "*/15 * * * *")
        self.assertEqual(compaction_schedule.cron_schedule, "0 */2 * * *")

    def test_asset_lineage_dependencies(self):
        """Kiểm tra cây phụ thuộc dữ liệu (Data Lineage DAG)."""
        repo = lambda_lakehouse_repo
        g = repo.asset_graph

        def get_parent_keys(asset_def):
            node = g.get(asset_def.key)
            return [p.key.to_user_string() for p in g.get_parents(node)]

        # silver phụ thuộc bronze
        self.assertIn("bronze_crypto_trades", get_parent_keys(all_assets[1]))

        # gold phụ thuộc silver
        self.assertIn("silver_cleaned_trades", get_parent_keys(all_assets[2]))

        # clickhouse sync phụ thuộc gold
        self.assertIn("gold_market_aggregates", get_parent_keys(all_assets[3]))

        # watermark sync phụ thuộc clickhouse sync
        self.assertIn("clickhouse_batch_sync", get_parent_keys(all_assets[4]))

    def test_in_memory_materialization(self):
        """Thực thi materialization trong bộ nhớ toàn bộ 6 assets phải thành công 100%."""
        result = materialize(all_assets)
        self.assertTrue(result.success)
        step_successes = [e.step_key for e in result.all_events if e.is_step_success]
        self.assertEqual(len(step_successes), 6)


if __name__ == "__main__":
    unittest.main()
