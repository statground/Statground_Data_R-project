#!/usr/bin/env python3

from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "admin-pipeline-snapshot-finalize.yml"
TEXT = WORKFLOW.read_text(encoding="utf-8")


class AdminPipelineSnapshotFinalizeWorkflowTest(unittest.TestCase):
    def test_runs_after_each_monitored_r_project_workflow(self) -> None:
        self.assertIn("workflow_run:", TEXT)
        self.assertIn("types:\n      - completed", TEXT)
        for workflow_name in (
            "R Project Package Collection",
            "R Project Social Collection",
            "R Project Community Collection",
            "R Project Community Digest",
            "R Project Notebook Generation",
            "R Project CDN Publish",
        ):
            self.assertIn(f"- {workflow_name}", TEXT)

    def test_only_publishes_the_small_admin_snapshot(self) -> None:
        self.assertIn("export_admin_pipeline_monitor.py", TEXT)
        self.assertIn("admin/pipelines/latest.json", TEXT)
        self.assertNotIn("r-project-all.yml", TEXT)
        self.assertNotIn("export_r_package_cdn.py", TEXT)
        self.assertNotIn("export_community_cdn.py", TEXT)

    def test_clickhouse_queries_remain_bounded(self) -> None:
        self.assertIn('ADMIN_PIPELINE_MONITOR_CH_MAX_THREADS: "1"', TEXT)
        self.assertIn('ADMIN_PIPELINE_MONITOR_CH_MAX_EXECUTION_TIME: "20"', TEXT)
        self.assertIn("timeout-minutes: 15", TEXT)
        self.assertIn("cancel-in-progress: false", TEXT)

    def test_release_pointer_is_recorded_and_verified(self) -> None:
        self.assertIn("record_web_r_cdn_release.py", TEXT)
        self.assertIn("verify_web_r_cdn_release.py", TEXT)
        self.assertGreaterEqual(TEXT.count("--scope web-r-admin-pipelines"), 2)


if __name__ == "__main__":
    unittest.main()
