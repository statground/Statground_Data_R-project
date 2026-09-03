#!/usr/bin/env python3

from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "admin-pipeline-snapshot-finalize.yml"
TEXT = WORKFLOW.read_text(encoding="utf-8")
MAIN_WORKFLOW = (ROOT / ".github" / "workflows" / "r-project-all.yml").read_text(
    encoding="utf-8"
)


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

    def test_installs_pinned_python_dependencies_before_validation(self) -> None:
        setup_position = TEXT.index("- name: Set up Python")
        install_position = TEXT.index("- name: Install Python dependencies")
        validation_position = TEXT.index("- name: Validate snapshot finalizer contract")

        self.assertIn("cache-dependency-path: scripts/requirements.txt", TEXT)
        self.assertIn("python -m pip install -r scripts/requirements.txt", TEXT)
        self.assertLess(setup_position, install_position)
        self.assertLess(install_position, validation_position)

    def test_clickhouse_queries_remain_bounded(self) -> None:
        self.assertIn('ADMIN_PIPELINE_MONITOR_CH_MAX_THREADS: "1"', TEXT)
        self.assertIn('ADMIN_PIPELINE_MONITOR_CH_MAX_EXECUTION_TIME: "20"', TEXT)
        self.assertIn("timeout-minutes: 15", TEXT)
        self.assertIn("cancel-in-progress: false", TEXT)

    def test_clickhouse_proxy_path_reaches_both_publish_paths(self) -> None:
        self.assertIn(
            "CLICKHOUSE_HTTP_URL_PATH: "
            "${{ secrets.CH_HTTP_URL_PATH || secrets.CLICKHOUSE_HTTP_URL_PATH }}",
            TEXT,
        )
        self.assertIn(
            "CH_HTTP_URL_PATH: "
            "${{ secrets.CH_HTTP_URL_PATH || secrets.CLICKHOUSE_HTTP_URL_PATH }}",
            TEXT,
        )
        admin_export = MAIN_WORKFLOW.split(
            "- name: Export admin pipeline monitor CDN snapshot", 1
        )[1].split("- name: Commit and push Web-R CDN2 admin pipeline snapshot", 1)[0]
        self.assertIn("CLICKHOUSE_HTTP_URL_PATH:", admin_export)
        admin_record = MAIN_WORKFLOW.split(
            "- name: Record Web-R CDN2 admin pipeline snapshot release", 1
        )[1].split("- name: Verify Web-R CDN2 admin pipeline snapshot release", 1)[0]
        self.assertIn("CH_HTTP_URL_PATH:", admin_record)

    def test_release_pointer_is_recorded_and_verified(self) -> None:
        self.assertIn("record_web_r_cdn_release.py", TEXT)
        self.assertIn("verify_web_r_cdn_release.py", TEXT)
        self.assertGreaterEqual(TEXT.count("--scope web-r-admin-pipelines"), 2)

    def test_release_record_is_pressure_gated_before_write(self) -> None:
        gate_position = TEXT.index(
            "- name: Gate ClickHouse release record on storage pressure"
        )
        record_position = TEXT.index(
            "- name: Record Web-R CDN2 admin pipeline snapshot release"
        )

        self.assertLess(gate_position, record_position)
        self.assertIn("id: clickhouse_gate", TEXT)
        self.assertIn("python scripts/clickhouse_pressure_gate.py", TEXT)
        self.assertIn(
            "CLICKHOUSE_PRESSURE_GATE_TARGETS: "
            "replica:Data_R_Community_Service.web_r_cdn_release_log_local",
            TEXT,
        )
        self.assertNotIn("CLICKHOUSE_PRESSURE_GATE_MAX_DISTRIBUTED_", TEXT)

    def test_both_admin_snapshot_publishers_rebuild_on_latest_main(self) -> None:
        finalizer_publish = TEXT.split(
            "- name: Commit and push completed admin pipeline snapshot", 1
        )[1].split("- name: Record Web-R CDN2 admin pipeline snapshot release", 1)[0]
        main_publish = MAIN_WORKFLOW.split(
            "- name: Commit and push Web-R CDN2 admin pipeline snapshot", 1
        )[1].split("- name: Record Web-R CDN2 admin pipeline snapshot release", 1)[0]

        for publish in (finalizer_publish, main_publish):
            self.assertIn(
                'snapshot_file="${RUNNER_TEMP}/web-r-admin-pipelines-latest.json"',
                publish,
            )
            self.assertIn(
                "git restore --source=HEAD --staged --worktree "
                "admin/pipelines/latest.json",
                publish,
            )
            self.assertIn("for attempt in 1 2 3; do", publish)
            self.assertIn("git fetch --no-tags origin main", publish)
            self.assertIn("git switch --detach FETCH_HEAD", publish)
            self.assertIn(
                'cp "$snapshot_file" admin/pipelines/latest.json', publish
            )
            self.assertIn("if git push origin HEAD:main; then", publish)
            self.assertLess(
                publish.index("git fetch --no-tags origin main"),
                publish.index("git commit -m"),
            )
            self.assertLess(
                publish.index("git commit -m"),
                publish.index("if git push origin HEAD:main; then"),
            )

    def test_release_record_failure_does_not_skip_pointer_verification(self) -> None:
        finalizer_record = TEXT.split(
            "- name: Record Web-R CDN2 admin pipeline snapshot release", 1
        )[1].split("- name: Verify Web-R CDN2 admin pipeline snapshot release", 1)[0]
        finalizer_verify = TEXT.split(
            "- name: Verify Web-R CDN2 admin pipeline snapshot release", 1
        )[1]
        self.assertIn("record_deferred", finalizer_record)
        self.assertIn("failed finalization", finalizer_record)
        self.assertIn(
            "if: always() && steps.snapshot.outcome == 'success' && "
            "steps.clickhouse_gate.outcome == 'success'",
            finalizer_verify,
        )
        self.assertNotIn("skipping pointer verification", finalizer_verify)

        main_record = MAIN_WORKFLOW.split(
            "- name: Record Web-R CDN2 admin pipeline snapshot release", 1
        )[1].split("- name: Verify Web-R CDN2 admin pipeline snapshot release", 1)[0]
        main_verify = MAIN_WORKFLOW.split(
            "- name: Verify Web-R CDN2 admin pipeline snapshot release", 1
        )[1].split("- name: Mark deferred CDN exports as degraded", 1)[0]
        self.assertIn("record_deferred", main_record)
        self.assertIn("failed CDN publish", main_record)
        self.assertIn("if: always()", main_verify)
        self.assertIn("steps.web_r_cdn2_admin_pipelines.outcome == 'success'", main_verify)
        self.assertNotIn("skipping admin pipeline release pointer verification", main_verify)


if __name__ == "__main__":
    unittest.main()
