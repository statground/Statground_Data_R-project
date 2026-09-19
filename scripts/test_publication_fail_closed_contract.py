#!/usr/bin/env python3

from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
MAIN_WORKFLOW = (ROOT / ".github" / "workflows" / "r-project-all.yml").read_text(
    encoding="utf-8"
)
FINALIZER_WORKFLOW = (
    ROOT / ".github" / "workflows" / "admin-pipeline-snapshot-finalize.yml"
).read_text(encoding="utf-8")
BOOTSTRAP_WORKFLOW = (
    ROOT / ".github" / "workflows" / "r-project-community-bootstrap.yml"
).read_text(encoding="utf-8")
SQL_SHA = "6ee7206c13d6694de1d1ef6bf95b3f6b2b7a7022"


class PublicationFailClosedContractTest(unittest.TestCase):
    def test_cdn_publication_scopes_share_one_non_cancelling_workflow_group(self) -> None:
        concurrency = MAIN_WORKFLOW.split("concurrency:", 1)[1].split("jobs:", 1)[0]
        self.assertIn("inputs.scope == 'all' || inputs.scope == 'cdn'", concurrency)
        self.assertIn("'r-project-publication'", concurrency)
        self.assertIn("cancel-in-progress: false", concurrency)
        self.assertNotIn("cancel-in-progress: true", concurrency)

    def test_scheduled_publishers_do_not_default_to_fail_open(self) -> None:
        fail_open_lines = [
            line.strip()
            for line in MAIN_WORKFLOW.splitlines()
            if "FAIL_OPEN:" in line
        ]
        self.assertTrue(fail_open_lines)
        for line in fail_open_lines:
            self.assertNotIn("|| 'true'", line)
            self.assertTrue(
                "|| 'false'" in line or line.endswith(': "false"'), line
            )

        finalizer_lines = [
            line.strip()
            for line in FINALIZER_WORKFLOW.splitlines()
            if "FAIL_OPEN:" in line
        ]
        self.assertTrue(finalizer_lines)
        for line in finalizer_lines:
            self.assertTrue(line.endswith(': "false"'), line)

    def test_cdn_job_fails_when_any_legacy_release_record_is_incomplete(self) -> None:
        finalizer = MAIN_WORKFLOW.split(
            "- name: Mark deferred CDN exports as degraded", 1
        )[1]
        for path in (
            "/tmp/web_r_cdn_record_contents.json",
            "/tmp/web_r_cdn_record_packages.json",
        ):
            self.assertIn(path, finalizer)
        self.assertIn("release_record_missing", finalizer)
        self.assertIn('record.get("record_deferred")', finalizer)
        self.assertIn('record.get("record_skipped")', finalizer)
        self.assertIn("raise SystemExit(1)", finalizer)

    def test_community_publication_orders_load_export_commit_preflight_publish(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        ordered = [
            "- name: Preflight and load exact community generation",
            "- name: Export the exact loaded community generation",
            "- name: Commit and push exact Web-R community generation",
            "- name: Verify immutable jsDelivr generation proof",
            "- name: Preflight exact application reader transition",
            "- name: Publish and reopen exact application readers",
        ]
        positions = [job.index(name) for name in ordered]
        self.assertEqual(positions, sorted(positions))

    def test_scheduled_workflow_never_bootstraps_or_calls_drain_readers(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertNotIn('"$publisher" bootstrap', MAIN_WORKFLOW)
        self.assertNotIn("drain-readers", MAIN_WORKFLOW)
        self.assertIn("bootstrap_required: dispatch R Project Community Bootstrap", MAIN_WORKFLOW)
        self.assertIn("group: statground-internal", job)
        self.assertIn("labels: [self-hosted, linux, x64]", job)
        self.assertIn("environment: web-r-community-publication", job)

    def test_statground_sql_checkout_is_exact_immutable_commit(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertIn("repository: statground/Statground_SQL", job)
        self.assertIn(f"ref: {SQL_SHA}", job)
        self.assertIn(f"STATGROUND_SQL_COMMIT_SHA: {SQL_SHA}", job)
        self.assertIn('git -C statground-sql rev-parse HEAD', job)
        self.assertIn("STATGROUND_SQL_READ_TOKEN", job)

    def test_community_export_receives_exact_loader_generation(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertIn('echo "generation=$generation" >> "$GITHUB_OUTPUT"', job)
        self.assertIn('--generation "${{ steps.community_loader.outputs.generation }}"', job)
        self.assertIn('report.get("generation") != generation', job)
        self.assertIn('proof.get("generation") != generation', job)
        self.assertIn('report.get("workshop_complete") is not True', job)
        self.assertIn('int(report.get("workshop_post_count") or 0) <= 0', job)

    def test_scheduled_null_pointer_fails_with_bootstrap_required(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertIn('receipt.get("current_generation") is None', job)
        self.assertIn("bootstrap_required: dispatch R Project Community Bootstrap", job)
        self.assertNotIn('"$publisher" bootstrap', job)

    def test_community_publish_receipt_requires_release_and_withdrawal_ack(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        publish = job.split("- name: Publish and reopen exact application readers", 1)[1]
        self.assertEqual(job.count("--reader-timeout-seconds 60"), 1)
        self.assertNotIn("--reader-timeout-seconds 600", job)
        self.assertEqual(BOOTSTRAP_WORKFLOW.count("--reader-timeout-seconds 60"), 1)
        self.assertNotIn("--reader-timeout-seconds 600", BOOTSTRAP_WORKFLOW)
        self.assertIn('{"released", "already_released"}', publish)
        self.assertIn('receipt.get("withdrawal_ack") is not True', publish)
        self.assertIn('int(receipt.get("reader_count") or 0) <= 0', publish)
        self.assertIn('(\"transition_id\", \"ack_uuid\")', publish)
        self.assertIn('(\"marker_id\", \"pointer_id\")', publish)

    def test_legacy_community_record_and_verify_are_absent(self) -> None:
        self.assertNotIn("/tmp/web_r_cdn_record_community.json", MAIN_WORKFLOW)
        self.assertNotIn("--scope web-r-community", MAIN_WORKFLOW)
        self.assertNotIn(
            "verify_web_r_cdn_release.py \\\n+              --scope web-r-community",
            MAIN_WORKFLOW,
        )

    def test_community_secret_files_are_owner_only_and_always_cleaned(self) -> None:
        for workflow in (MAIN_WORKFLOW, BOOTSTRAP_WORKFLOW):
            self.assertIn("$RUNNER_TEMP", workflow)
            self.assertIn("mkdir -m 700", workflow)
            self.assertIn("! -perm 0600", workflow)
            self.assertIn('runner_temp.glob("webr-community-*")', workflow)
            self.assertIn("refusing unsafe stale community runtime cleanup", workflow)
            cleanup = workflow.split(
                "- name: Always remove community publication secrets", 1
            )[1]
            self.assertIn("if: always()", cleanup)
            self.assertIn("shutil.rmtree(runtime)", cleanup)
            self.assertIn("refusing to clean a path outside RUNNER_TEMP", cleanup)
        self.assertIn("WEBR_COMMUNITY_CLICKHOUSE_CONFIGS_B64", MAIN_WORKFLOW)
        self.assertIn("WEBR_COMMUNITY_READER_INVENTORY_B64", MAIN_WORKFLOW)
        self.assertIn("WEBR_COMMUNITY_READER_TOKENS_B64", MAIN_WORKFLOW)
        community_job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertIn('export GIT_ASKPASS="$askpass"', community_job)
        self.assertNotIn("https://x-access-token:${STATGROUND_CDN2_ADMIN_TOKEN}", community_job)

    def test_exact_cdn_url_uses_community_commit_output(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        exact = (
            "https://cdn.jsdelivr.net/gh/statground/web-r_CDN2_community@"
            "${{ steps.web_r_cdn2_community.outputs.commit_sha }}"
        )
        self.assertGreaterEqual(job.count(exact), 3)
        self.assertIn("--asset community/ko/index.json", job)
        self.assertIn("--asset community/ko/workshop/index.json", job)

    def test_pressure_gate_covers_exact_publication_targets(self) -> None:
        required = {
            "local:mart_webr.community_feed_snapshot_v1_local",
            "local:mart_webr.community_feed_generation_proof_v1_local",
            "replica:mart_webr.community_feed_publish_lease_v1_local",
            "replica:mart_webr.community_feed_generation_marker_v1_local",
            "replica:mart_webr.community_feed_serving_pointer_v1_local",
            "replica:mart_webr.community_publication_reader_transition_ack_v2_local",
        }
        for workflow in (MAIN_WORKFLOW, BOOTSTRAP_WORKFLOW):
            gate = workflow.split(
                "- name: Gate exact community publication write targets", 1
            )[1].split("run: python scripts/clickhouse_pressure_gate.py", 1)[0]
            for target in required:
                self.assertIn(target, gate)

    def test_manual_bootstrap_is_dispatch_only_protected_and_one_shot(self) -> None:
        trigger = BOOTSTRAP_WORKFLOW.split("permissions:", 1)[0]
        self.assertIn("workflow_dispatch:", trigger)
        for forbidden in ("schedule:", "push:", "workflow_call:"):
            self.assertNotIn(forbidden, trigger)
        self.assertIn("BOOTSTRAP_WEBR_COMMUNITY_ONCE", BOOTSTRAP_WORKFLOW)
        self.assertIn("group: statground-internal", BOOTSTRAP_WORKFLOW)
        self.assertIn("labels: [self-hosted, linux, x64]", BOOTSTRAP_WORKFLOW)
        self.assertIn("environment: web-r-community-publication-bootstrap", BOOTSTRAP_WORKFLOW)
        self.assertIn("group: r-project-publication", BOOTSTRAP_WORKFLOW)
        self.assertIn("cancel-in-progress: false", BOOTSTRAP_WORKFLOW)
        self.assertIn(f"ref: {SQL_SHA}", BOOTSTRAP_WORKFLOW)
        self.assertEqual(BOOTSTRAP_WORKFLOW.count('"$publisher" bootstrap'), 1)
        self.assertIn("if: steps.preflight.outputs.bootstrap_required == 'true'", BOOTSTRAP_WORKFLOW)
        self.assertIn('current not in (None, expected)', BOOTSTRAP_WORKFLOW)

    def test_clickhouse_client_is_checksum_pinned_on_protected_runners(self) -> None:
        checksums = (
            "10a8e6bf05ddbbfeeedbf6fae7050b84e0104962a0ab8bf324b8bd83bf779fb87c00602e3660dc462d01b91b9f53c530498be32ba652e13a01cf86fd3edc33cc",
            "cdcb883b3ea3632434da3014bd8bca00a0ba1a5a50571fe14bddbf859ce31da3c43306e3bcee66185276cf8c05f4743730d2751dd6a86ac9ec6494d77f9cc0fb",
        )
        for workflow in (MAIN_WORKFLOW, BOOTSTRAP_WORKFLOW):
            self.assertIn("CLICKHOUSE_CLIENT_VERSION: 26.1.2.11", workflow)
            for checksum in checksums:
                self.assertIn(checksum, workflow)
            self.assertGreaterEqual(workflow.count("sha512sum --check --strict"), 2)
            self.assertIn("https://packages.clickhouse.com/tgz/stable/", workflow)

    def test_runtime_helpers_also_default_to_fail_closed(self) -> None:
        expected = {
            "cmd/rproject-collector/main.go": (
                'envBool("R_YOUTUBE_PUBLISH_TRANSIENT_FAIL_OPEN", false)',
                'envBool("R_COMMUNITY_PUBLISH_TRANSIENT_FAIL_OPEN", false)',
                'envBool("R_COMMUNITY_DIGEST_TRANSIENT_FAIL_OPEN", false)',
            ),
            "cmd/rblogger-pipeline/main.go": (
                'envBool("RBLOGGER_PUBLISH_TRANSIENT_FAIL_OPEN", false)',
            ),
            "scripts/generate_webr_notebook_daily.py": (
                'env_bool(env, "WEBR_NOTEBOOK_DAILY_PUBLISH_TRANSIENT_FAIL_OPEN", False)',
            ),
            "scripts/export_r_ecosystem_cdn.py": (
                'env_bool(env, "R_ECOSYSTEM_CDN_EXPORT_TRANSIENT_FAIL_OPEN", False)',
            ),
            "scripts/export_community_cdn.py": (
                'env_bool(env, "WEBR_COMMUNITY_CDN_EXPORT_TRANSIENT_FAIL_OPEN", False)',
            ),
            "scripts/record_web_r_cdn_release.py": (
                'env_bool(os.environ, "WEB_R_CDN_RELEASE_RECORD_TRANSIENT_FAIL_OPEN", False)',
            ),
            "scripts/verify_web_r_cdn_release.py": (
                'env_bool(os.environ, "WEB_R_CDN_RELEASE_VERIFY_TRANSIENT_FAIL_OPEN", False)',
                'env_bool(os.environ, "WEB_R_CDN_RELEASE_VERIFY_POINTER_MISMATCH_FAIL_OPEN", False)',
            ),
        }
        for relative, snippets in expected.items():
            text = (ROOT / relative).read_text(encoding="utf-8")
            for snippet in snippets:
                self.assertIn(snippet, text, f"{relative}: {snippet}")


if __name__ == "__main__":
    unittest.main()
