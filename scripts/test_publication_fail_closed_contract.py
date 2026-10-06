#!/usr/bin/env python3

from __future__ import annotations

import ast
import base64
import json
import os
import pathlib
import sys
import subprocess
import textwrap
import unittest
import fnmatch

import yaml


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
RECONCILE_WORKFLOW = (
    ROOT / ".github" / "workflows" / "r-project-community-publication.yml"
).read_text(encoding="utf-8")
SQL_SHA = "3f74f752395d742e278244af69dac60347dc7f66"
BOOTSTRAP_SQL_SHA = "488f88ffe3370f71430162a60b47fc075a68b7db"


class PublicationFailClosedContractTest(unittest.TestCase):
    def test_scoped_publication_tags_keep_the_existing_native_entrypoints(self) -> None:
        wrapper = yaml.load(RECONCILE_WORKFLOW, Loader=yaml.BaseLoader)
        self.assertEqual(wrapper["on"], {
            "schedule": [{"cron": "7,37 * * * *"}],
            "push": {"tags": ["webr-community-publish-*"]},
            "workflow_dispatch": "",
        })
        self.assertEqual(wrapper["permissions"], {"contents": "write"})
        self.assertEqual(wrapper["jobs"], {"publication": {
            "name": "Publish complete Web-R community generation",
            "uses": "./.github/workflows/r-project-all.yml",
            "with": {"scope": "publication"},
            "secrets": "inherit",
        }})
        pattern, = wrapper["on"]["push"]["tags"]
        self.assertTrue(fnmatch.fnmatchcase(
            "webr-community-publish-recovery-20261006T000000Z-4dd774b9c206", pattern))
        for unrelated in ("main", "webr-cdn-publish-recovery-20261006", "webr-community-key-readonly-20261006"):
            self.assertFalse(fnmatch.fnmatchcase(unrelated, pattern), unrelated)

    def test_publication_tag_scope_does_not_admit_cdn_exports_or_failed_intake(self) -> None:
        from test_workflow_target_concurrency import evaluate

        wrapper = yaml.load(RECONCILE_WORKFLOW, Loader=yaml.BaseLoader)
        scope = wrapper["jobs"]["publication"]["with"]["scope"]
        worker = yaml.safe_load(MAIN_WORKFLOW)
        self.assertFalse(evaluate(worker["jobs"]["cdn-export"]["if"], scope, 1))
        condition = worker["jobs"]["community-publication"]["if"]
        for intake, cancelled, allowed in (("success", False, True), ("failure", False, False), ("success", True, False)):
            with self.subTest(intake=intake, cancelled=cancelled):
                self.assertEqual(evaluate(condition, scope, 1, **{
                    "needs.collect.result": intake,
                    "needs.cdn-export.result": "skipped",
                    "cancelled": cancelled,
                }), allowed)

    def test_protected_settings_fail_before_checkout_and_keep_other_credentials_required(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertLess(job.index("- name: Validate protected publication settings"), job.index("- name: Checkout immutable Statground SQL publisher"))
        validation = job.split("- name: Validate protected publication settings", 1)[1].split("\n      - name:", 1)[0]
        script = textwrap.dedent(validation.split("        run: |\n", 1)[1])
        required = (
            "STATGROUND_SQL_READ_TOKEN", "STATGROUND_CDN2_ADMIN_TOKEN", "R_ECOSYSTEM_CONTENT_KEY",
            "WEBR_COMMUNITY_CLICKHOUSE_CONFIGS_B64", "WEBR_COMMUNITY_READER_INVENTORY_B64", "WEBR_COMMUNITY_READER_TOKENS_B64",
        )
        marker = "fixture-private-never-output"
        healthy = {name: marker for name in required}
        bundles = [dict.fromkeys(("s1r1", "s1r2", "s2r1", "s2r2"), "<config/>"),
                   {"schema": "web-r.community.reader-inventory.v1", "reader_inventory_revision": 1,
                    "readers": [{"app_service": "web-r", "instance_id": "fixture", "inventory_endpoint": "https://reader.example/inventory", "url": "https://reader.example/transition"}]},
                   {"fixture": marker}]
        healthy.update({name: base64.b64encode(json.dumps(value).encode()).decode() for name, value in zip(required[3:], bundles)})
        healthy["PATH"] = os.path.dirname(sys.executable) + ":/usr/bin:/bin"
        complete = subprocess.run(["/bin/bash", "-c", script], env=healthy, cwd=ROOT, capture_output=True, text=True, timeout=3)
        self.assertEqual(complete.returncode, 0)
        self.assertEqual(json.loads(complete.stdout), {"status": "validated", "endpoint_count": 4, "reader_count": 1})
        self.assertEqual(complete.stderr, "")
        absent = (required[0], *required[3:])
        incomplete = subprocess.run(["/bin/bash", "-c", script], env={name: value for name, value in healthy.items() if name not in absent}, cwd=ROOT, capture_output=True, text=True, timeout=3)
        self.assertNotEqual(incomplete.returncode, 0)
        self.assertEqual(incomplete.stdout, "")
        self.assertEqual(incomplete.stderr.splitlines(), [required[0] + " is required"])
        self.assertNotIn(marker, incomplete.stdout + incomplete.stderr)
        for missing in (required[3:], required[3:5]):
            unavailable = subprocess.run(["/bin/bash", "-c", script], env={name: value for name, value in healthy.items() if name not in missing}, cwd=ROOT, capture_output=True, text=True, timeout=3)
            self.assertNotEqual(unavailable.returncode, 0); self.assertEqual(unavailable.stdout, "")
            self.assertNotIn(marker, unavailable.stderr)
        for name in required[:3]:
            unavailable = subprocess.run(["/bin/bash", "-c", script], env={key: value for key, value in healthy.items() if key != name}, cwd=ROOT, capture_output=True, text=True, timeout=3)
            self.assertNotEqual(unavailable.returncode, 0);self.assertEqual(unavailable.stderr.strip(), name + " is required")

    def test_local_owner_input_option_is_scoped_to_existing_protected_publisher(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertEqual(job.count("--owner-input-dir /var/lib/webr-community-publication-inputs"), 2)
        self.assertEqual(job.count('--source-sha "$GITHUB_SHA" --sql-sha "$STATGROUND_SQL_COMMIT_SHA"'), 2)
        self.assertIn("runs-on: [self-hosted, linux, x64, webr-community-publisher]", job)
        self.assertIn("environment: web-r-community-publication", job)
        self.assertNotIn("--owner-input-dir", BOOTSTRAP_WORKFLOW)
        self.assertEqual(MAIN_WORKFLOW.count("--owner-input-dir"), 2)

    def test_new_posts_trigger_the_existing_fenced_publication_job(self) -> None:
        self.assertIn('cron: "7,37 * * * *"', RECONCILE_WORKFLOW)
        self.assertIn("workflow_dispatch:", RECONCILE_WORKFLOW)
        self.assertIn("uses: ./.github/workflows/r-project-all.yml", RECONCILE_WORKFLOW)
        self.assertIn("scope: publication", RECONCILE_WORKFLOW)
        self.assertIn("all|package|social|youtube-availability|community|community-digest|notebook|cdn|publication)", MAIN_WORKFLOW)
        collect = MAIN_WORKFLOW.split("  collect:", 1)[1].split("  cdn-export:", 1)[0]
        self.assertIn("if: steps.opts.outputs.scope != 'publication'", collect)
        self.assertIn("if: steps.opts.outputs.scope != 'publication' && steps.opts.outputs.scope != 'cdn' && (steps.opts.outputs.scope != 'notebook'", collect)
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertIn("inputs.scope == 'publication' || ((inputs.scope == 'all' || inputs.scope == 'cdn') && needs.cdn-export.result == 'success')", job)
        self.assertIn("Gate exact community publication write targets", job)
        for workflow in (MAIN_WORKFLOW, BOOTSTRAP_WORKFLOW):
            self.assertNotIn("group: statground-internal", workflow)

    def test_publication_owner_is_released_independently_from_cdn_exports(self) -> None:
        concurrency = MAIN_WORKFLOW.split("concurrency:", 1)[1].split("jobs:", 1)[0]
        self.assertIn("github.run_id", concurrency)
        self.assertNotIn("'r-project-publication'", concurrency)
        self.assertIn("cancel-in-progress: false", concurrency)
        self.assertNotIn("cancel-in-progress: true", concurrency)
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertIn("group: r-project-publication", job)
        self.assertIn("cancel-in-progress: false", job)

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
        self.assertIn("runs-on: [self-hosted, linux, x64, webr-community-publisher]", job)
        self.assertIn("environment: web-r-community-publication", job)

    def test_statground_sql_checkout_is_exact_immutable_commit(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        self.assertIn("repository: statground/Statground_SQL", job)
        self.assertIn(f"ref: {SQL_SHA}", job)
        self.assertIn(f"STATGROUND_SQL_COMMIT_SHA: {SQL_SHA}", job)
        materializer = ast.parse((ROOT / "scripts" / "materialize_community_publication_runtime.py").read_text())
        sql_pin = next(node.value.value for node in materializer.body if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == "SQL_SHA" for target in node.targets))
        self.assertEqual(sql_pin, SQL_SHA)
        self.assertIn('git -C statground-sql rev-parse HEAD', job)
        self.assertIn("STATGROUND_SQL_READ_TOKEN", job)

    def test_existing_admin_sql_fallback_is_limited_to_immutable_checkout(self) -> None:
        job = MAIN_WORKFLOW.split("  community-publication:", 1)[1]
        expression = "${{ secrets.STATGROUND_SQL_READ_TOKEN || secrets.STATGROUND_CDN2_ADMIN_TOKEN }}"
        validation = job.split("- name: Validate protected publication settings", 1)[1].split("\n      - name:", 1)[0]
        self.assertIn("STATGROUND_SQL_READ_TOKEN: " + expression, validation)
        for workflow, pin in ((job, SQL_SHA), (BOOTSTRAP_WORKFLOW, BOOTSTRAP_SQL_SHA)):
            checkout = workflow.split("- name: Checkout immutable Statground SQL publisher", 1)[1].split("\n      - name:", 1)[0]
            self.assertIn("token: " + expression, checkout)
            self.assertIn("persist-credentials: false", checkout)
            self.assertIn(f"ref: {pin}", checkout)
        # No publisher client, token bundle or collection setting can receive
        # this checkout-only credential, and no broader GitHub token is used.
        self.assertEqual(MAIN_WORKFLOW.count(expression), 2)
        self.assertEqual(BOOTSTRAP_WORKFLOW.count(expression), 1)
        self.assertNotIn("secrets.GITHUB_TOKEN", validation)

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
        self.assertIn("runs-on: [self-hosted, linux, x64, webr-community-publisher]", BOOTSTRAP_WORKFLOW)
        self.assertIn("environment: web-r-community-publication-bootstrap", BOOTSTRAP_WORKFLOW)
        self.assertIn("group: r-project-publication", BOOTSTRAP_WORKFLOW)
        self.assertIn("cancel-in-progress: false", BOOTSTRAP_WORKFLOW)
        self.assertIn(f"ref: {BOOTSTRAP_SQL_SHA}", BOOTSTRAP_WORKFLOW)
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
