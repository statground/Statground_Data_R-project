from __future__ import annotations

import contextlib
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
from unittest import mock

import yaml
import webr_runner_inventory_audit as audit


def runner(identifier, *, publisher=True, name="webr-community-publisher-testgo", status="online", busy=False):
    labels = ("self-hosted", "Linux", "X64", "webr-community-publisher") if publisher else ("self-hosted", "Linux", "X64")
    return {"id": identifier, "name": name, "status": status, "busy": busy, "labels": [{"name": value} for value in labels]}


class Response:
    status = 200
    def __init__(self, url, data):
        self.url, self.raw = url, json.dumps(data).encode()
    def geturl(self): return self.url
    def read(self, cap): return self.raw[:cap]
    def __enter__(self): return self
    def __exit__(self, *_): return False


class Opener:
    def __init__(self, pages):
        self.pages, self.requests = list(pages), []
    def open(self, request, timeout):
        self.requests.append(request)
        if timeout != 8: raise AssertionError("changed request budget")
        value = self.pages.pop(0)
        if isinstance(value, Exception): raise value
        return Response(request.full_url, value)


class RunnerInventoryAuditTests(unittest.TestCase):
    def test_complete_pagination_uses_only_fixed_get_and_filters_exact_labels(self):
        rows = [runner(i, publisher=False, name="unrelated-private-name") for i in range(1, 101)]
        opener = Opener([{"total_count": 101, "runners": rows}, {"total_count": 101, "runners": [runner(101)]}])
        result = audit.audit("fixture-token", opener)
        self.assertTrue(result["complete"])
        self.assertEqual((result["total_count"], result["publisher_count"], result["publisher_idle_count"]), (101, 1, 1))
        self.assertNotIn("unrelated-private-name", json.dumps(result))
        self.assertNotIn("fixture-token", json.dumps(result))
        self.assertEqual([r.get_method() for r in opener.requests], ["GET", "GET"])
        self.assertEqual([r.full_url for r in opener.requests], [audit.ENDPOINT + "?per_page=100&page=1", audit.ENDPOINT + "?per_page=100&page=2"])

    def test_denied_or_missing_inventory_never_reports_zero(self):
        for status in (401, 403, 404, 500):
            with self.subTest(status=status):
                error = urllib.error.HTTPError(audit.ENDPOINT, status, "fixture-token/private body", {}, io.BytesIO(b"fixture-token/private body"))
                result = audit.audit("fixture-token", Opener([error]))
                self.assertFalse(result["complete"])
                self.assertIsNone(result["publisher_count"])
                self.assertNotIn("fixture-token", json.dumps(result))
                self.assertEqual(result["http_status"], status)
        self.assertIsNone(audit.audit("")["total_count"])

    def test_healthy_complete_empty_is_zero(self):
        result = audit.audit("fixture-token", Opener([{"total_count": 0, "runners": []}]))
        self.assertTrue(result["complete"])
        self.assertEqual(result["total_count"], 0)
        self.assertEqual(result["publisher_count"], 0)

    def test_duplicate_changed_count_and_truncated_pages_stay_unknown(self):
        cases = [[{"total_count": 2, "runners": [runner(1), runner(1)]}],
                 [{"total_count": 2, "runners": [runner(1)]}],
                 [{"total_count": 101, "runners": [runner(i) for i in range(1, 101)]}, {"total_count": 102, "runners": [runner(101)]}]]
        for pages in cases:
            result = audit.audit("fixture-token", Opener(pages))
            self.assertFalse(result["complete"])
            self.assertEqual(result["state"], "incomplete")
            self.assertIsNone(result["total_count"])

    def test_limit_and_malformed_inventory_do_not_claim_completeness(self):
        for value in ({"total_count": 1001, "runners": []}, {"total_count": True, "runners": []}, {"total_count": 1, "runners": [dict(runner(1), busy="false")]}, {"total_count": 1, "runners": [dict(runner(1), status="unexpected")]}, {"total_count": 1, "runners": [dict(runner(1), labels=[{}])]}):
            result = audit.audit("fixture-token", Opener([value]))
            self.assertFalse(result["complete"])
            self.assertIsNone(result["publisher_count"])

    def test_safe_names_and_online_busy_semantics(self):
        rows = [runner(1, name="10.0.0.1"), runner(2, name="ip-10-0-0-1", busy=True), runner(3, name="2001:db8::1", status="offline"), runner(4, name="github_pat_fixture_secret")]
        result = audit.audit("fixture-token", Opener([{"total_count": 4, "runners": rows}]))
        self.assertEqual([r["name"] for r in result["publishers"]], ["redacted"] * 4)
        self.assertEqual((result["publisher_online_count"], result["publisher_idle_count"]), (3, 2))

    def test_redirects_and_transport_errors_never_leak_private_messages(self):
        with self.assertRaises(audit.AuditFailure):
            audit.NoRedirect().redirect_request(None, None, 302, "fixture-token", {}, "https://other.invalid")
        result = audit.audit("fixture-token", Opener([RuntimeError("fixture-token/private-host")]))
        self.assertEqual(result["reason"], "transport_or_parse_failed")
        self.assertNotIn("fixture-token", json.dumps(result))

    def test_wall_budget_and_body_cap(self):
        ticks = iter((0, 90))
        opener = Opener([])
        result = audit.audit("fixture-token", opener, clock=lambda: next(ticks))
        self.assertEqual(result["reason"], "wall_deadline")
        self.assertFalse(opener.requests)
        opener = Opener([{"total_count": 0, "runners": [], "padding": "x" * audit.MAX_BODY}])
        self.assertEqual(audit.audit("fixture-token", opener)["reason"], "body_limit")

    def test_safe_github_outputs_preserve_unknown_and_real_zero(self):
        with tempfile.TemporaryDirectory() as path:
            output = str(pathlib.Path(path) / "outputs")
            audit.write_outputs(audit.failure_result("access-denied", "http_error", 403), output)
            first = pathlib.Path(output).read_text()
            self.assertIn("total_count=unknown", first)
            self.assertNotIn("total_count=0", first)
            audit.write_outputs(audit.audit("fixture-token", Opener([{"total_count": 0, "runners": []}])), output)
            self.assertIn("total_count=0", pathlib.Path(output).read_text())

    def test_cli_failure_outputs_fixed_unknown_without_live_secret_or_network(self):
        stdout = io.StringIO()
        with mock.patch.dict(audit.os.environ, {}, clear=True), mock.patch.object(audit.signal, "alarm"), mock.patch.object(audit.signal, "signal"), contextlib.redirect_stdout(stdout):
            self.assertEqual(audit.main(), 1)
        result = json.loads(stdout.getvalue())
        self.assertFalse(result["complete"])
        self.assertIsNone(result["total_count"])

    def test_workflow_is_manual_isolated_read_only_and_aggregate_report_observable(self):
        root = pathlib.Path(__file__).resolve().parents[1]
        workflow = yaml.load((root / ".github/workflows/webr-runner-inventory-audit.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow["on"]), {"workflow_dispatch", "push"})
        self.assertEqual(workflow["on"]["push"]["tags"], ["webr-runner-inventory-audit-*"])
        self.assertEqual(workflow["permissions"], {"contents": "read"})
        jobs = workflow["jobs"]
        self.assertEqual(set(jobs), {"audit", "report"})
        steps = jobs["audit"]["steps"]
        self.assertEqual(steps[0]["with"]["persist-credentials"], "false")
        self.assertEqual(steps[1]["env"], {"RUNNER_AUDIT_TOKEN": "${{ secrets.STATGROUND_CDN2_ADMIN_TOKEN }}"})
        self.assertEqual(steps[1]["run"], "python3 scripts/webr_runner_inventory_audit.py")
        self.assertEqual(jobs["audit"]["runs-on"], "ubuntu-latest")
        self.assertEqual(jobs["report"]["if"], "${{ always() }}")
        self.assertIn("'unknown'", jobs["report"]["name"])
        self.assertNotIn("publishers", jobs["report"]["name"])


if __name__ == "__main__":
    unittest.main()
