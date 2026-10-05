import io
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch
import webr_runner_sealed_bootstrap as m

class FakeAPI:
    def __init__(self, pages, receipt=None):
        self.pages, self.receipt, self.calls = list(pages), receipt, []
    def request(self, method, suffix):
        self.calls.append((method, suffix))
        if suffix == "": return {"full_name": m.REPO}
        if method == "POST": return self.receipt.copy()
        return self.pages.pop(0)

def env(ref=None):
    return {"GITHUB_REPOSITORY": m.REPO, "GITHUB_SHA": "a" * 40, "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_REF": ref or "refs/tags/webr-runner-sealed-bootstrap-" + m.NONCE}

def row(identity=1, name=m.NAME, labels=None):
    return {"id": identity, "name": name, "labels": [{"name": v} for v in (labels or m.LABELS)], "status": "offline", "busy": False}

EMPTY = {"total_count": 0, "runners": []}
TOKEN = {"token": "FAKESHORTTOKENFOROFFLINETEST", "expires_at": "2033-05-18T04:33:20+00:00"}

class EnrollmentTests(unittest.TestCase):
    def test_empty_before_and_after_mint_is_required(self):
        api = FakeAPI([EMPTY, EMPTY], TOKEN)
        seen = []
        result = m.bootstrap(api, env(), clock=lambda: 2000000000, sealer=lambda p: seen.append(p.copy()) or [])
        self.assertEqual(result["state"], "sealed")
        self.assertEqual([c[0] for c in api.calls], ["GET", "GET", "POST", "GET", "GET"])
        self.assertEqual(seen[0][2], TOKEN["token"])
        self.assertNotIn(TOKEN["token"], json.dumps(result))

    def test_no_post_for_any_existing_runner_or_name_collision(self):
        for candidate in [row(), row(name=m.NAME.upper(), labels=["linux"]), row(name="unrelated", labels=["linux"])]:
            api = FakeAPI([{"total_count": 1, "runners": [candidate]}], TOKEN)
            with self.assertRaises(m.Blocked): m.bootstrap(api, env())
            self.assertFalse(any(method == "POST" for method, _ in api.calls))

    def test_runner_created_after_mint_prohibits_receipt_release(self):
        api = FakeAPI([EMPTY, {"total_count": 1, "runners": [row()]}], TOKEN)
        with self.assertRaises(m.Blocked), patch.object(m, "seal") as sealed:
            m.bootstrap(api, env(), clock=lambda: 2000000000, sealer=sealed)
        sealed.assert_not_called()

    def test_incomplete_and_duplicate_inventories_never_mint(self):
        for page in [{"total_count": 101, "runners": []}, {"total_count": 2, "runners": [row(), row()]}]:
            api = FakeAPI([page], TOKEN)
            with self.assertRaises(m.Blocked): m.bootstrap(api, env())
            self.assertFalse(any(method == "POST" for method, _ in api.calls))

    def test_mint_cannot_run_from_branch_or_retry(self):
        for e in [env("refs/heads/main"), {**env(), "GITHUB_RUN_ATTEMPT": "2"}, {**env(), "GITHUB_REPOSITORY": "other/repo"}]:
            api = FakeAPI([], TOKEN)
            with self.assertRaises(m.Blocked): m.bootstrap(api, e)
            self.assertEqual(api.calls, [])

    def test_readback_is_get_only_and_requires_exact_unique_runner(self):
        api = FakeAPI([{"total_count": 1, "runners": [row(identity=17)]}])
        result = m.bootstrap(api, env("refs/tags/webr-runner-sealed-readback-" + m.NONCE + "-17"), clock=lambda: 2000000000)
        self.assertEqual(result["runner_id"], 17)
        self.assertEqual(result["status"], "offline")
        self.assertFalse(any(method == "POST" for method, _ in api.calls))
        for rows in [[row(identity=18)], [row(identity=17), row(identity=18, name="other-protected")], [row(identity=17, labels=["linux"])]]:
            api = FakeAPI([{"total_count": len(rows), "runners": rows}])
            with self.assertRaises(m.Blocked): m.bootstrap(api, env("refs/tags/webr-runner-sealed-readback-" + m.NONCE + "-17"))
            self.assertFalse(any(method == "POST" for method, _ in api.calls))

    def test_malformed_or_expired_token_cannot_be_released(self):
        for receipt in [{**TOKEN, "token": "bad\nprivate"}, {**TOKEN, "expires_at": "2020-01-01T00:00:00Z"}, {**TOKEN, "expires_at": "2099-01-01T00:00:00Z"}]:
            with self.assertRaises(m.Blocked): m.bootstrap(FakeAPI([EMPTY], receipt), env(), clock=lambda: 2000000000)

    def test_real_public_key_seals_bounded_payload_without_plaintext(self):
        payload = [1, m.REPO, TOKEN["token"], 2000003600, 2000000000, "a" * 40, 123, m.NONCE, m.KEY_SHA]
        chunks = m.seal(payload)
        self.assertEqual(len(chunks), 8)
        encoded = "".join(item["chunk"] for item in chunks)
        self.assertEqual(len(encoded), 683)
        self.assertNotIn(TOKEN["token"], encoded)
        self.assertTrue(all(len(item["chunk"]) <= 86 for item in chunks))
        with self.assertRaises(m.Blocked): m.seal(["x" * 447])

    def test_api_rejects_unrelated_writes_before_network(self):
        api = m.RepoAPI("FAKEOPAQUEKEY")
        for method, suffix in [("DELETE", "/actions/runners/1"), ("POST", "/actions/workflows/x/dispatches"), ("GET", "/actions/runners?per_page=100&page=11")]:
            with self.assertRaises(m.Blocked): api.request(method, suffix)

    def test_operator_failure_does_not_print_secret_or_raw_exception(self):
        with patch.object(m, "bootstrap", side_effect=ValueError(TOKEN["token"])), patch.dict(os.environ, {"RUNNER_BOOTSTRAP_TOKEN": "FAKELONGLIVEDKEY"}), patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(m.main(), 1)
        self.assertNotIn(TOKEN["token"], output.getvalue())
        self.assertNotIn("FAKELONGLIVEDKEY", output.getvalue())

    def test_workflow_only_exposes_public_context_and_ciphertext(self):
        text = (Path(__file__).parents[1] / ".github/workflows/webr-runner-sealed-bootstrap.yml").read_text()
        self.assertNotIn("schedule:", text)
        self.assertEqual(text.count("secrets.STATGROUND_CDN2_ADMIN_TOKEN"), 1)
        self.assertIn('Sealed chunk ${{ matrix.index }}/8 ${{ matrix.chunk }}', text)
        self.assertNotIn("upload-artifact", text)
        self.assertNotIn("config.sh", text)

if __name__ == "__main__": unittest.main()
