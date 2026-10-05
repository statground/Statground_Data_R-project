from __future__ import annotations

import base64
import contextlib
import fnmatch
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

import yaml

import webr_community_protected_key_readonly as reader

TOKEN = "opaque-private-fixture-without-provider-prefix"
SHA = "a" * 40
PUBLIC_KEY = base64.b64encode(bytes(range(32))).decode("ascii")
KEY_ID = "012345678912345678"


class Response:
    status = 200

    def __init__(self, data, url=None, status=200):
        self.data, self.url, self.status = data, url, status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def geturl(self):
        return self.url

    def read(self, limit):
        raw = self.data if isinstance(self.data, bytes) else json.dumps(self.data).encode()
        return raw[:limit]


class Opener:
    def __init__(self, responses):
        self.responses, self.requests, self.timeouts = list(responses), [], []

    def open(self, request, timeout):
        self.requests.append(request)
        self.timeouts.append(timeout)
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        value = value if isinstance(value, Response) else Response(value)
        value.url = value.url or request.full_url
        return value


def http_error(code):
    return urllib.error.HTTPError("https://private.invalid/" + TOKEN, code, TOKEN, {}, io.BytesIO(TOKEN.encode()))


def responses(metadata=None):
    return [{"id": 123, "full_name": reader.REPOSITORY, "ignored": TOKEN},
            {"key": PUBLIC_KEY, "key_id": KEY_ID}] + (metadata if metadata is not None else [http_error(404) for _ in reader.SECRET_NAMES])


def environment(output, event="push"):
    ref = "refs/tags/" + reader.TAG if event == "push" else "refs/heads/main"
    return {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": reader.REPOSITORY,
            "GITHUB_SERVER_URL": "https://github.com", "GITHUB_API_URL": reader.ORIGIN,
            "GITHUB_RUN_ATTEMPT": "1", "PROTECTED_KEY_READONLY_NONCE": reader.NONCE,
            "RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_OS": "Linux",
            "GITHUB_SHA": SHA, "GITHUB_WORKFLOW_SHA": SHA,
            "GITHUB_WORKFLOW_REF": reader.REPOSITORY + "/" + reader.WORKFLOW + "@" + ref,
            "GITHUB_RUN_ID": "12345", "GITHUB_REPOSITORY_ID": "123",
            "GITHUB_EVENT_NAME": event, "GITHUB_REF": ref,
            "GITHUB_OUTPUT": str(output), "RUNNER_TEMP": str(output.parent),
            "STATGROUND_CDN2_ADMIN_TOKEN": TOKEN}


class ProtectedKeyReadTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name) / "set_output"
        self.output.touch(mode=0o600)
        self.env = environment(self.output)
        self.context = reader.workflow_context(self.env, SHA)

    def main_with(self, opener, env=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, env or self.env, clear=True), \
             mock.patch.object(sys, "argv", [reader.__file__]), \
             mock.patch.object(reader, "checkout_head", return_value=SHA), \
             mock.patch.object(reader.urllib.request, "build_opener", return_value=opener), \
             contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = reader.main()
        return code, json.loads(stdout.getvalue()), stdout.getvalue() + stderr.getvalue()

    def test_five_exact_gets_order_opaque_auth_and_finite_receipt(self):
        metadata = [{"name": name, "created_at": TOKEN, "ignored_value": TOKEN} for name in reader.SECRET_NAMES]
        opener = Opener(responses(metadata))
        code, result, printed = self.main_with(opener)
        self.assertEqual(code, 0)
        self.assertTrue(result["complete"])
        self.assertEqual(result["key"], PUBLIC_KEY)
        self.assertEqual(result["key_id"], KEY_ID)
        self.assertEqual(result["metadata_get_200"], dict.fromkeys(reader.SECRET_NAMES, True))
        self.assertEqual(result["repository_id"], 123)
        self.assertEqual(result["source_sha"], SHA)
        self.assertEqual(result["run_attempt"], 1)
        self.assertRegex(result["observed_at"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
        self.assertEqual([req.full_url for req in opener.requests],
                         [reader.REPO_URL, reader.SECRETS_URL + "public-key"] + [reader.SECRETS_URL + name for name in reader.SECRET_NAMES])
        for request in opener.requests:
            self.assertEqual(request.get_method(), "GET")
            self.assertIsNone(request.data)
            self.assertEqual(request.get_header("Authorization"), "Bearer " + TOKEN)
        self.assertTrue(all(0 < timeout <= 12 for timeout in opener.timeouts))
        self.assertNotIn(TOKEN, printed + self.output.read_text())
        self.assertNotIn("ignored", printed + self.output.read_text())
        self.assertIn("key=" + PUBLIC_KEY + "\n", self.output.read_text())

    def test_metadata_404_is_only_get200_false_not_write_permission(self):
        code, result, printed = self.main_with(Opener(responses()))
        self.assertEqual(code, 0)
        self.assertEqual(result["metadata_get_200"], dict.fromkeys(reader.SECRET_NAMES, False))
        self.assertNotIn("absent", printed)
        self.assertNotIn("write_permission", printed)

    def test_permission_failures_never_emit_private_errors_or_partial_key(self):
        for position in range(5):
            for status in (401, 403):
                with self.subTest(position=position, status=status):
                    values = responses()
                    values[position] = http_error(status)
                    opener = Opener(values)
                    self.output.write_text("")
                    code, result, printed = self.main_with(opener)
                    self.assertEqual(code, 1)
                    self.assertEqual(result["reason"], "http_error")
                    self.assertEqual(result["http_status"], status)
                    self.assertIsNone(result["key"])
                    self.assertIsNone(result["metadata_get_200"])
                    self.assertEqual(len(opener.requests), position + 1)
                    self.assertNotIn(TOKEN, printed + self.output.read_text())
                    self.assertNotIn("private.invalid", printed)
                    self.assertNotIn(PUBLIC_KEY, printed + self.output.read_text())
        for position in (0, 1):
            with self.subTest(position=position, status=404):
                values = responses()
                values[position] = http_error(404)
                code, result, _ = self.main_with(Opener(values))
                self.assertEqual((code, result["http_status"]), (1, 404))
                self.assertIsNone(result["metadata_get_200"])

    def test_wrong_repository_never_reaches_environment(self):
        for value in ({"id": 999, "full_name": reader.REPOSITORY},
                      {"id": 123, "full_name": "other/private"},
                      {"id": True, "full_name": reader.REPOSITORY}):
            with self.subTest(value=value):
                opener = Opener([value])
                code, result, _ = self.main_with(opener)
                self.assertEqual((code, result["reason"]), (1, "repository_identity_invalid"))
                self.assertEqual(len(opener.requests), 1)

    def test_public_key_size_base64_key_id_and_injection_fail_closed(self):
        values = [
            {"key": base64.b64encode(b"x" * 31).decode(), "key_id": KEY_ID},
            {"key": base64.b64encode(b"x" * 33).decode(), "key_id": KEY_ID},
            {"key": PUBLIC_KEY[:-1] + "!", "key_id": KEY_ID},
            {"key": PUBLIC_KEY, "key_id": "1\nkey=" + TOKEN},
            {"key": PUBLIC_KEY, "key_id": "${{ secrets.ADMIN }}"},
            {"key": PUBLIC_KEY, "key_id": 123},
            {"key": PUBLIC_KEY, "key_id": "1" * 33},
            {"key": PUBLIC_KEY, "key_id": ""},
        ]
        for value in values:
            with self.subTest(value=value):
                opener = Opener([responses()[0], value])
                code, result, printed = self.main_with(opener)
                self.assertEqual((code, result["reason"]), (1, "public_key_shape_invalid"))
                self.assertEqual(len(opener.requests), 2)
                self.assertNotIn(TOKEN, printed)

    def test_secret_metadata_name_must_match_fixed_request(self):
        for value in ({"name": "OTHER_TOKEN"}, {"name": reader.SECRET_NAMES[0] + "\n" + TOKEN}, {}, []):
            with self.subTest(value=value):
                opener = Opener(responses([value]))
                code, result, printed = self.main_with(opener)
                self.assertEqual(code, 1)
                self.assertIn(result["reason"], ("secret_metadata_identity_invalid", "json_invalid"))
                self.assertEqual(len(opener.requests), 3)
                self.assertNotIn(TOKEN, printed)

    def test_redirect_identity_status_body_and_duplicate_json_rejected(self):
        cases = [
            (Response({}, url="https://other.invalid/" + TOKEN), "response_identity_invalid"),
            (Response({}, status=302), "response_identity_invalid"),
            (Response(b"x" * (reader.MAX_BODY + 1)), "body_limit"),
            (Response(b'{"id":123,"id":123}'), "json_invalid"),
            (Response(b'{"id":NaN}'), "json_invalid"),
            (Response(b"invalid-" + TOKEN.encode()), "json_invalid"),
        ]
        for response, reason in cases:
            with self.subTest(reason=reason):
                code, result, printed = self.main_with(Opener([response]))
                self.assertEqual((code, result["reason"]), (1, reason))
                self.assertNotIn(TOKEN, printed)
        with self.assertRaises(reader.ReadFailure) as failure:
            reader.NoRedirect().redirect_request(None, None, 302, TOKEN, {}, "https://other.invalid")
        self.assertEqual(failure.exception.reason, "redirect_rejected")

    def test_network_exception_is_private_static_failure(self):
        code, result, printed = self.main_with(Opener([RuntimeError(TOKEN + "/private-body")]))
        self.assertEqual((code, result["reason"]), (1, "transport_unavailable"))
        self.assertNotIn(TOKEN, printed)

    def test_expired_read_budget_stops_without_further_gets(self):
        opener = Opener(responses())
        ticks = iter((0, 0, reader.READ_SECONDS))
        with self.assertRaises(reader.ReadFailure) as failure:
            reader.read_protected_key(TOKEN, self.context, opener, clock=lambda: next(ticks))
        self.assertEqual(failure.exception.reason, "wall_deadline")
        self.assertEqual(len(opener.requests), 1)

    def test_missing_and_header_unsafe_credential_never_network(self):
        for value in ("", "a\r\nAuthorization: " + TOKEN, "\x00" + TOKEN, "é" + TOKEN, "x" * 4097):
            opener = Opener([])
            with self.assertRaises(reader.ReadFailure):
                reader.read_protected_key(value, self.context, opener)
            self.assertEqual(opener.requests, [])

    def test_public_material_cannot_echo_private_credential(self):
        opener = Opener(responses())
        with self.assertRaises(reader.ReadFailure) as failure:
            reader.read_protected_key(PUBLIC_KEY, self.context, opener)
        self.assertEqual(failure.exception.reason, "public_material_collision")
        self.assertEqual(len(opener.requests), 2)

    def test_build_opener_disables_proxies_and_rejects_redirects(self):
        with mock.patch.object(reader.urllib.request, "build_opener", return_value=Opener(responses())) as factory:
            reader.read_protected_key(TOKEN, self.context)
        proxy, redirect = factory.call_args.args
        self.assertIsInstance(proxy, urllib.request.ProxyHandler)
        self.assertEqual(proxy.proxies, {})
        self.assertIsInstance(redirect, reader.NoRedirect)

    def test_context_exact_ref_nonce_attempt_checkout_and_native_platform(self):
        bad = {"GITHUB_ACTIONS": "false", "GITHUB_REPOSITORY": "other/repo", "GITHUB_API_URL": "https://private.invalid",
               "GITHUB_SERVER_URL": "https://private.invalid", "GITHUB_RUN_ATTEMPT": "2",
               "PROTECTED_KEY_READONLY_NONCE": "wrong", "RUNNER_ENVIRONMENT": "self-hosted",
               "RUNNER_OS": "Windows", "GITHUB_WORKFLOW_SHA": "b" * 40,
               "GITHUB_WORKFLOW_REF": "other.yml", "GITHUB_REPOSITORY_ID": "123\n" + TOKEN,
               "GITHUB_RUN_ID": "1\n" + TOKEN, "GITHUB_SHA": "A" * 40,
               "GITHUB_REF": "refs/tags/" + reader.TAG + "-other", "GITHUB_EVENT_NAME": "pull_request"}
        for name, value in bad.items():
            with self.subTest(name=name):
                env = dict(self.env, **{name: value})
                opener = Opener([])
                code, result, printed = self.main_with(opener, env)
                self.assertEqual((code, result["reason"]), (1, "context_invalid"))
                self.assertEqual(opener.requests, [])
                self.assertNotIn(TOKEN, printed)
        with self.assertRaises(reader.ReadFailure) as failure:
            reader.workflow_context(self.env, "b" * 40)
        self.assertEqual(failure.exception.reason, "checkout_identity_invalid")
        manual = reader.workflow_context(environment(self.output, "workflow_dispatch"), SHA)
        self.assertEqual(manual["ref"], "refs/heads/main")

    def test_checkout_command_has_no_token_or_network_and_suppresses_stderr(self):
        with mock.patch.object(reader.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, SHA.encode())) as process:
            self.assertEqual(reader.checkout_head(), SHA)
        self.assertEqual(process.call_args.args[0], ["git", "rev-parse", "HEAD"])
        self.assertEqual(process.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertNotIn(TOKEN, repr(process.call_args))
        with mock.patch.object(reader.subprocess, "run", side_effect=RuntimeError(TOKEN)):
            with self.assertRaises(reader.ReadFailure) as failure:
                reader.checkout_head()
        self.assertEqual(failure.exception.reason, "checkout_identity_invalid")

    def test_output_rejects_symlink_or_path_outside_runner_temp(self):
        for output in (self.output.parent / "link", self.output.parent.parent / "outside"):
            if output.name == "link":
                output.symlink_to(self.output)
            env = dict(self.env, GITHUB_OUTPUT=str(output))
            code, result, _ = self.main_with(Opener(responses()), env)
            self.assertEqual((code, result["reason"]), (1, "safe_output_unavailable"))
            self.assertIsNone(result["key"])
            self.assertFalse(output.exists() and output.name == "outside")

    def test_invalid_cli_is_nonzero_and_never_networks(self):
        command = subprocess.run([sys.executable, str(Path(reader.__file__).resolve()), "--invalid"],
                                 env={"PATH": os.environ.get("PATH", "")}, capture_output=True, text=True, timeout=5)
        self.assertEqual(command.returncode, 1)
        result = json.loads(command.stdout)
        self.assertFalse(result["complete"])
        self.assertIsNone(result["key"])
        self.assertEqual(command.stderr, "")

    def test_workflow_has_one_exact_tag_protected_token_and_read_only_reports(self):
        root = Path(__file__).resolve().parent.parent
        text = (root / reader.WORKFLOW).read_text()
        workflow = yaml.load(text, Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow["on"]), {"workflow_dispatch", "push"})
        self.assertEqual(workflow["on"]["push"]["tags"], [reader.TAG])
        self.assertEqual(workflow["permissions"], {"contents": "read"})
        self.assertEqual(workflow["concurrency"]["cancel-in-progress"], "false")
        jobs = workflow["jobs"]
        self.assertEqual(set(jobs), {"read", "key", "presence", "receipt"})
        job = jobs["read"]
        self.assertEqual(job["environment"], reader.ENVIRONMENT)
        self.assertEqual(job["runs-on"], "ubuntu-latest")
        self.assertEqual(job["timeout-minutes"], "2")
        self.assertIn("github.run_attempt == 1", job["if"])
        self.assertIn("github.repository == '" + reader.REPOSITORY + "'", job["if"])
        self.assertIn("refs/tags/" + reader.TAG, job["if"])
        self.assertIn("refs/heads/main", job["if"])
        self.assertEqual(len(job["steps"]), 2)
        checkout, read = job["steps"]
        self.assertEqual(checkout["with"], {"ref": "${{ github.sha }}", "persist-credentials": "false"})
        self.assertEqual(read["env"], {"STATGROUND_CDN2_ADMIN_TOKEN": "${{ secrets.STATGROUND_CDN2_ADMIN_TOKEN }}",
                                      "PROTECTED_KEY_READONLY_NONCE": reader.NONCE})
        self.assertEqual(read["run"], "python3 scripts/webr_community_protected_key_readonly.py")
        self.assertEqual(text.count("secrets.STATGROUND_CDN2_ADMIN_TOKEN"), 1)
        for name in ("key", "presence", "receipt"):
            self.assertNotIn("environment", jobs[name])
            self.assertNotIn("env", jobs[name])
            self.assertEqual(len(jobs[name]["steps"]), 1)
            self.assertTrue(jobs[name]["steps"][0]["run"].startswith("echo "))
        self.assertIn("False records metadata404", text)
        self.assertIn("does not establish setter permission", text)
        self.assertEqual(reader.WALL_SECONDS, 100)

    def test_exact_readonly_tag_cannot_trigger_other_active_workflows(self):
        root = Path(__file__).resolve().parent.parent
        matching = []
        for path in sorted((root / ".github/workflows").glob("*.yml")):
            workflow = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
            event = workflow.get("on", {})
            if isinstance(event, (str, list)):
                matches = "push" == event or "push" in event
            elif "push" not in event:
                matches = False
            else:
                push = event["push"] or {}
                if "tags" in push:
                    matches = False
                    for pattern in push["tags"]:
                        negative = pattern.startswith("!")
                        if fnmatch.fnmatchcase(reader.TAG, pattern[1:] if negative else pattern):
                            matches = not negative
                elif "branches" in push or "branches-ignore" in push:
                    matches = False
                else:
                    matches = not any(fnmatch.fnmatchcase(reader.TAG, value) for value in push.get("tags-ignore", []))
            if matches:
                matching.append(path.name)
        self.assertEqual(matching, [Path(reader.WORKFLOW).name])


if __name__ == "__main__":
    unittest.main()
