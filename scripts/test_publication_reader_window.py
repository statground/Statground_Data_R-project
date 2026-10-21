"""Scheduled CLI contract and optional pinned-SQL, RAM-only protocol checks.

Set STATGROUND_SQL_PUBLISHER_SOURCE to the pinned READY95 publisher to run the
cross-repository checks. Data-only CI always checks the emitted command and
schedule; it reports the unavailable private SQL integration explicitly.
"""
from __future__ import annotations

import ast
import copy
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock
from urllib import error as urllib_error
import uuid

import yaml

ROOT = Path(__file__).resolve().parents[1]
SQL_SHA = "37e3f5d2e9a382b75d5b3b4a1f559e1d4f75549e"
PUBLISHER_SHA256 = "abb3786c2456c50fc7b3e49df9ff07ab1d0209d493e597083a246509eedb2744"


def scheduled_arguments():
    workflow = yaml.safe_load((ROOT / ".github/workflows/r-project-all.yml").read_text())
    step, = [s for s in workflow["jobs"]["community-publication"]["steps"]
             if s.get("name") == "Publish and reopen exact application readers"]
    prefix, separator, _ = step["run"].partition("python - <<'PY'")
    assert separator
    prefix = prefix.replace("${{ steps.community_loader.outputs.generation }}",
                            "2026-09-20 14:20:30.123456")
    prefix = prefix.replace("${{ steps.web_r_cdn2_community.outputs.commit_sha }}", "a" * 40)
    assert "${{" not in prefix
    with tempfile.TemporaryDirectory() as directory:
        tmp = Path(directory)
        (tmp / "runtime").mkdir()
        executable = tmp / "python"
        executable.write_text(
            "#!" + sys.executable + "\nimport json,os,sys\n"
            "open(os.environ['WINDOW_ARGV_CAPTURE'],'w').write(json.dumps(sys.argv[1:]))\n"
            "print('{}')\n")
        executable.chmod(0o700)
        environment = {"PATH": str(tmp) + ":" + os.defpath, "LC_ALL": "C",
                       "COMMUNITY_RUNTIME_DIR": str(tmp / "runtime"),
                       "WINDOW_ARGV_CAPTURE": str(tmp / "argv.json")}
        result = subprocess.run(["/bin/bash", "-e", "-u", "-o", "pipefail", "-c", prefix],
                                cwd=ROOT, env=environment, capture_output=True, timeout=3)
        assert result.returncode == 0, result.stderr.decode()
        return json.loads((tmp / "argv.json").read_text())


def checked(path, size, digest):
    path = Path(path)
    assert path.is_absolute() and path.resolve(strict=True) == path
    before = path.lstat()
    raw = path.read_bytes()
    after = path.lstat()
    fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink",
              "st_size", "st_mtime_ns", "st_ctime_ns")
    assert stat.S_ISREG(before.st_mode) and before.st_nlink == 1
    assert tuple(getattr(before, k) for k in fields) == tuple(getattr(after, k) for k in fields)
    assert len(raw) == size and hashlib.sha256(raw).hexdigest() == digest
    return raw


def pinned_fixtures(path):
    raw = checked(path, 199273, PUBLISHER_SHA256)
    module = types.ModuleType("natural_reader95_pinned_sql")
    module.__file__ = str(path)
    sys.modules[module.__name__] = module
    exec(compile(raw, str(path), "exec"), vars(module))
    source = Path(path).parent / "tests/test_webr_community_generation_publisher.py"
    raw = checked(source, 119241,
                  "050b2b5cfc8215c5568e67f2285d7a08ce169c83ffe652e02d4576fa72e88adb")
    constants = {"GENERATION", "OLD_GENERATION", "CDN_BASE_URL", "OLD_CDN_BASE_URL",
                 "INVENTORY_UUID", "EPOCH_UUID", "RESTARTED_EPOCH_UUID", "READER_ENDPOINT",
                 "TRANSITION_ENDPOINT", "ACK_PRIVILEGE_SQL"}
    definitions = {"evidence", "write_cdn_proof", "reader", "inventory", "old_authority",
                   "FakeClient", "ReaderSimulator"}
    selected = [n for n in ast.parse(raw).body
                if (isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
                    and n.targets[0].id in constants)
                or (isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in definitions)]
    assert len(selected) == len(constants) + len(definitions)
    fixture = types.ModuleType("natural_reader90_ram_fixture")
    vars(fixture).update(module=module, dt=dt, Path=Path, json=json, os=os,
                         uuid=uuid, mock=mock, urllib_error=urllib_error)
    # Only original constants and RAM fixture definitions are compiled. The
    # original fixture's independent filesystem/module loader is not executed.
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(source), "exec"), vars(fixture))
    source = Path(path).parent / "tests/test_webr_community_pre_pointer_commit_budget.py"
    raw = checked(source, 28049,
                  "4b99103306f3f0b7957efb38d5b059fb101d4a43096cb7cd01ce39b453552f1b")
    selected = [n for n in ast.parse(raw).body if isinstance(n, ast.ClassDef)
                and n.name in {"Response", "WireHarness"}]
    assert len(selected) == 2
    wire = types.ModuleType("natural_reader90_ram_wire")
    vars(wire).update(fixture=fixture, module=module, copy=copy, io=io, Path=Path,
                      tempfile=tempfile, ExitStack=ExitStack, mock=mock)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(source), "exec"), vars(wire))
    return module, fixture, wire


class ScheduledCommandTests(unittest.TestCase):
    def test_actual_scheduled_shell_argument(self):
        args = scheduled_arguments()
        self.assertTrue(args[0].endswith("webr_community_generation_publisher.py"))
        self.assertEqual(args[1], "publish")
        self.assertEqual(args[args.index("--reader-timeout-seconds") + 1], "90")
        self.assertEqual(args.count("--reader-timeout-seconds"), 1)

    def test_schedule_and_protected_sql_pin_are_unchanged(self):
        wrapper = ROOT / ".github/workflows/r-project-community-publication.yml"
        self.assertEqual(hashlib.sha256(wrapper.read_bytes()).hexdigest(),
                         "22e76db7d5c8bf127f79740dc9573e1275bcacbc84e12273ac98832f1a850871")
        workflow = (ROOT / ".github/workflows/r-project-all.yml").read_text()
        self.assertIn("ref: " + SQL_SHA, workflow)
        tree = ast.parse((ROOT / "scripts/materialize_community_publication_runtime.py").read_bytes())
        value, = [n.value.value for n in tree.body if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "SQL_SHA" for t in n.targets)]
        self.assertEqual(value, SQL_SHA)


class PinnedPublisherWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = os.environ.get("STATGROUND_SQL_PUBLISHER_SOURCE")
        if not path:
            raise unittest.SkipTest("Pinned private SQL integration requires STATGROUND_SQL_PUBLISHER_SOURCE")
        cls.m, cls.fixture, cls.wire = pinned_fixtures(Path(path))

    def run_cli(self, harness, *, use_default=False):
        m, f = self.m, self.fixture
        args = scheduled_arguments()[1:]
        if use_default:
            index = args.index("--reader-timeout-seconds")
            del args[index:index + 2]
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            proof = Path(directory) / "proof.json"
            f.write_cdn_proof(proof)
            args[args.index("--cdn-proof") + 1] = str(proof)
            for obj, name, value in ((m.time, "monotonic", lambda: harness.clock),
                                      (m.time, "sleep", lambda _: None),
                                      (m, "reader_token", lambda _: "synthetic-only")):
                stack.enter_context(mock.patch.object(obj, name, side_effect=value))
            stack.enter_context(mock.patch.object(m, "STRICT_HTTP_OPENER", harness))
            stack.enter_context(mock.patch.object(m, "parse_endpoints", return_value=list(harness.client.endpoints.values())))
            stack.enter_context(mock.patch.object(m, "parse_reader_inventory", return_value=harness.inventory))
            stack.enter_context(mock.patch.object(m, "Client", return_value=harness.client))
            stack.enter_context(mock.patch.object(m, "Publisher", return_value=harness.service))
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                return m.main(args)

    def test_original_cli_reaches_reconcile_and_commit(self):
        h = self.wire.WireHarness(0)
        with mock.patch.object(h.service, "reconcile_readers", wraps=h.service.reconcile_readers) as reconcile, \
                mock.patch.object(h.service, "commit_readers", wraps=h.service.commit_readers) as commit:
            self.assertEqual(self.run_cli(h), 0)
        self.assertEqual(reconcile.call_args.args[1], 90)
        self.assertEqual(commit.call_args.args[2], 90)
        self.assertEqual([timeout for op, _, timeout in h.requests if op == "commit"], [90, 90])
        self.assertTrue(all(s.phase == "steady" and s.admission_open for s in h.sims.values()))

    def test_cli_default_stays60_and_original_lease_clips_or_refuses90(self):
        h = self.wire.WireHarness(0)
        with mock.patch.object(h.service, "reconcile_readers", wraps=h.service.reconcile_readers) as reconcile:
            self.assertEqual(self.run_cli(h, use_default=True), 0)
        self.assertEqual(reconcile.call_args.args[1], 60)
        h.service._lease_deadline_monotonic = 295
        for now, expected in ((0, 90), (210, 80), (289, 1)):
            with mock.patch.object(self.m.time, "monotonic", return_value=now):
                self.assertEqual(h.service._transition_timeout(90), expected)
        for now, reserve in ((290, False), (260, True)):
            with mock.patch.object(self.m.time, "monotonic", return_value=now):
                with self.assertRaises(self.m.TransitionBudgetUnavailable):
                    h.service._transition_timeout(90, reserve_abort=reserve)

    def test_shared95_http_retry_envelope_decreases_advertised_window(self):
        m = self.m
        clock = [0.0]
        requests = []
        url = m.transition_endpoint(self.fixture.reader(), "/commit")
        def opening(request, timeout):
            requests.append((timeout, json.loads(request.data)["timeout_seconds"]))
            if len(requests) < 3:
                clock[0] += 20
                raise urllib_error.HTTPError(url, 503, "synthetic", None, io.BytesIO(b""))
            return self.wire.Response(url, {})
        with mock.patch.object(m.time, "monotonic", side_effect=lambda: clock[0]), \
                mock.patch.object(m.time, "sleep", side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)), \
                mock.patch.object(m, "reader_token", return_value="synthetic-only"), \
                mock.patch.object(m.STRICT_HTTP_OPENER, "open", side_effect=opening):
            result = m.request_transition_json(self.fixture.reader(), "POST", url,
                {"format": m.TRANSITION_COMMIT_REQUEST_FORMAT, "timeout_seconds": 90}, 90)
        self.assertEqual(result, {})
        self.assertEqual([window for _, window in requests], [90, 69, 49])
        self.assertTrue(all(abs(clock_start + timeout - 95) < .000001
                            for clock_start, (timeout, _) in zip((0, 20.1, 40.3), requests)))

    def assert_no_pointer_and_old_reopened(self, h):
        self.assertEqual(h.client.pointer, h.old_pointer)
        self.assertEqual(h.client.current_authority(), h.old)
        self.assertTrue(all(s.phase == "steady" and s.admission_open for s in h.sims.values()))
        self.assertFalse(h.client.ack_rows)
        self.assertFalse(any(t[0] == "insert" and t[2] == self.m.POINTER for t in h.client.trace))
        self.assertEqual([op for op, _, _ in h.requests if op != "inventory"],
                         ["prepare", "prepare", "abort", "abort"])

    def test_original_reserve185_boundary_and_original_same_old_abort(self):
        with redirect_stderr(io.StringIO()), mock.patch.object(
                self.m.Publisher, "ready_commit_pair", return_value=None):
            for seconds, boundary in ((60, 135.0), (90, 75.0)):
                h = self.wire.WireHarness(boundary)
                self.assertEqual(h.publish(seconds)["status"], "released")
            for now in (75.001, 135.0):
                h = self.wire.WireHarness(now)
                with self.assertRaisesRegex(self.m.PublishError, "all reader COMMIT budgets before pointer"):
                    h.publish(90)
                self.assert_no_pointer_and_old_reopened(h)

    def test_scheduled_cli_uses_owned_ready95_or_original_same_old_abort(self):
        for elapsed, status in ((126.532, 0), (165.0, 0), (165.001, 2)):
            with self.subTest(elapsed=elapsed):
                h = self.wire.WireHarness(elapsed)
                pairs = []
                original = h.service.ready_commit_pair
                def capture(*args):
                    pair = original(*args)
                    pairs.append(pair)
                    return pair
                h.service.ready_commit_pair = capture
                insert = h.client.insert
                def guarded_pointer(endpoint, table, *args):
                    if table == self.m.POINTER:
                        self.assertEqual(pairs[0].state, "parked")
                        self.assertTrue(pairs[0].ready_verified)
                        self.assertFalse(any(op == "commit" for op, _, _ in h.requests))
                    return insert(endpoint, table, *args)
                h.client.insert = guarded_pointer
                self.assertEqual(self.run_cli(h), status)
                self.assertEqual(len(pairs), 1)
                self.assertTrue(pairs[0].joined)
                self.assertTrue(all(f.done() for f in pairs[0].futures))
                if status == 0:
                    self.assertEqual(pairs[0].state, "opened")
                    self.assertEqual([timeout for op, _, timeout in h.requests if op == "commit"], [90, 90])
                else:
                    self.assertEqual(pairs[0].state, "cancelled")
                    self.assert_no_pointer_and_old_reopened(h)

    def test_insufficient_reserve_with_abort_guard_failure_never_writes_pointer(self):
        h = self.wire.WireHarness(75.001, drift="current")
        with redirect_stderr(io.StringIO()), mock.patch.object(
                h.service, "ready_commit_pair", return_value=None), self.assertRaises(self.m.PublishError):
            h.publish(90)
        self.assertTrue(h.aborting)
        self.assertFalse(h.client.ack_rows)
        self.assertFalse(any(t[0] == "insert" and t[2] == self.m.POINTER for t in h.client.trace))


if __name__ == "__main__":
    unittest.main()
