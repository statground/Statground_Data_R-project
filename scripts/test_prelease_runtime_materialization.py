"""Source-only owner/bundle fixtures; no production qualification is supplied."""
import base64
import builtins
import ast
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import shlex
import time
import types
import tempfile
import unittest
from unittest import mock

import materialize_community_publication_runtime as M
import webr_community_prelease_runtime_proof as P
import test_materialize_community_publication_runtime as original_tests
from test_webr_community_prelease_runtime_proof import fixture as public_fixture


class PreleaseMaterialization(unittest.TestCase):
    @contextlib.contextmanager
    def fixture(self):
        original = original_tests.OwnerPublicationInputsTest()
        original.setUp()
        bundle, _, _, normalized = public_fixture()
        original.inventory = copy.deepcopy(normalized)
        for row in original.inventory["readers"]:
            row["url"] = row["inventory_endpoint"].replace("/inventory", "/transition")
        original.tokens = {row["instance_id"]: "fixture-not-operational" for row in original.inventory["readers"]}
        with original.fixture() as (owner, manifest):
            source = {"data_source_sha": manifest["source_sha"], "sql_commit_sha": manifest["sql_commit_sha"],
                      "workflow_sha256": manifest["workflow_sha256"], "materializer_sha256": manifest["materializer_sha256"]}
            bundle["source_binding"] = source
            body = P.canonical_bytes(bundle) + b"\n"
            selected = {"bundle_sha256": hashlib.sha256(body).hexdigest(), "source_binding": copy.deepcopy(source),
                        "qualification_contract": copy.deepcopy(bundle["qualification_contract"])}
            manifest["schema"] = "web-r.community.owner-inputs.v2"
            manifest["reader_inventory_revision"] = normalized["reader_inventory_revision"]
            manifest["prelease_runtime_proof"] = {"bundle_file": M.PRELEASE_BUNDLE_FILE,
                "bundle_sha256": selected["bundle_sha256"], "qualification_contract": copy.deepcopy(selected["qualification_contract"]),
                "qualification_contract_sha256": P.canonical_sha256(selected["qualification_contract"])}
            (owner / "manifest.json").write_bytes(json.dumps(manifest).encode())
            path = owner / M.PRELEASE_BUNDLE_FILE
            path.write_bytes(body); path.chmod(0o600)
            with mock.patch.object(M, "workflow_source_selection", return_value=selected):
                yield original, owner, manifest, body, selected

    def load(self, original, owner, environment=None):
        return M.input_bundles(original.environment if environment is None else environment, owner,
                               original.source_sha, M.SQL_SHA, prelease_runtime_proof_bundle=owner / M.PRELEASE_BUNDLE_FILE)

    def test_unbound_source_refuses_cli_before_inputs_or_output(self):
        self.assertIsNone(P.PRELEASE_RUNTIME_SOURCE_SELECTION)
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(M, "private_owner_bytes") as owner_read, \
             mock.patch.object(M, "decode_bundle") as env_read, mock.patch.object(M.subprocess, "check_output") as git_read, \
             mock.patch.object(M, "workflow_source_selection", return_value=None), \
             contextlib.redirect_stderr(io.StringIO()) as errors:
            runtime = Path(directory) / "runtime"
            rc = M.main(["--runtime-dir", str(runtime), "--runner-temp", directory,
                         "--owner-input-dir", str(M.OWNER_INPUT_DIR), "--prelease-runtime-proof-bundle",
                         str(M.OWNER_INPUT_DIR / M.PRELEASE_BUNDLE_FILE)])
            self.assertEqual(rc, 2); self.assertFalse(runtime.exists())
            self.assertIn("unbound", errors.getvalue())
            owner_read.assert_not_called(); env_read.assert_not_called(); git_read.assert_not_called()

    def test_fixed_owner_bundle_and_runtime_copy_preserve_private_inputs(self):
        with self.fixture() as (original, owner, manifest, body, _):
            before = {p.name: p.read_bytes() for p in owner.iterdir()}
            loaded = self.load(original, owner)
            self.assertEqual(loaded[3], body)
            runtime = owner.parent / "runtime"
            result = M.materialize(runtime, owner.parent, *loaded[:3], prelease_runtime_proof=loaded[3])
            self.assertEqual(result, {"status": "materialized", "endpoint_count": 4, "reader_count": 2,
                                      "reader_inventory_revision": 7})
            path = runtime / M.PRELEASE_BUNDLE_FILE
            self.assertEqual(path.read_bytes(), body)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(runtime.stat().st_mode), 0o700)
            self.assertEqual(before, {p.name: p.read_bytes() for p in owner.iterdir()})
            self.assertNotIn("fixture", json.dumps(result)); self.assertNotIn("sha256", json.dumps(result))
            env = dict(original.environment, **{name: base64.b64encode(json.dumps(value).encode()).decode()
                for name, value in zip((M.CONFIGS_ENV, M.INVENTORY_ENV, M.TOKENS_ENV), loaded[:3])})
            self.assertEqual(self.load(original, owner, env), loaded)
            changed = dict(loaded[2]); changed[next(iter(changed))] = "different-fixture"
            env[M.TOKENS_ENV] = base64.b64encode(json.dumps(changed).encode()).decode()
            with self.assertRaisesRegex(M.RuntimeInputError, "protected and owner"):
                self.load(original, owner, env)

    def test_owner_provenance_extra_file_and_public_bytes_refuse(self):
        for kind in ("source", "contract", "bundle_name", "content", "mode", "symlink", "extra", "arbitrary_path"):
            with self.subTest(kind=kind), self.fixture() as (original, owner, manifest, body, selected):
                path = owner / M.PRELEASE_BUNDLE_FILE
                if kind == "source": manifest["source_sha"] = "9" * 40
                if kind == "contract": manifest["prelease_runtime_proof"]["qualification_contract"]["peer"]["verdict"] = "SELF_TRUE"
                if kind == "bundle_name": manifest["prelease_runtime_proof"]["bundle_file"] = "other.json"
                if kind == "content": path.write_bytes(body + b" ")
                if kind == "mode": path.chmod(0o644)
                if kind == "symlink":
                    other = owner.parent / "public-fixture"; path.replace(other); path.symlink_to(other)
                if kind == "extra": (owner / "unexpected").write_text("fixture")
                if kind == "arbitrary_path":
                    with self.assertRaisesRegex(M.RuntimeInputError, "fixed owner"):
                        M.input_bundles(original.environment, owner, original.source_sha, M.SQL_SHA,
                                       prelease_runtime_proof_bundle=owner.parent / M.PRELEASE_BUNDLE_FILE)
                    continue
                (owner / "manifest.json").write_bytes(json.dumps(manifest).encode())
                with self.assertRaises((M.RuntimeInputError, OSError)):
                    self.load(original, owner)
                self.assertFalse((owner.parent / "runtime").exists())

    def test_new_public_read_tracks_gid_and_does_not_return_unverified_bytes(self):
        with self.fixture() as (_, owner, _, _, _):
            path = owner / M.PRELEASE_BUNDLE_FILE
            actual = path.stat(); changed = list(actual); changed[5] += 1
            with mock.patch.object(M.os, "fstat", side_effect=[actual, os.stat_result(changed)]):
                with self.assertRaisesRegex(M.RuntimeInputError, "changed"):
                    M.public_owner_bytes(path)

    def test_enabled_parser_must_match_fixed_current_committed_source(self):
        with self.fixture() as (original, owner, _, _, _):
            root = Path(M.__file__).resolve().parents[1]
            parser = root / "scripts/webr_community_prelease_runtime_proof.py"
            M.checked_prelease_parser_source(root, original.source_sha)
            read_bytes = Path.read_bytes
            def changed(path):
                raw = read_bytes(path)
                return raw + b"\n# fixture changed parser\n" if path == parser else raw
            # Git's committed bytes remain genuine; only the working-file read
            # is altered at the fixed parser boundary.
            def git(argv, **kwargs):
                if argv[-2:] == ["rev-parse", "HEAD"]:
                    return (original.source_sha + "\n").encode()
                return read_bytes(root / argv[-1].split(":", 1)[1])
            with mock.patch.object(Path, "read_bytes", autospec=True, side_effect=changed), \
                 mock.patch.object(M.subprocess, "check_output", side_effect=git):
                with self.assertRaisesRegex(M.RuntimeInputError, "parser differs"):
                    self.load(original, owner)
            with mock.patch.object(P, "__file__", str(owner / "arbitrary-parser.py")):
                with self.assertRaisesRegex(M.RuntimeInputError, "parser source path"):
                    self.load(original, owner)

    def test_fixed_workflow_validation_and_materialization_share_source_mode(self):
        with self.fixture() as (original, owner, _, body, selected), \
             mock.patch.dict(os.environ, original.environment), contextlib.redirect_stdout(io.StringIO()):
            argv = ["--owner-input-dir", str(owner), "--source-sha", original.source_sha,
                    "--sql-sha", M.SQL_SHA]
            self.assertEqual(M.main(["--validate-inputs", *argv]), 0)
            runtime = owner.parent / "auto-runtime"
            self.assertEqual(M.main(["--runtime-dir", str(runtime), "--runner-temp", str(owner.parent), *argv]), 0)
            self.assertEqual((runtime / M.PRELEASE_BUNDLE_FILE).read_bytes(), body)
            self.assertEqual(M.prelease_source_selection(), selected)
        with mock.patch.object(M, "workflow_source_selection", side_effect=[None, {"changed": True}]), \
             mock.patch.object(M, "input_bundles", return_value=({}, {}, {})), \
             mock.patch.object(M, "validate_bundles", return_value=({}, [])), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(M.main(["--validate-inputs"]), 2)


class SourceSelectedDispatch(unittest.TestCase):
    @contextlib.contextmanager
    def repository(self, *, tracked_selector=True):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            workflow = root / ".github/workflows/r-project-all.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_bytes((Path(M.__file__).resolve().parents[1] / ".github/workflows/r-project-all.yml").read_bytes())
            materializer = scripts / "materialize_community_publication_runtime.py"
            materializer.write_bytes(Path(M.__file__).read_bytes())
            (scripts / "webr_community_prelease_runtime_proof.py").write_bytes(Path(P.__file__).read_bytes())
            selector = scripts / "webr_community_prelease_selection.py"
            if tracked_selector:
                selector.write_text("PRELEASE_RUNTIME_SOURCE_SELECTION = None\n")
            def git(*arguments):
                return subprocess.check_output(["git", "-C", str(root), *arguments], stderr=subprocess.DEVNULL).decode().strip()
            git("init", "-q")
            git("-c", "user.name=Source fixture", "-c", "user.email=fixture@invalid", "add", ".")
            git("-c", "user.name=Source fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "synthetic base source")
            yield root, materializer, workflow, selector, git

    def test_selector_only_child_has_no_self_digest_and_retains_exact_consumers(self):
        with self.repository() as (root, materializer, workflow, selector, git):
            base = git("rev-parse", "HEAD")
            before = (materializer.read_bytes(), workflow.read_bytes())
            with mock.patch.object(M, "__file__", str(materializer)), mock.patch.dict(os.environ, {"GITHUB_SHA": base}), \
                 mock.patch.object(M, "private_owner_bytes") as owner_read:
                self.assertIsNone(M.workflow_source_selection())
                owner_read.assert_not_called()
            bundle, _, _, _ = public_fixture()
            bundle["source_binding"] = {"data_source_sha": base, "sql_commit_sha": "b" * 40,
                "workflow_sha256": hashlib.sha256(before[1]).hexdigest(),
                "materializer_sha256": hashlib.sha256(before[0]).hexdigest()}
            body = P.canonical_bytes(bundle) + b"\n"
            selected = {"bundle_sha256": hashlib.sha256(body).hexdigest(), "source_binding": bundle["source_binding"],
                        "qualification_contract": bundle["qualification_contract"]}
            selector.write_text("PRELEASE_RUNTIME_SOURCE_SELECTION = " + repr(selected) + "\n")
            git("add", "scripts/webr_community_prelease_selection.py")
            git("-c", "user.name=Source fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "synthetic selector only")
            child = git("rev-parse", "HEAD")
            self.assertNotEqual(base, child)
            self.assertEqual(git("diff", "--name-only", base, child), "scripts/webr_community_prelease_selection.py")
            self.assertEqual((materializer.read_bytes(), workflow.read_bytes()), before)
            git("merge-base", "--is-ancestor", base, child)
            with mock.patch.object(M, "__file__", str(materializer)), mock.patch.dict(os.environ, {"GITHUB_SHA": child}):
                self.assertEqual(M.workflow_source_selection(), selected)
                self.assertEqual(P.validate_runtime_proof_bundle(body, expected_selection=selected,
                    inventory=public_fixture()[3])["reader_inventory_revision"], 7)
                with mock.patch.dict(os.environ, {"GITHUB_SHA": base}):
                    with self.assertRaises(M.RuntimeInputError): M.workflow_source_selection()
                selector.write_text("PRELEASE_RUNTIME_SOURCE_SELECTION = None\n")
                with self.assertRaises(M.RuntimeInputError): M.workflow_source_selection()
                selector.write_text("PRELEASE_RUNTIME_SOURCE_SELECTION = " + repr(selected) + "\n")
                selector.write_text(selector.read_text() + "# synthetic local mutation\n")
                with self.assertRaises(M.RuntimeInputError): M.workflow_source_selection()
            selector.write_text("PRELEASE_RUNTIME_SOURCE_SELECTION = " + repr(selected) + "\n")
            (root / "unrelated-source").write_text("synthetic child mutation\n")
            git("add", "unrelated-source")
            git("-c", "user.name=Source fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "synthetic non-selector change")
            with mock.patch.object(M, "__file__", str(materializer)), mock.patch.dict(os.environ, {"GITHUB_SHA": git("rev-parse", "HEAD")}):
                with self.assertRaisesRegex(M.RuntimeInputError, "only the source selector"): M.workflow_source_selection()

    def test_untracked_or_executable_selector_cannot_supply_trust(self):
        for executable in (False, True, None):
            with self.repository(tracked_selector=False) as (root, materializer, _, selector, git):
                bundle, _, selected, _ = public_fixture()
                marker = root / "must-not-be-created"
                literal = repr(selected)
                selector.write_text("PRELEASE_RUNTIME_SOURCE_SELECTION = " +
                    ("__import__('pathlib').Path(" + repr(str(marker)) + ").touch()" if executable else
                     "None" if executable is None else literal) + "\n")
                with mock.patch.object(M, "__file__", str(materializer)), \
                     mock.patch.dict(os.environ, {"GITHUB_SHA": git("rev-parse", "HEAD")}):
                    with self.assertRaises(M.RuntimeInputError): M.workflow_source_selection()
                self.assertFalse(marker.exists())

    def test_none_preserves_legacy_argv_but_cannot_enter_prelease(self):
        argv = ["publish", "--generation", "synthetic-fixture"]
        class OriginalExec(Exception): pass
        with mock.patch.object(M, "workflow_source_selection", return_value=None), \
             mock.patch.object(M.os, "execv", side_effect=OriginalExec) as execute, \
             mock.patch.object(M, "input_bundles") as owner:
            with self.assertRaises(OriginalExec): M.invoke_source_selected_publisher(argv)
            publisher = Path(M.__file__).resolve().parents[1] / "statground-sql/docker-compose/50005_Clickhouse_Statground/webr_community_generation_publisher.py"
            execute.assert_called_once_with(sys.executable, [sys.executable, str(publisher), *argv])
            with self.assertRaisesRegex(M.RuntimeInputError, "unbound"): M.invoke_prelease_publisher(argv)
            owner.assert_not_called()

    def test_actual_fixed_inline_import_and_committed_none_use_original_cli(self):
        with self.repository() as (root, _, workflow, _, git):
            publisher = root / "statground-sql/docker-compose/50005_Clickhouse_Statground/webr_community_generation_publisher.py"
            publisher.parent.mkdir(parents=True)
            publisher.write_text("import json,sys\nprint(json.dumps(sys.argv[1:]))\n")
            line, = [line.strip().rstrip(" \\") for line in workflow.read_text().splitlines()
                     if "m.invoke_source_selected_publisher(sys.argv[1:])" in line]
            command = shlex.split(line)
            self.assertEqual(command[:2], ["python", "-c"])
            argv = ["publish", "--generation", "synthetic-fixture"]
            environment = {**os.environ, "GITHUB_SHA": git("rev-parse", "HEAD"), "PYTHONDONTWRITEBYTECODE": "1"}
            result = subprocess.run([sys.executable, "-B", "-c", command[2], *argv],
                                    cwd=root, env=environment, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(json.loads(result.stdout), argv)

    @unittest.skipUnless(os.environ.get("STATGROUND_SQL_PUBLISHER_SOURCE"), "original SQL source fixture unavailable")
    def test_fixed_dispatch_reaches_original_SQL_main_and_configuration_before_inert_publish(self):
        # Source modules are the real delivered SQL feature bytes. The copied
        # Git revision and qualification documents below are synthetic fixtures.
        original = Path(os.environ["STATGROUND_SQL_PUBLISHER_SOURCE"]).resolve()
        original_root = original.parents[2]
        self.assertEqual(subprocess.check_output(["git", "-C", str(original_root), "rev-parse", "HEAD"]).decode().strip(), M.SQL_SHA)
        names = ("webr_community_prelease_runtime_proof", "webr_community_prelease_artifacts",
                 "webr_community_generation_publisher")
        sources = {}
        for name in names:
            path = original.parent / (name + ".py")
            sources[name] = path.read_bytes()
            self.assertEqual(sources[name], subprocess.check_output(["git", "-C", str(original_root), "show",
                M.SQL_SHA + ":docker-compose/50005_Clickhouse_Statground/" + name + ".py"]))
        self.assertEqual(hashlib.sha256(sources[names[0]]).hexdigest(),
                         "ab0ae07b1c5fb2afd7a3b128b6ec83d938be921831042cae4271aefd942587db")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            materializer = root / "scripts/materialize_community_publication_runtime.py"
            materializer.parent.mkdir(); materializer.write_bytes(Path(M.__file__).read_bytes())
            sql_root = root / "statground-sql"
            sql_directory = sql_root / "docker-compose/50005_Clickhouse_Statground"
            sql_directory.mkdir(parents=True)
            for name, body in sources.items(): (sql_directory / (name + ".py")).write_bytes(body)
            def git(*arguments):
                return subprocess.check_output(["git", "-C", str(sql_root), *arguments], stderr=subprocess.DEVNULL).decode().strip()
            git("init", "-q"); git("add", ".")
            git("-c", "user.name=Source fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "synthetic SQL source")
            sql_sha = git("rev-parse", "HEAD")
            bundle, _, _, normalized = public_fixture()
            bundle["source_binding"]["sql_commit_sha"] = sql_sha
            body = P.canonical_bytes(bundle) + b"\n"
            selected = {"bundle_sha256": hashlib.sha256(body).hexdigest(), "source_binding": bundle["source_binding"],
                        "qualification_contract": bundle["qualification_contract"]}
            inventory = copy.deepcopy(normalized)
            for row in inventory["readers"]:
                row["url"] = row["inventory_endpoint"].replace("/inventory", "/transition")
            configs = {endpoint: "<config><host>192.0.2.1</host><user>" + M.PUBLISHER_USER +
                       "</user><password>fixture-not-operational</password></config>" for endpoint in M.ENDPOINTS}
            tokens = {row["instance_id"]: "fixture-not-operational" for row in inventory["readers"]}
            runtime = root / "runtime"
            with mock.patch.object(M, "workflow_source_selection", return_value=selected):
                M.materialize(runtime, root, configs, inventory, tokens, prelease_runtime_proof=body)
            args = ["publish", "--generation", "2026-10-22 00:00:00.000001", "--cdn-proof", str(root / "unused-proof"),
                    "--cdn-base-url", "https://cdn.jsdelivr.net/gh/statground/Statground_CDN@" + "a" * 40,
                    "--reader-inventory", str(runtime / "reader-inventory.json"), "--reader-timeout-seconds", "90"]
            for endpoint in M.ENDPOINTS: args.extend(("--endpoint", endpoint + "=" + str(runtime / "endpoints" / (endpoint + ".xml"))))
            observed, selections = [], []
            def execute(code, namespace):
                builtins.exec(code, namespace)
                if namespace.get("__name__") == names[1]:
                    source_selection = namespace["source_selection"]
                    def trace_selection(**kwargs):
                        selections.append(copy.deepcopy(kwargs["expected_source_selection"]))
                        return source_selection(**kwargs)
                    namespace["source_selection"] = trace_selection
                if namespace.get("__name__") == names[2]:
                    def inert_publish(publisher, *arguments):
                        preparation = publisher._prelease_artifacts
                        self.assertIsInstance(preparation, sys.modules[names[1]].PreleasePreparation)
                        self.assertEqual(P.canonical_bytes(bundle), preparation._bundle_bytes)
                        self.assertEqual(preparation.stage_attempts, 0)
                        observed.append(arguments)
                        return {"status": "synthetic_inert_publish_boundary"}
                    namespace["Publisher"].publish = inert_publish
            previous = {name: sys.modules.get(name) for name in names}
            with mock.patch.object(M, "__file__", str(materializer)), mock.patch.object(M, "SQL_SHA", sql_sha), \
                 mock.patch.object(M, "workflow_source_selection", return_value=selected), \
                 mock.patch.object(M, "input_bundles", return_value=(configs, inventory, tokens, body)), \
                 mock.patch.object(M, "exec", side_effect=execute, create=True), \
                 mock.patch.dict(os.environ, {"RUNNER_TEMP": str(root)}), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(M.invoke_source_selected_publisher(args), 0)
                self.assertEqual(len(observed), 1)
                self.assertEqual(selections, [selected, selected])
                self.assertEqual({name: sys.modules.get(name) for name in names}, previous)
                parser_path = sql_directory / (names[0] + ".py")
                parser_path.write_bytes(sources[names[0]] + b"\n# uncommitted fixture mutation\n")
                with self.assertRaisesRegex(M.RuntimeInputError, "SQL source changed"):
                    M.invoke_source_selected_publisher(args)
                self.assertEqual(len(observed), 1)

    @unittest.skipUnless(os.environ.get("STATGROUND_OWNER_CENSUS_SOURCE"), "original owner source fixture unavailable")
    def test_fixed_inline_entry_remains_visible_to_the_original_owner_census(self):
        source = Path(os.environ["STATGROUND_OWNER_CENSUS_SOURCE"]).read_bytes()
        self.assertEqual(len(source), 17763)
        self.assertEqual(hashlib.sha256(source).hexdigest(),
                         "26816c820913c4160e34f5e5c9b11e999fd04de8fb00b56c62cac22114f40790")
        nodes = ast.parse(source).body
        selected = [node for node in nodes if isinstance(node, ast.FunctionDef) and
                    node.name in ("need", "owner_process_state") or isinstance(node, ast.Assign) and
                    any(isinstance(target, ast.Name) and target.id.startswith("OWNER_SCAN_") for target in node.targets)]
        namespace = {"os": os, "time": time, "Path": Path}
        exec(compile(ast.Module(body=selected, type_ignores=[]), "original_owner_census", "exec", dont_inherit=True), namespace)
        workflow = (Path(M.__file__).resolve().parents[1] / ".github/workflows/r-project-all.yml").read_text()
        line, = [line.strip().rstrip(" \\") for line in workflow.splitlines()
                 if "m.invoke_source_selected_publisher(sys.argv[1:])" in line]
        emitted = shlex.split(line)
        self.assertEqual(emitted[:2], ["python", "-c"])
        argv = [b"/usr/bin/python3", *(value.encode() for value in emitted[1:])]
        own_pid = os.getpid()
        namespace["process_record"] = lambda pid, root: (
            (own_pid, 1, 1, 994, "/usr/bin/python3", [b"synthetic-control"])
            if pid == own_pid else (123, 1, 2, 994, "/usr/bin/python3", argv))
        namespace["runner_supervision"] = mock.Mock(side_effect=AssertionError("must detect publisher first"))
        root = types.SimpleNamespace(iterdir=lambda: [types.SimpleNamespace(name="123", stat=lambda: types.SimpleNamespace(st_uid=994))])
        details = []
        self.assertEqual(namespace["owner_process_state"](Path("/synthetic/clickhouse"), (), root, details), "active")
        self.assertEqual(details, ["worker_or_publisher_active"])
        namespace["runner_supervision"].assert_not_called()


if __name__ == "__main__":
    unittest.main()
