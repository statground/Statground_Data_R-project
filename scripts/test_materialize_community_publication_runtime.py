from __future__ import annotations

import json
import base64
import contextlib
import hashlib
import io
import os
import stat
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

from materialize_community_publication_runtime import RuntimeInputError, materialize
import materialize_community_publication_runtime as runtime_inputs


class CommunityPublicationRuntimeTest(unittest.TestCase):
    @staticmethod
    def reader_inventory() -> dict:
        return {
            "schema": "web-r.community.reader-inventory.v1",
            "reader_inventory_revision": 17,
            "readers": [{
                "app_service": "web-r",
                "instance_id": "reader-1",
                "inventory_endpoint": "https://reader.example/inventory",
                "url": "https://reader.example/internal/community-publication/transition",
            }],
        }

    def test_materializes_owner_config_and_current_clickhouse_roots_byte_exact(self) -> None:
        for root_name in ("config", "clickhouse"):
            with self.subTest(root=root_name), tempfile.TemporaryDirectory() as raw_temp:
                # Mirrors _client_config_bytes in the pinned SQL publisher owner.
                configs = {
                    endpoint: (
                        f"<{root_name}><host>192.0.2.{index}</host><port>9000</port>"
                        "<user>webr_community_generation_publisher</user><password>"
                        + xml_escape(f"fixture-<&>한글-{endpoint}")
                        + f"</password><history_file>/dev/null</history_file></{root_name}>\n"
                    )
                    for index, endpoint in enumerate(("s1r1", "s1r2", "s2r1", "s2r2"), 1)
                }
                runner_temp = Path(raw_temp)
                runtime = runner_temp / "runtime"
                result = materialize(
                    runtime, runner_temp, configs, self.reader_inventory(), {"reader-1": "fixture-token"}
                )
                self.assertEqual(result["endpoint_count"], 4)
                self.assertNotIn("fixture-", json.dumps(result))
                for directory in (runtime, runtime / "endpoints", runtime / "reader-tokens"):
                    self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
                for endpoint, xml in configs.items():
                    path = runtime / "endpoints" / f"{endpoint}.xml"
                    self.assertEqual(path.read_bytes(), xml.encode("utf-8"))
                    self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                    self.assertEqual(path.stat().st_uid, os.geteuid())

    def test_invalid_xml_contracts_fail_before_creating_runtime(self) -> None:
        valid = {endpoint: "<config><host>192.0.2.1</host></config>"
                 for endpoint in ("s1r1", "s1r2", "s2r1", "s2r2")}
        cases = [
            ("invalid-root", "<server><host>192.0.2.1</host></server>", "root must"),
            ("namespace-root", '<config xmlns="urn:fixture"/>', "root must"),
            ("doctype-owner", '<!DOCTYPE config><config/>', "forbidden XML declarations"),
            ("doctype-current", '<!DOCTYPE clickhouse><clickhouse/>', "forbidden XML declarations"),
            ("entity", '<!ENTITY fixture "value"><config/>', "forbidden XML declarations"),
            ("oversize", "<config>" + "x" * (64 * 1024) + "</config>", "invalid shape"),
        ]
        bundles = [(name, {**valid, "s1r1": xml}, reason) for name, xml, reason in cases]
        bundles.extend([
            ("missing-endpoint", {k: v for k, v in valid.items() if k != "s2r2"}, "exactly four"),
            ("extra-endpoint", {**valid, "s3r1": "<config/>"}, "exactly four"),
        ])
        for name, configs, reason in bundles:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as raw_temp:
                runner_temp = Path(raw_temp)
                runtime = runner_temp / "runtime"
                with self.assertRaisesRegex(RuntimeInputError, reason):
                    materialize(
                        runtime, runner_temp, configs, self.reader_inventory(), {"reader-1": "fixture-token"}
                    )
                self.assertFalse(runtime.exists())

    def test_materializes_exact_owner_only_files_without_secret_content_in_result(self) -> None:
        configs = {
            endpoint: f"<clickhouse><host>{endpoint}</host><password>secret-{endpoint}</password></clickhouse>"
            for endpoint in ("s1r1", "s1r2", "s2r1", "s2r2")
        }
        inventory = {
            "schema": "web-r.community.reader-inventory.v1",
            "reader_inventory_revision": 17,
            "readers": [
                {
                    "app_service": "web-r",
                    "instance_id": "web-r-1",
                    "inventory_endpoint": "https://reader.example/internal/community-publication/inventory",
                    "url": "https://reader.example/internal/community-publication/transition",
                }
            ],
        }
        tokens = {"web-r-1": "bearer-secret"}
        with tempfile.TemporaryDirectory() as raw_temp:
            runner_temp = Path(raw_temp)
            runtime = runner_temp / "runtime"

            result = materialize(runtime, runner_temp, configs, inventory, tokens)

            self.assertEqual(result["endpoint_count"], 4)
            self.assertEqual(result["reader_count"], 1)
            self.assertNotIn("secret", json.dumps(result))
            for directory in (runtime, runtime / "endpoints", runtime / "reader-tokens"):
                self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
            files = [path for path in runtime.rglob("*") if path.is_file()]
            self.assertEqual(len(files), 6)
            for path in files:
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            rendered = json.loads((runtime / "reader-inventory.json").read_text(encoding="utf-8"))
            token_path = Path(rendered["readers"][0]["token_file"])
            self.assertTrue(token_path.is_absolute())
            self.assertEqual(token_path.read_text(encoding="utf-8"), "bearer-secret")

    def test_rejects_reader_token_identity_drift_before_creating_runtime(self) -> None:
        configs = {
            endpoint: "<clickhouse><host>host</host></clickhouse>"
            for endpoint in ("s1r1", "s1r2", "s2r1", "s2r2")
        }
        inventory = {
            "schema": "web-r.community.reader-inventory.v1",
            "reader_inventory_revision": 1,
            "readers": [
                {
                    "app_service": "web-r",
                    "instance_id": "reader-1",
                    "inventory_endpoint": "https://reader.example/inventory",
                    "url": "https://reader.example/internal/community-publication/transition",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as raw_temp:
            runner_temp = Path(raw_temp)
            runtime = runner_temp / "runtime"
            with self.assertRaisesRegex(RuntimeInputError, "identities differ"):
                materialize(runtime, runner_temp, configs, inventory, {"reader-2": "token"})
            self.assertFalse(runtime.exists())


class OwnerPublicationInputsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source_sha = "1" * 40
        self.environment = {"RUNNER_NAME": runtime_inputs.RUNNER_NAME, "RUNNER_ENVIRONMENT": "self-hosted",
                            "GITHUB_REPOSITORY": "statground/Statground_Data_R-project", "GITHUB_SHA": self.source_sha}
        self.configs = {endpoint: "<config><host>192.0.2.1</host><user>" + runtime_inputs.PUBLISHER_USER +
                        "</user><password>dummy-not-operational</password></config>" for endpoint in runtime_inputs.ENDPOINTS}
        self.inventory = CommunityPublicationRuntimeTest.reader_inventory()
        self.tokens = {"reader-1": "dummy-not-operational"}

    @contextlib.contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as temp:
            owner = Path(temp) / "owner"; owner.mkdir(mode=0o700)
            bodies = dict(zip(runtime_inputs.OWNER_FILES, [self.configs, self.inventory, self.tokens]))
            for name, value in bodies.items():
                path = owner / name; path.write_bytes(json.dumps(value).encode()); path.chmod(0o600)
            source_root = Path(runtime_inputs.__file__).resolve().parents[1]
            manifest = {"schema": "web-r.community.owner-inputs.v1", "source_sha": self.source_sha,
                "sql_commit_sha": runtime_inputs.SQL_SHA, "runner_name": runtime_inputs.RUNNER_NAME,
                "reader_inventory_revision": 17,
                "materializer_sha256": hashlib.sha256(Path(runtime_inputs.__file__).read_bytes()).hexdigest(),
                "workflow_sha256": hashlib.sha256((source_root / ".github/workflows/r-project-all.yml").read_bytes()).hexdigest(),
                "files": {name: hashlib.sha256((owner / name).read_bytes()).hexdigest() for name in bodies}}
            path = owner / "manifest.json";path.write_bytes(json.dumps(manifest).encode());path.chmod(0o600)
            original_lstat = Path.lstat
            def fixture_lstat(path):
                metadata = original_lstat(path)
                # Only emulate the production /var/lib ancestor for this dummy /tmp fixture.
                if path == Path("/tmp"):
                    fields = list(metadata); fields[0] &= ~0o022; return os.stat_result(fields)
                return metadata
            def git_output(argv, **kwargs):
                if argv[-2:] == ["rev-parse", "HEAD"]: return (self.source_sha + "\n").encode()
                return (source_root / argv[-1].split(":", 1)[1]).read_bytes()
            with mock.patch.object(runtime_inputs, "OWNER_INPUT_DIR", owner), \
                 mock.patch.object(runtime_inputs.subprocess, "check_output", side_effect=git_output), \
                 mock.patch.object(runtime_inputs.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)), \
                 mock.patch.object(Path, "lstat", autospec=True, side_effect=fixture_lstat):
                yield owner, manifest

    def load(self, owner):
        return runtime_inputs.input_bundles(self.environment, owner, self.source_sha, runtime_inputs.SQL_SHA)

    def test_all_protected_inputs_do_not_read_local_files_or_source(self):
        values = [self.configs, self.inventory, self.tokens]
        environment = dict(self.environment, **{name: base64.b64encode(json.dumps(value).encode()).decode()
                           for name, value in zip((runtime_inputs.CONFIGS_ENV, runtime_inputs.INVENTORY_ENV, runtime_inputs.TOKENS_ENV), values)})
        with mock.patch.object(runtime_inputs, "owner_input_bundles", side_effect=AssertionError("must not read owner input")):
            self.assertEqual(runtime_inputs.input_bundles(environment, runtime_inputs.OWNER_INPUT_DIR, "", ""), tuple(values))

    def test_partial_protected_inputs_never_select_local_fallback(self):
        names = (runtime_inputs.CONFIGS_ENV, runtime_inputs.INVENTORY_ENV, runtime_inputs.TOKENS_ENV)
        for mask in range(1, 7):
            environment = {name: "dummy" for index, name in enumerate(names) if mask & (1 << index)}
            with mock.patch.object(runtime_inputs, "owner_input_bundles") as local:
                with self.assertRaisesRegex(RuntimeInputError, "all present or all absent"):
                    runtime_inputs.input_bundles(environment, runtime_inputs.OWNER_INPUT_DIR, self.source_sha, runtime_inputs.SQL_SHA)
                local.assert_not_called()
        with self.assertRaisesRegex(RuntimeInputError, "required"):
            runtime_inputs.input_bundles({}, None, "", "")

    def test_owner_input_materializes_existing_contract_without_mutating_persistent_files(self):
        with self.fixture() as (owner, _):
            before = {path.name: path.read_bytes() for path in owner.iterdir()}
            bundles = self.load(owner)
            runner_temp = owner.parent; runtime = runner_temp / "runtime"
            result = materialize(runtime, runner_temp, *bundles)
            self.assertEqual(result["reader_count"], 1)
            self.assertNotIn("dummy", json.dumps(result));self.assertNotIn("sha", json.dumps(result))
            self.assertEqual({path.name: path.read_bytes() for path in owner.iterdir()}, before)
            for endpoint in runtime_inputs.ENDPOINTS:
                self.assertEqual((runtime / "endpoints" / (endpoint + ".xml")).read_text(), self.configs[endpoint])
            self.assertTrue(all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in runtime.rglob("*") if path.is_file()))

    def test_cli_validation_has_no_runtime_creation_or_private_output(self):
        with self.fixture() as (owner, _), mock.patch.dict(os.environ, self.environment, clear=True), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            result = runtime_inputs.main(["--validate-inputs", "--owner-input-dir", str(owner),
                                          "--source-sha", self.source_sha, "--sql-sha", runtime_inputs.SQL_SHA])
            self.assertEqual(result, 0)
            self.assertEqual(json.loads(output.getvalue()), {"status": "validated", "endpoint_count": 4, "reader_count": 1})
            self.assertNotIn("dummy", output.getvalue()); self.assertFalse((owner.parent / "runtime").exists())

    def test_local_context_and_source_mismatch_reject_before_private_reads(self):
        for key, value in (("RUNNER_NAME", "other"), ("RUNNER_ENVIRONMENT", "github-hosted"),
                           ("GITHUB_REPOSITORY", "other/repo"), ("GITHUB_SHA", "2" * 40)):
            with self.fixture() as (owner, _), mock.patch.dict(self.environment, {key: value}), \
                 mock.patch.object(runtime_inputs, "private_owner_bytes") as reader:
                with self.assertRaises(RuntimeInputError): self.load(owner)
                reader.assert_not_called()
        with self.fixture() as (owner, _), mock.patch.object(runtime_inputs.os, "geteuid", return_value=0):
            with self.assertRaises(RuntimeInputError): self.load(owner)
        with self.fixture() as (owner, _), mock.patch.object(runtime_inputs.subprocess, "check_output", return_value=b"changed"):
            with self.assertRaisesRegex(RuntimeInputError, "consumer differs"): self.load(owner)

    def test_manifest_binding_and_revision_drift_do_not_create_runtime(self):
        changes = {"source_sha": "not-a-sha", "sql_commit_sha": "2" * 40, "workflow_sha256": "0" * 64,
                   "materializer_sha256": "0" * 64, "runner_name": "other", "reader_inventory_revision": 18,
                   "extra": True}
        for key, value in changes.items():
            with self.fixture() as (owner, manifest):
                manifest[key] = value; (owner / "manifest.json").write_text(json.dumps(manifest))
                with self.assertRaises(RuntimeInputError): self.load(owner)
                self.assertFalse((owner.parent / "runtime").exists())

    def test_file_hash_schema_principal_and_identity_drift_reject(self):
        for change in ("bytes", "world-readable", "hardlink", "symlink", "oversize", "extra-entry", "principal", "tokens"):
            with self.fixture() as (owner, manifest):
                path = owner / "reader-tokens.json"
                if change == "bytes": path.write_text('{"reader-1":"changed"}')
                elif change == "world-readable": path.chmod(0o644)
                elif change == "hardlink": os.link(path, owner.parent / "link")
                elif change == "symlink": path.rename(owner.parent / "outside");path.symlink_to(owner.parent / "outside")
                elif change == "oversize": path.write_bytes(b"x" * (runtime_inputs.MAX_BUNDLE_BYTES + 1))
                elif change == "extra-entry": (owner / "extra").write_text("dummy")
                else:
                    path = owner / ("clickhouse-configs.json" if change == "principal" else "reader-tokens.json")
                    body = {key: value.replace(runtime_inputs.PUBLISHER_USER, "wrong-principal") for key, value in self.configs.items()} if change == "principal" else {"wrong-reader": "dummy"}
                    path.write_text(json.dumps(body));manifest["files"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
                    (owner / "manifest.json").write_text(json.dumps(manifest))
                with self.assertRaises(RuntimeInputError): self.load(owner)

    def test_duplicate_json_and_untrusted_parent_remain_closed(self):
        with self.fixture() as (owner, _):
            (owner / "manifest.json").write_text('{"schema":1,"schema":2}')
            with self.assertRaises(RuntimeInputError): self.load(owner)
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeInputError, "parent"):
                runtime_inputs.private_owner_directory(Path(temp))

    def test_descendant_source_accepts_only_unchanged_consumer_blobs(self):
        original_output = subprocess.check_output
        original_run = subprocess.run
        with self.fixture() as (owner, manifest):
            source = owner.parent / "source";source.mkdir()
            paths = ("scripts/materialize_community_publication_runtime.py", ".github/workflows/r-project-all.yml")
            actual_root = Path(runtime_inputs.__file__).resolve().parents[1]
            for name in paths:
                path = source / name;path.parent.mkdir(parents=True, exist_ok=True);path.write_bytes((actual_root / name).read_bytes())
            def git(*arguments):
                return subprocess.check_call(["git", "-C", str(source), *arguments], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            def native_output(*arguments):
                with subprocess.Popen(["git", "-C", str(source), *arguments], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as process:
                    output, _ = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 0)
                    return output.decode().strip()
            def commit():
                git("add", "--all");git("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture")
                # check_output uses subprocess.run, which the dummy fixture mocks.
                return native_output("rev-parse", "HEAD")
            git("init", "-q");reviewed = commit()
            (source / "unrelated.txt").write_text("unrelated public fixture")
            descendant = commit();manifest["source_sha"] = reviewed
            (owner / "manifest.json").write_text(json.dumps(manifest))
            self.source_sha = descendant;self.environment["GITHUB_SHA"] = descendant
            with mock.patch.object(runtime_inputs, "__file__", str(source / paths[0])), \
                 mock.patch.object(runtime_inputs.subprocess, "check_output", side_effect=original_output), \
                 mock.patch.object(runtime_inputs.subprocess, "run", side_effect=original_run):
                self.assertEqual(self.load(owner)[1]["reader_inventory_revision"], 17)
                tree = native_output("rev-parse", reviewed + "^{tree}")
                disconnected = native_output("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid",
                                             "commit-tree", tree, "-m", "disconnected fixture")
                manifest["source_sha"] = disconnected;(owner / "manifest.json").write_text(json.dumps(manifest))
                with self.assertRaisesRegex(RuntimeInputError, "not an ancestor"):self.load(owner)
                manifest["source_sha"] = reviewed
                (source / paths[1]).write_bytes((source / paths[1]).read_bytes() + b"\n# changed consumer fixture\n")
                current = commit();self.source_sha = current;self.environment["GITHUB_SHA"] = current
                manifest["workflow_sha256"] = hashlib.sha256((source / paths[1]).read_bytes()).hexdigest()
                (owner / "manifest.json").write_text(json.dumps(manifest))
                with self.assertRaisesRegex(RuntimeInputError, "consumers changed"):self.load(owner)

    def test_fd_owner_and_changed_read_are_rejected_without_private_content(self):
        with self.fixture() as (owner, _):
            path = owner / "reader-tokens.json"; metadata = path.stat()
            wrong = list(metadata);wrong[4] = metadata.st_uid + 1
            with mock.patch.object(runtime_inputs.os, "fstat", return_value=os.stat_result(wrong)):
                with self.assertRaisesRegex(RuntimeInputError, "identity"):
                    runtime_inputs.private_owner_bytes(path, runtime_inputs.MAX_BUNDLE_BYTES)
            changed = list(metadata);changed[6] += 1
            with mock.patch.object(runtime_inputs.os, "fstat", side_effect=[metadata, os.stat_result(changed)]):
                with self.assertRaisesRegex(RuntimeInputError, "changed"):
                    runtime_inputs.private_owner_bytes(path, runtime_inputs.MAX_BUNDLE_BYTES)


if __name__ == "__main__":
    unittest.main()
