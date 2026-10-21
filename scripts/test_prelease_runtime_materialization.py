"""Source-only owner/bundle fixtures; no production qualification is supplied."""
import base64
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
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
            with mock.patch.object(P, "PRELEASE_RUNTIME_SOURCE_SELECTION", selected):
                yield original, owner, manifest, body, selected

    def load(self, original, owner, environment=None):
        return M.input_bundles(original.environment if environment is None else environment, owner,
                               original.source_sha, M.SQL_SHA, prelease_runtime_proof_bundle=owner / M.PRELEASE_BUNDLE_FILE)

    def test_unbound_source_refuses_cli_before_inputs_or_output(self):
        self.assertIsNone(P.PRELEASE_RUNTIME_SOURCE_SELECTION)
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(M, "private_owner_bytes") as owner_read, \
             mock.patch.object(M, "decode_bundle") as env_read, mock.patch.object(M.subprocess, "check_output") as git_read, \
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


if __name__ == "__main__":
    unittest.main()
