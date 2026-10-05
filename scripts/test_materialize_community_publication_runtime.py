from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

from materialize_community_publication_runtime import RuntimeInputError, materialize


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


if __name__ == "__main__":
    unittest.main()
