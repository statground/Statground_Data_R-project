from __future__ import annotations

import json
import stat
import tempfile
import unittest
from pathlib import Path

from materialize_community_publication_runtime import RuntimeInputError, materialize


class CommunityPublicationRuntimeTest(unittest.TestCase):
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
