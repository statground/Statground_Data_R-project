#!/usr/bin/env python3

from __future__ import annotations

import base64
import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from export_r_ecosystem_cdn import derive_key, write_json_atomic  # noqa: E402
from export_r_package_cdn import (  # noqa: E402
    PACKAGE_REGISTRY_SCHEMA,
    build_package_registry_v2,
    build_package_detail_documents_from_spool,
    document_metadata,
    filter_package_detail_sql,
    package_detail_queries,
    package_detail_spool_path,
    package_v2_shard_id,
)
from validate_r_package_registry_v2 import RegistryValidationError, validate_registry_v2  # noqa: E402


def decode_b64url(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def decrypt_document(document: dict[str, object], key: bytes, path: str) -> dict[str, object]:
    payload = AESGCM(key).decrypt(
        decode_b64url(str(document["nonce"])),
        decode_b64url(str(document["ciphertext"])),
        path.encode("utf-8"),
    )
    if document.get("compression") == "gzip":
        payload = gzip.decompress(payload)
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise AssertionError("decrypted document is not an object")
    return decoded


class PackageRegistryV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.key = derive_key("unit-test-package-registry-v2-secret")
        self.packages = {
            f"cran|package{index:02d}": {
                "key": f"cran|package{index:02d}",
                "repository": "CRAN",
                "package_name": f"package{index:02d}",
                "latest_version": "1.0.0",
                "path": f"packages/ko/details/package{index:02d}.json",
            }
            for index in range(40)
        }
        self.detail_paths = {
            f"package{index:02d}": f"packages/ko/details/package{index:02d}.json"
            for index in range(40)
        }
        self.detail_document_metadata = {
            key: document_metadata(
                path,
                {
                    "schema": "encrypted-test",
                    "path": path,
                    "nonce": f"nonce-{key}",
                    "ciphertext": f"ciphertext-{key}",
                },
            )
            for key, path in self.detail_paths.items()
        }
        self.versions = {
            key: [{"repository": "CRAN", "package_version": "1.0.0"}]
            for key in self.packages
        }
        self.news_manifest = {
            f"00000000-0000-7000-8000-{index:012d}": {
                "uuid": f"00000000-0000-7000-8000-{index:012d}",
                "path": f"packages/ko/news/2026/07/{index:02d}.json",
                "title": f"package news {index}",
            }
            for index in range(9)
        }
        self.news_document_metadata = {
            uuid: document_metadata(
                row["path"],
                {
                    "schema": "encrypted-test",
                    "path": row["path"],
                    "nonce": f"nonce-{uuid}",
                    "ciphertext": f"ciphertext-{uuid}",
                },
            )
            for uuid, row in self.news_manifest.items()
        }

    def build(self, generated_at: str = "2026-07-25T00:00:00+00:00"):
        return build_package_registry_v2(
            language="ko",
            generated_at=generated_at,
            shard_count=16,
            key=self.key,
            packages=self.packages,
            detail_paths_by_package=self.detail_paths,
            detail_document_metadata=self.detail_document_metadata,
            versions_by_key=self.versions,
            news_manifest=self.news_manifest,
            news_document_metadata=self.news_document_metadata,
            previous_release_base_url=(
                "https://cdn.jsdelivr.net/gh/statground/"
                "web-r_CDN2_packages@0123456789abcdef0123456789abcdef01234567"
            ),
        )

    def test_registry_counts_digests_and_assignment(self) -> None:
        documents, result = self.build()
        registry_path = str(result["registry_v2"])
        registry = decrypt_document(documents[registry_path], self.key, registry_path)
        self.assertEqual(PACKAGE_REGISTRY_SCHEMA, registry["schema"])
        self.assertTrue(registry["shadow"])
        self.assertEqual(
            {"catalog": 40, "details": 40, "versions": 40, "news": 9},
            registry["totals"],
        )

        for kind, expected_count in registry["totals"].items():
            rows = registry[f"{kind}_shards"]
            self.assertEqual(16, len(rows))
            self.assertEqual(expected_count, sum(int(row["item_count"]) for row in rows))
            seen: set[str] = set()
            for row in rows:
                path = str(row["path"])
                self.assertEqual(document_metadata(path, documents[path])["sha256"], row["manifest_digest"])
                self.assertEqual(document_metadata(path, documents[path])["bytes"], row["total_bytes"])
                shard = decrypt_document(documents[path], self.key, path)
                for item_key in shard["items"]:
                    self.assertNotIn(item_key, seen)
                    seen.add(item_key)
                    self.assertEqual(int(row["range"]["bucket"]), package_v2_shard_id(item_key, 16))
            self.assertEqual(expected_count, len(seen))

    def test_generated_registry_passes_publication_validation(self) -> None:
        documents, _ = self.build()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for rel_path, document in documents.items():
                write_json_atomic(root / rel_path, document)
            result = validate_registry_v2(root, self.key)
        self.assertEqual(64, result["shards"])
        self.assertEqual(
            {"catalog": 40, "details": 40, "versions": 40, "news": 9},
            result["totals"],
        )

    def test_publication_validation_rejects_digest_corruption(self) -> None:
        documents, _ = self.build()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for rel_path, document in documents.items():
                write_json_atomic(root / rel_path, document)
            path = root / "packages/ko/v2/catalog/00.json"
            path.write_bytes(path.read_bytes() + b" ")
            with self.assertRaises(RegistryValidationError):
                validate_registry_v2(root, self.key)

    def test_shards_and_release_id_are_stable_when_only_generated_at_changes(self) -> None:
        first_documents, first_result = self.build("2026-07-25T00:00:00+00:00")
        second_documents, second_result = self.build("2026-07-26T00:00:00+00:00")
        registry_path = str(first_result["registry_v2"])
        self.assertEqual(first_result["registry_v2_release_id"], second_result["registry_v2_release_id"])
        self.assertNotEqual(first_documents[registry_path], second_documents[registry_path])
        for path in first_documents:
            if path != registry_path:
                self.assertEqual(first_documents[path], second_documents[path], path)

    def test_aad_path_mismatch_is_rejected(self) -> None:
        documents, result = self.build()
        registry_path = str(result["registry_v2"])
        with self.assertRaises(Exception):
            decrypt_document(documents[registry_path], self.key, registry_path + ".wrong")

    def test_invalid_shard_count_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            package_v2_shard_id("cran|r-base", 8)

    def test_small_detail_export_uses_bounded_spool_shards(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_root = Path(temp_dir)
            package_key = "package00"
            shard_index = package_v2_shard_id(package_key, 16)
            for group_name, (_sql, _query_name, _first_only) in package_detail_queries().items():
                path = package_detail_spool_path(spool_root, group_name, shard_index)
                path.parent.mkdir(parents=True, exist_ok=True)
                row = {"package_key": package_key}
                if group_name == "checks":
                    row.update({"status": "OK", "platform": "linux"})
                path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            metadata = build_package_detail_documents_from_spool(
                spool_root=spool_root,
                shard_count=16,
                details_by_package={
                    package_key: {
                        "source_profiles": [
                            {
                                "repository": "CRAN",
                                "package_name": "package00",
                                "latest_version": "1.0.0",
                            }
                        ]
                    }
                },
                detail_paths_by_package={package_key: "packages/ko/details/package00.json"},
                versions_by_package={package_key: [{"package_version": "1.0.0"}]},
                key=self.key,
                language="ko",
                cdn_root=spool_root / "unused-cdn",
                dry_run=True,
            )
            self.assertEqual({package_key}, set(metadata))
            self.assertGreater(metadata[package_key]["bytes"], 0)

    def test_small_package_filter_wraps_detail_query(self) -> None:
        query = "SELECT package_key, value FROM source\nFORMAT JSONEachRow\nSETTINGS max_threads = 1"
        filtered = filter_package_detail_sql(query, {"package-a", "package-b"})
        self.assertIn("WHERE package_key IN ('package-a','package-b')", filtered)
        self.assertIn("FORMAT JSONEachRow", filtered)
        self.assertIn("SETTINGS max_threads = 1", filtered)


if __name__ == "__main__":
    unittest.main()
