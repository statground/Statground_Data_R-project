import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import verify_community_generation_cdn as verifier
from export_community_cdn import (
    GENERATION_MANIFEST_SCHEMA,
    GENERATION_PROOF_SCHEMA,
    encrypt_document,
    go_canonical_json_bytes,
)


class FakeResponse:
    def __init__(self, url: str, body: bytes, commit: str) -> None:
        self.url = url
        self.body = body
        self.status = 200
        self.headers = {"x-jsd-version": commit, "x-jsd-version-type": "commit"}

    def __enter__(self):  # noqa: ANN001
        return self

    def __exit__(self, *args):  # noqa: ANN002, ANN001
        return False

    def read(self, limit: int) -> bytes:
        return self.body[:limit]

    def geturl(self) -> str:
        return self.url


class FakeOpener:
    def __init__(self, bodies: dict[str, bytes], commit: str) -> None:
        self.bodies = bodies
        self.commit = commit

    def open(self, request, timeout: int):  # noqa: ANN001
        url = request.full_url
        return FakeResponse(url, self.bodies[url], self.commit)


class CommunityGenerationCDNVerifyTest(unittest.TestCase):
    def test_encrypted_v2_index_and_proof_require_exact_immutable_remote_bytes(self) -> None:
        generation = "2026-09-27 00:00:00.123456"
        uuid = "00000000-0000-0000-0000-000000000001"
        items = {uuid: {"uuid": uuid, "title": "<notice>&", "path": "community/ko/rcommunity/x.json"}}
        proof = {
            "schema": GENERATION_PROOF_SCHEMA,
            "generation": generation,
            "complete": True,
            "item_count": 1,
            "identity_hash": hashlib.sha256(uuid.encode()).hexdigest(),
            "content_hash": hashlib.sha256(go_canonical_json_bytes(items)).hexdigest(),
            "withdrawal_revision": 1,
            "account_authority_revision": 2,
            "category_authority_revision": 3,
        }
        manifest = {
            "schema": GENERATION_MANIFEST_SCHEMA,
            "language": "ko",
            **{key: value for key, value in proof.items() if key != "schema"},
            "generated_at": "2026-09-27T00:00:00+00:00",
            "items": items,
        }
        key = b"x" * 32
        path = "community/ko/index.json"
        encrypted = encrypt_document(manifest, key, path, "ko", "")
        nonce = base64.urlsafe_b64decode(encrypted["nonce"] + "==")
        ciphertext = base64.urlsafe_b64decode(encrypted["ciphertext"] + "==")
        self.assertEqual(
            json.loads(AESGCM(key).decrypt(nonce, ciphertext, path.encode())),
            manifest,
        )

        proof_bytes = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        index_bytes = json.dumps(encrypted, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        commit = "a" * 40
        base = f"https://cdn.jsdelivr.net/gh/statground/web-r_CDN2_community@{commit}"
        proof_url = base + "/community/ko/generation-proof.json"
        index_url = base + "/" + path
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            proof_path = root / "generation-proof.json"
            index_path = root / "index.json"
            proof_path.write_bytes(proof_bytes)
            index_path.write_bytes(index_bytes)
            with mock.patch.object(
                verifier.urllib.request,
                "build_opener",
                return_value=FakeOpener({proof_url: proof_bytes, index_url: index_bytes}, commit),
            ):
                result = verifier.verify(base, proof_path, [(path, index_path)], 1, 0)
            self.assertEqual(result["status"], "verified")
            self.assertEqual(result["asset_count"], 1)

            corrupted_index = index_bytes.replace(b'"ciphertext":"', b'"ciphertext":"X', 1)
            with mock.patch.object(
                verifier.urllib.request,
                "build_opener",
                return_value=FakeOpener({proof_url: proof_bytes, index_url: corrupted_index}, commit),
            ):
                with self.assertRaisesRegex(ValueError, "immutable CDN asset differs"):
                    verifier.verify(base, proof_path, [(path, index_path)], 1, 0)

            corrupted_proof = json.dumps(
                {**proof, "content_hash": "c" * 64}, sort_keys=True, separators=(",", ":")
            ).encode() + b"\n"
            with mock.patch.object(
                verifier.urllib.request,
                "build_opener",
                return_value=FakeOpener({proof_url: corrupted_proof, index_url: index_bytes}, commit),
            ):
                with self.assertRaisesRegex(ValueError, "immutable community proof verification failed"):
                    verifier.verify(base, proof_path, [(path, index_path)], 1, 0)


if __name__ == "__main__":
    unittest.main()
