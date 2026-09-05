#!/usr/bin/env python3
"""Validate an encrypted Web-R package registry v2 before publication."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import re
from pathlib import Path
from typing import Any
from workspace_paths import workspace_repo

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from export_r_ecosystem_cdn import content_secret, derive_key, load_env
from export_r_package_cdn import PACKAGE_REGISTRY_SCHEMA, package_v2_shard_id

EXACT_COMMIT_BASE = re.compile(
    r"^https://cdn\.jsdelivr\.net/gh/statground/web-r_CDN2_packages@[0-9a-f]{40}$"
)
KINDS = ("catalog", "details", "versions", "news")


class RegistryValidationError(ValueError):
    pass


def decode_b64url(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def load_encrypted_document(root: Path, rel_path: str, key: bytes) -> tuple[dict[str, Any], bytes]:
    path = root / rel_path
    try:
        body = path.read_bytes()
        document = json.loads(body)
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryValidationError(f"cannot read encrypted document: {rel_path}") from exc
    if not isinstance(document, dict) or document.get("path") != rel_path:
        raise RegistryValidationError(f"encrypted path/AAD mismatch: {rel_path}")
    try:
        payload = AESGCM(key).decrypt(
            decode_b64url(str(document["nonce"])),
            decode_b64url(str(document["ciphertext"])),
            rel_path.encode("utf-8"),
        )
        if document.get("compression") == "gzip":
            payload = gzip.decompress(payload)
        decoded = json.loads(payload)
    except (KeyError, ValueError, OSError, json.JSONDecodeError) as exc:
        raise RegistryValidationError(f"decrypt/AAD failure: {rel_path}") from exc
    if not isinstance(decoded, dict):
        raise RegistryValidationError(f"decrypted document is not an object: {rel_path}")
    return decoded, body


def validate_registry_v2(root: Path, key: bytes, language: str = "ko") -> dict[str, Any]:
    registry_path = f"packages/{language}/v2/registry.json"
    registry, registry_body = load_encrypted_document(root, registry_path, key)
    if registry.get("schema") != PACKAGE_REGISTRY_SCHEMA or registry.get("shadow") is not True:
        raise RegistryValidationError("registry schema or shadow marker is invalid")
    shard_count = int(registry.get("shard_count") or 0)
    if shard_count not in (16, 32):
        raise RegistryValidationError("registry shard_count must be 16 or 32")
    previous_base = str((registry.get("previous_release") or {}).get("immutable_base_url") or "")
    if not EXACT_COMMIT_BASE.fullmatch(previous_base):
        raise RegistryValidationError("previous release must use an exact immutable jsDelivr commit")

    totals = registry.get("totals")
    if not isinstance(totals, dict):
        raise RegistryValidationError("registry totals are missing")
    release_seed: dict[str, list[dict[str, Any]]] = {}
    validated_totals: dict[str, int] = {}
    total_bytes = len(registry_body)

    for kind in KINDS:
        rows = registry.get(f"{kind}_shards")
        if not isinstance(rows, list) or len(rows) != shard_count:
            raise RegistryValidationError(f"{kind} shard list does not match shard_count")
        shard_ids: set[str] = set()
        item_keys: set[str] = set()
        item_count = 0
        release_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise RegistryValidationError(f"{kind} shard metadata is not an object")
            shard_id = str(row.get("shard_id") or "")
            rel_path = str(row.get("path") or "")
            expected_path = f"packages/{language}/v2/{kind}/{shard_id}.json"
            if shard_id in shard_ids or rel_path != expected_path:
                raise RegistryValidationError(f"{kind} shard id/path is invalid: {shard_id}")
            shard_ids.add(shard_id)
            shard, body = load_encrypted_document(root, rel_path, key)
            if shard.get("kind") != kind or shard.get("shard_id") != shard_id:
                raise RegistryValidationError(f"{kind} shard identity mismatch: {shard_id}")
            if int(shard.get("shard_count") or 0) != shard_count:
                raise RegistryValidationError(f"{kind} shard count mismatch: {shard_id}")
            items = shard.get("items")
            if not isinstance(items, dict):
                raise RegistryValidationError(f"{kind} shard items are invalid: {shard_id}")
            expected_bucket = int(shard_id)
            for item_key in items:
                if item_key in item_keys:
                    raise RegistryValidationError(f"{kind} duplicate item: {item_key}")
                if package_v2_shard_id(item_key, shard_count) != expected_bucket:
                    raise RegistryValidationError(f"{kind} item assigned to wrong shard: {item_key}")
                item_keys.add(item_key)
            digest = hashlib.sha256(body).hexdigest()
            if int(row.get("item_count") or 0) != len(items):
                raise RegistryValidationError(f"{kind} item count mismatch: {shard_id}")
            if int(row.get("total_bytes") or 0) != len(body) or row.get("manifest_digest") != digest:
                raise RegistryValidationError(f"{kind} byte/digest mismatch: {shard_id}")
            item_count += len(items)
            total_bytes += len(body)
            release_rows.append(
                {
                    "shard_id": shard_id,
                    "item_count": len(items),
                    "total_bytes": len(body),
                    "manifest_digest": digest,
                }
            )
        if item_count != int(totals.get(kind) or 0):
            raise RegistryValidationError(f"{kind} registry total mismatch")
        validated_totals[kind] = item_count
        release_seed[kind] = release_rows

    release_id = hashlib.sha256(
        json.dumps(release_seed, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    if registry.get("release_id") != release_id:
        raise RegistryValidationError("registry release_id mismatch")
    return {
        "registry": registry_path,
        "release_id": release_id,
        "shards": shard_count * len(KINDS),
        "totals": validated_totals,
        "bytes": total_bytes,
        "sha256": hashlib.sha256(registry_body).hexdigest(),
        "shadow": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="web_r_go .env path")
    parser.add_argument("--cdn-root", default=str(workspace_repo("web-r_CDN2_packages")), help="package CDN checkout")
    parser.add_argument("--language", default="ko", help="content language")
    args = parser.parse_args()
    root = Path(args.cdn_root).resolve()
    env = load_env(Path(args.env).resolve())
    result = validate_registry_v2(root, derive_key(content_secret(env)), args.language)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
