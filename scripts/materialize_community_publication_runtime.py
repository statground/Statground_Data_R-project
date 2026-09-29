#!/usr/bin/env python3
"""Materialize owner-only Web-R community publisher runtime inputs."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
import stat
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


ENDPOINTS = ("s1r1", "s1r2", "s2r1", "s2r2")
CONFIGS_ENV = "WEBR_COMMUNITY_CLICKHOUSE_CONFIGS_B64"
INVENTORY_ENV = "WEBR_COMMUNITY_READER_INVENTORY_B64"
TOKENS_ENV = "WEBR_COMMUNITY_READER_TOKENS_B64"
MAX_BUNDLE_BYTES = 256 * 1024
INSTANCE_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class RuntimeInputError(RuntimeError):
    """A secret bundle or destination violates the runtime contract."""


def strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise RuntimeInputError("runtime bundle contains a duplicate JSON key")
        value[key] = item
    return value


def decode_bundle(name: str, raw: str) -> Any:
    if not raw:
        raise RuntimeInputError(f"{name} is required")
    try:
        decoded = base64.b64decode(raw, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise RuntimeInputError(f"{name} is not strict base64") from exc
    if not decoded or len(decoded) > MAX_BUNDLE_BYTES:
        raise RuntimeInputError(f"{name} has an invalid decoded size")
    try:
        return json.loads(decoded.decode("utf-8"), object_pairs_hook=strict_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeInputError(f"{name} is not strict UTF-8 JSON") from exc


def checked_destination(runtime_dir: Path, runner_temp: Path) -> tuple[Path, Path]:
    runtime_dir = runtime_dir.absolute()
    runner_temp = runner_temp.resolve(strict=True)
    if not runner_temp.is_dir() or runtime_dir.parent.resolve(strict=True) != runner_temp:
        raise RuntimeInputError("runtime directory must be a direct child of RUNNER_TEMP")
    if runtime_dir.exists() or runtime_dir.is_symlink():
        raise RuntimeInputError("runtime directory already exists")
    return runtime_dir, runner_temp


def write_owner_only(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)
    os.chmod(path, 0o600)
    metadata = path.stat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise RuntimeInputError("runtime secret file is not owner-only")


def validate_xml_configs(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != set(ENDPOINTS):
        raise RuntimeInputError("ClickHouse config bundle must contain exactly four named endpoints")
    configs: dict[str, str] = {}
    for endpoint in ENDPOINTS:
        xml = value[endpoint]
        if not isinstance(xml, str) or not xml.strip() or len(xml.encode("utf-8")) > 64 * 1024:
            raise RuntimeInputError("ClickHouse endpoint config has an invalid shape")
        lowered = xml.lower()
        if "<!doctype" in lowered or "<!entity" in lowered:
            raise RuntimeInputError("ClickHouse endpoint config contains forbidden XML declarations")
        try:
            root = ET.fromstring(xml)
        except ET.ParseError as exc:
            raise RuntimeInputError("ClickHouse endpoint config is invalid XML") from exc
        if root.tag != "clickhouse":
            raise RuntimeInputError("ClickHouse endpoint config root must be clickhouse")
        configs[endpoint] = xml
    return configs


def materialize(
    runtime_dir: Path,
    runner_temp: Path,
    configs_bundle: Any,
    inventory_bundle: Any,
    tokens_bundle: Any,
) -> dict[str, int | str]:
    runtime_dir, _ = checked_destination(runtime_dir, runner_temp)
    configs = validate_xml_configs(configs_bundle)
    if (
        not isinstance(inventory_bundle, dict)
        or set(inventory_bundle) != {"schema", "reader_inventory_revision", "readers"}
        or inventory_bundle.get("schema") != "web-r.community.reader-inventory.v1"
        or isinstance(inventory_bundle.get("reader_inventory_revision"), bool)
        or not isinstance(inventory_bundle.get("reader_inventory_revision"), int)
        or inventory_bundle["reader_inventory_revision"] <= 0
        or not isinstance(inventory_bundle.get("readers"), list)
        or not inventory_bundle["readers"]
    ):
        raise RuntimeInputError("reader inventory bundle has an unexpected shape")
    if not isinstance(tokens_bundle, dict):
        raise RuntimeInputError("reader token bundle must be an object")

    readers: list[dict[str, Any]] = []
    instance_ids: set[str] = set()
    for raw in inventory_bundle["readers"]:
        expected = {"app_service", "instance_id", "inventory_endpoint", "url"}
        if not isinstance(raw, dict) or set(raw) != expected:
            raise RuntimeInputError("reader inventory entry has an unexpected shape")
        instance = raw.get("instance_id")
        if (
            raw.get("app_service") != "web-r"
            or not isinstance(instance, str)
            or not INSTANCE_RE.fullmatch(instance)
            or instance in instance_ids
        ):
            raise RuntimeInputError("reader inventory contains an invalid or duplicate instance")
        instance_ids.add(instance)
        readers.append(dict(raw))
    if set(tokens_bundle) != instance_ids:
        raise RuntimeInputError("reader token bundle identities differ from the inventory")
    for token in tokens_bundle.values():
        if not isinstance(token, str) or not token or len(token) > 4096 or "\n" in token or "\r" in token:
            raise RuntimeInputError("reader token bundle contains an invalid token")

    old_umask = os.umask(0o077)
    try:
        runtime_dir.mkdir(mode=0o700)
        endpoints_dir = runtime_dir / "endpoints"
        tokens_dir = runtime_dir / "reader-tokens"
        endpoints_dir.mkdir(mode=0o700)
        tokens_dir.mkdir(mode=0o700)
        for endpoint, xml in configs.items():
            write_owner_only(endpoints_dir / f"{endpoint}.xml", xml.encode("utf-8"))
        for reader in readers:
            instance = str(reader["instance_id"])
            token_name = hashlib.sha256(instance.encode("utf-8")).hexdigest() + ".token"
            token_path = tokens_dir / token_name
            write_owner_only(token_path, str(tokens_bundle[instance]).encode("utf-8"))
            reader["token_file"] = str(token_path.absolute())
        inventory = {
            "schema": inventory_bundle["schema"],
            "reader_inventory_revision": inventory_bundle["reader_inventory_revision"],
            "readers": readers,
        }
        write_owner_only(
            runtime_dir / "reader-inventory.json",
            (json.dumps(inventory, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"),
        )
    finally:
        os.umask(old_umask)
    for directory in (runtime_dir, runtime_dir / "endpoints", runtime_dir / "reader-tokens"):
        os.chmod(directory, 0o700)
        if stat.S_IMODE(directory.stat().st_mode) != 0o700:
            raise RuntimeInputError("runtime secret directory is not owner-only")
    return {
        "status": "materialized",
        "endpoint_count": len(configs),
        "reader_count": len(readers),
        "reader_inventory_revision": inventory_bundle["reader_inventory_revision"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--runner-temp", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = materialize(
            args.runtime_dir,
            args.runner_temp,
            decode_bundle(CONFIGS_ENV, os.environ.get(CONFIGS_ENV, "")),
            decode_bundle(INVENTORY_ENV, os.environ.get(INVENTORY_ENV, "")),
            decode_bundle(TOKENS_ENV, os.environ.get(TOKENS_ENV, "")),
        )
    except RuntimeInputError as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
