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
import subprocess
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
OWNER_INPUT_DIR = Path("/var/lib/webr-community-publication-inputs")
RUNNER_NAME = "webr-community-publisher-local"
PUBLISHER_USER = "webr_community_generation_publisher"
SQL_SHA = "a23a9fe617c9fdbff07911404254c0145018e149"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA64 = re.compile(r"^[0-9a-f]{64}$")
OWNER_FILES = ("clickhouse-configs.json", "reader-inventory.json", "reader-tokens.json")


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
        # The versioned SQL owner emits <config>; existing inputs use <clickhouse>.
        if root.tag not in ("config", "clickhouse"):
            raise RuntimeInputError("ClickHouse endpoint config root must be config or clickhouse")
        configs[endpoint] = xml
    return configs


def validate_bundles(configs_bundle: Any, inventory_bundle: Any, tokens_bundle: Any) -> tuple[dict[str, str], list[dict[str, Any]]]:
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
    return configs, readers


def private_owner_directory(path: Path) -> os.stat_result:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise RuntimeInputError("owner input directory identity or mode differs")
    for parent in path.parents:
        entry = parent.lstat()
        if not stat.S_ISDIR(entry.st_mode) or entry.st_uid not in (0, os.geteuid()) or stat.S_IMODE(entry.st_mode) & 0o022:
            raise RuntimeInputError("owner input parent is not trusted")
    return metadata


def private_owner_bytes(path: Path, limit: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid() or before.st_nlink != 1 or
                stat.S_IMODE(before.st_mode) != 0o600 or not 0 < before.st_size <= limit):
            raise RuntimeInputError("owner input file identity, mode or size differs")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            body = stream.read(limit + 1)
        after = os.fstat(descriptor)
        current = path.lstat()
        identity = lambda entry: (entry.st_dev, entry.st_ino, entry.st_uid, entry.st_mode, entry.st_nlink,
                                  entry.st_size, entry.st_mtime_ns, entry.st_ctime_ns)
        if identity(before) != identity(after) or identity(before) != identity(current) or len(body) != before.st_size:
            raise RuntimeInputError("owner input file changed during validation")
        return body
    finally:
        os.close(descriptor)


def private_json(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=strict_object)
    except (UnicodeError, ValueError):
        raise RuntimeInputError("owner input JSON is invalid") from None


def owner_input_bundles(owner_dir: Path, source_sha: str, sql_sha: str, environment: dict[str, str]) -> tuple[Any, Any, Any]:
    """Persistent inputs remain readable to the existing runner owner outside jobs.

    Environment/Git checks prove consistency, not protected-job authentication.
    """
    if (owner_dir != OWNER_INPUT_DIR or os.geteuid() == 0 or
            environment.get("RUNNER_NAME") != RUNNER_NAME or environment.get("RUNNER_ENVIRONMENT") != "self-hosted" or
            environment.get("GITHUB_REPOSITORY") != "statground/Statground_Data_R-project" or
            not SHA40.fullmatch(source_sha) or sql_sha != SQL_SHA):
        raise RuntimeInputError("owner input runner or source context differs")
    source_root = Path(__file__).resolve().parents[1]
    try:
        actual_source = subprocess.check_output(["git", "-C", str(source_root), "rev-parse", "HEAD"],
                                                stderr=subprocess.DEVNULL, timeout=5).decode().strip()
        source_hashes = {}
        for name in ("scripts/materialize_community_publication_runtime.py", ".github/workflows/r-project-all.yml"):
            committed = subprocess.check_output(["git", "-C", str(source_root), "show", source_sha + ":" + name],
                                                stderr=subprocess.DEVNULL, timeout=5)
            current = (source_root / name).read_bytes()
            if len(committed) > 512 * 1024 or current != committed:
                raise RuntimeInputError("owner input consumer differs from committed source")
            source_hashes[name] = hashlib.sha256(committed).hexdigest()
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise RuntimeInputError("owner input source identity unavailable") from None
    if actual_source != source_sha or environment.get("GITHUB_SHA") != source_sha:
        raise RuntimeInputError("owner input source identity differs")
    before = private_owner_directory(owner_dir)
    with os.scandir(owner_dir) as entries:
        names = set()
        for entry in entries:
            names.add(entry.name)
            if len(names) > 4:
                raise RuntimeInputError("owner input directory has unexpected entries")
    if names != {"manifest.json", *OWNER_FILES}:
        raise RuntimeInputError("owner input directory has unexpected entries")
    manifest = private_json(private_owner_bytes(owner_dir / "manifest.json", 16 * 1024))
    expected = {"schema", "source_sha", "sql_commit_sha", "workflow_sha256", "materializer_sha256",
                "runner_name", "reader_inventory_revision", "files"}
    if (not isinstance(manifest, dict) or set(manifest) != expected or
            manifest.get("schema") != "web-r.community.owner-inputs.v1" or
            not isinstance(manifest.get("source_sha"), str) or not SHA40.fullmatch(manifest["source_sha"]) or
            manifest.get("sql_commit_sha") != sql_sha or
            manifest.get("runner_name") != RUNNER_NAME or
            manifest.get("materializer_sha256") != source_hashes["scripts/materialize_community_publication_runtime.py"] or
            manifest.get("workflow_sha256") != source_hashes[".github/workflows/r-project-all.yml"] or
            type(manifest.get("reader_inventory_revision")) is not int or manifest["reader_inventory_revision"] <= 0 or
            not isinstance(manifest.get("files"), dict) or set(manifest["files"]) != set(OWNER_FILES) or
            not all(isinstance(value, str) and SHA64.fullmatch(value) for value in manifest["files"].values())):
        raise RuntimeInputError("owner input manifest identity or schema differs")
    try:
        ancestry = subprocess.run(["git", "-C", str(source_root), "merge-base", "--is-ancestor",
                                   manifest["source_sha"], source_sha], stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL, timeout=5, check=False)
        if ancestry.returncode != 0:
            raise RuntimeInputError("owner input reviewed source is not an ancestor")
        for name in source_hashes:
            reviewed = subprocess.check_output(["git", "-C", str(source_root), "show", manifest["source_sha"] + ":" + name],
                                               stderr=subprocess.DEVNULL, timeout=5)
            if len(reviewed) > 512 * 1024 or hashlib.sha256(reviewed).hexdigest() != source_hashes[name]:
                raise RuntimeInputError("owner input consumers changed since reviewed source")
    except (OSError, subprocess.SubprocessError):
        raise RuntimeInputError("owner input reviewed source unavailable") from None
    bundles = []
    for filename in OWNER_FILES:
        body = private_owner_bytes(owner_dir / filename, MAX_BUNDLE_BYTES)
        if hashlib.sha256(body).hexdigest() != manifest["files"][filename]:
            raise RuntimeInputError("owner input bytes differ from reviewed manifest")
        bundles.append(private_json(body))
    after = private_owner_directory(owner_dir)
    if (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns):
        raise RuntimeInputError("owner input directory changed during validation")
    validate_bundles(*bundles)
    for xml in bundles[0].values():
        root = ET.fromstring(xml)
        users = root.findall(".//user")
        if len(users) != 1 or root.find("user") is not users[0] or users[0].text != PUBLISHER_USER:
            raise RuntimeInputError("owner input publisher principal differs")
    if bundles[1]["reader_inventory_revision"] != manifest["reader_inventory_revision"]:
        raise RuntimeInputError("owner input reader revision differs")
    return tuple(bundles)


def input_bundles(environment: dict[str, str], owner_dir: Path | None, source_sha: str, sql_sha: str) -> tuple[Any, Any, Any]:
    names = (CONFIGS_ENV, INVENTORY_ENV, TOKENS_ENV)
    present = [bool(environment.get(name, "")) for name in names]
    if all(present):
        return tuple(decode_bundle(name, environment[name]) for name in names)
    if any(present):
        raise RuntimeInputError("protected publication inputs must be all present or all absent")
    if owner_dir is None:
        raise RuntimeInputError("protected publication inputs are required")
    try:
        return owner_input_bundles(owner_dir, source_sha, sql_sha, environment)
    except (OSError, KeyError):
        raise RuntimeInputError("owner publication inputs unavailable; private details withheld") from None


def materialize(
    runtime_dir: Path,
    runner_temp: Path,
    configs_bundle: Any,
    inventory_bundle: Any,
    tokens_bundle: Any,
) -> dict[str, int | str]:
    runtime_dir, _ = checked_destination(runtime_dir, runner_temp)
    configs, readers = validate_bundles(configs_bundle, inventory_bundle, tokens_bundle)

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
    parser.add_argument("--runtime-dir", type=Path)
    parser.add_argument("--runner-temp", type=Path)
    parser.add_argument("--validate-inputs", action="store_true")
    parser.add_argument("--owner-input-dir", type=Path)
    parser.add_argument("--source-sha", default="")
    parser.add_argument("--sql-sha", default="")
    args = parser.parse_args(argv)
    try:
        bundles = input_bundles(dict(os.environ), args.owner_input_dir, args.source_sha, args.sql_sha)
        if args.validate_inputs:
            _, readers = validate_bundles(*bundles)
            result = {"status": "validated", "endpoint_count": 4, "reader_count": len(readers)}
        else:
            if args.runtime_dir is None or args.runner_temp is None:
                raise RuntimeInputError("runtime-dir and runner-temp are required")
            result = materialize(args.runtime_dir, args.runner_temp, *bundles)
    except RuntimeInputError as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
