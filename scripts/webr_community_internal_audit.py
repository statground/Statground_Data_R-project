#!/usr/bin/env python3
"""Read-only connectivity audit for an internal Web-R community publisher host.

This intentionally does not invoke the generation loader or publisher. In
particular, publisher ``preflight`` performs SYSTEM SYNC REPLICA and is not a
read-only substitute for this audit. Use a trusted installed ClickHouse client;
the only query is a fixed, single-row SELECT 1.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


ENDPOINT_NAMES = ("s1r1", "s1r2", "s2r1", "s2r2")
SELECT_ONE = "SELECT 1 FORMAT TSVRaw"
INVENTORY_SCHEMA = "web-r.community.reader-inventory.v1"
TRANSITION_PATH = "/internal/community-publication/transition"
TRANSITION_INVENTORY_FORMAT = "statground.publication-reader-transition-inventory.v2"
INSTANCE_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
MAX_CONFIG_BYTES = 64 * 1024
MAX_INVENTORY_BYTES = 256 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
MAX_READERS = 16
TOTAL_TIMEOUT_SECONDS = 60.0


class AuditError(Exception):
    """A failed audit with a sanitized, non-secret explanation."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


HTTP_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AuditError("JSON contains duplicate keys")
        result[key] = value
    return result


def owner_file(path: Path, max_bytes: int, label: str) -> bytes:
    if not path.is_absolute() or path != Path(os.path.normpath(path)):
        raise AuditError(f"{label} path must be normalized and absolute")
    try:
        if path.resolve(strict=True) != path:
            raise AuditError(f"{label} path contains a symlink")
        parent = path.parent.stat()
        if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o022:
            raise AuditError(f"{label} parent directory is not owner-controlled")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) & 0o077
            ):
                raise AuditError(f"{label} must be an owner-only regular file")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                payload = stream.read(max_bytes + 1)
        finally:
            os.close(descriptor)
    except AuditError:
        raise
    except (OSError, RuntimeError) as exc:
        raise AuditError(f"cannot read {label}") from exc
    if not payload or len(payload) > max_bytes:
        raise AuditError(f"{label} has an invalid size")
    return payload


def endpoint_configs(values: list[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    addresses: set[tuple[str, int]] = set()
    for value in values:
        name, separator, raw_path = value.partition("=")
        if not separator or name not in ENDPOINT_NAMES or name in parsed:
            raise AuditError("exactly four unique named DB endpoints are required")
        path = Path(raw_path)
        payload = owner_file(path, MAX_CONFIG_BYTES, "DB client config")
        if b"<!doctype" in payload.lower() or b"<!entity" in payload.lower():
            raise AuditError("DB client config contains an XML declaration")
        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise AuditError("DB client config is invalid XML") from exc
        if root.tag != "clickhouse":
            raise AuditError("DB client config has an unexpected root")
        hosts = root.findall("host")
        ports = root.findall("port")
        if len(hosts) != 1 or len(ports) != 1:
            raise AuditError("DB client config must specify one host and port")
        host = (hosts[0].text or "").strip()
        try:
            port = int((ports[0].text or "").strip())
        except ValueError as exc:
            raise AuditError("DB client config has an invalid port") from exc
        if not host or any(character.isspace() for character in host) or not 1 <= port <= 65535:
            raise AuditError("DB client config has an invalid host or port")
        address = (host.lower(), port)
        if address in addresses:
            raise AuditError("DB client configs contain duplicate network endpoints")
        addresses.add(address)
        parsed[name] = path
    if set(parsed) != set(ENDPOINT_NAMES):
        raise AuditError("exactly four unique named DB endpoints are required")
    return {name: parsed[name] for name in ENDPOINT_NAMES}


def canonical_url(raw: Any, *, exact_path: str | None = None) -> urllib.parse.SplitResult:
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise AuditError("reader inventory contains an invalid URL")
    try:
        parsed = urllib.parse.urlsplit(raw)
        host = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise AuditError("reader inventory contains an invalid URL") from exc
    loopback = host in {"localhost", "127.0.0.1", "::1"}
    if (
        not host
        or parsed.scheme not in ({"http", "https"} if loopback else {"https"})
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or (exact_path is not None and parsed.path != exact_path)
        or urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", "")) != raw
    ):
        raise AuditError("reader inventory contains an unsafe URL")
    _ = port
    return parsed


def reader_inventory(path: Path) -> list[dict[str, str]]:
    payload = owner_file(path, MAX_INVENTORY_BYTES, "reader inventory")
    try:
        value = json.loads(payload, object_pairs_hook=strict_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AuditError("reader inventory is invalid JSON") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"schema", "reader_inventory_revision", "readers"}
        or value["schema"] != INVENTORY_SCHEMA
        or type(value["reader_inventory_revision"]) is not int
        or value["reader_inventory_revision"] <= 0
        or not isinstance(value["readers"], list)
        or not 1 <= len(value["readers"]) <= MAX_READERS
    ):
        raise AuditError("reader inventory has an unexpected shape")
    readers: list[dict[str, str]] = []
    identities: set[str] = set()
    transition_urls: set[str] = set()
    inventory_urls: set[str] = set()
    for raw in value["readers"]:
        if not isinstance(raw, dict) or set(raw) != {
            "app_service", "instance_id", "inventory_endpoint", "url", "token_file"
        }:
            raise AuditError("reader inventory entry has an unexpected shape")
        instance = raw["instance_id"]
        if raw["app_service"] != "web-r" or not isinstance(instance, str) or not INSTANCE_RE.fullmatch(instance):
            raise AuditError("reader inventory contains an invalid instance")
        inventory_url = canonical_url(raw["inventory_endpoint"])
        transition_url = canonical_url(raw["url"], exact_path=TRANSITION_PATH)
        inventory_port = inventory_url.port or (443 if inventory_url.scheme == "https" else 80)
        transition_port = transition_url.port or (443 if transition_url.scheme == "https" else 80)
        if (
            inventory_url.scheme != transition_url.scheme
            or inventory_url.hostname != transition_url.hostname
            or inventory_port != transition_port
            or instance in identities
            or raw["inventory_endpoint"] in inventory_urls
            or raw["url"] in transition_urls
        ):
            raise AuditError("reader inventory identities or origins do not match")
        if not isinstance(raw["token_file"], str):
            raise AuditError("reader token path is invalid")
        token = owner_file(Path(raw["token_file"]), 4096, "reader token")
        if b"\n" in token or b"\r" in token:
            raise AuditError("reader token contains a newline")
        try:
            decoded_token = token.decode("utf-8")
        except UnicodeError as exc:
            raise AuditError("reader token is invalid UTF-8") from exc
        if len(decoded_token) < 32 or any(not 0x21 <= ord(character) <= 0x7E for character in decoded_token):
            raise AuditError("reader token is invalid")
        identities.add(instance)
        inventory_urls.add(raw["inventory_endpoint"])
        transition_urls.add(raw["url"])
        readers.append({"instance_id": instance, "url": raw["url"], "token": decoded_token})
    return readers


def remaining_timeout(deadline: float, per_call: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise AuditError("audit exceeded its total timeout")
    return min(per_call, remaining)


def check_db(binary: Path, configs: dict[str, Path], deadline: float, per_call: float) -> None:
    if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise AuditError("ClickHouse client binary is unavailable")
    for name, path in configs.items():
        try:
            result = subprocess.run(
                [str(binary), "--config-file", str(path), "--query", SELECT_ONE],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False,
                timeout=remaining_timeout(deadline, per_call),
            )
        except subprocess.TimeoutExpired as exc:
            raise AuditError(f"{name} DB read timed out") from exc
        except OSError as exc:
            raise AuditError(f"{name} DB read could not start") from exc
        if result.returncode != 0 or result.stdout.strip() != "1":
            raise AuditError(f"{name} DB read failed")


def check_readers(readers: list[dict[str, str]], deadline: float, per_call: float) -> None:
    for reader in readers:
        nonce = str(uuid.uuid4())
        url = reader["url"] + "?" + urllib.parse.urlencode({"inventory_nonce": nonce})
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Authorization": "Bearer " + reader["token"],
                "Accept": "application/json",
                "Cache-Control": "no-store",
            },
        )
        try:
            with HTTP_OPENER.open(request, timeout=remaining_timeout(deadline, per_call)) as response:
                if response.status != 200 or response.geturl() != url:
                    raise AuditError("reader inventory GET was not a direct HTTP 200")
                payload = response.read(MAX_RESPONSE_BYTES + 1)
        except AuditError:
            raise
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise AuditError("reader inventory GET failed") from exc
        if len(payload) > MAX_RESPONSE_BYTES:
            raise AuditError("reader inventory response is too large")
        try:
            value = json.loads(payload, object_pairs_hook=strict_object)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise AuditError("reader inventory response is invalid JSON") from exc
        if (
            not isinstance(value, dict)
            or value.get("format") != TRANSITION_INVENTORY_FORMAT
            or value.get("app_service") != "web-r"
            or value.get("domain") != "community"
            or value.get("reader_instance") != reader["instance_id"]
            or value.get("inventory_nonce") != nonce
            or value.get("phase") != "steady"
            or value.get("admission_open") is not True
            or type(value.get("inflight")) is not int
            or value["inflight"] < 0
        ):
            raise AuditError("reader inventory GET did not prove a steady reader")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", action="append", required=True, help="name=/absolute/owner-only/client.xml")
    parser.add_argument("--reader-inventory", type=Path, help="required unless --db-only is set")
    parser.add_argument("--clickhouse-client", type=Path, default=Path("/usr/bin/clickhouse-client"))
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--db-only", action="store_true", help="skip reader GETs and report DB connectivity only")
    args = parser.parse_args(argv)
    try:
        if not 0 < args.timeout_seconds <= 10:
            raise AuditError("per-call timeout must be between 0 and 10 seconds")
        configs = endpoint_configs(args.endpoint)
        readers: list[dict[str, str]] = []
        if not args.db_only:
            if args.reader_inventory is None:
                raise AuditError("reader inventory is required for the full audit")
            readers = reader_inventory(args.reader_inventory)
        deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
        check_db(args.clickhouse_client, configs, deadline, args.timeout_seconds)
        if not args.db_only:
            check_readers(readers, deadline, args.timeout_seconds)
        result = {
            "status": "db_connectivity_only" if args.db_only else "audit_pass",
            "database_endpoint_count": len(configs),
            "reader_count": len(readers),
            "reader_get_checked": not args.db_only,
            "publication_ready": False,
        }
        print(json.dumps(result, sort_keys=True))
        return 0
    except AuditError as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
