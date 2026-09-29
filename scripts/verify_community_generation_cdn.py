#!/usr/bin/env python3
"""Verify an exact community generation proof at an immutable jsDelivr commit."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


BASE_RE = re.compile(
    r"^https://cdn[.]jsdelivr[.]net/gh/statground/web-r_CDN2_community@([0-9a-f]{40})$"
)
MAX_PROOF_BYTES = 64 * 1024
PROOF_KEYS = {
    "schema", "generation", "complete", "item_count", "identity_hash",
    "content_hash", "withdrawal_revision", "account_authority_revision",
    "category_authority_revision",
}
ASSET_RE = re.compile(r"^community/ko/(?:index[.]json|workshop/index[.]json)$")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def load_local_proof(path: Path) -> tuple[bytes, dict[str, Any]]:
    body = path.read_bytes()
    if not body or len(body) > MAX_PROOF_BYTES:
        raise ValueError("local generation proof has an invalid size")
    value = json.loads(body.decode("utf-8"), object_pairs_hook=strict_object)
    if (
        not isinstance(value, dict)
        or set(value) != PROOF_KEYS
        or value.get("schema") != "web-r.community.generation-proof.v2"
        or value.get("complete") is not True
        or isinstance(value.get("item_count"), bool)
        or not isinstance(value.get("item_count"), int)
        or value["item_count"] <= 0
    ):
        raise ValueError("local generation proof is incomplete")
    return body, value


def immutable_bytes(
    opener: urllib.request.OpenerDirector,
    base_url: str,
    commit: str,
    relative_path: str,
    local_path: Path,
    attempts: int,
    backoff: float,
) -> None:
    if not ASSET_RE.fullmatch(relative_path):
        raise ValueError("immutable CDN asset path is not allowed")
    local_body = local_path.read_bytes()
    if not local_body or len(local_body) > 32 * 1024 * 1024:
        raise ValueError("local immutable CDN asset has an invalid size")
    url = base_url + "/" + relative_path
    last_error = "unavailable"
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
        try:
            with opener.open(request, timeout=30) as response:
                remote_body = response.read(len(local_body) + 1)
                if response.status != 200 or response.geturl() != url or remote_body != local_body:
                    raise ValueError("immutable CDN asset differs from the committed local bytes")
                if response.headers.get("x-jsd-version", "").strip().lower() != commit:
                    raise ValueError("jsDelivr x-jsd-version does not match the exact commit")
                if response.headers.get("x-jsd-version-type", "").strip().lower() != "commit":
                    raise ValueError("jsDelivr did not identify the immutable version as a commit")
                return
        except (OSError, ValueError, urllib.error.URLError) as exc:
            last_error = str(exc) or exc.__class__.__name__
        if attempt < attempts:
            time.sleep(backoff * attempt)
    raise ValueError(f"immutable CDN asset verification failed for {relative_path}: {last_error}")


def verify(
    base_url: str,
    proof_path: Path,
    assets: list[tuple[str, Path]],
    attempts: int,
    backoff: float,
) -> dict[str, Any]:
    match = BASE_RE.fullmatch(base_url.strip().rstrip("/"))
    if not match:
        raise ValueError("CDN base URL must contain an exact 40-character community commit")
    commit = match.group(1)
    local_body, local_value = load_local_proof(proof_path)
    url = base_url.strip().rstrip("/") + "/community/ko/generation-proof.json"
    opener = urllib.request.build_opener(NoRedirect())
    last_error = "unavailable"
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
        try:
            with opener.open(request, timeout=30) as response:
                remote_body = response.read(MAX_PROOF_BYTES + 1)
                remote_value = json.loads(remote_body.decode("utf-8"), object_pairs_hook=strict_object)
                if response.status != 200 or response.geturl() != url:
                    raise ValueError("immutable proof did not return an exact 200 response")
                if len(remote_body) > MAX_PROOF_BYTES or remote_body != local_body or remote_value != local_value:
                    raise ValueError("immutable proof bytes differ from the committed local proof")
                if response.headers.get("x-jsd-version", "").strip().lower() != commit:
                    raise ValueError("jsDelivr x-jsd-version does not match the exact commit")
                if response.headers.get("x-jsd-version-type", "").strip().lower() != "commit":
                    raise ValueError("jsDelivr did not identify the immutable version as a commit")
                break
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError, urllib.error.URLError) as exc:
            last_error = str(exc) or exc.__class__.__name__
        if attempt < attempts:
            time.sleep(backoff * attempt)
    else:
        raise ValueError(f"immutable community proof verification failed: {last_error}")
    for relative_path, local_path in assets:
        immutable_bytes(opener, base_url, commit, relative_path, local_path, attempts, backoff)
    return {
        "status": "verified",
        "commit_sha": commit,
        "generation": local_value["generation"],
        "item_count": local_value["item_count"],
        "asset_count": len(assets),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument(
        "--asset",
        action="append",
        nargs=2,
        metavar=("CDN_PATH", "LOCAL_PATH"),
        default=[],
    )
    parser.add_argument("--attempts", type=int, default=6)
    parser.add_argument("--backoff-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)
    if not 1 <= args.attempts <= 12 or not 0 <= args.backoff_seconds <= 60:
        raise SystemExit("invalid retry policy")
    try:
        result = verify(
            args.base_url,
            args.proof,
            [(relative, Path(local)) for relative, local in args.asset],
            args.attempts,
            args.backoff_seconds,
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
