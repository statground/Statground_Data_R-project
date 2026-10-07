#!/usr/bin/env python3
"""Verify an exact community generation proof at an immutable jsDelivr commit."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import json
import os
import re
import sys
import threading
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
WORKSHOP_PATH_RE = re.compile(
    r"^community/ko/workshop/(?:curated|[0-9]{4}/(?:0[1-9]|1[0-2]))/[A-Za-z0-9_-]+[.]json$"
)
MAX_ASSET_BYTES = 32 * 1024 * 1024
MAX_WORKSHOP_BYTES = 16 * 1024 * 1024
HTTP_TIMEOUT_SECONDS = 30
WORKSHOP_WORKERS = 4
MAX_AGGREGATE_SECONDS = 825
WORKSHOP_UNSIGNED = {"capacity", "paid_count", "total_count"}
WORKSHOP_INTS = WORKSHOP_UNSIGNED | {"member_price", "nonmember_price", "sort_order", "paid_amount"}
WORKSHOP_BOOLS = {"active", "external", "is_new"}
POST_BOOLS = {"active", "imported", "is_new"}


class VerificationBudget:
    def __init__(self, attempts: int, backoff: float) -> None:
        # Existing proof + two index requests, with their original retry limits.
        self.seconds = min(MAX_AGGREGATE_SECONDS, 3 * (attempts * HTTP_TIMEOUT_SECONDS + backoff * attempts * (attempts - 1) / 2))
        self.deadline = time.monotonic() + self.seconds
        self.cancelled = threading.Event()

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if self.cancelled.is_set() or remaining <= 0:
            raise ValueError("immutable CDN verification canceled or aggregate budget exhausted")
        return remaining

    def timeout(self) -> float:
        return min(HTTP_TIMEOUT_SECONDS, self.remaining())

    def pause(self, seconds: float) -> None:
        self.cancelled.wait(min(seconds, self.remaining()))
        self.remaining()


def read_bounded(response: Any, limit: int, budget: VerificationBudget | None) -> bytes:
    if budget is None:
        return response.read(limit)
    # HTTPResponse.read1 performs one buffered/socket read, so slow successful
    # chunks cannot postpone our shared deadline until the whole body arrives.
    read_once = getattr(response, "read1", response.read)
    chunks = []
    size = 0
    while size < limit:
        budget.remaining()
        chunk = read_once(min(64 * 1024, limit - size))
        budget.remaining()
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
    return b"".join(chunks)


def decrypt_workshop(body: bytes, key: bytes, path: str) -> dict[str, Any]:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from export_community_cdn import ENCRYPTED_SCHEMA

    if not body or len(body) > MAX_WORKSHOP_BYTES:
        raise ValueError("Workshop object exceeds the runtime body limit")
    doc = json.loads(body.decode("utf-8"), object_pairs_hook=strict_object)
    if not isinstance(doc, dict) or (
        doc.get("schema") != ENCRYPTED_SCHEMA or doc.get("alg") != "AES-256-GCM"
        or doc.get("kdf") != "SHA256(secret+purpose:v1)" or doc.get("language") != "ko"
        or doc.get("path") != path
    ):
        raise ValueError("Workshop encrypted schema or path differs from the export contract")
    try:
        nonce = base64.b64decode(doc["nonce"] + "=" * (-len(doc["nonce"]) % 4), altchars=b"-_", validate=True)
        cipher = base64.b64decode(doc["ciphertext"] + "=" * (-len(doc["ciphertext"]) % 4), altchars=b"-_", validate=True)
        plain = AESGCM(key).decrypt(nonce, cipher, path.encode("utf-8"))
        result = json.loads(plain.decode("utf-8"), object_pairs_hook=strict_object)
    except Exception as exc:
        raise ValueError("Workshop authenticated decryption failed") from exc
    if not isinstance(result, dict):
        raise ValueError("Workshop plaintext has an invalid shape")
    return result


def validate_typed_fields(value: Any, *, post: bool = False) -> None:
    if not isinstance(value, dict):
        raise ValueError("Workshop item has an invalid shape")
    for name, item in value.items():
        booleans = POST_BOOLS if post else WORKSHOP_BOOLS
        if name in booleans:
            valid = isinstance(item, bool)
        elif not post and name in WORKSHOP_INTS:
            valid = isinstance(item, int) and not isinstance(item, bool) and -(2**63) <= item < 2**63
            if name in WORKSHOP_UNSIGNED:
                valid = isinstance(item, int) and not isinstance(item, bool) and 0 <= item < 2**64
        else:
            valid = isinstance(item, str)
        if not valid:
            raise ValueError("Workshop item fields do not match the runtime types")


def workshop_inventory(root: Path, key: bytes, base_url: str, budget: VerificationBudget | None = None) -> tuple[list[tuple[str, bytes]], int]:
    from export_community_cdn import WORKSHOP_CONTENT_SCHEMA, WORKSHOP_MANIFEST_SCHEMA, workshop_export_proof

    root = root.resolve(strict=True)
    index_path = "community/ko/workshop/index.json"
    index = (root / index_path).read_bytes()
    manifest = decrypt_workshop(index, key, index_path)
    items = manifest.get("items")
    if (
        manifest.get("schema") != WORKSHOP_MANIFEST_SCHEMA or manifest.get("language") != "ko"
        or not isinstance(manifest.get("generated_at"), str) or not manifest["generated_at"].strip()
        or not isinstance(manifest.get("catalog_token"), str) or not re.fullmatch(r"[0-9a-f]{64}", manifest["catalog_token"])
        or not isinstance(items, dict) or not items
    ):
        raise ValueError("Workshop manifest is incomplete")
    inventory = [(index_path, index)]
    seen = set()
    posts_by_board: dict[str, list[dict[str, Any]]] = {}
    for identity in sorted(items):
        if budget is not None:
            budget.remaining()
        item = items[identity]
        validate_typed_fields(item)
        path = item.get("path", "")
        if (
            not identity.strip() or not item.get("title", "").strip()
            or not any(item.get(name, "").strip() for name in ("uuid", "slug", "board_key"))
            or item.get("language", "") not in {"", "ko"}
            or item.get("base_url", "").strip().rstrip("/") not in {"", base_url}
            or not WORKSHOP_PATH_RE.fullmatch(path) or path in seen
            or identity != item.get("uuid")
        ):
            raise ValueError("Workshop descriptor identity, path or base URL is invalid")
        seen.add(path)
        target = (root / path).resolve(strict=True)
        if not target.is_relative_to(root):
            raise ValueError("Workshop path escaped the committed root")
        body = target.read_bytes()
        payload = decrypt_workshop(body, key, path)
        posts = payload.get("posts")
        validate_typed_fields(payload.get("workshop"))
        if payload.get("schema") != WORKSHOP_CONTENT_SCHEMA or payload.get("workshop") != item or not isinstance(posts, list):
            raise ValueError("Workshop detail does not match its complete manifest descriptor")
        board = item.get("board_key", "").strip() or item.get("uuid", "").strip() or item.get("slug", "").strip()
        for post in posts:
            validate_typed_fields(post, post=True)
            if post.get("workshop_key", "").strip() != board:
                raise ValueError("Workshop post belongs to a different board")
        if board in posts_by_board and posts_by_board[board] != posts:
            raise ValueError("Workshop repeated board contents differ")
        posts_by_board[board] = posts
        inventory.append((path, body))
    proof = workshop_export_proof(items, posts_by_board)
    if proof["catalog_token"] != manifest["catalog_token"] or proof["item_count"] != len(items):
        raise ValueError("Workshop full catalog content/count proof differs")
    return inventory, len(items)


def verify_workshop_inventory(
    opener: urllib.request.OpenerDirector, base_url: str, commit: str,
    inventory: list[tuple[str, bytes]], attempts: int, backoff: float, budget: VerificationBudget,
) -> None:
    def one(asset: tuple[str, bytes]) -> None:
        path, expected = asset
        for attempt in range(1, attempts + 1):
            budget.remaining()
            url = base_url + "/" + path
            request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
            try:
                with opener.open(request, timeout=budget.timeout()) as response:
                    actual = read_bounded(response, len(expected) + 1, budget)
                    if (
                        response.status != 200 or response.geturl() != url or actual != expected
                        or response.headers.get("x-jsd-version", "").strip().lower() != commit
                        or response.headers.get("x-jsd-version-type", "").strip().lower() != "commit"
                    ):
                        raise ValueError("Workshop exact immutable bytes or commit headers differ")
                budget.remaining()
                return
            except (OSError, ValueError, urllib.error.URLError):
                if attempt == attempts:
                    raise ValueError("Workshop complete immutable inventory is unavailable") from None
                budget.pause(backoff * attempt)
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=WORKSHOP_WORKERS)
    futures = [pool.submit(one, asset) for asset in inventory]
    try:
        for future in concurrent.futures.as_completed(futures, timeout=budget.remaining()):
            future.result()
        budget.remaining()
    except BaseException as exc:
        budget.cancelled.set()
        for future in futures:
            future.cancel()
        if isinstance(exc, concurrent.futures.TimeoutError):
            raise ValueError("immutable CDN aggregate verification budget exhausted") from None
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


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
    budget: VerificationBudget | None = None,
) -> None:
    if not ASSET_RE.fullmatch(relative_path):
        raise ValueError("immutable CDN asset path is not allowed")
    local_body = local_path.read_bytes()
    if not local_body or len(local_body) > MAX_ASSET_BYTES:
        raise ValueError("local immutable CDN asset has an invalid size")
    url = base_url + "/" + relative_path
    last_error = "unavailable"
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
        try:
            with opener.open(request, timeout=budget.timeout() if budget else HTTP_TIMEOUT_SECONDS) as response:
                remote_body = read_bounded(response, len(local_body) + 1, budget)
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
            budget.pause(backoff * attempt) if budget else time.sleep(backoff * attempt)
    raise ValueError(f"immutable CDN asset verification failed for {relative_path}: {last_error}")


def verify(
    base_url: str,
    proof_path: Path,
    assets: list[tuple[str, Path]],
    attempts: int,
    backoff: float,
    *, workshop_root: Path | None = None, workshop_key: bytes | None = None,
) -> dict[str, Any]:
    match = BASE_RE.fullmatch(base_url.strip().rstrip("/"))
    if not match:
        raise ValueError("CDN base URL must contain an exact 40-character community commit")
    commit = match.group(1)
    base_url = base_url.strip().rstrip("/")
    if (workshop_root is None) != (workshop_key is None):
        raise ValueError("Workshop root and existing content key must be supplied together")
    budget = VerificationBudget(attempts, backoff) if workshop_root is not None else None
    inventory, workshop_count = workshop_inventory(workshop_root, workshop_key, base_url, budget) if workshop_root is not None else ([], 0)
    local_body, local_value = load_local_proof(proof_path)
    url = base_url.strip().rstrip("/") + "/community/ko/generation-proof.json"
    opener = urllib.request.build_opener(NoRedirect())
    last_error = "unavailable"
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
        try:
            with opener.open(request, timeout=budget.timeout() if budget else HTTP_TIMEOUT_SECONDS) as response:
                remote_body = read_bounded(response, MAX_PROOF_BYTES + 1, budget)
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
            budget.pause(backoff * attempt) if budget else time.sleep(backoff * attempt)
    else:
        raise ValueError(f"immutable community proof verification failed: {last_error}")
    for relative_path, local_path in assets:
        immutable_bytes(opener, base_url, commit, relative_path, local_path, attempts, backoff, budget)
    if budget is not None:
        verify_workshop_inventory(opener, base_url, commit, inventory, attempts, backoff, budget)
    return {
        "status": "verified",
        "commit_sha": commit,
        "generation": local_value["generation"],
        "item_count": local_value["item_count"],
        "asset_count": len(assets),
        **({"workshop_complete": True, "workshop_item_count": workshop_count,
            "workshop_verified_objects": len(inventory), "workshop_parallel_limit": WORKSHOP_WORKERS,
            "aggregate_budget_seconds": budget.seconds} if budget is not None else {}),
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
    parser.add_argument("--workshop-root", type=Path, help="committed CDN root for complete Workshop verification")
    parser.add_argument("--attempts", type=int, default=6)
    parser.add_argument("--backoff-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)
    if not 1 <= args.attempts <= 12 or not 0 <= args.backoff_seconds <= 60:
        raise SystemExit("invalid retry policy")
    try:
        workshop_key = None
        if args.workshop_root is not None:
            from export_community_cdn import content_secret, derive_key

            workshop_key = derive_key(content_secret(dict(os.environ)))
        result = verify(
            args.base_url,
            args.proof,
            [(relative, Path(local)) for relative, local in args.asset],
            args.attempts,
            args.backoff_seconds,
            workshop_root=args.workshop_root,
            workshop_key=workshop_key,
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
