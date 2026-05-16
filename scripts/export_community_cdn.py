#!/usr/bin/env python3
"""Export selected community rows as encrypted web-R CDN payloads."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from clickhouse_http import build_clickhouse_url


ENCRYPTED_SCHEMA = "web-r.community.encrypted.v1"
MANIFEST_SCHEMA = "web-r.community.manifest.plain.v1"
CONTENT_SCHEMA = "web-r.community.content.plain.v1"
KEY_PURPOSE = "web-r:community-content:v1"
R_COMMUNITY_BOT_UUID = "019e1127-f5d7-7304-a916-31914e58e1e9"
R_COMMUNITY_BOT_NAME = "R Community"
R_COMMUNITY_BOT_ROLE = "Bot"
NOTEBOOK_BOT_UUID = "7b1c9fc4-7216-44cb-81b8-5fe17f2158bc"
NOTEBOOK_BOT_NAME = "Web-R Notebook"
NOTEBOOK_BOT_ROLE = "Bot"
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
DATE_RE = re.compile(r"(\d{4})-(\d{2})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="web_r_go .env path")
    parser.add_argument("--cdn-root", default="../web-r_CDN2_community", help="web-r_CDN2_community checkout path")
    parser.add_argument("--language", default="ko", help="content language code")
    parser.add_argument("--limit", type=int, default=0, help="optional per-source row limit for smoke exports")
    parser.add_argument("--dry-run", action="store_true", help="query and encrypt without writing files")
    args = parser.parse_args()

    repo_root = Path.cwd()
    env = load_env(repo_root / args.env)
    language = normalize_language(args.language)
    key = derive_key(content_secret(env))
    cdn_root = (repo_root / args.cdn_root).resolve()

    digest_rows = fetch_json_rows(env, digest_sql(args.limit))
    notebook_rows = fetch_json_rows(env, notebook_sql(args.limit))

    payloads: dict[str, tuple[str, dict[str, Any]]] = {}
    manifest_items: dict[str, dict[str, Any]] = {}
    duplicate_count = 0

    for row in digest_rows:
        uuid = normalize_uuid(row.get("uuid"))
        if not uuid:
            continue
        if uuid in payloads:
            duplicate_count += 1
            continue
        published_at = first_text(row.get("published_at"), row.get("updated_at"))
        rel_path = community_payload_path(language, "rcommunity", uuid, published_at)
        item = {
            "uuid": uuid,
            "kind": "rcommunity",
            "source": "rcommunity",
            "language": language,
            "published_at": text(published_at),
            "updated_at": text(row.get("updated_at")),
            "path": rel_path,
            "user_uuid": R_COMMUNITY_BOT_UUID,
            "user_nickname": R_COMMUNITY_BOT_NAME,
            "user_role": R_COMMUNITY_BOT_ROLE,
            "category": "R Community",
            "category_url": "rcommunity",
            "category_url_sub": "",
            "source_type": text(row.get("source_type")),
            "source_id": text(row.get("source_id")),
            "source_name": text(row.get("source_name")),
            "platform": text(row.get("platform")),
            "title": text(row.get("title")) or "R Community 일일 요약",
            "summary": text(row.get("summary")),
            "content": text(row.get("summary")),
            "source_items_json": text(row.get("source_items_json")),
            "url": f"/community/read/{uuid}/",
            "deduped_item_count": int_value(row.get("deduped_item_count")),
        }
        payloads[uuid] = (rel_path, {"schema": CONTENT_SCHEMA, "item": item})
        manifest_items[uuid] = item

    for row in notebook_rows:
        uuid = normalize_uuid(row.get("uuid"))
        if not uuid:
            continue
        if uuid in payloads:
            duplicate_count += 1
            continue
        published_at = first_text(row.get("published_at"), row.get("updated_at"))
        rel_path = community_payload_path(language, "notebook", uuid, published_at)
        title = text(row.get("title")) or "제목 없음"
        description = text(row.get("description"))
        item = {
            "uuid": uuid,
            "kind": "notebook",
            "source": "notebook",
            "language": language,
            "published_at": text(published_at),
            "updated_at": text(row.get("updated_at")),
            "path": rel_path,
            "user_uuid": NOTEBOOK_BOT_UUID,
            "user_nickname": NOTEBOOK_BOT_NAME,
            "user_role": NOTEBOOK_BOT_ROLE,
            "category": "Web-R Notebook",
            "category_url": "notebook",
            "category_url_sub": "",
            "source_type": "notebook",
            "source_id": "web-r-notebook",
            "source_name": NOTEBOOK_BOT_NAME,
            "platform": "Web-R",
            "title": title,
            "summary": description,
            "content": description,
            "source_items_json": "",
            "url": text(row.get("url")),
            "deduped_item_count": 0,
        }
        payloads[uuid] = (rel_path, {"schema": CONTENT_SCHEMA, "item": item})
        manifest_items[uuid] = item

    manifest = {
        "schema": MANIFEST_SCHEMA,
        "language": language,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "items": manifest_items,
    }

    if args.dry_run:
        print(json.dumps({"digest": len(digest_rows), "notebook": len(notebook_rows), "export": len(payloads), "duplicates": duplicate_count}, ensure_ascii=False))
        return 0

    for uuid, (rel_path, payload) in payloads.items():
        encrypted = encrypt_document(payload, key, rel_path, language, uuid)
        write_json_atomic(cdn_root / rel_path, encrypted)

    manifest_path = f"community/{language}/index.json"
    encrypted_manifest = encrypt_document(manifest, key, manifest_path, language, "")
    write_json_atomic(cdn_root / manifest_path, encrypted_manifest)

    print(json.dumps({"digest": len(digest_rows), "notebook": len(notebook_rows), "export": len(payloads), "duplicates": duplicate_count}, ensure_ascii=False))
    return 0


def digest_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT toString(digest_uuid) AS uuid,
       digest_id,
       toString(digest_date) AS digest_date,
       source_type,
       source_id,
       source_name,
       platform,
       source_url,
       title,
       summary,
       toString(source_items_json) AS source_items_json,
       toInt64(deduped_item_count) AS deduped_item_count,
       concat(toString(digest_date), ' 23:59:00') AS published_at,
       formatDateTime(updated_at, '%Y-%m-%d %H:%i:%S') AS updated_at
  FROM Data_R_Community_Service.v_r_community_daily_digest_latest
 WHERE notEmpty(summary)
 ORDER BY digest_date DESC, updated_at DESC{suffix}
 FORMAT JSONEachRow
"""


def notebook_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT toString(n.uuid) AS uuid,
       toString(n.uuid_user) AS user_uuid,
       if(isNull(n.uuid_share), toString(n.uuid), toString(n.uuid_share)) AS share_uuid,
       coalesce(nullIf(n.title, ''), '제목 없음') AS title,
       coalesce(n.description, '') AS description,
       formatDateTime(n.created_at, '%Y-%m-%d %H:%i:%S') AS published_at,
       if(isNull(n.updated_at), '', formatDateTime(n.updated_at, '%Y-%m-%d %H:%i:%S')) AS updated_at,
       concat('/webr/notebook/view/', if(isNull(n.uuid_share), toString(n.uuid), toString(n.uuid_share)), '/') AS url
  FROM webr_webr.v_d1_notebook n
 WHERE coalesce(n.active, 0) = 1
   AND coalesce(n.share, 0) = 1
   AND n.uuid_user = toUUID('{NOTEBOOK_BOT_UUID}')
 ORDER BY n.created_at DESC{suffix}
 FORMAT JSONEachRow
"""


def community_payload_path(language: str, kind: str, uuid: str, published_at: str) -> str:
    year, month = published_year_month(published_at)
    return f"community/{language}/{kind}/{year}/{month}/{uuid}.json"


def load_env(path: Path) -> dict[str, str]:
    env = dict(os.environ)
    if not path.exists():
        return env
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and not env.get(key):
            env[key] = value
    return env


def content_secret(env: dict[str, str]) -> str:
    for key in ("R_ECOSYSTEM_CONTENT_KEY", "WEBR_R_ECOSYSTEM_CONTENT_KEY", "SESSION_SECRET"):
        value = env.get(key, "").strip()
        if value:
            return value
    raise SystemExit("community content key is not configured")


def derive_key(secret: str) -> bytes:
    return hashlib.sha256((secret.strip() + "\0" + KEY_PURPOSE).encode("utf-8")).digest()


def encrypt_document(plain: dict[str, Any], key: bytes, rel_path: str, language: str, uuid: str) -> dict[str, Any]:
    plaintext = json.dumps(plain, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    nonce = hmac.new(key, normalize_path(rel_path).encode("utf-8") + b"\0" + plaintext, hashlib.sha256).digest()[:12]
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, normalize_path(rel_path).encode("utf-8"))
    doc = {
        "schema": ENCRYPTED_SCHEMA,
        "alg": "AES-256-GCM",
        "kdf": "SHA256(secret+purpose:v1)",
        "language": language,
        "path": normalize_path(rel_path),
        "nonce": b64url(nonce),
        "ciphertext": b64url(ciphertext),
    }
    if uuid:
        doc["uuid"] = uuid
    return doc


def fetch_json_rows(env: dict[str, str], sql: str) -> list[dict[str, Any]]:
    user = env.get("CLICKHOUSE_USER", "").strip()
    password = env.get("CLICKHOUSE_PASSWORD", "")
    if not user:
        raise SystemExit("ClickHouse connection environment is incomplete")
    url = build_clickhouse_url(env, default_format="JSONEachRow", max_execution_time="120")
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise SystemExit(f"ClickHouse export query failed: {exc.__class__.__name__}") from exc
    rows: list[dict[str, Any]] = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False) as tmp:
        tmp.write(body)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def published_year_month(value: str) -> tuple[str, str]:
    match = DATE_RE.search(value or "")
    if match:
        return match.group(1), match.group(2)
    now = datetime.now()
    return f"{now.year:04d}", f"{now.month:02d}"


def normalize_uuid(value: Any) -> str:
    value = text(value).lower()
    if UUID_RE.match(value):
        return value
    return ""


def normalize_language(value: str) -> str:
    value = (value or "ko").strip().lower()
    return value or "ko"


def normalize_path(value: str) -> str:
    return "/".join(part for part in value.strip().strip("/").split("/") if part)


def first_text(*values: Any) -> str:
    for value in values:
        value = text(value)
        if value:
            return value
    return ""


def int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


if __name__ == "__main__":
    sys.exit(main())
