#!/usr/bin/env python3
"""Export encrypted R ecosystem read payloads into the web-R CDN checkout."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import hmac
import http.client
import io
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


ENCRYPTED_SCHEMA = "web-r.r-ecosystem.encrypted.v1"
CONTENT_SCHEMA = "web-r.r-ecosystem.content.plain.v1"
MANIFEST_SCHEMA = "web-r.r-ecosystem.manifest.plain.v1"
KEY_PURPOSE = "web-r:r-ecosystem-content:v1"
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
DATE_RE = re.compile(r"(\d{4})-(\d{2})")
TRANSIENT_CLICKHOUSE_EXPORT_CATEGORIES = {
    "TIMEOUT_EXCEEDED",
    "NOT_INITIALIZED",
    "TOO_MANY_SIMULTANEOUS_QUERIES",
    "KEEPER_EXCEPTION",
    "TABLE_IS_READ_ONLY",
    "CLICKHOUSE_NETWORK",
    "INCOMPLETE_READ",
    "HTTP_CLIENT_ERROR",
    "CODE_159",
    "CODE_202",
    "CODE_667",
}
FATAL_CLICKHOUSE_EXPORT_CATEGORIES = {
    "ACCESS_DENIED",
    "UNKNOWN_TABLE",
    "UNKNOWN_IDENTIFIER",
    "SYNTAX_ERROR",
    "NO_SUCH_COLUMN",
    "TYPE_MISMATCH",
    "CANNOT_PARSE",
    "BAD_ARGUMENTS",
}


class ClickHouseExportError(RuntimeError):
    def __init__(self, query_name: str, status_code: int, category: str, detail: str = "") -> None:
        self.query_name = safe_query_name(query_name)
        self.status_code = int(status_code or 0)
        self.category = clickhouse_error_category(category or detail)
        self.detail = detail
        super().__init__(str(self))

    def __str__(self) -> str:
        if self.status_code:
            return f"ClickHouse export query failed ({self.query_name}): HTTP {self.status_code} {self.category}"
        return f"ClickHouse export query failed ({self.query_name}): {self.category}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="web_r_go .env path")
    parser.add_argument("--cdn-root", default="../web-r_CDN2_contents", help="web-r_CDN2_contents checkout path")
    parser.add_argument("--language", default="ko", help="content language code")
    parser.add_argument("--limit", type=int, default=0, help="optional row limit for smoke exports")
    parser.add_argument("--dry-run", action="store_true", help="query and encrypt without writing files")
    args = parser.parse_args()

    repo_root = Path.cwd()
    env = load_env(repo_root / args.env)
    language = normalize_language(args.language)
    key = derive_key(content_secret(env))
    cdn_root = (repo_root / args.cdn_root).resolve()

    try:
        community_rows = fetch_json_rows(env, community_sql(args.limit), query_name="r_ecosystem_community")
        article_rows = fetch_json_rows(env, article_sql(args.limit), query_name="r_ecosystem_article")
    except ClickHouseExportError as exc:
        if env_bool(env, "R_ECOSYSTEM_CDN_EXPORT_TRANSIENT_FAIL_OPEN", True) and is_transient_clickhouse_export_failure(exc.status_code, exc.category, exc.detail):
            print(
                f"[warn] R ecosystem CDN export deferred query={exc.query_name} reason={exc.category}",
                file=sys.stderr,
            )
            print(json.dumps(deferred_export_result(exc), ensure_ascii=False))
            return 0
        raise SystemExit(str(exc)) from exc

    payloads: dict[str, dict[str, Any]] = {}
    manifest_items: dict[str, dict[str, str]] = {}
    duplicate_count = 0

    for row in community_rows:
        uuid = normalize_uuid(row.get("item_uuid"))
        if not uuid:
            continue
        if uuid in payloads:
            duplicate_count += 1
            continue
        payload = {
            "schema": CONTENT_SCHEMA,
            "kind": "community",
            "community_item": {
                "item_uuid": uuid,
                "external_id": text(row.get("external_id")),
                "source_id": text(row.get("source_id")),
                "source_name": text(row.get("source_name")),
                "source_type": text(row.get("source_type")),
                "platform": text(row.get("platform")),
                "source_url": text(row.get("source_url")),
                "canonical_url": text(row.get("canonical_url")),
                "title": text(row.get("title")),
                "summary": text(row.get("summary")),
                "author": text(row.get("author")),
                "language": text(row.get("language")) or language,
                "tags_json": text(row.get("tags_json")),
                "raw_json": text(row.get("raw_json")),
                "payload_json": text(row.get("payload_json")),
                "published_at": text(row.get("published_at_text")),
                "collected_at": text(row.get("collected_at_text")),
            },
        }
        published_at = first_text(payload["community_item"]["published_at"], payload["community_item"]["collected_at"])
        register_payload(payloads, manifest_items, uuid, "community", language, published_at, payload, {
            "item_kind": "community",
            "source_id": payload["community_item"]["source_id"],
            "source_name": payload["community_item"]["source_name"],
            "source_type": payload["community_item"]["source_type"],
            "platform": payload["community_item"]["platform"],
            "title": payload["community_item"]["title"],
            "summary": payload["community_item"]["summary"],
            "author": payload["community_item"]["author"],
            "canonical_url": payload["community_item"]["canonical_url"],
        })

    for row in article_rows:
        uuid = normalize_uuid(row.get("uuid"))
        if not uuid:
            continue
        if uuid in payloads:
            duplicate_count += 1
            continue
        source = text(row.get("source"))
        created_at = text(row.get("created_at"))
        payload = {
            "schema": CONTENT_SCHEMA,
            "kind": "article",
            "article": {
                "source": source,
                "uuid": uuid,
                "title": text(row.get("title")),
                "content": text(row.get("content")),
                "url": text(row.get("url")),
                "internal_url": f"/r-ecosystem/read/{uuid}/",
                "created_at": created_at,
                "author": text(row.get("author")),
                "platform": text(row.get("platform")),
                "category": text(row.get("category")),
                "language": text(row.get("language")) or language,
            },
        }
        register_payload(payloads, manifest_items, uuid, "article", language, created_at, payload, {
            "item_kind": "article",
            "article_source": source,
            "source_id": source,
            "source_name": source_label(source),
            "source_type": article_category_key(source),
            "platform": payload["article"]["platform"],
            "title": payload["article"]["title"],
            "summary": payload["article"]["content"],
            "author": payload["article"]["author"],
            "canonical_url": payload["article"]["url"],
        })

    manifest = {
        "schema": MANIFEST_SCHEMA,
        "language": language,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "items": manifest_items,
    }

    if args.dry_run:
        print(json.dumps(export_result(len(community_rows), len(article_rows), len(payloads), duplicate_count), ensure_ascii=False))
        return 0

    for uuid, payload in payloads.items():
        item = manifest_items[uuid]
        rel_path = item["path"]
        encrypted = encrypt_document(payload, key, rel_path, language, uuid)
        write_json_atomic(cdn_root / rel_path, encrypted)

    manifest_path = f"contents/{language}/index.json"
    encrypted_manifest = encrypt_document(manifest, key, manifest_path, language, "")
    write_json_atomic(cdn_root / manifest_path, encrypted_manifest)

    print(json.dumps(export_result(len(community_rows), len(article_rows), len(payloads), duplicate_count), ensure_ascii=False))
    return 0


def export_result(community_count: int, article_count: int, export_count: int, duplicate_count: int) -> dict[str, Any]:
    return {
        "community": community_count,
        "article": article_count,
        "export": export_count,
        "duplicates": duplicate_count,
        "export_deferred": False,
    }


def deferred_export_result(exc: ClickHouseExportError) -> dict[str, Any]:
    result = export_result(0, 0, 0, 0)
    result.update(
        {
            "export_deferred": True,
            "deferred_query": exc.query_name,
            "deferred_reason": exc.category,
            "deferred_http_status": exc.status_code,
        }
    )
    return result


def register_payload(payloads: dict[str, dict[str, Any]], manifest_items: dict[str, dict[str, str]], uuid: str, kind: str, language: str, published_at: str, payload: dict[str, Any], meta: dict[str, str]) -> None:
    year, month = published_year_month(published_at)
    rel_path = f"contents/{language}/{year}/{month}/{uuid}.json"
    payloads[uuid] = payload
    manifest_item = {
        "uuid": uuid,
        "kind": kind,
        "language": language,
        "published_at": published_at,
        "year": year,
        "month": month,
        "path": rel_path,
    }
    for key, value in meta.items():
        value = text(value)
        if value:
            manifest_item[key] = value
    manifest_items[uuid] = manifest_item


def source_label(source: str) -> str:
    source = text(source).lower()
    if source == "rblogger":
        return "R-Blogger"
    if source == "rproject":
        return "R Project"
    return source


def article_category_key(source: str) -> str:
    source = text(source).lower()
    if source == "rblogger":
        return "aggregator_blog"
    if source == "rproject":
        return "official_blog"
    return ""


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
    raise SystemExit("R ecosystem content key is not configured")


def derive_key(secret: str) -> bytes:
    return hashlib.sha256((secret.strip() + "\0" + KEY_PURPOSE).encode("utf-8")).digest()


def encrypt_document(plain: dict[str, Any], key: bytes, rel_path: str, language: str, uuid: str, compress: bool = False) -> dict[str, Any]:
    plaintext = json.dumps(plain, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    payload = gzip_stable(plaintext) if compress else plaintext
    nonce = hmac.new(key, normalize_path(rel_path).encode("utf-8") + b"\0" + payload, hashlib.sha256).digest()[:12]
    ciphertext = AESGCM(key).encrypt(nonce, payload, normalize_path(rel_path).encode("utf-8"))
    doc = {
        "schema": ENCRYPTED_SCHEMA,
        "alg": "AES-256-GCM",
        "kdf": "SHA256(secret+purpose:v1)",
        "language": language,
        "path": normalize_path(rel_path),
        "nonce": b64url(nonce),
        "ciphertext": b64url(ciphertext),
    }
    if compress:
        doc["compression"] = "gzip"
    if uuid:
        doc["uuid"] = uuid
    return doc


def gzip_stable(data: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, compresslevel=9, mtime=0) as gz_file:
        gz_file.write(data)
    return buffer.getvalue()


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False) as tmp:
        tmp.write(body)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def fetch_json_rows(env: dict[str, str], sql: str, query_name: str = "query") -> list[dict[str, Any]]:
    user = env.get("CLICKHOUSE_USER", "").strip()
    password = env.get("CLICKHOUSE_PASSWORD", "")
    if not user:
        raise SystemExit("ClickHouse connection environment is incomplete")
    url = build_clickhouse_url(
        env,
        default_format="JSONEachRow",
        max_execution_time=env.get("R_ECOSYSTEM_CDN_CH_MAX_EXECUTION_TIME", "120"),
        max_threads=env.get("R_ECOSYSTEM_CDN_CH_MAX_THREADS", "2"),
    )
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=int(env.get("R_ECOSYSTEM_CDN_HTTP_TIMEOUT", "150"))) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read(800).decode("utf-8", errors="replace")
        category = clickhouse_error_category(detail)
        raise ClickHouseExportError(query_name, exc.code, category, detail) from exc
    except urllib.error.URLError as exc:
        raise ClickHouseExportError(query_name, 0, "CLICKHOUSE_NETWORK", str(exc)) from exc
    except http.client.IncompleteRead as exc:
        raise ClickHouseExportError(query_name, 0, "INCOMPLETE_READ", str(exc)) from exc
    except http.client.HTTPException as exc:
        raise ClickHouseExportError(query_name, 0, "HTTP_CLIENT_ERROR", str(exc)) from exc
    except TimeoutError as exc:
        raise ClickHouseExportError(query_name, 0, "TIMEOUT_EXCEEDED", str(exc)) from exc
    except OSError as exc:
        raise ClickHouseExportError(query_name, 0, "CLICKHOUSE_NETWORK", str(exc)) from exc
    rows: list[dict[str, Any]] = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def safe_query_name(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.:-]+", "_", text(value))[:80]
    return value or "query"


def clickhouse_error_category(detail: str) -> str:
    upper = text(detail).strip().upper()
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", upper) or re.fullmatch(r"CODE_\d+", upper):
        return upper
    for marker in (
        "TIMEOUT_EXCEEDED",
        "NOT_INITIALIZED",
        "MEMORY_LIMIT_EXCEEDED",
        "TOO_MANY_SIMULTANEOUS_QUERIES",
        "KEEPER_EXCEPTION",
        "TABLE_IS_READ_ONLY",
        "ACCESS_DENIED",
        "UNKNOWN_TABLE",
        "UNKNOWN_IDENTIFIER",
        "SYNTAX_ERROR",
        "NO_SUCH_COLUMN",
        "TYPE_MISMATCH",
        "CANNOT_PARSE",
        "BAD_ARGUMENTS",
        "CLICKHOUSE_NETWORK",
        "INCOMPLETE_READ",
        "HTTP_CLIENT_ERROR",
    ):
        if marker in upper:
            return marker
    if "MAX_EXECUTION_TIME" in upper or "TIMEOUT EXCEEDED" in upper or "TIMEOUT" in upper:
        return "TIMEOUT_EXCEEDED"
    if "MEMORY LIMIT" in upper:
        return "MEMORY_LIMIT_EXCEEDED"
    match = re.search(r"CODE:\s*(\d+)", detail, re.IGNORECASE)
    if match:
        return f"CODE_{match.group(1)}"
    return "CLICKHOUSE_ERROR"


def is_transient_clickhouse_export_failure(status_code: int, category: str, detail: str = "") -> bool:
    category = clickhouse_error_category(category or detail)
    detail_upper = text(detail).upper()
    if category in FATAL_CLICKHOUSE_EXPORT_CATEGORIES:
        return False
    if any(marker in detail_upper for marker in FATAL_CLICKHOUSE_EXPORT_CATEGORIES):
        return False
    if category in TRANSIENT_CLICKHOUSE_EXPORT_CATEGORIES:
        return True
    if "TIMEOUT" in detail_upper or "NOT INITIALIZED" in detail_upper or "SERVER IS OVERLOADED" in detail_upper:
        return True
    return int(status_code or 0) in {408, 429, 500, 502, 503, 504}


def env_bool(env: dict[str, str], key: str, default: bool = False) -> bool:
    value = text(env.get(key, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def community_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT external_id,
       toString(item_uuid) AS item_uuid,
       source_id,
       source_name,
       source_type,
       platform,
       source_url,
       canonical_url,
       coalesce(
           nullIf(
               multiIf(
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'Daily News about R-devel/NEWS') > 0,
                       'R-devel/NEWS 일일 업데이트',
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'EMODnet Biology Geospatial R Tutorials') > 0,
                       'EMODnet Biology 지리공간 R 튜토리얼',
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'R Conferences and Meetups') > 0,
                       'R 컨퍼런스와 모임 목록',
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'Conference') > 0
                       AND positionCaseInsensitiveUTF8(title, 'Events') > 0,
                       '컨퍼런스와 이벤트',
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'dslc.io') > 0,
                       'Data Science Learning Community 참여 안내',
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'coolbutuseless') > 0,
                       'R에서 그린 실시간 숲 애니메이션',
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'aRtsy_package') > 0,
                       'R과 ggplot2로 생성한 오늘의 아트워크',
                   source_id = 'community:rweekly'
                       AND positionCaseInsensitiveUTF8(title, 'CougarStats') > 0,
                       'CougarStats: 통계 교육용 오픈소스 웹 앱',
                   ''
               ),
               ''
           ),
           nullIf(JSONExtractString(payload_json, 'title_ko'), ''),
           nullIf(JSONExtractString(raw_json, 'title_ko'), ''),
           title
       ) AS title,
       coalesce(
           nullIf(
               multiIf(
                   source_id = 'community:rweekly'
                       AND (
                           positionCaseInsensitiveUTF8(summary, 'Discovered from') > 0
                           OR positionCaseInsensitiveUTF8(summary, 'discovered') > 0
                           OR positionCaseInsensitiveUTF8(summary, '발견') > 0
                           OR positionCaseInsensitiveUTF8(summary, 'targets vs dbt') > 0
                           OR positionCaseInsensitiveUTF8(summary, 'Shiny webAwesome') > 0
                           OR positionCaseInsensitiveUTF8(summary, 'ggplot2 트릭') > 0
                           OR positionCaseInsensitiveUTF8(summary, 'knitr') > 0
                           OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'discovered') > 0
                           OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), '발견') > 0
                           OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'S at 50') > 0
                           OR (
                               positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'R Weekly') > 0
                               AND (
                                   positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'targets vs dbt') > 0
                                   OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'Shiny webAwesome') > 0
                                   OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'ggplot2 트릭') > 0
                                   OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'knitr') > 0
                               )
                           )
                       ),
                       concat(
                           'R Weekly에서 소개된 ',
                           multiIf(
                               positionCaseInsensitiveUTF8(title, 'Daily News about R-devel/NEWS') > 0 OR positionCaseInsensitiveUTF8(title, 'R-devel/NEWS') > 0, 'R-devel/NEWS 변경 사항',
                               positionCaseInsensitiveUTF8(title, 'EMODnet Biology Geospatial R Tutorials') > 0, 'EMODnet Biology 지리공간 R 튜토리얼',
                               positionCaseInsensitiveUTF8(title, 'R Conferences and Meetups') > 0, 'R 컨퍼런스와 모임',
                               positionCaseInsensitiveUTF8(title, 'Conference') > 0 AND positionCaseInsensitiveUTF8(title, 'Events') > 0, '컨퍼런스와 이벤트',
                               positionCaseInsensitiveUTF8(title, 'dslc.io') > 0, 'Data Science Learning Community',
                               positionCaseInsensitiveUTF8(title, 'coolbutuseless') > 0, 'R 실시간 숲 애니메이션',
                               positionCaseInsensitiveUTF8(title, 'aRtsy_package') > 0, 'R과 ggplot2 아트워크',
                               positionCaseInsensitiveUTF8(title, 'CougarStats') > 0, '통계 교육용 오픈소스 웹 앱 CougarStats',
                               notEmpty(JSONExtractString(raw_json, 'link_text')) AND lengthUTF8(JSONExtractString(raw_json, 'link_text')) <= 120, JSONExtractString(raw_json, 'link_text'),
                               title
                           ),
                           ' 관련 자료입니다. 원문에서 자세한 내용을 확인할 수 있습니다.'
                       ),
                   ''
               ),
               ''
           ),
           nullIf(JSONExtractString(payload_json, 'summary_ko'), ''),
           nullIf(JSONExtractString(raw_json, 'summary_ko'), ''),
           nullIf(JSONExtractString(payload_json, 'content_ko'), ''),
           nullIf(JSONExtractString(raw_json, 'content_ko'), ''),
           summary
       ) AS summary,
       author,
       if(
           notEmpty(JSONExtractString(payload_json, 'title_ko'))
           OR notEmpty(JSONExtractString(payload_json, 'summary_ko'))
           OR notEmpty(JSONExtractString(payload_json, 'content_ko'))
           OR notEmpty(JSONExtractString(raw_json, 'title_ko'))
           OR notEmpty(JSONExtractString(raw_json, 'summary_ko'))
           OR notEmpty(JSONExtractString(raw_json, 'content_ko'))
           OR (
               source_id = 'community:rweekly'
               AND (
                   positionCaseInsensitiveUTF8(summary, 'Discovered from') > 0
                   OR positionCaseInsensitiveUTF8(summary, 'discovered') > 0
                   OR positionCaseInsensitiveUTF8(summary, '발견') > 0
                   OR positionCaseInsensitiveUTF8(summary, 'targets vs dbt') > 0
                   OR positionCaseInsensitiveUTF8(summary, 'Shiny webAwesome') > 0
                   OR positionCaseInsensitiveUTF8(summary, 'ggplot2 트릭') > 0
                   OR positionCaseInsensitiveUTF8(summary, 'knitr') > 0
                   OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'discovered') > 0
                   OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), '발견') > 0
                   OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'S at 50') > 0
                   OR (
                       positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'R Weekly') > 0
                       AND (
                           positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'targets vs dbt') > 0
                           OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'Shiny webAwesome') > 0
                           OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'ggplot2 트릭') > 0
                           OR positionCaseInsensitiveUTF8(JSONExtractString(payload_json, 'summary_ko'), 'knitr') > 0
                       )
                   )
               )
           ),
           'ko',
           language
       ) AS language,
       tags_json,
       toString(raw_json) AS raw_json,
       toString(payload_json) AS payload_json,
       if(isNull(original_published_at), '', formatDateTime(original_published_at, '%Y-%m-%d %H:%i:%S')) AS published_at_text,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at_text
  FROM Data_R_Community_Service.r_community_item_read_current
 WHERE active = 1
   AND notEmpty(coalesce(nullIf(JSONExtractString(payload_json, 'title_ko'), ''), nullIf(JSONExtractString(raw_json, 'title_ko'), ''), title))
   AND notEmpty(canonical_url)
   AND source_type IN ('official_release_notes', 'official_blog', 'official_journal', 'organization_social', 'organization_blog', 'newsletter', 'bot_feed')
   AND source_id NOT IN ('official:r-mail:r-packages')
   AND positionCaseInsensitive(canonical_url, 'journal.r-project.org/issues.html') = 0
   AND positionCaseInsensitive(canonical_url, 'community.rstudio.com/c/irl') = 0
   AND positionCaseInsensitive(canonical_url, 'forum.posit.co/c/irl') = 0
   AND NOT (
       source_id = 'community:rweekly'
       AND (
           positionCaseInsensitive(canonical_url, 'bsky.app/profile') > 0
           OR positionCaseInsensitive(canonical_url, 'diffify.com/R/') > 0
           OR positionCaseInsensitive(canonical_url, 'r-universe.dev/search') > 0
           OR positionCaseInsensitive(canonical_url, 'dirk.eddelbuettel.com/cranberries/cran/new/') > 0
           OR positionCaseInsensitive(canonical_url, 'jumpingrivers.github.io/meetingsR/events.html') > 0
           OR positionCaseInsensitive(canonical_url, 'developer.r-project.org/blosxom.cgi/R-devel/NEWS') > 0
           OR positionCaseInsensitive(canonical_url, 'serve.podhome.fm/r-weekly-highlights') > 0
           OR positionCaseInsensitive(canonical_url, 'forum.posit.co/c/irl') > 0
           OR positionCaseInsensitive(canonical_url, 'community.rstudio.com/c/irl') > 0
           OR positionCaseInsensitive(canonical_url, 'journal.r-project.org/issues.html') > 0
           OR positionCaseInsensitive(canonical_url, 'youtube.com') > 0
           OR positionCaseInsensitive(canonical_url, 'youtu.be') > 0
           OR positionCaseInsensitive(canonical_url, 'youtube-nocookie.com') > 0
           OR positionCaseInsensitiveUTF8(concat(title, ' ', summary, ' ', JSONExtractString(payload_json, 'title_ko'), ' ', JSONExtractString(payload_json, 'summary_ko')), 'R Conferences and Meetups') > 0
           OR (
               positionCaseInsensitiveUTF8(concat(title, ' ', summary, ' ', JSONExtractString(payload_json, 'title_ko'), ' ', JSONExtractString(payload_json, 'summary_ko')), 'Conference') > 0
               AND positionCaseInsensitiveUTF8(concat(title, ' ', summary, ' ', JSONExtractString(payload_json, 'title_ko'), ' ', JSONExtractString(payload_json, 'summary_ko')), 'Events') > 0
           )
           OR (
               positionCaseInsensitiveUTF8(concat(title, ' ', summary, ' ', JSONExtractString(payload_json, 'title_ko'), ' ', JSONExtractString(payload_json, 'summary_ko')), '컨퍼런스') > 0
               AND positionCaseInsensitiveUTF8(concat(title, ' ', summary, ' ', JSONExtractString(payload_json, 'title_ko'), ' ', JSONExtractString(payload_json, 'summary_ko')), '이벤트') > 0
           )
           OR (
               positionCaseInsensitive(canonical_url, 'dirk.eddelbuettel.com/blog/') > 0
               AND position(canonical_url, '#') > 0
           )
       )
   )
 ORDER BY item_uuid ASC, version DESC, collected_at DESC, ingested_at DESC
 LIMIT 1 BY item_uuid{suffix}
 FORMAT JSONEachRow
"""


def article_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT a.source,
       toString(a.uuid) AS uuid,
       coalesce(a.title, '') AS title,
       coalesce(a.content, '') AS content,
       if(a.source = 'rblogger',
          coalesce(
             nullIf(if(positionCaseInsensitive(coalesce(raw.canonical_url, ''), 'r-bloggers.com') > 0, coalesce(raw.canonical_url, ''), ''), ''),
             nullIf(if(positionCaseInsensitive(coalesce(raw.source_url, ''), 'r-bloggers.com') > 0, coalesce(raw.source_url, ''), ''), ''),
             nullIf(if(positionCaseInsensitive(coalesce(rb.url, ''), 'r-bloggers.com') > 0, coalesce(rb.url, ''), ''), ''),
             nullIf(if(positionCaseInsensitive(coalesce(a.url, ''), 'r-bloggers.com') > 0, coalesce(a.url, ''), ''), ''),
             nullIf(raw.canonical_url, ''), nullIf(raw.source_url, ''), nullIf(rb.url, ''), a.url, ''
          ),
          coalesce(a.url, '')
       ) AS url,
       if(a.source = 'rblogger',
          if(isNull(raw.article_published_at),
             if(rb.article_dt_utc IS NULL,
                if(isNull(a.created_at), '', formatDateTime(a.created_at, '%Y-%m-%d %H:%i:%S')),
                formatDateTime(toTimeZone(rb.article_dt_utc, 'Asia/Seoul'), '%Y-%m-%d %H:%i:%S')
             ),
             formatDateTime(raw.article_published_at, '%Y-%m-%d %H:%i:%S')
          ),
          if(isNull(a.created_at), '', formatDateTime(a.created_at, '%Y-%m-%d %H:%i:%S'))
       ) AS created_at,
       if(a.source = 'rblogger', coalesce(raw.article_author, ''), multiIf(a.source = 'rproject', 'R Project', a.source)) AS author,
       multiIf(a.source = 'rblogger', 'R-Blogger', a.source = 'rproject', 'R Project', 'Web-R') AS platform,
       multiIf(a.source = 'rblogger', '블로그·해설', a.source = 'rproject', '공식 발표', '게시판') AS category,
       a.language_code AS language
  FROM webr_board.v_article a
  LEFT JOIN Data_R_Community_Service.v_rblogger rb ON rb.uuid = a.uuid
  LEFT JOIN
  (
      SELECT uuid,
             argMax(canonical_url, created_at_key) AS canonical_url,
             argMax(url, created_at_key) AS source_url,
             argMax(article_author, created_at_key) AS article_author,
             argMax(article_published_at, created_at_key) AS article_published_at
        FROM Data_R_Community_Raw.r_blogger_article_raw
       WHERE active = 1
       GROUP BY uuid
  ) raw ON raw.uuid = a.uuid
 WHERE a.source IN ('rblogger', 'rproject')
   AND a.language_code = 'ko'
 ORDER BY if(a.source = 'rblogger' AND rb.article_dt_utc IS NOT NULL, toUnixTimestamp(rb.article_dt_utc), toUnixTimestamp(a.created_at)) DESC,
          a.created_at DESC{suffix}
 FORMAT JSONEachRow
"""


def published_year_month(value: str) -> tuple[str, str]:
    match = DATE_RE.search(value or "")
    if match:
        return match.group(1), match.group(2)
    now = datetime.now()
    return f"{now.year:04d}", f"{now.month:02d}"


def normalize_uuid(value: Any) -> str:
    text_value = text(value).lower()
    if UUID_RE.match(text_value):
        return text_value
    return ""


def normalize_language(value: str) -> str:
    value = (value or "ko").strip().lower()
    return value or "ko"


def normalize_path(value: str) -> str:
    return "/".join(part for part in value.strip().strip("/").split("/") if part)


def first_text(*values: str) -> str:
    for value in values:
        value = text(value)
        if value:
            return value
    return ""


def text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


if __name__ == "__main__":
    sys.exit(main())
