#!/usr/bin/env python3
"""Export selected community rows as encrypted web-R CDN payloads."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.client
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
from workspace_paths import workspace_repo
from export_r_ecosystem_cdn import (
    ClickHouseExportError,
    clickhouse_error_category,
    env_bool,
    is_transient_clickhouse_export_failure,
)


ENCRYPTED_SCHEMA = "web-r.community.encrypted.v1"
MANIFEST_SCHEMA = "web-r.community.manifest.plain.v1"
CONTENT_SCHEMA = "web-r.community.content.plain.v1"
WORKSHOP_MANIFEST_SCHEMA = "web-r.community.workshop.manifest.plain.v1"
WORKSHOP_CONTENT_SCHEMA = "web-r.community.workshop.content.plain.v1"
WORKSHOP_SOURCE_PARITY_SCHEMA = "web-r.community.workshop.source-parity.v1"
WORKSHOP_WITHDRAWAL_AUTHORITY_SCHEMA = "web-r.community.workshop.withdrawal-authority.v1"
WORKSHOP_AUTHORITY_REGISTRY_SCHEMA = "web-r.community.workshop.authority-registry.v1"
KEY_PURPOSE = "web-r:community-content:v1"
R_COMMUNITY_BOT_UUID = "019e1127-f5d7-7304-a916-31914e58e1e9"
R_COMMUNITY_BOT_NAME = "R Community"
R_COMMUNITY_BOT_ROLE = "Bot"
NOTEBOOK_BOT_UUID = "7b1c9fc4-7216-44cb-81b8-5fe17f2158bc"
NOTEBOOK_BOT_NAME = "Web-R Notebook"
NOTEBOOK_BOT_ROLE = "Bot"
R_PROJECT_BOT_UUID = "2aeeb31a-5cb1-47d8-bbb0-cb2d271c32ce"
R_PROJECT_BOT_NAME = "R Project"
R_PROJECT_BOT_ROLE = "Bot"
R_PROJECT_CONFERENCE_ID = "official:r:conferences"
POSIT_COMMUNITY_EVENTS_ID = "community:posit:events"
USE_R2026_WORKSHOP_KEY = "rconf-user-2026"
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DATE_RE = re.compile(r"(\d{4})-(\d{2})")
SAFE_ID_RE = re.compile(r"[^0-9A-Za-z._-]+")
MONTH_LOOKUP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="web_r_go .env path")
    parser.add_argument("--cdn-root", default=str(workspace_repo("web-r_CDN2_community")), help="web-r_CDN2_community checkout path")
    parser.add_argument("--language", default="ko", help="content language code")
    parser.add_argument("--limit", type=int, default=0, help="optional per-source row limit for smoke exports")
    parser.add_argument("--dry-run", action="store_true", help="query and encrypt without writing files")
    parser.add_argument(
        "--workshop-withdrawal-authority-manifest",
        default="",
        help="explicit durable JSON authority for an empty Posit workshop source set",
    )
    parser.add_argument(
        "--workshop-authority-registry",
        default="",
        help="published Posit workshop authority registry (defaults inside the CDN checkout)",
    )
    args = parser.parse_args()

    repo_root = Path.cwd()
    env = load_env(repo_root / args.env)
    language = normalize_language(args.language)
    key = derive_key(content_secret(env))
    cdn_root = (repo_root / args.cdn_root).resolve()
    workshop_scope = workshop_source_scope(language)
    workshop_authority_registry_path = resolve_optional_path(repo_root, args.workshop_authority_registry)
    if workshop_authority_registry_path is None:
        workshop_authority_registry_path = cdn_root / f"community/{language}/workshop/source-authority.json"
    workshop_withdrawal_authority_path = resolve_optional_path(repo_root, args.workshop_withdrawal_authority_manifest)

    try:
        digest_rows = fetch_json_rows(env, digest_sql(args.limit), query_name="community_digest")
        notebook_rows = fetch_json_rows(env, notebook_sql(args.limit), query_name="community_notebook")
        workshop_rows = fetch_json_rows(env, workshop_sql(args.limit), query_name="community_workshop")
        workshop_event_rows = fetch_json_rows(env, workshop_event_sql(args.limit), query_name="community_workshop_event")
        workshop_post_rows = fetch_json_rows(env, workshop_post_sql(args.limit), query_name="community_workshop_post")
    except ClickHouseExportError as exc:
        if env_bool(env, "WEBR_COMMUNITY_CDN_EXPORT_TRANSIENT_FAIL_OPEN", True) and is_transient_clickhouse_export_failure(exc.status_code, exc.category, exc.detail):
            print(
                f"[warn] Web-R community CDN export deferred query={exc.query_name} reason={exc.category}",
                file=sys.stderr,
            )
            print(json.dumps(deferred_community_export_result(exc), ensure_ascii=False))
            return 0
        raise SystemExit(str(exc)) from exc

    payloads: dict[str, tuple[str, dict[str, Any]]] = {}
    manifest_items: dict[str, dict[str, Any]] = {}
    workshop_manifest_items: dict[str, dict[str, Any]] = {}
    workshop_posts: dict[str, list[dict[str, Any]]] = {}
    duplicate_count = 0
    posit_workshop_source_identities = workshop_posit_source_identity_set(workshop_event_rows)

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

    for row in workshop_post_rows:
        post = workshop_post_item(row)
        workshop_key = text(post.get("workshop_key"))
        if not workshop_key:
            continue
        workshop_posts.setdefault(workshop_key, []).append(post)

    for row in workshop_rows:
        item = workshop_catalog_item(row, language)
        uuid = text(item.get("uuid"))
        if not uuid:
            continue
        if uuid in workshop_manifest_items:
            duplicate_count += 1
            continue
        rel_path = workshop_payload_path(language, uuid, first_text(item.get("starts_at"), item.get("updated_at"), item.get("published_at")))
        item["path"] = rel_path
        item["url"] = f"/workshop/read/{urllib.parse.quote(uuid)}/"
        posts = workshop_posts.get(text(item.get("board_key")), [])
        payloads["workshop:" + uuid] = (rel_path, {"schema": WORKSHOP_CONTENT_SCHEMA, "workshop": item, "posts": posts})
        workshop_manifest_items[uuid] = item

    for row in workshop_event_rows:
        item = workshop_event_item(row, language)
        uuid = text(item.get("uuid"))
        if not uuid:
            continue
        source_identity = workshop_source_identity(row) if is_posit_workshop_event(row) else ""
        if uuid in workshop_manifest_items:
            if source_identity:
                add_workshop_source_representation(workshop_manifest_items[uuid], source_identity)
            continue
        if source_identity:
            add_workshop_source_representation(item, source_identity)
        rel_path = workshop_payload_path(language, uuid, first_text(item.get("starts_at"), item.get("updated_at"), item.get("published_at")))
        item["path"] = rel_path
        item["url"] = f"/workshop/read/{urllib.parse.quote(uuid)}/"
        posts = workshop_posts.get(text(item.get("board_key")), [])
        payloads["workshop:" + uuid] = (rel_path, {"schema": WORKSHOP_CONTENT_SCHEMA, "workshop": item, "posts": posts})
        workshop_manifest_items[uuid] = item

    posit_workshop_represented_identities = workshop_represented_source_identity_set(workshop_manifest_items)
    require_workshop_source_parity(
        posit_workshop_source_identities,
        posit_workshop_represented_identities,
    )
    current_workshop_authority = load_workshop_authority_registry(
        workshop_authority_registry_path,
        workshop_scope,
    )
    workshop_authority_revision, empty_withdrawal_authorized = authorize_empty_workshop_source(
        source_identities=posit_workshop_source_identities,
        scope=workshop_scope,
        current_registry=current_workshop_authority,
        authority_manifest_path=workshop_withdrawal_authority_path,
    )
    workshop_source_parity = workshop_source_parity_receipt(
        source_identities=posit_workshop_source_identities,
        represented_identities=posit_workshop_represented_identities,
        scope=workshop_scope,
        authority_revision=workshop_authority_revision,
        empty_withdrawal_authorized=empty_withdrawal_authorized,
    )

    manifest = {
        "schema": MANIFEST_SCHEMA,
        "language": language,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "items": manifest_items,
    }
    workshop_manifest = {
        "schema": WORKSHOP_MANIFEST_SCHEMA,
        "language": language,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "catalog_token": workshop_catalog_token(workshop_manifest_items, workshop_posts),
        "source_parity": workshop_source_parity,
        "items": workshop_manifest_items,
    }
    current_payload_paths = {rel_path for rel_path, _payload in payloads.values()}

    if args.dry_run:
        print(
            json.dumps(
                community_export_result(
                    len(digest_rows),
                    len(notebook_rows),
                    len(manifest_items),
                    len(workshop_manifest_items),
                    sum(len(posts) for posts in workshop_posts.values()),
                    len(workshop_manifest_items),
                    len(payloads),
                    duplicate_count,
                    len(posit_workshop_source_identities),
                    len(posit_workshop_represented_identities),
                    workshop_source_parity,
                ),
                ensure_ascii=False,
            )
        )
        return 0

    for uuid, (rel_path, payload) in payloads.items():
        encrypted = encrypt_document(payload, key, rel_path, language, encrypted_doc_uuid(payload, uuid))
        write_json_atomic(cdn_root / rel_path, encrypted)

    manifest_path = f"community/{language}/index.json"
    encrypted_manifest = encrypt_document(manifest, key, manifest_path, language, "")
    write_json_atomic(cdn_root / manifest_path, encrypted_manifest)
    workshop_manifest_path = f"community/{language}/workshop/index.json"
    encrypted_workshop_manifest = encrypt_document(workshop_manifest, key, workshop_manifest_path, language, "")
    write_json_atomic(cdn_root / workshop_manifest_path, encrypted_workshop_manifest)
    write_json_atomic(
        workshop_authority_registry_path,
        workshop_authority_registry(workshop_source_parity),
    )
    pruned_payloads = 0
    if args.limit <= 0:
        pruned_payloads = prune_stale_notebook_payloads(cdn_root, language, current_payload_paths)

    result = community_export_result(
        len(digest_rows),
        len(notebook_rows),
        len(manifest_items),
        len(workshop_manifest_items),
        sum(len(posts) for posts in workshop_posts.values()),
        len(workshop_manifest_items),
        len(payloads),
        duplicate_count,
        len(posit_workshop_source_identities),
        len(posit_workshop_represented_identities),
        workshop_source_parity,
    )
    result["pruned_payloads"] = pruned_payloads
    print(json.dumps(result, ensure_ascii=False))
    return 0


def community_export_result(
    digest_count: int,
    notebook_count: int,
    community_export_count: int,
    workshop_count: int,
    workshop_post_count: int,
    workshop_export_count: int,
    export_count: int,
    duplicate_count: int,
    posit_workshop_source_count: int = 0,
    posit_workshop_export_count: int = 0,
    workshop_source_parity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "digest": digest_count,
        "notebook": notebook_count,
        "community_export": community_export_count,
        "workshop": workshop_count,
        "workshop_posts": workshop_post_count,
        "workshop_export": workshop_export_count,
        "export": export_count,
        "duplicates": duplicate_count,
        "workshop_posit_source": posit_workshop_source_count,
        "workshop_posit_export": posit_workshop_export_count,
        "export_deferred": False,
    }
    if workshop_source_parity is not None:
        result["workshop_source_parity"] = workshop_source_parity
    return result


def deferred_community_export_result(exc: ClickHouseExportError) -> dict[str, Any]:
    result = community_export_result(0, 0, 0, 0, 0, 0, 0, 0)
    result.update(
        {
            "export_deferred": True,
            "deferred_query": exc.query_name,
            "deferred_reason": exc.category,
            "deferred_http_status": exc.status_code,
        }
    )
    return result


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
       formatDateTime(d.created_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS published_at,
       formatDateTime(updated_at, '%Y-%m-%d %H:%i:%S') AS updated_at
  FROM Data_R_Community_Service.v_r_community_daily_digest_latest AS d
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
  FROM
  (
      SELECT *
        FROM
        (
            SELECT *,
                   row_number() OVER
                   (
                       PARTITION BY uuid
                       ORDER BY if(ifNull(active, 0) = 1, 0, 1) ASC,
                                ifNull(updated_at, created_at) DESC
                   ) AS rn,
                   argMax(
                       JSONExtractString(ifNull(updated_log, '{{}}'), 'action'),
                       ifNull(updated_at, created_at)
                   ) OVER (PARTITION BY uuid) AS latest_action
              FROM
              (
                  -- CDN is the public read source, so export from every physical
                  -- replica and deduplicate exact copies. A lagging randomly chosen
                  -- replica must not make a newly published Notebook disappear.
                  SELECT DISTINCT
                         uuid, uuid_share, uuid_user, title, description,
                         share, active, created_at, updated_at, updated_log
                    FROM clusterAllReplicas('statground_cluster', 'webr_webr', 'notebook_local')
                   WHERE uuid_user = toUUID('{NOTEBOOK_BOT_UUID}')
              ) AS replica_versions
        ) AS ranked_versions
       WHERE rn = 1
         AND ifNull(active, 0) = 1
         AND latest_action != 'delete'
  ) AS n
 WHERE coalesce(n.active, 0) = 1
   AND coalesce(n.share, 0) = 1
 ORDER BY n.created_at DESC{suffix}
 FORMAT JSONEachRow
"""


def workshop_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT toString(w.uuid) AS uuid,
       ifNull(w.slug, '') AS slug,
       w.title AS title,
       ifNull(w.subtitle, '') AS subtitle,
       ifNull(w.summary, '') AS summary,
       ifNull(w.description, '') AS description,
       ifNull(w.cover_image_url, '') AS cover_image_url,
       ifNull(w.venue, '') AS venue,
       if(isNull(w.starts_at), '', formatDateTime(w.starts_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul')) AS starts_at,
       if(isNull(w.ends_at), '', formatDateTime(w.ends_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul')) AS ends_at,
       if(isNull(w.capacity), 0, toUInt64(w.capacity)) AS capacity,
       w.status AS status,
       w.registration_mode AS registration_mode,
       if(isNull(w.member_product_uuid), '', toString(w.member_product_uuid)) AS member_product_uuid,
       ifNull(mp.title, '') AS member_product_title,
       ifNull(toInt64(mp.price), 0) AS member_price,
       if(isNull(w.nonmember_product_uuid), '', toString(w.nonmember_product_uuid)) AS nonmember_product_uuid,
       ifNull(np.title, '') AS nonmember_product_title,
       ifNull(toInt64(np.price), 0) AS nonmember_price,
       toUInt8(w.active) AS active,
       toInt32(w.sort_order) AS sort_order,
       ifNull(stats.paid_count, 0) AS paid_count,
       ifNull(stats.total_count, 0) AS total_count,
       ifNull(stats.paid_amount, 0) AS paid_amount,
       if(w.latest_version_at >= now64(3, 'Asia/Seoul') - toIntervalDay(7), 1, 0) AS is_new,
       formatDateTime(w.latest_version_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS updated_at
  FROM webr_workshop.v_workshop AS w
  LEFT JOIN webr_code.product AS mp ON mp.uuid = w.member_product_uuid
  LEFT JOIN webr_code.product AS np ON np.uuid = w.nonmember_product_uuid
  LEFT JOIN
  (
      SELECT
             uuid_workshop,
             countIf(status = 'paid') AS paid_count,
             count() AS total_count,
             sumIf(amount, status = 'paid') AS paid_amount
        FROM webr_workshop.registration
       GROUP BY uuid_workshop
  ) AS stats ON stats.uuid_workshop = w.uuid
 WHERE w.active = 1
 ORDER BY w.sort_order ASC, w.starts_at DESC, w.title ASC{suffix}
 FORMAT JSONEachRow
"""


def workshop_event_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT external_id,
       source_id,
       source_name,
       source_type,
       platform,
       source_url,
       canonical_url,
       title,
       summary,
       tags_json,
       if(isNull(original_published_at), '', formatDateTime(original_published_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul')) AS published_at,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS collected_at,
       toUInt64OrZero(extract(concat(title, ' ', canonical_url, ' ', summary), '([12][0-9]{{3}})')) AS event_year
  FROM Data_R_Community_Service.v_r_community_latest_dedup
 WHERE notEmpty(title)
   AND notEmpty(canonical_url)
   AND (
          (
              source_id = '{R_PROJECT_CONFERENCE_ID}'
              AND title NOT IN ('local copy', 'R: Conferences')
              AND (
                     positionCaseInsensitiveUTF8(concat(title, ' ', canonical_url, ' ', summary), 'useR') > 0
                     OR positionCaseInsensitiveUTF8(concat(title, ' ', canonical_url, ' ', summary), 'DSC') > 0
                     OR positionCaseInsensitiveUTF8(concat(title, ' ', canonical_url, ' ', summary), 'R/Basel') > 0
                     OR positionCaseInsensitiveUTF8(concat(title, ' ', canonical_url, ' ', summary), 'R Summit') > 0
                  )
          )
          OR source_id = '{POSIT_COMMUNITY_EVENTS_ID}'
          OR (
                 platform = 'posit-community'
                 AND has(JSONExtract(tags_json, 'Array(String)'), 'Conferences & Events')
             )
       )
 ORDER BY event_year DESC,
          published_at DESC,
          collected_at DESC,
          title ASC{suffix}
 SETTINGS distributed_product_mode = 'global'
 FORMAT JSONEachRow
"""


def workshop_post_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT external_id,
       source_id,
       source_name,
       source_url,
       canonical_url,
       title,
       summary,
       if(isNull(original_published_at), '', formatDateTime(original_published_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul')) AS published_at,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS collected_at
  FROM Data_R_Community_Service.v_r_community_latest_dedup
 WHERE source_id = 'mastodon:account:user-conf'
   AND notEmpty(canonical_url)
 ORDER BY published_at DESC,
          collected_at DESC,
          title ASC{suffix}
 SETTINGS distributed_product_mode = 'global'
 FORMAT JSONEachRow
"""


def community_payload_path(language: str, kind: str, uuid: str, published_at: str) -> str:
    year, month = published_year_month(published_at)
    return f"community/{language}/{kind}/{year}/{month}/{uuid}.json"


def workshop_payload_path(language: str, uuid: str, published_at: str) -> str:
    year, month = published_year_month(published_at)
    return f"community/{language}/workshop/{year}/{month}/{safe_path_id(uuid)}.json"


def workshop_catalog_item(row: dict[str, Any], language: str) -> dict[str, Any]:
    uuid = text(row.get("uuid"))
    slug = text(row.get("slug"))
    board_key = first_text(slug, uuid)
    starts_at = text(row.get("starts_at"))
    updated_at = text(row.get("updated_at"))
    return {
        "uuid": uuid,
        "slug": slug,
        "board_key": board_key,
        "language": language,
        "published_at": starts_at,
        "updated_at": updated_at,
        "path": "",
        "base_url": "",
        "title": text(row.get("title")),
        "subtitle": text(row.get("subtitle")),
        "summary": text(row.get("summary")),
        "description": text(row.get("description")),
        "cover_image_url": text(row.get("cover_image_url")),
        "venue": text(row.get("venue")),
        "starts_at": starts_at,
        "ends_at": text(row.get("ends_at")),
        "capacity": int_value(row.get("capacity")),
        "status": text(row.get("status")),
        "registration_mode": text(row.get("registration_mode")),
        "member_product_uuid": text(row.get("member_product_uuid")),
        "member_product_title": text(row.get("member_product_title")),
        "member_price": int_value(row.get("member_price")),
        "nonmember_product_uuid": text(row.get("nonmember_product_uuid")),
        "nonmember_product_title": text(row.get("nonmember_product_title")),
        "nonmember_price": int_value(row.get("nonmember_price")),
        "active": bool_value(row.get("active")),
        "sort_order": int_value(row.get("sort_order")),
        "paid_count": int_value(row.get("paid_count")),
        "total_count": int_value(row.get("total_count")),
        "paid_amount": int_value(row.get("paid_amount")),
        "external": False,
        "source_id": "",
        "source_name": "",
        "source_type": "",
        "source_url": "",
        "canonical_url": "",
        "external_id": "",
        "source_note": "",
        "is_new": bool_value(row.get("is_new")),
        "url": "",
    }


def workshop_event_item(row: dict[str, Any], language: str) -> dict[str, Any]:
    title = text(row.get("title"))
    summary = text(row.get("summary"))
    canonical_url = text(row.get("canonical_url"))
    board_key = classify_r_conference_key(" ".join([title, summary, canonical_url]))
    source_id = text(row.get("source_id"))
    published_at = first_text(row.get("published_at"), row.get("collected_at"))
    if not board_key:
        if not is_posit_workshop_event(row):
            return {}
        start_at, end_at = event_date_range_from_text(" ".join([title, summary, canonical_url]))
        event_id = text(row.get("external_id")) or canonical_url or title
        event_hash = hashlib.sha256(("posit-community-event:" + event_id).encode("utf-8")).hexdigest()[:24]
        board_key = "posit-event-" + event_hash
        description = first_text(summary, title)
        return {
            "uuid": board_key,
            "slug": board_key,
            "board_key": board_key,
            "language": language,
            "published_at": first_text(start_at, published_at),
            "updated_at": text(row.get("collected_at")),
            "path": "",
            "base_url": "",
            "title": title,
            "subtitle": first_text(row.get("source_name"), "Posit Community event"),
            "summary": summary,
            "description": description,
            "cover_image_url": "",
            "venue": event_venue_from_text(summary),
            "starts_at": start_at,
            "ends_at": end_at,
            "capacity": 0,
            "status": "published",
            "registration_mode": "external",
            "member_product_uuid": "",
            "member_product_title": "",
            "member_price": 0,
            "nonmember_product_uuid": "",
            "nonmember_product_title": "",
            "nonmember_price": 0,
            "active": True,
            "sort_order": 70,
            "paid_count": 0,
            "total_count": 0,
            "paid_amount": 0,
            "external": True,
            "source_id": source_id,
            "source_name": text(row.get("source_name")),
            "source_type": text(row.get("source_type")),
            "source_url": text(row.get("source_url")),
            "canonical_url": canonical_url,
            "external_id": text(row.get("external_id")),
            "source_note": "Posit Community Conferences & Events category",
            "is_new": False,
            "url": "",
        }
    return {
        "uuid": board_key,
        "slug": board_key,
        "board_key": board_key,
        "language": language,
        "published_at": published_at,
        "updated_at": text(row.get("collected_at")),
        "path": "",
        "base_url": "",
        "title": title,
        "subtitle": "R Project conference",
        "summary": summary,
        "description": summary,
        "cover_image_url": "",
        "venue": "",
        "starts_at": "",
        "ends_at": "",
        "capacity": 0,
        "status": "published",
        "registration_mode": "external",
        "member_product_uuid": "",
        "member_product_title": "",
        "member_price": 0,
        "nonmember_product_uuid": "",
        "nonmember_product_title": "",
        "nonmember_price": 0,
        "active": True,
        "sort_order": 80,
        "paid_count": 0,
        "total_count": 0,
        "paid_amount": 0,
        "external": True,
        "source_id": source_id,
        "source_name": text(row.get("source_name")),
        "source_type": text(row.get("source_type")),
        "source_url": text(row.get("source_url")),
        "canonical_url": canonical_url,
        "external_id": text(row.get("external_id")),
        "source_note": "",
        "is_new": False,
        "url": "",
    }


def is_posit_workshop_event(row: dict[str, Any]) -> bool:
    if text(row.get("source_id")) == POSIT_COMMUNITY_EVENTS_ID:
        return True
    if text(row.get("platform")).lower() != "posit-community":
        return False
    raw_tags = row.get("tags_json")
    tags: list[str] = []
    if isinstance(raw_tags, list):
        tags = [text(value) for value in raw_tags]
    else:
        raw = text(raw_tags)
        if raw:
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError:
                decoded = []
            if isinstance(decoded, list):
                tags = [text(value) for value in decoded]
            elif "conferences & events" in raw.lower():
                return True
    return any(tag.lower() == "conferences & events" for tag in tags)


def workshop_source_scope(language: str) -> str:
    return f"web-r-community:workshop:posit-events:{normalize_language(language)}"


def workshop_source_identity(row: dict[str, Any]) -> str:
    source_id = text(row.get("source_id"))
    stable_id = first_text(row.get("external_id"), row.get("canonical_url"))
    if not source_id or not stable_id:
        raise SystemExit(
            "Web-R workshop source row has no stable source_id plus external_id/canonical_url identity; "
            "preserving the previous CDN release"
        )
    canonical = json.dumps(
        {"source_id": source_id, "stable_id": stable_id},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def workshop_posit_source_identity_set(rows: list[dict[str, Any]]) -> set[str]:
    return {
        workshop_source_identity(row)
        for row in rows
        if is_posit_workshop_event(row)
    }


def add_workshop_source_representation(item: dict[str, Any], source_identity: str) -> None:
    identity = text(source_identity).lower()
    if not SHA256_RE.fullmatch(identity):
        raise SystemExit("Web-R workshop export produced an invalid source identity")
    raw = item.get("represented_source_identities")
    if raw is None:
        identities: list[str] = []
    elif isinstance(raw, list):
        identities = [text(value).lower() for value in raw]
    else:
        raise SystemExit("Web-R workshop export has an invalid represented source identity list")
    if any(not SHA256_RE.fullmatch(value) for value in identities):
        raise SystemExit("Web-R workshop export has a malformed represented source identity")
    if identity not in identities:
        identities.append(identity)
    item["represented_source_identities"] = sorted(identities)


def workshop_represented_source_identity_set(items: dict[str, dict[str, Any]]) -> set[str]:
    represented: set[str] = set()
    for item in items.values():
        raw = item.get("represented_source_identities")
        if raw is None:
            continue
        if not isinstance(raw, list):
            raise SystemExit("Web-R workshop export has an invalid represented source identity list")
        for value in raw:
            identity = text(value).lower()
            if not SHA256_RE.fullmatch(identity):
                raise SystemExit("Web-R workshop export has a malformed represented source identity")
            if identity in represented:
                raise SystemExit(
                    "Web-R workshop export represents one source identity in more than one catalog item; "
                    "preserving the previous CDN release"
                )
            represented.add(identity)
    return represented


def workshop_source_identity_digest(identities: set[str]) -> str:
    normalized = sorted(text(identity).lower() for identity in identities)
    if any(not SHA256_RE.fullmatch(identity) for identity in normalized):
        raise SystemExit("Web-R workshop source identity set is malformed")
    body = json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def require_workshop_source_parity(source_identities: set[str], represented_identities: set[str]) -> None:
    source = set(source_identities)
    represented = set(represented_identities)
    if source == represented:
        return
    missing = source - represented
    unexpected = represented - source
    raise SystemExit(
        "Web-R workshop source/export identity parity failed "
        f"source={len(source)} represented={len(represented)} "
        f"missing={len(missing)} unexpected={len(unexpected)}; "
        "preserving the previous CDN release"
    )


def resolve_optional_path(root: Path, value: str) -> Path | None:
    raw = text(value)
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = root / path
    return path.absolute()


def load_json_object(path: Path, description: str) -> dict[str, Any]:
    if path.is_symlink():
        raise SystemExit(f"{description} must not be a symbolic link: {path}")
    if not path.is_file():
        raise SystemExit(f"{description} is missing: {path}")
    if path.stat().st_size > 64 * 1024:
        raise SystemExit(f"{description} is too large: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"{description} is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"{description} must be a JSON object: {path}")
    return value


def strict_uint(value: Any, field: str, *, allow_zero: bool) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SystemExit(f"{field} must be an unsigned JSON integer")
    if value < 0 or value > (1 << 64) - 1 or (value == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "strictly positive"
        raise SystemExit(f"{field} must be a {qualifier} UInt64")
    return value


def validate_identity_receipt(value: dict[str, Any], source_identities: set[str], description: str) -> None:
    expected_count = len(source_identities)
    expected_digest = workshop_source_identity_digest(source_identities)
    count = strict_uint(value.get("source_identity_count"), f"{description}.source_identity_count", allow_zero=True)
    digest = text(value.get("source_identity_sha256")).lower()
    if count != expected_count or digest != expected_digest:
        raise SystemExit(
            f"{description} does not match the exact workshop source identity set; "
            "preserving the previous CDN release"
        )


def load_workshop_authority_registry(path: Path, scope: str) -> dict[str, Any] | None:
    if not path.exists() and not path.is_symlink():
        return None
    registry = load_json_object(path, "published workshop authority registry")
    if registry.get("schema") != WORKSHOP_AUTHORITY_REGISTRY_SCHEMA:
        raise SystemExit("published workshop authority registry has an unsupported schema")
    if registry.get("scope") != scope:
        raise SystemExit("published workshop authority registry scope does not match this export")
    strict_uint(registry.get("authority_revision"), "published registry authority_revision", allow_zero=True)
    source_count = strict_uint(
        registry.get("source_identity_count"),
        "published registry source_identity_count",
        allow_zero=True,
    )
    source_digest = text(registry.get("source_identity_sha256")).lower()
    if not SHA256_RE.fullmatch(source_digest):
        raise SystemExit("published workshop authority registry source identity digest is invalid")
    represented_count = strict_uint(
        registry.get("represented_identity_count"),
        "published registry represented_identity_count",
        allow_zero=True,
    )
    represented_digest = text(registry.get("represented_identity_sha256")).lower()
    if not SHA256_RE.fullmatch(represented_digest):
        raise SystemExit("published workshop authority registry represented identity digest is invalid")
    if (
        registry.get("exact") is not True
        or source_count != represented_count
        or source_digest != represented_digest
    ):
        raise SystemExit("published workshop authority registry does not prove exact source/export parity")
    empty_authorized = registry.get("empty_withdrawal_authorized")
    if not isinstance(empty_authorized, bool):
        raise SystemExit("published workshop authority registry withdrawal state is invalid")
    if empty_authorized and (
        source_count != 0 or source_digest != workshop_source_identity_digest(set())
    ):
        raise SystemExit("published workshop authority registry has an inconsistent empty withdrawal receipt")
    return registry


def authorize_empty_workshop_source(
    *,
    source_identities: set[str],
    scope: str,
    current_registry: dict[str, Any] | None,
    authority_manifest_path: Path | None,
) -> tuple[int, bool]:
    current_revision = 0
    if current_registry is not None:
        current_revision = strict_uint(
            current_registry.get("authority_revision"),
            "published registry authority_revision",
            allow_zero=True,
        )
    if source_identities:
        return current_revision, False

    expected_digest = workshop_source_identity_digest(source_identities)
    if (
        current_registry is not None
        and current_registry.get("empty_withdrawal_authorized") is True
        and current_registry.get("source_identity_count") == 0
        and text(current_registry.get("source_identity_sha256")).lower() == expected_digest
    ):
        return current_revision, True

    if authority_manifest_path is None:
        raise SystemExit(
            "Web-R workshop Posit source is empty without a durable withdrawal authority manifest; "
            "preserving the previous CDN release"
        )
    if current_registry is None:
        raise SystemExit(
            "Web-R workshop withdrawal authority cannot be ordered without the current published registry; "
            "preserving the previous CDN release"
        )

    authority = load_json_object(authority_manifest_path, "workshop withdrawal authority manifest")
    if authority.get("schema") != WORKSHOP_WITHDRAWAL_AUTHORITY_SCHEMA:
        raise SystemExit("workshop withdrawal authority manifest has an unsupported schema")
    if authority.get("scope") != scope:
        raise SystemExit("workshop withdrawal authority manifest scope does not match this export")
    revision = strict_uint(authority.get("authority_revision"), "withdrawal authority_revision", allow_zero=False)
    if revision <= current_revision:
        raise SystemExit(
            "workshop withdrawal authority_revision must be greater than the current published revision"
        )
    if authority.get("allow_empty_withdrawal") is not True:
        raise SystemExit("workshop withdrawal authority does not explicitly allow an empty withdrawal")
    validate_identity_receipt(authority, source_identities, "workshop withdrawal authority")
    return revision, True


def workshop_source_parity_receipt(
    *,
    source_identities: set[str],
    represented_identities: set[str],
    scope: str,
    authority_revision: int,
    empty_withdrawal_authorized: bool,
) -> dict[str, Any]:
    require_workshop_source_parity(source_identities, represented_identities)
    revision = strict_uint(authority_revision, "workshop authority_revision", allow_zero=True)
    if not isinstance(empty_withdrawal_authorized, bool):
        raise SystemExit("workshop empty withdrawal authorization must be boolean")
    return {
        "schema": WORKSHOP_SOURCE_PARITY_SCHEMA,
        "scope": scope,
        "source_identity_count": len(source_identities),
        "source_identity_sha256": workshop_source_identity_digest(source_identities),
        "represented_identity_count": len(represented_identities),
        "represented_identity_sha256": workshop_source_identity_digest(represented_identities),
        "exact": True,
        "authority_revision": revision,
        "empty_withdrawal_authorized": empty_withdrawal_authorized,
    }


def workshop_authority_registry(source_parity: dict[str, Any]) -> dict[str, Any]:
    if source_parity.get("schema") != WORKSHOP_SOURCE_PARITY_SCHEMA or source_parity.get("exact") is not True:
        raise SystemExit("workshop source parity receipt is invalid")
    return {
        "schema": WORKSHOP_AUTHORITY_REGISTRY_SCHEMA,
        "scope": source_parity.get("scope"),
        "authority_revision": source_parity.get("authority_revision"),
        "source_identity_count": source_parity.get("source_identity_count"),
        "source_identity_sha256": source_parity.get("source_identity_sha256"),
        "represented_identity_count": source_parity.get("represented_identity_count"),
        "represented_identity_sha256": source_parity.get("represented_identity_sha256"),
        "exact": source_parity.get("exact"),
        "empty_withdrawal_authorized": source_parity.get("empty_withdrawal_authorized"),
    }


def event_date_range_from_text(value: str) -> tuple[str, str]:
    normalized = text(value)
    if not normalized:
        return "", ""
    iso_match = re.search(r"\b([12][0-9]{3})[-/.](0?[1-9]|1[0-2])[-/.](0?[1-9]|[12][0-9]|3[01])\b", normalized)
    if iso_match:
        return format_event_datetime(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3))), ""
    year_match = re.search(r"\b([12][0-9]{3})\b", normalized)
    fallback_year = int(year_match.group(1)) if year_match else datetime.now().year
    month_pattern = (
        r"\b("
        r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
        r")\.?\s+([0-9]{1,2})(?:\s*(?:-|–|—|to)\s*([0-9]{1,2}))?(?:,?\s*([12][0-9]{3}))?"
    )
    month_match = re.search(month_pattern, normalized, re.IGNORECASE)
    if not month_match:
        return "", ""
    month_key = month_match.group(1).lower().rstrip(".")
    month = MONTH_LOOKUP.get(month_key[:3], MONTH_LOOKUP.get(month_key, 0))
    if not month:
        return "", ""
    year = int(month_match.group(4)) if month_match.group(4) else fallback_year
    day = int(month_match.group(2))
    end_day = int(month_match.group(3)) if month_match.group(3) else 0
    start = format_event_datetime(year, month, day)
    end = format_event_datetime(year, month, end_day, end_of_day=True) if end_day else ""
    return start, end


def format_event_datetime(year: int, month: int, day: int, *, end_of_day: bool = False) -> str:
    try:
        parsed = datetime(year, month, day, 23 if end_of_day else 0, 59 if end_of_day else 0, 59 if end_of_day else 0)
    except ValueError:
        return ""
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def event_venue_from_text(value: str) -> str:
    body = " ".join(text(value).split())
    if not body:
        return ""
    match = re.search(r"Location:\s*(.*?)(?:\s+Date:|\s+Register|\s+Description\b|$)", body, re.IGNORECASE)
    if match:
        return text(match.group(1))[:160]
    if re.search(r"\bonline\b", body, re.IGNORECASE):
        return "Online"
    return ""


def workshop_post_item(row: dict[str, Any]) -> dict[str, Any]:
    title = text(row.get("title"))
    content = text(row.get("summary"))
    canonical_url = text(row.get("canonical_url"))
    workshop_key = classify_r_conference_key(" ".join([title, content, canonical_url])) or USE_R2026_WORKSHOP_KEY
    if not title or title.startswith("http://") or title.startswith("https://"):
        title = first_text_line(content, 120)
    if not title:
        title = "useR! conference update"
    created_at = first_text(row.get("published_at"), row.get("collected_at"))
    external_id = text(row.get("external_id"))
    uuid = "import-" + hashlib.sha256(("workshop-board-import:" + external_id + ":" + canonical_url).encode("utf-8")).hexdigest()[:24]
    return {
        "uuid": uuid,
        "workshop_key": workshop_key,
        "title": title,
        "content": content,
        "author_uuid": R_PROJECT_BOT_UUID,
        "author_name": R_PROJECT_BOT_NAME,
        "author_role": R_PROJECT_BOT_ROLE,
        "source_id": text(row.get("source_id")),
        "source_name": text(row.get("source_name")),
        "source_url": text(row.get("source_url")),
        "canonical_url": canonical_url,
        "external_id": external_id,
        "created_at": created_at,
        "updated_at": "",
        "active": True,
        "imported": True,
        "is_new": False,
    }


def classify_r_conference_key(value: str) -> str:
    lower = text(value).lower()
    if not lower:
        return ""
    if "r/basel" in lower or "r-basel" in lower:
        return "rconf-r-basel-2023"
    match = re.search(r"r summit\s*([12][0-9]{3})", lower)
    if match:
        return "rconf-r-summit-" + match.group(1)
    match = re.search(r"user!?\s*([12][0-9]{3})", lower)
    if match:
        return "rconf-user-" + match.group(1)
    match = re.search(r"user([12][0-9]{3})", lower)
    if match:
        return "rconf-user-" + match.group(1)
    match = re.search(r"dsc[-/\s]*([12][0-9]{3})", lower)
    if match:
        return "rconf-dsc-" + match.group(1)
    return ""


def workshop_catalog_token(items: dict[str, dict[str, Any]], posts: dict[str, list[dict[str, Any]]]) -> str:
    body = json.dumps({"items": items, "posts": posts}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def first_text_line(value: str, max_len: int) -> str:
    for line in text(value).splitlines():
        line = line.strip()
        if line:
            return line[:max_len] if max_len > 0 else line
    return ""


def encrypted_doc_uuid(payload: dict[str, Any], fallback: str) -> str:
    for key in ("item", "workshop"):
        value = payload.get(key)
        if isinstance(value, dict):
            uuid = text(value.get("uuid"))
            if uuid:
                return uuid
    return text(fallback)


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
    for key in ("R_ECOSYSTEM_CONTENT_KEY", "WEBR_R_ECOSYSTEM_CONTENT_KEY"):
        value = env.get(key, "").strip()
        if value:
            return value
    raise SystemExit("R_ECOSYSTEM_CONTENT_KEY is not configured; refusing to reuse an application session secret")


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


def fetch_json_rows(env: dict[str, str], sql: str, query_name: str = "community") -> list[dict[str, Any]]:
    user = env.get("CLICKHOUSE_USER", "").strip()
    password = env.get("CLICKHOUSE_PASSWORD", "")
    if not user:
        raise SystemExit("ClickHouse connection environment is incomplete")
    url = build_clickhouse_url(
        env,
        default_format="JSONEachRow",
        max_execution_time=env.get("WEBR_COMMUNITY_CDN_CH_MAX_EXECUTION_TIME", "120"),
        max_threads=env.get("WEBR_COMMUNITY_CDN_CH_MAX_THREADS", "2"),
    )
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=int(env.get("WEBR_COMMUNITY_CDN_HTTP_TIMEOUT", "150"))) as response:
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


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False) as tmp:
        tmp.write(body)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def prune_stale_notebook_payloads(cdn_root: Path, language: str, current_payload_paths: set[str]) -> int:
    notebook_root = cdn_root / "community" / language / "notebook"
    if not notebook_root.exists():
        return 0
    pruned = 0
    for path in notebook_root.rglob("*.json"):
        rel_path = path.relative_to(cdn_root).as_posix()
        if rel_path in current_payload_paths:
            continue
        path.unlink()
        pruned += 1
    return pruned


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


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return text(value).lower() in {"1", "true", "t", "yes", "y"}


def safe_path_id(value: Any) -> str:
    raw = text(value)
    safe = SAFE_ID_RE.sub("-", raw).strip("-._")
    if safe:
        return safe
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


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
