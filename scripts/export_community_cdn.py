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
WORKSHOP_MANIFEST_SCHEMA = "web-r.community.workshop.manifest.plain.v1"
WORKSHOP_CONTENT_SCHEMA = "web-r.community.workshop.content.plain.v1"
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
    workshop_rows = fetch_json_rows(env, workshop_sql(args.limit))
    workshop_event_rows = fetch_json_rows(env, workshop_event_sql(args.limit))
    workshop_post_rows = fetch_json_rows(env, workshop_post_sql(args.limit))

    payloads: dict[str, tuple[str, dict[str, Any]]] = {}
    manifest_items: dict[str, dict[str, Any]] = {}
    workshop_manifest_items: dict[str, dict[str, Any]] = {}
    workshop_posts: dict[str, list[dict[str, Any]]] = {}
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
        if uuid in workshop_manifest_items:
            continue
        rel_path = workshop_payload_path(language, uuid, first_text(item.get("starts_at"), item.get("updated_at"), item.get("published_at")))
        item["path"] = rel_path
        item["url"] = f"/workshop/read/{urllib.parse.quote(uuid)}/"
        posts = workshop_posts.get(text(item.get("board_key")), [])
        payloads["workshop:" + uuid] = (rel_path, {"schema": WORKSHOP_CONTENT_SCHEMA, "workshop": item, "posts": posts})
        workshop_manifest_items[uuid] = item

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
        "items": workshop_manifest_items,
    }
    current_payload_paths = {rel_path for rel_path, _payload in payloads.values()}

    if args.dry_run:
        print(json.dumps({
            "digest": len(digest_rows),
            "notebook": len(notebook_rows),
            "community_export": len(manifest_items),
            "workshop": len(workshop_manifest_items),
            "workshop_posts": sum(len(posts) for posts in workshop_posts.values()),
            "workshop_export": len(workshop_manifest_items),
            "export": len(payloads),
            "duplicates": duplicate_count,
        }, ensure_ascii=False))
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
    pruned_payloads = 0
    if args.limit <= 0:
        pruned_payloads = prune_stale_notebook_payloads(cdn_root, language, current_payload_paths)

    print(json.dumps({
        "digest": len(digest_rows),
        "notebook": len(notebook_rows),
        "community_export": len(manifest_items),
        "workshop": len(workshop_manifest_items),
        "workshop_posts": sum(len(posts) for posts in workshop_posts.values()),
        "workshop_export": len(workshop_manifest_items),
        "export": len(payloads),
        "duplicates": duplicate_count,
        "pruned_payloads": pruned_payloads,
    }, ensure_ascii=False))
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
        if source_id != POSIT_COMMUNITY_EVENTS_ID:
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
    url = build_clickhouse_url(
        env,
        default_format="JSONEachRow",
        max_execution_time=env.get("WEBR_COMMUNITY_CDN_CH_MAX_EXECUTION_TIME", "60"),
        max_threads=env.get("WEBR_COMMUNITY_CDN_CH_MAX_THREADS", "2"),
    )
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=int(env.get("WEBR_COMMUNITY_CDN_HTTP_TIMEOUT", "75"))) as response:
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
