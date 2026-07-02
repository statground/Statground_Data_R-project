#!/usr/bin/env python3
"""Backfill Korean display fields for collected R-packages mailing-list rows."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from clickhouse_http import build_clickhouse_url
from export_r_ecosystem_cdn import ClickHouseExportError, fetch_json_rows, load_env, text
from export_r_package_cdn import (
    looks_korean,
    package_news_display_fields,
    package_news_json_with_korean,
    parse_json_object,
)


TARGET_SOURCE_ID = "official:r-mail:r-packages"
REPAIR_TAG = "r_packages_korean_payload_repair_20260519"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="web_r_go .env path")
    parser.add_argument("--limit", type=int, default=0, help="optional row limit for smoke repair")
    parser.add_argument("--dry-run", action="store_true", help="build repaired rows without inserting")
    args = parser.parse_args()

    repo_root = Path.cwd()
    env = load_env(repo_root / args.env)
    try:
        rows = fetch_json_rows(env, source_sql(args.limit))
    except ClickHouseExportError as exc:
        raise SystemExit(str(exc)) from exc
    repaired = [row for row in (repair_row(row) for row in rows) if row]

    if args.dry_run:
        print(json.dumps({"selected": len(rows), "repaired": len(repaired)}, ensure_ascii=False))
        return 0
    if repaired:
        insert_rows(env, repaired)
    print(json.dumps({"selected": len(rows), "inserted": len(repaired)}, ensure_ascii=False))
    return 0


def source_sql(limit: int) -> str:
    suffix = f"\n LIMIT {int(limit)}" if limit and limit > 0 else ""
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
       author,
       language,
       tags_json,
       toString(raw_json) AS raw_json,
       toString(payload_json) AS payload_json,
       if(isNull(published_at), '', formatDateTime(published_at, '%Y-%m-%d %H:%i:%S')) AS published_at_text,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS old_collected_at
  FROM Data_R_Community_Service.v_r_community_latest_dedup
 WHERE source_id = '{TARGET_SOURCE_ID}'
   AND startsWith(title, '[R-pkgs]')
   AND match(canonical_url, '/pipermail/r-packages/[0-9]{{4}}/[0-9]+[.]html')
   AND (
        empty(JSONExtractString(payload_json, 'summary_ko'))
        OR empty(JSONExtractString(payload_json, 'content_ko'))
        OR empty(JSONExtractString(raw_json, 'summary_ko'))
        OR empty(JSONExtractString(raw_json, 'content_ko'))
   )
 ORDER BY if(isNull(published_at), parseDateTime64BestEffortOrNull(old_collected_at, 3, 'Asia/Seoul'), published_at) DESC{suffix}
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def repair_row(row: dict[str, Any]) -> dict[str, Any] | None:
    title_ko, summary_ko = package_news_display_fields(row)
    if not looks_korean(summary_ko):
        return None

    raw_json = package_news_json_with_korean(text(row.get("raw_json")), title_ko, summary_ko)
    payload = parse_json_object(row.get("payload_json"))
    if not payload:
        payload = {}
    payload["title_ko"] = title_ko
    payload["summary_ko"] = summary_ko
    payload["content_ko"] = summary_ko
    payload["translation_status"] = "translated"
    payload["translation_language"] = "ko"
    payload["translation_model"] = "deterministic-r-packages-body-summary"
    payload["translation_updated_at"] = now_kst_iso()
    payload["repair_source"] = REPAIR_TAG
    payload["raw_json"] = raw_json
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    event_uuid = str(uuid.uuid4())
    now_text = now_kst_text()
    published_at = text(row.get("published_at_text")) or None
    return {
        "uuid": event_uuid,
        "event_id": event_uuid,
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
        "language": text(row.get("language")) or "en",
        "tags_json": text(row.get("tags_json")) or "[]",
        "raw_json": raw_json,
        "published_at": published_at,
        "collected_at": now_text,
        "collection_status": "collected",
        "payload_hash": hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        "payload_json": payload_json,
        "active": 1,
        "ingested_at": now_text,
    }


def insert_rows(env: dict[str, str], rows: list[dict[str, Any]]) -> None:
    user = env.get("CLICKHOUSE_USER", "").strip()
    password = env.get("CLICKHOUSE_PASSWORD", "")
    if not user:
        raise SystemExit("ClickHouse connection environment is incomplete")
    body = "INSERT INTO Data_R_Community_Raw.r_community_item_raw FORMAT JSONEachRow\n"
    body += "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows)
    body += "\n"

    url = build_clickhouse_url(env, default_format="JSONEachRow", max_execution_time="120")
    request = urllib.request.Request(url, data=body.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            response.read()
    except urllib.error.URLError as exc:
        raise SystemExit(f"ClickHouse repair insert failed: {exc.__class__.__name__}") from exc


def now_kst_text() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def now_kst_iso() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")


if __name__ == "__main__":
    sys.exit(main())
