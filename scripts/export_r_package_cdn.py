#!/usr/bin/env python3
"""Export encrypted R package list/news metadata into the Web-R CDN checkout."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from export_r_ecosystem_cdn import (
    CONTENT_SCHEMA,
    derive_key,
    encrypt_document,
    fetch_json_rows,
    first_text,
    load_env,
    normalize_language,
    normalize_uuid,
    published_year_month,
    text,
    write_json_atomic,
    content_secret,
)

PACKAGE_MANIFEST_SCHEMA = "web-r.r-ecosystem.package-manifest.plain.v1"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="web_r_go .env path")
    parser.add_argument("--cdn-root", default="../web-R_CDN", help="web-R_CDN checkout path")
    parser.add_argument("--language", default="ko", help="content language code")
    parser.add_argument("--package-limit", type=int, default=0, help="optional package row limit for smoke exports")
    parser.add_argument("--news-limit", type=int, default=0, help="optional package news limit for smoke exports")
    parser.add_argument("--dry-run", action="store_true", help="query and encrypt without writing files")
    args = parser.parse_args()

    repo_root = Path.cwd()
    env = load_env(repo_root / args.env)
    language = normalize_language(args.language)
    key = derive_key(content_secret(env))
    cdn_root = (repo_root / args.cdn_root).resolve()

    package_rows = fetch_json_rows(env, package_sql(args.package_limit))
    news_rows = fetch_json_rows(env, package_news_sql(args.news_limit))

    packages: dict[str, dict[str, Any]] = {}
    news_manifest: dict[str, dict[str, str]] = {}
    news_payloads: dict[str, dict[str, Any]] = {}

    for row in package_rows:
        repository = text(row.get("repository"))
        package_name = text(row.get("package_name"))
        if not repository or not package_name:
            continue
        key_name = f"{repository.lower()}|{package_name.lower()}"
        packages[key_name] = {
            "key": key_name,
            "repository": repository,
            "package_name": package_name,
            "latest_version": text(row.get("latest_version")),
            "title": text(row.get("title")),
            "description": text(row.get("description")),
            "maintainer": text(row.get("maintainer")),
            "license_text": text(row.get("license_text")),
            "published_at": text(row.get("published_at")),
            "last_observed_at": text(row.get("last_observed_at")),
            "downloads_30d": int(row.get("downloads_30d") or 0),
            "reverse_depends_count": int(row.get("reverse_depends_count") or 0),
            "reverse_imports_count": int(row.get("reverse_imports_count") or 0),
            "cran_check_worst_status": text(row.get("cran_check_worst_status")),
            "lifecycle_status": text(row.get("lifecycle_status")),
        }

    for row in news_rows:
        uuid = normalize_uuid(row.get("item_uuid"))
        if not uuid:
            continue
        published_at = first_text(text(row.get("published_at_text")), text(row.get("collected_at_text")))
        year, month = published_year_month(published_at)
        rel_path = f"packages/{language}/news/{year}/{month}/{uuid}.json"
        item = {
            "uuid": uuid,
            "kind": "community",
            "language": language,
            "published_at": published_at,
            "year": year,
            "month": month,
            "path": rel_path,
            "item_kind": "community",
            "source_id": text(row.get("source_id")),
            "source_name": text(row.get("source_name")),
            "source_type": text(row.get("source_type")),
            "platform": text(row.get("platform")),
            "title": text(row.get("title")),
            "summary": text(row.get("summary")),
            "author": text(row.get("author")),
            "canonical_url": text(row.get("canonical_url")),
        }
        news_manifest[uuid] = {k: v for k, v in item.items() if v != ""}
        news_payloads[uuid] = {
            "schema": CONTENT_SCHEMA,
            "kind": "community",
            "community_item": {
                "item_uuid": uuid,
                "external_id": text(row.get("external_id")),
                "source_id": item["source_id"],
                "source_name": item["source_name"],
                "source_type": item["source_type"],
                "platform": item["platform"],
                "source_url": text(row.get("source_url")),
                "canonical_url": item["canonical_url"],
                "title": item["title"],
                "summary": item["summary"],
                "author": item["author"],
                "language": language,
                "tags_json": text(row.get("tags_json")),
                "raw_json": text(row.get("raw_json")),
                "payload_json": text(row.get("payload_json")),
                "published_at": published_at,
                "collected_at": text(row.get("collected_at_text")),
            },
        }

    manifest = {
        "schema": PACKAGE_MANIFEST_SCHEMA,
        "language": language,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "packages": packages,
        "news": news_manifest,
    }

    if args.dry_run:
        print(json.dumps({"packages": len(packages), "news": len(news_manifest)}, ensure_ascii=False))
        return 0

    for uuid, payload in news_payloads.items():
        rel_path = news_manifest[uuid]["path"]
        write_json_atomic(cdn_root / rel_path, encrypt_document(payload, key, rel_path, language, uuid))

    manifest_path = f"packages/{language}/index.json"
    write_json_atomic(cdn_root / manifest_path, encrypt_document(manifest, key, manifest_path, language, ""))
    print(json.dumps({"packages": len(packages), "news": len(news_manifest), "manifest": manifest_path}, ensure_ascii=False))
    return 0


def package_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT repository,
       package_name,
       latest_version,
       title,
       left(description, 260) AS description,
       maintainer,
       license_text,
       if(isNull(published_sort_key), '', formatDateTime(published_sort_key, '%Y-%m-%d %H:%i:%S')) AS published_at,
       formatDateTime(last_observed_at, '%Y-%m-%d %H:%i:%S') AS last_observed_at,
       downloads_30d,
       reverse_depends_count,
       reverse_imports_count,
       cran_check_worst_status,
       lifecycle_status
  FROM
(
    SELECT b.repository,
           b.package_name,
           b.latest_version,
           b.title,
           b.description,
           b.maintainer,
           b.license_text,
           coalesce(cran_page.published_at, package_current.published_at, version_history.version_published_at) AS published_sort_key,
           b.last_observed_at,
           ifNull(metrics.downloads_30d, 0) AS downloads_30d,
           ifNull(metrics.reverse_depends_count, 0) AS reverse_depends_count,
           ifNull(metrics.reverse_imports_count, 0) AS reverse_imports_count,
           ifNull(metrics.cran_check_worst_status, '') AS cran_check_worst_status,
           ifNull(metrics.lifecycle_status, 'active') AS lifecycle_status
      FROM
    (
        SELECT lowerUTF8(pc.repository) AS repository_key,
               lowerUTF8(pc.package_name) AS package_key,
               argMax(pc.repository, pc.version) AS repository,
               argMax(pc.package_name, pc.version) AS package_name,
               argMax(pc.latest_version, pc.version) AS latest_version,
               argMax(pc.title, pc.version) AS title,
               argMax(pc.description, pc.version) AS description,
               argMax(pc.maintainer, pc.version) AS maintainer,
               argMax(pc.license_text, pc.version) AS license_text,
               argMax(pc.date_publication, pc.version) AS date_publication,
               max(pc.last_observed_at) AS last_observed_at
          FROM Data_R_Package_Service.package_current AS pc
         WHERE notEmpty(pc.package_name)
         GROUP BY repository_key, package_key
    ) AS b
      LEFT JOIN
    (
        SELECT lowerUTF8(repository) AS repository_key,
               lowerUTF8(package_name) AS package_key,
               max(downloads_30d) AS downloads_30d,
               max(reverse_depends_count) AS reverse_depends_count,
               max(reverse_imports_count) AS reverse_imports_count,
               argMax(cran_check_worst_status, last_observed_at) AS cran_check_worst_status,
               argMax(lifecycle_status, last_observed_at) AS lifecycle_status
          FROM Data_R_Package_Mart.v_package_profile_latest
         GROUP BY repository_key, package_key
    ) AS metrics ON b.repository_key = metrics.repository_key AND b.package_key = metrics.package_key
      LEFT JOIN
    (
        SELECT 'cran' AS repository_key,
               lowerUTF8(package_name) AS package_key,
               max(parseDateTime64BestEffortOrNull(nullIf(published, ''), 3, 'Asia/Seoul')) AS published_at
          FROM Data_R_Package_Service.package_cran_page_current
         WHERE notEmpty(published)
         GROUP BY repository_key, package_key
    ) AS cran_page ON b.repository_key = cran_page.repository_key AND b.package_key = cran_page.package_key
      LEFT JOIN
    (
        SELECT lowerUTF8(repository) AS repository_key,
               lowerUTF8(package_name) AS package_key,
               max(parseDateTime64BestEffortOrNull(nullIf(date_publication, ''), 3, 'Asia/Seoul')) AS published_at
          FROM Data_R_Package_Service.package_current
         WHERE notEmpty(date_publication)
         GROUP BY repository_key, package_key
    ) AS package_current ON b.repository_key = package_current.repository_key AND b.package_key = package_current.package_key
      LEFT JOIN
    (
        SELECT lowerUTF8(repository) AS repository_key,
               lowerUTF8(package_name) AS package_key,
               package_version,
               max(published_at) AS version_published_at
          FROM Data_R_Package_Service.package_version_history
         WHERE published_at IS NOT NULL
         GROUP BY repository_key, package_key, package_version
    ) AS version_history ON b.repository_key = version_history.repository_key
                        AND b.package_key = version_history.package_key
                        AND b.latest_version = version_history.package_version
)
 ORDER BY lowerUTF8(repository), lowerUTF8(package_name){suffix}
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def package_news_sql(limit: int) -> str:
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
       coalesce(nullIf(JSONExtractString(payload_json, 'title_ko'), ''), nullIf(JSONExtractString(raw_json, 'title_ko'), ''), title) AS title,
       coalesce(nullIf(JSONExtractString(payload_json, 'summary_ko'), ''), nullIf(JSONExtractString(raw_json, 'summary_ko'), ''), nullIf(JSONExtractString(payload_json, 'content_ko'), ''), nullIf(JSONExtractString(raw_json, 'content_ko'), ''), summary) AS summary,
       author,
       tags_json,
       toString(raw_json) AS raw_json,
       toString(payload_json) AS payload_json,
       if(isNull(original_published_at), '', formatDateTime(original_published_at, '%Y-%m-%d %H:%i:%S')) AS published_at_text,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at_text
  FROM Data_R_Community_Service.r_community_item_read_current
 WHERE active = 1
   AND notEmpty(canonical_url)
   AND notEmpty(coalesce(nullIf(JSONExtractString(payload_json, 'title_ko'), ''), nullIf(JSONExtractString(raw_json, 'title_ko'), ''), title))
   AND (
          source_type = 'package_update_feed'
          OR source_id = 'official:r-mail:r-packages'
       )
   AND (
          source_id != 'official:r-mail:r-packages'
          OR startsWith(title, '[R-pkgs]')
       )
 ORDER BY if(isNull(original_published_at), collected_at, original_published_at) DESC,
          sipHash64(if(notEmpty(external_id), external_id, canonical_url)) ASC{suffix}
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


if __name__ == "__main__":
    sys.exit(main())
