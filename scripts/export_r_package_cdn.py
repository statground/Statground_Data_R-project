#!/usr/bin/env python3
"""Export encrypted R package list/news metadata into the Web-R CDN checkout."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

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
PACKAGE_VERSIONS_SCHEMA = "web-r.r-ecosystem.package-versions.plain.v1"

PACKAGE_NEWS_TITLE_SQL = r"""
coalesce(
    nullIf(
        multiIf(
            startsWith(source_id, 'community:runiverse:') AND notEmpty(title),
                concat(
                    replaceRegexpOne(title, '^\\[[^\\]]+\\]\\s+', ''),
                    if(positionCaseInsensitiveUTF8(title, 'R-universe 업데이트') > 0, '', ' R-universe 업데이트')
                ),
            ''
        ),
        ''
    ),
    nullIf(JSONExtractString(payload_json, 'title_ko'), ''),
    nullIf(JSONExtractString(raw_json, 'title_ko'), ''),
    nullIf(
        multiIf(
            source_id = 'official:r-mail:r-packages'
                AND startsWith(title, '[R-pkgs]')
                AND notEmpty(extract(title, '^\\[R-pkgs\\]\\s+New package:\\s*([^\\s]+)')),
                concat(extract(title, '^\\[R-pkgs\\]\\s+New package:\\s*([^\\s]+)'), ' 신규 CRAN 패키지 공지'),
            source_id = 'official:r-mail:r-packages'
                AND startsWith(title, '[R-pkgs]')
                AND notEmpty(extract(title, '^\\[R-pkgs\\]\\s+([^:]+)')),
                concat(
                    extract(title, '^\\[R-pkgs\\]\\s+([^:]+)'),
                    ' CRAN 패키지 공지'
                ),
            startsWith(source_id, 'community:runiverse:')
                AND notEmpty(extract(title, '^\\[[^\\]]+\\]\\s+([^\\s]+)')),
                concat(
                    extract(title, '^\\[[^\\]]+\\]\\s+([^\\s]+)'),
                    if(notEmpty(extract(title, '^\\[[^\\]]+\\]\\s+[^\\s]+\\s+([^\\s]+)')), concat(' ', extract(title, '^\\[[^\\]]+\\]\\s+[^\\s]+\\s+([^\\s]+)')), ''),
                    ' R-universe 업데이트'
                ),
            source_id = 'official:bioconductor:release-announcements'
                AND notEmpty(extract(concat(title, ' ', canonical_url), '(?i)Bioconductor[^0-9]*([0-9]+(?:\\.[0-9]+)?)')),
                concat('Bioconductor ', extract(concat(title, ' ', canonical_url), '(?i)Bioconductor[^0-9]*([0-9]+(?:\\.[0-9]+)?)'), ' 릴리스'),
            ''
        ),
        ''
    ),
    title
)"""

PACKAGE_NEWS_SUMMARY_SQL = r"""
coalesce(
    nullIf(JSONExtractString(payload_json, 'summary_ko'), ''),
    nullIf(JSONExtractString(raw_json, 'summary_ko'), ''),
    nullIf(JSONExtractString(payload_json, 'content_ko'), ''),
    nullIf(JSONExtractString(raw_json, 'content_ko'), ''),
    nullIf(
        multiIf(
            source_id = 'official:bioconductor:release-announcements'
                AND notEmpty(extract(concat(title, ' ', canonical_url), '(?i)Bioconductor[^0-9]*([0-9]+(?:\\.[0-9]+)?)')),
                concat(
                    'Bioconductor ',
                    extract(concat(title, ' ', canonical_url), '(?i)Bioconductor[^0-9]*([0-9]+(?:\\.[0-9]+)?)'),
                    ' 릴리스 공지입니다. 원문에는 새 패키지, 기존 패키지 NEWS, deprecated/defunct 패키지 등 릴리스 변경 사항이 정리되어 있습니다.'
                ),
            startsWith(source_id, 'community:runiverse:') AND notEmpty(title),
                concat(
                    if(positionCaseInsensitiveUTF8(source_id, 'ropensci') > 0, 'rOpenSci R-universe', source_name),
                    '에서 확인된 ',
                    replaceRegexpOne(replaceRegexpOne(title, '^\\[[^\\]]+\\]\\s+', ''), '\\s+R-universe 업데이트$', ''),
                    ' 패키지 업데이트입니다. 빌드 결과와 패키지 설명은 원문 링크에서 확인할 수 있습니다.'
                ),
            ''
        ),
        ''
    ),
    summary
)"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="web_r_go .env path")
    parser.add_argument("--cdn-root", default="../web-r_CDN2_packages", help="web-r_CDN2_packages checkout path")
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
    version_rows = fetch_json_rows(env, package_version_sql())
    detail_groups = fetch_package_detail_groups(env)

    packages: dict[str, dict[str, Any]] = {}
    detail_paths_by_package: dict[str, str] = {}
    details_by_package: dict[str, dict[str, Any]] = {}
    news_manifest: dict[str, dict[str, str]] = {}
    news_payloads: dict[str, dict[str, Any]] = {}
    news_source_counts: dict[str, int] = {}
    news_latest_by_source: dict[str, str] = {}
    versions_by_key: dict[str, list[dict[str, str]]] = {}
    versions_by_package: dict[str, list[dict[str, str]]] = {}

    for row in version_rows:
        repository = text(row.get("repository"))
        package_name = text(row.get("package_name"))
        package_version = text(row.get("package_version"))
        if not repository or not package_name or not package_version:
            continue
        key_name = f"{repository.lower()}|{package_name.lower()}"
        versions_by_key.setdefault(key_name, []).append(
            {
                "repository": repository,
                "package_version": package_version,
                "published_at": text(row.get("published_at")),
                "first_seen_at": text(row.get("first_seen_at")),
                "last_seen_at": text(row.get("last_seen_at")),
            }
        )
        versions_by_package.setdefault(package_name.lower(), []).append(versions_by_key[key_name][-1])

    for row in package_rows:
        repository = text(row.get("repository"))
        package_name = text(row.get("package_name"))
        if not repository or not package_name:
            continue
        key_name = f"{repository.lower()}|{package_name.lower()}"
        package_key = package_name.lower()
        detail_path = package_detail_path(language, package_name)
        detail_paths_by_package[package_key] = detail_path
        profile = package_profile_from_row(row)
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
            "first_seen_at": text(row.get("first_seen_at")),
            "last_observed_at": text(row.get("last_observed_at")),
            "downloads_30d": int(row.get("downloads_30d") or 0),
            "reverse_depends_count": int(row.get("reverse_depends_count") or 0),
            "reverse_imports_count": int(row.get("reverse_imports_count") or 0),
            "cran_check_worst_status": text(row.get("cran_check_worst_status")),
            "lifecycle_status": text(row.get("lifecycle_status")),
            "path": detail_path,
        }
        details_by_package.setdefault(package_key, {"source_profiles": []})["source_profiles"].append(profile)

    for row in news_rows:
        uuid = normalize_uuid(row.get("item_uuid"))
        if not uuid:
            continue
        news_title, news_summary = package_news_display_fields(row)
        payload_json = text(row.get("payload_json"))
        raw_json = text(row.get("raw_json"))
        if text(row.get("source_id")) == "official:r-mail:r-packages" and news_summary:
            payload_json = package_news_json_with_korean(payload_json, news_title, news_summary)
            raw_json = package_news_json_with_korean(raw_json, news_title, news_summary)
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
            "title": news_title,
            "summary": news_summary,
            "author": text(row.get("author")),
            "canonical_url": text(row.get("canonical_url")),
        }
        source_id = item["source_id"]
        if source_id:
            news_source_counts[source_id] = news_source_counts.get(source_id, 0) + 1
            if published_at and published_at > news_latest_by_source.get(source_id, ""):
                news_latest_by_source[source_id] = published_at
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
                "raw_json": raw_json,
                "payload_json": payload_json,
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
    versions_manifest = {
        "schema": PACKAGE_VERSIONS_SCHEMA,
        "language": language,
        "generated_at": manifest["generated_at"],
        "versions": {key: rows for key, rows in versions_by_key.items() if key in packages and rows},
    }
    detail_payloads = build_package_detail_payloads(
        details_by_package,
        detail_paths_by_package,
        versions_by_package,
        detail_groups,
    )

    if args.dry_run:
        print(json.dumps({
            "packages": len(packages),
            "package_details": len(detail_payloads),
            "news": len(news_manifest),
            "news_sources": news_source_counts,
            "news_latest_by_source": news_latest_by_source,
            "versions": sum(len(v) for v in versions_manifest["versions"].values()),
        }, ensure_ascii=False))
        return 0

    for package_key, payload in detail_payloads.items():
        rel_path = detail_paths_by_package[package_key]
        write_json_atomic(cdn_root / rel_path, encrypt_document(payload, key, rel_path, language, "", compress=True))

    for uuid, payload in news_payloads.items():
        rel_path = news_manifest[uuid]["path"]
        write_json_atomic(cdn_root / rel_path, encrypt_document(payload, key, rel_path, language, uuid, compress=True))

    manifest_path = f"packages/{language}/index.json"
    write_json_atomic(cdn_root / manifest_path, encrypt_document(manifest, key, manifest_path, language, "", compress=True))
    versions_path = f"packages/{language}/versions.json"
    write_json_atomic(cdn_root / versions_path, encrypt_document(versions_manifest, key, versions_path, language, "", compress=True))
    print(json.dumps({
        "packages": len(packages),
        "package_details": len(detail_payloads),
        "news": len(news_manifest),
        "news_sources": news_source_counts,
        "news_latest_by_source": news_latest_by_source,
        "versions": sum(len(v) for v in versions_manifest["versions"].values()),
        "manifest": manifest_path,
        "versions_manifest": versions_path,
    }, ensure_ascii=False))
    return 0


def package_detail_path(language: str, package_name: str) -> str:
    return f"packages/{language}/details/{quote(package_name.lower(), safe='._-')}.json"


def package_news_display_fields(row: dict[str, Any]) -> tuple[str, str]:
    title = text(row.get("title"))
    summary = text(row.get("summary"))
    if text(row.get("source_id")) != "official:r-mail:r-packages":
        return title, summary
    payload = parse_json_object(row.get("payload_json"))
    raw = parse_json_object(row.get("raw_json"))
    nested_raw = parse_json_object(payload.get("raw_json")) if payload else {}
    raw_title = first_text(
        text(raw.get("target_title")),
        text(nested_raw.get("target_title")),
        text(payload.get("title")),
        title,
    )
    body = first_text(
        text(raw.get("target_content_text")),
        text(nested_raw.get("target_content_text")),
        text(raw.get("target_abstract")),
        text(nested_raw.get("target_abstract")),
        text(payload.get("summary")),
        summary,
    )
    package, version, is_new_package = r_packages_subject_parts(raw_title)
    summary_ko = r_packages_korean_summary(raw_title, body, package, version, is_new_package)
    if summary_ko and (not looks_korean(summary) or is_weak_package_news_summary(summary)):
        summary = summary_ko
    return title, summary


def parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    raw = text(value)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def package_news_json_with_korean(value: str, title: str, summary: str) -> str:
    row = parse_json_object(value)
    if not row:
        row = {}
    row["title_ko"] = title
    row["summary_ko"] = summary
    row["content_ko"] = summary
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"))


def looks_korean(value: str) -> bool:
    return bool(re.search(r"[가-힣]", value or ""))


def is_weak_package_news_summary(value: str) -> bool:
    lowered = (value or "").lower()
    return any(
        marker in lowered
        for marker in (
            "discovered from",
            "원문 링크에서 확인",
            "원문에는 패키지 배포 배경",
            "r-packages 메일링 리스트에 올라온",
        )
    )


def r_packages_subject_parts(title: str | None) -> tuple[str, str, bool]:
    title_text = re.sub(r"\s+", " ", str(title or "")).strip()
    is_new_package = False
    package = ""
    match = re.search(r"^\[R-pkgs\]\s+New package:\s*([^\s,;:]+)", title_text, re.IGNORECASE)
    if match:
        is_new_package = True
        package = match.group(1).strip()
    if not package:
        match = re.search(r"^\[R-pkgs\]\s+([^:]+)", title_text)
        if match:
            package = match.group(1).strip()
            package = re.sub(r"\bnew\s+version\b.*$", "", package, flags=re.IGNORECASE).strip()
            package = re.sub(r"\b(?:version|v)\s*[0-9][A-Za-z0-9._-]*\b", "", package, flags=re.IGNORECASE).strip()
    if not package:
        package = re.sub(r"\s+(?:신규\s+)?CRAN\s+패키지\s+공지$", "", title_text).strip()
    version = ""
    match = re.search(r"(?:version|v)\s*([0-9][A-Za-z0-9._-]*)", title_text, re.IGNORECASE)
    if match:
        version = match.group(1)
    return package, version, is_new_package


def r_packages_korean_summary(
    title: str | None,
    content_text: str | None,
    package: str,
    version: str,
    is_new_package: bool,
) -> str:
    message = r_packages_clean_message_text(content_text)
    if not message:
        return ""
    lower = message.lower()
    pkg = package or "해당 패키지"
    subject = " ".join(part for part in (pkg, version) if part).strip() or pkg
    feature_parts: list[str] = []
    if "terra package" in lower or "supersedes raster" in lower:
        feature_parts.append("terra 기반 전환")
    if "multi-core" in lower or "distributed computing" in lower:
        feature_parts.append("parallel 패키지를 통한 멀티코어/분산 계산 지원")
    if "new functions" in lower:
        feature_parts.append("새 함수 추가")
    if "improved documentation" in lower:
        feature_parts.append("문서 개선")
    if "pkgdown" in lower or "new webpage" in lower or "project website" in lower:
        feature_parts.append("pkgdown 기반 웹사이트 정비")
    if "graphical logo" in lower:
        feature_parts.append("새 로고")
    if "doi" in lower and "cran" in lower:
        feature_parts.append("CRAN DOI 반영")
    feature_sentence = ""
    if feature_parts:
        feature_sentence = " 주요 내용은 " + ", ".join(dict.fromkeys(feature_parts)) + "입니다."
    if "merge gridded datasets" in lower and "random forest" in lower:
        return (
            f"{pkg}는 Random Forest를 핵심 공간 예측 알고리즘으로 사용해 격자형 자료와 현장 관측값을 결합하는 R 패키지입니다. "
            f"{subject}가 CRAN에 공개되었습니다.{feature_sentence or ' 패키지 구조와 계산 성능을 개선한 변경 사항을 담고 있습니다.'}"
        )
    if "convex optimization" in lower and "cvxr" in lower:
        return (
            f"{subject}가 CRAN에 공개되어 R에서 convex optimization을 다루는 CVXR 기능을 새 구현으로 제공합니다. "
            "S7 class 기반 재작성, CVXPY 1.8.1과의 기능 대응, open source solver 지원, DPP/DGP/DQCP 지원과 시각화 기능을 포함합니다."
        )
    if "shiny application" in lower and "association" in lower:
        return (
            f"{pkg}는 다변량 데이터의 association 탐색을 위한 Shiny 애플리케이션을 제공하는 신규 CRAN 패키지입니다. "
            "정량·정성 변수에 맞는 association measure를 자동 선택하고, correlation network와 이변량 시각화를 통해 탐색적 분석을 지원합니다."
        )
    if is_new_package:
        return f"{pkg}가 신규 CRAN 패키지로 공개되었습니다.{feature_sentence or ' 본문에는 패키지의 분석 목적, 제공 기능, 관련 문서와 저장소 정보가 정리되어 있습니다.'}"
    if "released" in lower or "available on cran" in lower:
        return f"{subject}가 CRAN에 공개되었습니다.{feature_sentence or ' 본문에는 이번 릴리스의 주요 변경 사항과 참고 문서가 정리되어 있습니다.'}"
    return ""


def r_packages_clean_message_text(content_text: str | None) -> str:
    message = str(content_text or "").replace("\r\n", "\n").replace("\r", "\n")
    for marker in ("[[alternative HTML version deleted]]", "\n-- \n", "\n--\n"):
        index = message.find(marker)
        if index > 0:
            message = message[:index]
    message = re.sub(r"https?://\S+", " ", message)
    return re.sub(r"\s+", " ", message).strip()


def package_profile_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "repository": text(row.get("repository")),
        "package_name": text(row.get("package_name")),
        "latest_version": text(row.get("latest_version")),
        "title": text(row.get("title")),
        "description": text(row.get("description")),
        "maintainer": text(row.get("maintainer")),
        "author": text(row.get("author")),
        "authors_at_r": text(row.get("authors_at_r")),
        "license_text": text(row.get("license_text")),
        "depends": text(row.get("depends")),
        "imports": text(row.get("imports")),
        "suggests": text(row.get("suggests")),
        "linking_to": text(row.get("linking_to")),
        "enhances": text(row.get("enhances")),
        "system_requirements": text(row.get("system_requirements")),
        "needs_compilation": text(row.get("needs_compilation")),
        "date_publication": text(row.get("date_publication")),
        "published_at": text(row.get("published_at")),
        "package_url": text(row.get("package_url")),
        "bug_reports": text(row.get("bug_reports")),
        "last_observed_at": text(row.get("last_observed_at")),
        "downloads_30d": int(row.get("downloads_30d") or 0),
        "reverse_depends_count": int(row.get("reverse_depends_count") or 0),
        "reverse_imports_count": int(row.get("reverse_imports_count") or 0),
        "cran_check_worst_status": text(row.get("cran_check_worst_status")),
        "lifecycle_status": text(row.get("lifecycle_status")),
    }


def preferred_profile(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def score(row: dict[str, Any]) -> tuple[int, int, int, str]:
        repo = text(row.get("repository")).lower()
        repo_score = {"cran": 3, "bioconductor": 2, "r-universe": 1}.get(repo, 0)
        return (
            repo_score,
            int(row.get("reverse_imports_count") or 0),
            int(row.get("reverse_depends_count") or 0),
            text(row.get("package_name")).lower(),
        )

    if not rows:
        return {}
    return dict(sorted(rows, key=score, reverse=True)[0])


def group_rows(rows: list[dict[str, Any]], key_field: str = "package_key") -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        package_key = text(row.get(key_field)).lower()
        if not package_key:
            continue
        item = {k: v for k, v in row.items() if k != key_field}
        grouped.setdefault(package_key, []).append(item)
    return grouped


def first_rows(rows: list[dict[str, Any]], key_field: str = "package_key") -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        package_key = text(row.get(key_field)).lower()
        if package_key and package_key not in out:
            out[package_key] = {k: v for k, v in row.items() if k != key_field}
    return out


def fetch_package_detail_groups(env: dict[str, str]) -> dict[str, Any]:
    return {
        "cran_page": first_rows(fetch_json_rows(env, cran_page_sql())),
        "checks": group_rows(fetch_json_rows(env, cran_checks_sql())),
        "security": group_rows(fetch_json_rows(env, security_sql())),
        "bibliometric": group_rows(fetch_json_rows(env, bibliometric_sql())),
        "manual_topics": group_rows(fetch_json_rows(env, manual_topics_sql())),
        "artifacts": group_rows(fetch_json_rows(env, artifacts_sql())),
        "dependencies": group_rows(fetch_json_rows(env, dependencies_sql(reverse=False))),
        "reverse_dependencies": group_rows(fetch_json_rows(env, dependencies_sql(reverse=True))),
        "reverse_counts": group_rows(fetch_json_rows(env, reverse_counts_sql())),
        "github_repos": group_rows(fetch_json_rows(env, github_repos_sql())),
        "task_views": group_rows(fetch_json_rows(env, task_views_sql())),
        "website_mentions": group_rows(fetch_json_rows(env, website_mentions_sql())),
        "books": group_rows(fetch_json_rows(env, books_sql())),
    }


def build_package_detail_payloads(
    details_by_package: dict[str, dict[str, Any]],
    detail_paths_by_package: dict[str, str],
    versions_by_package: dict[str, list[dict[str, str]]],
    groups: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for package_key, detail_seed in details_by_package.items():
        if package_key not in detail_paths_by_package:
            continue
        source_profiles = detail_seed.get("source_profiles") or []
        detail = {
            "profile": preferred_profile(source_profiles),
            "source_profiles": source_profiles,
            "versions": versions_by_package.get(package_key, [])[:100],
            "dependencies": groups["dependencies"].get(package_key, []),
            "reverse_dependencies": groups["reverse_dependencies"].get(package_key, []),
            "reverse_counts": groups["reverse_counts"].get(package_key, []),
            "checks": groups["checks"].get(package_key, []),
            "security": groups["security"].get(package_key, []),
            "bibliometric": groups["bibliometric"].get(package_key, []),
            "manual_topics": groups["manual_topics"].get(package_key, []),
            "artifacts": groups["artifacts"].get(package_key, []),
            "github_repos": groups["github_repos"].get(package_key, []),
            "task_views": groups["task_views"].get(package_key, []),
            "website_mentions": groups["website_mentions"].get(package_key, []),
            "books": groups["books"].get(package_key, []),
        }
        cran_page = groups["cran_page"].get(package_key)
        if cran_page:
            detail["cran_page"] = cran_page
        payloads[package_key] = {
            "schema": CONTENT_SCHEMA,
            "kind": "package_detail",
            "detail": detail,
        }
    return payloads


def package_sql(limit: int) -> str:
    suffix = f"\nLIMIT {int(limit)}" if limit and limit > 0 else ""
    return f"""
SELECT repository,
       package_name,
       latest_version,
       title,
       left(description, 260) AS description,
       maintainer,
       author,
       authors_at_r,
       license_text,
       depends,
       imports,
       suggests,
       linking_to,
       enhances,
       system_requirements,
       needs_compilation,
       date_publication,
       package_url,
       bug_reports,
       if(isNull(published_sort_key), '', formatDateTime(published_sort_key, '%Y-%m-%d %H:%i:%S')) AS published_at,
       if(isNull(first_seen_at), '', formatDateTime(first_seen_at, '%Y-%m-%d %H:%i:%S')) AS first_seen_at,
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
           b.author,
           b.authors_at_r,
           b.license_text,
           b.depends,
           b.imports,
           b.suggests,
           b.linking_to,
           b.enhances,
           b.system_requirements,
           b.needs_compilation,
           b.date_publication,
           b.package_url,
           b.bug_reports,
           coalesce(cran_page.published_at, package_current.published_at, version_history.version_published_at) AS published_sort_key,
           package_seen.first_seen_at AS first_seen_at,
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
               argMax(pc.author, pc.version) AS author,
               argMax(pc.authors_at_r, pc.version) AS authors_at_r,
               argMax(pc.license_text, pc.version) AS license_text,
               argMax(pc.depends, pc.version) AS depends,
               argMax(pc.imports, pc.version) AS imports,
               argMax(pc.suggests, pc.version) AS suggests,
               argMax(pc.linking_to, pc.version) AS linking_to,
               argMax(pc.enhances, pc.version) AS enhances,
               argMax(pc.system_requirements, pc.version) AS system_requirements,
               argMax(pc.needs_compilation, pc.version) AS needs_compilation,
               argMax(pc.date_publication, pc.version) AS date_publication,
               argMax(pc.package_url, pc.version) AS package_url,
               argMax(pc.bug_reports, pc.version) AS bug_reports,
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
      LEFT JOIN
    (
        SELECT lowerUTF8(repository) AS repository_key,
               lowerUTF8(package_name) AS package_key,
               min(first_seen_at) AS first_seen_at
          FROM Data_R_Package_Service.package_version_history
         GROUP BY repository_key, package_key
    ) AS package_seen ON b.repository_key = package_seen.repository_key
                     AND b.package_key = package_seen.package_key
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
       {PACKAGE_NEWS_TITLE_SQL} AS title,
       {PACKAGE_NEWS_SUMMARY_SQL} AS summary,
       author,
       tags_json,
       toString(raw_json) AS raw_json,
       toString(payload_json) AS payload_json,
       if(isNull(original_published_at), '', formatDateTime(original_published_at, '%Y-%m-%d %H:%i:%S')) AS published_at_text,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at_text
  FROM Data_R_Community_Service.v_r_community_latest_dedup
 WHERE notEmpty(canonical_url)
   AND notEmpty({PACKAGE_NEWS_TITLE_SQL})
   AND (
          source_type = 'package_update_feed'
          OR source_id = 'official:r-mail:r-packages'
       )
   AND (
          source_id != 'official:r-mail:r-packages'
          OR match(canonical_url, '/pipermail/r-packages/[0-9]{{4}}/[0-9]+[.]html')
       )
   AND (
          source_id != 'official:bioconductor:release-announcements'
          OR positionCaseInsensitiveUTF8(canonical_url, '/news/bioc_') > 0
          OR positionCaseInsensitiveUTF8(title, 'Released') > 0
          OR positionCaseInsensitiveUTF8(title, '릴리스') > 0
       )
 ORDER BY if(isNull(original_published_at), collected_at, original_published_at) DESC,
          sipHash64(if(notEmpty(external_id), external_id, canonical_url)) ASC{suffix}
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def package_version_sql() -> str:
    return """
SELECT repository,
       package_name,
       package_version,
       if(isNull(published_at_value), '', formatDateTime(published_at_value, '%Y-%m-%d %H:%i:%S')) AS published_at,
       if(isNull(first_seen_at_value), '', formatDateTime(first_seen_at_value, '%Y-%m-%d %H:%i:%S')) AS first_seen_at,
       if(isNull(last_seen_at_value), '', formatDateTime(last_seen_at_value, '%Y-%m-%d %H:%i:%S')) AS last_seen_at
  FROM
(
    SELECT lowerUTF8(toString(vh.repository)) AS repository_key,
           lowerUTF8(toString(vh.package_name)) AS package_key,
           argMax(toString(vh.repository), vh.version) AS repository,
           argMax(toString(vh.package_name), vh.version) AS package_name,
           toString(vh.package_version) AS package_version,
           parseDateTime64BestEffortOrNull(nullIf(toString(argMax(vh.published_at, vh.version)), ''), 3, 'Asia/Seoul') AS published_at_value,
           parseDateTime64BestEffortOrNull(nullIf(toString(min(vh.first_seen_at)), ''), 3, 'Asia/Seoul') AS first_seen_at_value,
           parseDateTime64BestEffortOrNull(nullIf(toString(max(vh.last_seen_at)), ''), 3, 'Asia/Seoul') AS last_seen_at_value
      FROM Data_R_Package_Service.package_version_history AS vh
     WHERE notEmpty(toString(vh.package_name))
       AND notEmpty(toString(vh.package_version))
     GROUP BY repository_key, package_key, package_version
)
 ORDER BY repository_key ASC,
          package_key ASC,
          toUnixTimestamp64Milli(ifNull(published_at_value, toDateTime64('1970-01-01 00:00:00', 3, 'Asia/Seoul'))) DESC,
          package_version DESC
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def cran_page_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       'CRAN' AS repository,
       toString(package_version) AS package_version,
       toString(page_url) AS page_url,
       toString(fields_json) AS fields_json,
       toString(field_rows_json) AS field_rows_json,
       toString(links_json) AS links_json,
       toString(sections_json) AS sections_json,
       toString(doi) AS doi,
       toString(doi_url) AS doi_url,
       toString(citation_url) AS citation_url,
       toString(cran_checks_url) AS cran_checks_url,
       toString(reference_manual_html_url) AS reference_manual_html_url,
       toString(reference_manual_pdf_url) AS reference_manual_pdf_url,
       toString(package_source_url) AS package_source_url,
       toString(archive_url) AS archive_url,
       toString(in_views_json) AS in_views_json,
       toString(materials_json) AS materials_json,
       toString(readme_url) AS readme_url,
       toString(news_url) AS news_url,
       toString(documentation_json) AS documentation_json,
       toString(vignettes_json) AS vignettes_json,
       toString(downloads_json) AS downloads_json,
       toInt64(reverse_depends_count) AS reverse_depends_count,
       toInt64(reverse_imports_count) AS reverse_imports_count,
       toInt64(reverse_suggests_count) AS reverse_suggests_count,
       toInt64(reverse_linking_to_count) AS reverse_linking_to_count,
       toInt64(reverse_enhances_count) AS reverse_enhances_count,
       toInt64(all_links_count) AS all_links_count,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_R_Package_Service.package_cran_page_current
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC, version DESC
 LIMIT 1 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def cran_checks_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(flavor) AS flavor,
       toString(status) AS status,
       formatDateTime(checked_at, '%Y-%m-%d %H:%i:%S') AS checked_at,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at,
       toString(raw_cells_json) AS raw_cells_json
  FROM Data_R_Package_Service.package_cran_check_current
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC, status_rank DESC, flavor ASC
 LIMIT 40 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def security_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(ecosystem) AS ecosystem,
       toString(package_version) AS package_version,
       toInt64(vulnerability_count) AS vulnerability_count,
       toString(vuln_ids_json) AS vuln_ids_json,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_R_Package_Service.package_security_osv_current
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC, vulnerability_count DESC, ecosystem ASC
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def bibliometric_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(package_version) AS package_version,
       toString(query) AS query,
       toInt64(result_count) AS result_count,
       toString(top_work_id) AS top_work_id,
       toString(top_work_title) AS top_work_title,
       toInt64(top_work_year) AS top_work_year,
       toInt64(top_work_cited_by) AS top_work_cited_by,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_R_Package_Service.package_bibliometric_current
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC, result_count DESC, top_work_cited_by DESC
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def manual_topics_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(repository) AS repository,
       toString(package_name) AS package_name,
       toString(package_version) AS package_version,
       toString(page_url) AS page_url,
       toString(source_package_url) AS source_package_url,
       toString(topic_name) AS topic_name,
       toString(title) AS title,
       toString(description) AS description,
       toString(usage) AS usage,
       toString(arguments_json) AS arguments_json,
       toString(details) AS details,
       toString(value) AS value,
       toString(format_text) AS format_text,
       toString(source_text) AS source_text,
       toString(examples) AS examples,
       toString(seealso) AS seealso,
       toString(keywords_json) AS keywords_json,
       toString(aliases_json) AS aliases_json,
       toString(concepts_json) AS concepts_json,
       toString(doc_type) AS doc_type,
       toString(encoding) AS encoding,
       toString(custom_sections_json) AS custom_sections_json,
       toString(note) AS note,
       toString(author) AS author,
       toString(references_text) AS references_text,
       toString(rd_path) AS rd_path,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_R_Package_Service.package_document_manual_topic_current
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC,
          multiIf(repository = 'CRAN', 3, repository = 'Bioconductor', 2, repository = 'R-universe', 1, 0) DESC,
          topic_name ASC
 LIMIT 80 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def artifacts_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(repository) AS repository,
       toString(package_version) AS package_version,
       toString(artifact_type) AS artifact_type,
       toString(artifact_label) AS artifact_label,
       toString(artifact_url) AS artifact_url,
       toString(artifact_section) AS artifact_section,
       toString(content_type) AS content_type,
       toInt64(content_length) AS content_length,
       toString(title) AS title,
       toString(text_content) AS text_content,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_R_Package_Service.package_document_artifact_current
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC,
          multiIf(repository = 'CRAN', 3, repository = 'Bioconductor', 2, repository = 'R-universe', 1, 0) DESC,
          artifact_type ASC,
          artifact_label ASC,
          artifact_url ASC
 LIMIT 12 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def dependencies_sql(reverse: bool) -> str:
    if reverse:
        package_expr = "lowerUTF8(toString(edge.to_package))"
        row_key = "lowerUTF8(toString(edge.from_package))"
        order_key = "from_package"
    else:
        package_expr = "lowerUTF8(toString(edge.from_package))"
        row_key = "lowerUTF8(toString(edge.to_package))"
        order_key = "to_package"
    return f"""
SELECT package_key,
       from_repository,
       from_package,
       from_version,
       to_package,
       dependency_type,
       dependency_spec,
       collected_at
  FROM
(
    SELECT *,
           row_number() OVER (PARTITION BY package_key ORDER BY type_rank ASC, {order_key}_key ASC) AS rn
      FROM
    (
        SELECT {package_expr} AS package_key,
               {row_key} AS {order_key}_key,
               argMax(toString(edge.from_repository), edge.version) AS from_repository,
               any(toString(edge.from_package)) AS from_package,
               argMax(toString(edge.from_version), edge.version) AS from_version,
               any(toString(edge.to_package)) AS to_package,
               toString(edge.dependency_type) AS dependency_type,
               argMax(toString(edge.dependency_spec), edge.version) AS dependency_spec,
               formatDateTime(max(edge.collected_at), '%Y-%m-%d %H:%i:%S') AS collected_at,
               multiIf(dependency_type = 'Depends', 0, dependency_type = 'Imports', 1, dependency_type = 'LinkingTo', 2, dependency_type = 'Suggests', 3, dependency_type = 'Enhances', 4, 9) AS type_rank
          FROM Data_R_Package_Service.package_dependency_edge_current AS edge
         WHERE notEmpty(toString(edge.from_package))
           AND notEmpty(toString(edge.to_package))
         GROUP BY package_key, {order_key}_key, dependency_type
    )
)
 WHERE rn <= 120
 ORDER BY package_key ASC, type_rank ASC, {order_key}_key ASC
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def reverse_counts_sql() -> str:
    return """
SELECT lowerUTF8(toString(to_package)) AS package_key,
       toString(dependency_type) AS key,
       toInt64(countDistinct(from_package)) AS item_count,
       toInt64(0) AS view_count
  FROM Data_R_Package_Service.package_dependency_edge_current
 WHERE notEmpty(toString(to_package))
 GROUP BY package_key, dependency_type
HAVING item_count > 0
 ORDER BY package_key ASC,
          multiIf(dependency_type = 'Depends', 0, dependency_type = 'Imports', 1, dependency_type = 'LinkingTo', 2, dependency_type = 'Suggests', 3, dependency_type = 'Enhances', 4, 9),
          dependency_type ASC
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def github_repos_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(package_version) AS package_version,
       toString(repo_owner) AS repo_owner,
       toString(repo_name) AS repo_name,
       toString(html_url) AS html_url,
       toString(description) AS description,
       toInt64(stargazers_count) AS stargazers_count,
       toInt64(forks_count) AS forks_count,
       toInt64(open_issues_count) AS open_issues_count,
       toInt64(archived) AS archived,
       if(isNull(pushed_at), '', formatDateTime(pushed_at, '%Y-%m-%d %H:%i:%S')) AS pushed_at,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_R_Package_Service.package_github_repo_current
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC, stargazers_count DESC, forks_count DESC
 LIMIT 10 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def task_views_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(task_view) AS task_view,
       any(toString(source_url)) AS source_url,
       formatDateTime(max(collected_at), '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_R_Package_Raw.cran_task_view_package_edge_raw
 WHERE notEmpty(toString(package_name))
 GROUP BY package_key, task_view
 ORDER BY package_key ASC, task_view ASC
 LIMIT 40 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def website_mentions_sql() -> str:
    return """
SELECT lowerUTF8(toString(package_name)) AS package_key,
       toString(source_url) AS source_url,
       toString(mention_context) AS mention_context,
       toFloat64(confidence) AS confidence,
       formatDateTime(detected_at, '%Y-%m-%d %H:%i:%S') AS detected_at,
       toString(source) AS source
  FROM Data_R_Package_Raw.r_website_package_mention_raw
 WHERE notEmpty(toString(package_name))
 ORDER BY package_key ASC, detected_at DESC, confidence DESC
 LIMIT 20 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


def books_sql() -> str:
    return """
SELECT lowerUTF8(toString(search_query)) AS package_key,
       toString(isbn) AS isbn,
       toString(title) AS title,
       toString(link) AS link,
       toString(image) AS image,
       toString(author) AS author,
       toString(publisher) AS publisher,
       toString(pubdate) AS pubdate,
       toString(search_query) AS search_query,
       formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S') AS collected_at
  FROM Data_Book_NAVER_Service.naver_book_recent
 WHERE search_mode = 'r_package'
   AND notEmpty(toString(search_query))
 ORDER BY package_key ASC, collected_at DESC, title ASC
 LIMIT 12 BY package_key
 FORMAT JSONEachRow
 SETTINGS distributed_product_mode = 'global'
"""


if __name__ == "__main__":
    sys.exit(main())
