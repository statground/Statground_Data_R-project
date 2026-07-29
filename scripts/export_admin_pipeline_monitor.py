#!/usr/bin/env python3
"""Export the admin pipeline monitor snapshot to a CDN checkout."""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from clickhouse_http import build_clickhouse_url


KST = timezone(timedelta(hours=9), name="KST")
GITHUB_API_BASE = "https://api.github.com/repos/"
SNAPSHOT_PATH = "admin/pipelines/latest.json"


PIPELINES = [
    {
        "key": "r_project_package",
        "label": "R Project Package Collection",
        "repo": "statground/Statground_Data_R-project",
        "repo_label": "Statground_Data_R-project",
        "workflow_file": "r-project-package.yml",
        "output_group": "r_project",
        "stages": [
            {"key": "prepare", "label": "준비", "matches": ["checkout", "set run options", "validate clickhouse"]},
            {"key": "collect_packages", "label": "R 패키지", "matches": ["collect r package ecosystem"]},
        ],
    },
    {
        "key": "r_project_social",
        "label": "R Project Social Collection",
        "repo": "statground/Statground_Data_R-project",
        "repo_label": "Statground_Data_R-project",
        "workflow_file": "r-project-social.yml",
        "output_group": "r_project",
        "stages": [
            {"key": "prepare", "label": "준비", "matches": ["checkout", "set run options", "install yt-dlp", "validate clickhouse"]},
            {"key": "collect_youtube", "label": "YouTube", "matches": ["collect public r youtube"]},
            {"key": "collect_blogger", "label": "R-bloggers", "matches": ["collect r-bloggers"]},
            {"key": "collect_mastodon", "label": "Mastodon", "matches": ["collect r foundation public mastodon"]},
        ],
    },
    {
        "key": "r_project_community",
        "label": "R Project Community Collection",
        "repo": "statground/Statground_Data_R-project",
        "repo_label": "Statground_Data_R-project",
        "workflow_file": "r-project-community.yml",
        "output_group": "r_project",
        "stages": [
            {"key": "prepare", "label": "준비", "matches": ["checkout", "set run options", "install python", "validate clickhouse"]},
            {"key": "collect_community", "label": "커뮤니티", "matches": ["collect r community sources"]},
            {"key": "publish_clickhouse", "label": "ClickHouse 게시", "matches": ["publish r community events to clickhouse"]},
            {"key": "ingest_wait", "label": "수집 반영", "matches": ["wait for r community clickhouse ingestion"]},
        ],
    },
    {
        "key": "r_project_community_digest",
        "label": "R Project Community Digest",
        "repo": "statground/Statground_Data_R-project",
        "repo_label": "Statground_Data_R-project",
        "workflow_file": "r-project-community-digest.yml",
        "output_group": "r_project",
        "stages": [
            {"key": "prepare", "label": "준비", "matches": ["checkout", "set run options", "validate clickhouse"]},
            {"key": "summarize", "label": "요약", "matches": ["generate r community daily digests", "wait for r community daily digest visibility"]},
        ],
    },
    {
        "key": "r_project_notebook",
        "label": "R Project Notebook Generation",
        "repo": "statground/Statground_Data_R-project",
        "repo_label": "Statground_Data_R-project",
        "workflow_file": "r-project-notebook.yml",
        "output_group": "notebook",
        "stages": [
            {"key": "prepare", "label": "준비", "matches": ["checkout", "set run options", "install python", "install webr", "validate clickhouse"]},
            {"key": "notebook", "label": "Notebook 생성", "matches": ["generate and insert web-r notebook"]},
        ],
    },
    {
        "key": "r_project_cdn",
        "label": "R Project CDN Publish",
        "repo": "statground/Statground_Data_R-project",
        "repo_label": "Statground_Data_R-project",
        "workflow_file": "r-project-cdn.yml",
        "output_group": "r_project",
        "stages": [
            {"key": "prepare", "label": "준비", "matches": ["checkout", "set run options", "validate web-r cdn2 publish"]},
            {"key": "cdn_export", "label": "CDN 생성", "matches": ["validate web-r cdn2 publish", "ensure web-r cdn2", "checkout web-r cdn2", "export encrypted web-r cdn2 content"]},
            {"key": "cdn_publish", "label": "CDN 배치", "matches": ["commit and push web-r cdn2", "record web-r cdn2 releases", "verify web-r cdn2 release"]},
            {"key": "admin_snapshot", "label": "운영 스냅샷", "matches": ["export admin pipeline monitor", "commit and push web-r cdn2 admin pipeline snapshot", "record web-r cdn2 admin pipeline snapshot release", "verify web-r cdn2 admin pipeline snapshot release"]},
        ],
    },
    {
        "key": "kakao_book_scheduled",
        "label": "Kakao Book Scheduled Provider Pipeline",
        "repo": "statground/Statground_Data_NAVER_Book",
        "repo_label": "Statground_Data_NAVER_Book",
        "workflow_file": "kakao_book_schedule.yml",
        "output_group": "book",
        "stages": [
            {"key": "prepare", "label": "테스트·빌드", "matches": ["checkout", "setup go", "run secret-free tests and build"]},
            {"key": "validate_kakao", "label": "Kakao 키 검증", "matches": ["validate kakao api key"]},
            {"key": "validate_clickhouse", "label": "ClickHouse 검증", "matches": ["validate clickhouse endpoint", "validate clickhouse https transport", "validate clickhouse credentials"]},
            {"key": "collect", "label": "Kakao 도서 수집", "matches": ["collect kakao books into provider tables"]},
            {"key": "catalog", "label": "카탈로그 갱신", "matches": ["refresh provider-neutral serving catalogs"], "optional": True},
        ],
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdn-root", default="../web-r_CDN2", help="web-r_CDN2 checkout path")
    parser.add_argument("--language", default="ko")
    args = parser.parse_args()

    data = build_snapshot(args.language)
    target = Path(args.cdn_root) / SNAPSHOT_PATH
    write_json_atomic(target, data)

    report = {
        "schema": "web-r.admin-pipelines.export-report.v1",
        "language": args.language,
        "scope": "web-r-admin-pipelines",
        "export": int(data.get("summary", {}).get("output_count") or 0),
        "runs": len(data.get("recent_runs") or []),
        "pipelines": len(data.get("pipelines") or []),
        "item_count": int(data.get("summary", {}).get("output_count") or 0) + len(data.get("recent_runs") or []),
        "manifest_paths": [SNAPSHOT_PATH],
    }
    print(json.dumps(report, ensure_ascii=False, separators=(",", ":")))
    return 0


def build_snapshot(language: str) -> dict[str, Any]:
    warnings: list[str] = []
    pipelines: list[dict[str, Any]] = []
    recent_runs: list[dict[str, Any]] = []
    windows: dict[str, tuple[datetime, datetime]] = {}

    for definition in PIPELINES:
        try:
            runs = github_workflow_runs(definition["repo"], definition["workflow_file"], 6)
        except Exception:
            warnings.append(f"github_{definition['key']}_unavailable")
            pipelines.append(unavailable_pipeline(definition))
            continue
        if not runs:
            warnings.append(f"github_{definition['key']}_empty")
            pipelines.append(unavailable_pipeline(definition))
            continue

        latest = runs[0]
        try:
            jobs = github_workflow_jobs(definition["repo"], int(latest.get("id") or 0))
        except Exception:
            warnings.append(f"github_{definition['key']}_jobs_unavailable")
            jobs = []

        started = parse_github_time(str(latest.get("run_started_at") or "")) or parse_github_time(str(latest.get("created_at") or ""))
        ended = parse_github_time(str(latest.get("updated_at") or ""))
        if started:
            window_end = ended or datetime.now(timezone.utc)
            if window_end < started:
                window_end = datetime.now(timezone.utc)
            output_group = str(definition["output_group"])
            merged_end = window_end + timedelta(minutes=45)
            if output_group in windows:
                previous_start, previous_end = windows[output_group]
                windows[output_group] = (min(previous_start, started), max(previous_end, merged_end))
            else:
                windows[output_group] = (started, merged_end)

        recent_runs.extend(run_row(definition, run) for run in runs)
        pipelines.append(
            {
                "key": definition["key"],
                "label": definition["label"],
                "repo": definition["repo"],
                "repo_label": definition["repo_label"],
                "workflow_file": definition["workflow_file"],
                "output_group": definition["output_group"],
                "status": pipeline_status(str(latest.get("status") or ""), str(latest.get("conclusion") or "")),
                "conclusion": str(latest.get("conclusion") or "").strip(),
                "run_id": latest.get("id") or 0,
                "run_number": latest.get("run_number") or 0,
                "run_url": latest.get("html_url") or "",
                "event": latest.get("event") or "",
                "head_sha": latest.get("head_sha") or "",
                "created_at": format_time(parse_github_time(str(latest.get("created_at") or ""))),
                "started_at": format_time(started),
                "updated_at": format_time(ended),
                "duration_seconds": duration_seconds(started, ended),
                "stages": stage_rows(definition["stages"], jobs),
            }
        )

    recent_runs.sort(key=lambda row: str(row.get("started_at") or ""), reverse=True)
    source_groups, source_warnings = source_groups_snapshot()
    outputs, output_warnings = outputs_snapshot(windows)
    warnings.extend(source_warnings)
    warnings.extend(output_warnings)

    snapshot_summary = summary(pipelines, outputs, warnings)
    return {
        "ok": True,
        "partial": bool(snapshot_summary.get("unknown_count")),
        "schema": "web-r.admin-pipelines.snapshot.v1",
        "language": language,
        "generated_at": format_time(datetime.now(timezone.utc)),
        "snapshot_source": "cdn2",
        "summary": snapshot_summary,
        "pipelines": pipelines,
        "recent_runs": recent_runs,
        "source_groups": source_groups,
        "outputs": outputs,
        "warnings": unique(warnings),
    }


def unavailable_pipeline(definition: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": definition["key"],
        "label": definition["label"],
        "repo": definition["repo"],
        "repo_label": definition["repo_label"],
        "workflow_file": definition["workflow_file"],
        "output_group": definition["output_group"],
        "status": "unknown",
        "stages": [],
    }


def stage_rows(definitions: list[dict[str, Any]], jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    steps = [step for job in jobs for step in job.get("steps", []) or []]
    out: list[dict[str, Any]] = []
    for definition in definitions:
        matched = [step for step in steps if step_matches(str(step.get("name") or ""), definition.get("matches") or [])]
        out.append(
            {
                "key": definition["key"],
                "label": definition["label"],
                "status": stage_status(matched, bool(definition.get("optional"))),
                "optional": bool(definition.get("optional")),
                "steps": [step_row(step) for step in matched],
            }
        )
    return out


def step_matches(name: str, matches: list[str]) -> bool:
    needle = name.strip().lower()
    return any(match.strip().lower() in needle for match in matches if match.strip())


def stage_status(steps: list[dict[str, Any]], optional: bool) -> str:
    if not steps:
        return "skipped" if optional else "pending"
    any_running = False
    any_success = False
    all_skipped = True
    for step in steps:
        status = pipeline_status(str(step.get("status") or ""), str(step.get("conclusion") or ""))
        if status in {"failed", "failure", "cancelled", "timed_out", "action_required"}:
            return "failed"
        if status in {"running", "queued", "in_progress", "waiting", "pending", "requested"}:
            any_running = True
        elif status in {"success", "neutral"}:
            any_success = True
            all_skipped = False
        elif status != "skipped":
            all_skipped = False
    if any_running:
        return "running"
    if all_skipped:
        return "skipped"
    if any_success:
        return "success"
    return "unknown"


def step_row(step: dict[str, Any]) -> dict[str, Any]:
    started = parse_github_time(str(step.get("started_at") or ""))
    ended = parse_github_time(str(step.get("completed_at") or ""))
    return {
        "name": step.get("name") or "",
        "status": pipeline_status(str(step.get("status") or ""), str(step.get("conclusion") or "")),
        "conclusion": str(step.get("conclusion") or "").strip(),
        "number": step.get("number") or 0,
        "started_at": format_time(started),
        "completed_at": format_time(ended),
        "duration_seconds": duration_seconds(started, ended),
    }


def run_row(definition: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    started = parse_github_time(str(run.get("run_started_at") or "")) or parse_github_time(str(run.get("created_at") or ""))
    ended = parse_github_time(str(run.get("updated_at") or ""))
    return {
        "pipeline_key": definition["key"],
        "pipeline_label": definition["label"],
        "repo": definition["repo"],
        "repo_label": definition["repo_label"],
        "workflow_file": definition["workflow_file"],
        "run_id": run.get("id") or 0,
        "run_number": run.get("run_number") or 0,
        "run_url": run.get("html_url") or "",
        "event": run.get("event") or "",
        "status": pipeline_status(str(run.get("status") or ""), str(run.get("conclusion") or "")),
        "conclusion": str(run.get("conclusion") or "").strip(),
        "started_at": format_time(started),
        "updated_at": format_time(ended),
        "duration_seconds": duration_seconds(started, ended),
    }


def source_groups_snapshot() -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    out = {"r_project": [], "book": [], "naver_book": [], "cdn_release": []}
    warnings: list[str] = []
    out["r_project"], ok = safe_query(
        """
        SELECT
            source_id,
            anyLast(source_name) AS source_name,
            anyLast(source_type) AS source_type,
            anyLast(platform) AS platform,
            count() AS item_count,
            countIf(ingested_at >= now64(3, 'Asia/Seoul') - INTERVAL 24 HOUR) AS item_count_24h,
            max(ifNull(original_published_at, collected_at)) AS latest_source_at,
            max(collected_at) AS latest_collected_at,
            max(ingested_at) AS latest_ingested_at
        FROM Data_R_Community_Service.v_r_community_latest
        GROUP BY source_id
        ORDER BY latest_ingested_at DESC
        LIMIT 80
        """,
    )
    if not ok:
        warnings.append("r_project_sources_unavailable")
    out["book"], ok = safe_query(
        """
        SELECT
            search_mode,
            search_query,
            search_sort,
            count() AS log_count,
            sum(fetched_count) AS fetched_count,
            countIf(status = 'ERROR') AS error_count,
            argMax(status, collected_at) AS latest_status,
            max(collected_at) AS latest_collected_at,
            max(ingested_at) AS latest_ingested_at
        FROM Data_Book_KAKAO_Log.kakao_collect_log
        WHERE collected_at >= now64(3, 'Asia/Seoul') - INTERVAL 7 DAY
        GROUP BY search_mode, search_query, search_sort
        ORDER BY latest_ingested_at DESC
        LIMIT 100
        """,
    )
    out["naver_book"] = out["book"]
    if not ok:
        warnings.append("book_sources_unavailable")
    out["cdn_release"], ok = safe_query(
        """
        SELECT
            release_scope,
            language,
            cdn_repo,
            cdn_branch,
            commit_sha,
            base_url,
            item_count,
            published_at
        FROM Data_R_Community_Service.v_web_r_cdn_release_latest
        ORDER BY published_at DESC
        LIMIT 20
        """,
    )
    if not ok:
        warnings.append("cdn_release_sources_unavailable")
    return out, warnings


def outputs_snapshot(windows: dict[str, tuple[datetime, datetime]]) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    out = {
        "r_posts": [],
        "digests": [],
        "notebooks": [],
        "books": [],
        "naver_books": [],
        "r_book_catalog": [],
        "cdn_releases": [],
    }
    warnings: list[str] = []
    out["r_posts"], ok = safe_query(
        """
        SELECT
            external_id,
            source_id,
            source_name,
            source_type,
            platform,
            title,
            author,
            language,
            canonical_url,
            original_published_at,
            collected_at,
            ingested_at
        FROM Data_R_Community_Service.v_r_community_latest
        """
        + window_filter("ingested_at", windows.get("r_project"), 36)
        + """
        ORDER BY ingested_at DESC
        LIMIT 30
        """,
    )
    if not ok:
        warnings.append("r_project_outputs_unavailable")
    out["digests"], ok = safe_query(
        """
        SELECT
            toString(digest_uuid) AS digest_uuid,
            digest_date,
            source_id,
            source_name,
            source_type,
            platform,
            title,
            item_count,
            deduped_item_count,
            generation_status,
            updated_at,
            concat('/community/read/', toString(digest_uuid), '/') AS url
        FROM Data_R_Community_Service.v_r_community_daily_digest_latest
        """
        + window_filter("updated_at", windows.get("r_project"), 36)
        + """
        ORDER BY updated_at DESC
        LIMIT 30
        """,
    )
    if not ok:
        warnings.append("r_digest_outputs_unavailable")
    out["notebooks"], ok = safe_query(
        """
        SELECT
            toString(uuid) AS notebook_uuid,
            toString(if(isNull(uuid_share), uuid, uuid_share)) AS share_uuid,
            title,
            description,
            share,
            created_at,
            updated_at,
            concat('/webr/notebook/view/', toString(if(isNull(uuid_share), uuid, uuid_share)), '/') AS url
        FROM webr_webr.v_d1_notebook
        """
        + window_filter("created_at", windows.get("notebook"), 48)
        + """
          AND ifNull(share, 0) = 1
        ORDER BY created_at DESC
        LIMIT 20
        """,
    )
    if not ok:
        warnings.append("notebook_outputs_unavailable")
    out["books"], ok = safe_query(
        """
        SELECT
            isbn,
            toString(uuid) AS uuid,
            provider,
            title,
            author,
            publisher,
            search_mode,
            search_query,
            search_sort,
            link,
            updated_at,
            collected_at,
            ingested_at
        FROM Data_Book_Service.book_recent
        """
        + window_filter("updated_at", windows.get("book"), 36)
        + """
        ORDER BY updated_at DESC
        LIMIT 40
        """,
    )
    out["naver_books"] = out["books"]
    if not ok:
        warnings.append("book_outputs_unavailable")
    out["r_book_catalog"], ok = safe_query(
        """
        SELECT
            isbn,
            metadata_provider,
            title,
            author,
            publisher,
            source_kind,
            search_mode,
            search_query,
            source_collected_at,
            source_ingested_at,
            concat('/book/?q=', replaceAll(title, ' ', '+')) AS url
        FROM webr_book.v_book_catalog
        """
        + window_filter("source_ingested_at", windows.get("book"), 36)
        + """
        ORDER BY source_ingested_at DESC
        LIMIT 30
        """,
    )
    if not ok:
        warnings.append("r_book_catalog_outputs_unavailable")
    out["cdn_releases"], ok = safe_query(
        """
        SELECT
            release_scope,
            language,
            cdn_repo,
            commit_sha,
            base_url,
            item_count,
            published_at
        FROM Data_R_Community_Service.v_web_r_cdn_release_latest
        ORDER BY published_at DESC
        LIMIT 20
        """,
    )
    if not ok:
        warnings.append("cdn_release_outputs_unavailable")
    return out, warnings


def window_filter(column: str, window: tuple[datetime, datetime] | None, fallback_hours: int) -> str:
    if window:
        start, end = window
        return (
            f" WHERE {column} >= parseDateTime64BestEffort({quote_ch(format_time(start))}, 3, 'Asia/Seoul')"
            f" AND {column} <= parseDateTime64BestEffort({quote_ch(format_time(end))}, 3, 'Asia/Seoul')"
        )
    return f" WHERE {column} >= now64(3, 'Asia/Seoul') - INTERVAL {int(fallback_hours)} HOUR"


def summary(pipelines: list[dict[str, Any]], outputs: dict[str, list[dict[str, Any]]], warnings: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for pipeline in pipelines:
        status = str(pipeline.get("status") or "")
        counts[status] = counts.get(status, 0) + 1
    output_count = sum(len(outputs.get(key) or []) for key in ("r_posts", "digests", "notebooks", "books", "r_book_catalog", "cdn_releases"))
    return {
        "pipeline_count": len(pipelines),
        "success_count": counts.get("success", 0),
        "running_count": counts.get("in_progress", 0) + counts.get("queued", 0) + counts.get("running", 0),
        "failed_count": counts.get("failure", 0) + counts.get("failed", 0) + counts.get("cancelled", 0) + counts.get("timed_out", 0) + counts.get("action_required", 0),
        "unknown_count": counts.get("unknown", 0),
        "output_count": output_count,
        "warning_count": len(unique(warnings)),
    }


def github_workflow_runs(repo: str, workflow_file: str, per_page: int) -> list[dict[str, Any]]:
    path = "actions/workflows/" + urllib.parse.quote(workflow_file, safe="") + f"/runs?per_page={int(per_page)}"
    return github_json(repo, path).get("workflow_runs") or []


def github_workflow_jobs(repo: str, run_id: int) -> list[dict[str, Any]]:
    if run_id <= 0:
        return []
    return github_json(repo, f"actions/runs/{run_id}/jobs?per_page=100").get("jobs") or []


def github_json(repo: str, path: str) -> dict[str, Any]:
    request = urllib.request.Request(GITHUB_API_BASE + repo.strip("/") + "/" + path, method="GET")
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("User-Agent", "statground-admin-pipeline-exporter")
    token = first_env_optional("WEBR_GITHUB_ACTIONS_TOKEN", "GITHUB_ACTIONS_STATUS_TOKEN", "GITHUB_TOKEN")
    if token:
        request.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def safe_query(sql: str) -> tuple[list[dict[str, Any]], bool]:
    try:
        return clickhouse_rows(sql), True
    except Exception:
        return [], False


def clickhouse_rows(sql: str) -> list[dict[str, Any]]:
    request = urllib.request.Request(clickhouse_url(), data=sql.encode("utf-8"), method="POST")
    user = first_env("CH_USER", "CLICKHOUSE_USER")
    password = first_env("CH_PASSWORD", "CLICKHOUSE_PASSWORD")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=35) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read(300).decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse query failed: HTTP {exc.code}: {redact(detail)}") from exc
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def clickhouse_url() -> str:
    return build_clickhouse_url(
        os.environ,
        default_format="JSONEachRow",
        max_execution_time=os.environ.get("ADMIN_PIPELINE_MONITOR_CH_MAX_EXECUTION_TIME", "20"),
        max_threads=os.environ.get("ADMIN_PIPELINE_MONITOR_CH_MAX_THREADS", "1"),
    )


def pipeline_status(status: str, conclusion: str) -> str:
    status = status.strip().lower()
    conclusion = conclusion.strip().lower()
    if status and status != "completed":
        return status
    if conclusion:
        return conclusion
    return status or "unknown"


def parse_github_time(raw: str) -> datetime | None:
    raw = raw.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_time(value: datetime | None) -> str:
    if not value:
        return ""
    return value.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def duration_seconds(start: datetime | None, end: datetime | None) -> int:
    if not start or not end or end < start:
        return 0
    return int((end - start).total_seconds())


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def quote_ch(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        value = str(value or "").strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def first_env(*keys: str) -> str:
    value = first_env_optional(*keys)
    if not value:
        raise RuntimeError(f"required environment variable is missing: {keys[0]}")
    return value


def first_env_optional(*keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return ""


def redact(value: str) -> str:
    password = first_env_optional("CH_PASSWORD", "CLICKHOUSE_PASSWORD")
    if password:
        value = value.replace(password, "***")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
