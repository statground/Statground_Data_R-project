#!/usr/bin/env python3
"""Validate R Community collector output before publish/digest/CDN steps."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from clickhouse_http import build_clickhouse_url
from collect_r_sources import normalize_subreddit_name, parse_datetime


DEFAULT_REQUIRED_SOURCE_IDS = (
    "community:stackoverflow:r",
    "community:posit:latest-r-filtered",
    "reddit:r/rstats",
    "reddit:r/rprogramming",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/r_sources.yaml")
    parser.add_argument("--jsonl", default="data/collected/r/latest.jsonl")
    parser.add_argument("--report", default="data/collected/r/latest_report.json")
    parser.add_argument("--required-source", action="append", default=[])
    parser.add_argument("--required-sources", default="")
    parser.add_argument("--max-source-age-days", type=float, default=8)
    args = parser.parse_args()

    required = merge_required_sources(DEFAULT_REQUIRED_SOURCE_IDS, args.required_sources, args.required_source)
    config = load_json_or_yaml(Path(args.config))
    report = load_json(Path(args.report))
    rows = load_jsonl(Path(args.jsonl))

    failures: list[str] = []
    source_counts = report.get("source_counts") or {}
    report_errors = report.get("errors") or []
    errors_by_source: dict[str, list[str]] = defaultdict(list)
    for error in report_errors:
        source_id = str(error.get("source_id") or "").strip()
        if source_id:
            errors_by_source[source_id].append(str(error.get("message") or error.get("error_type") or "collector error"))

    report_observed_latest = report.get("source_observed_latest_item_at") or {}
    if not isinstance(report_observed_latest, dict):
        report_observed_latest = {}

    live_fresh_by_source: dict[str, dict[str, Any]] = {}
    satisfied_by: dict[str, str] = {}
    for source_id in required:
        inactive_for_window = source_inactive_for_collection_window(report, str(report_observed_latest.get(source_id) or ""))
        if (
            errors_by_source.get(source_id)
            and not inactive_for_window
            and source_unavailable_error(errors_by_source[source_id])
        ):
            live_fresh_by_source[source_id] = live_current_freshness(source_id, args.max_source_age_days)
            if live_fresh_by_source[source_id].get("fresh"):
                satisfied_by[source_id] = "live_current_after_source_unavailable"
        if errors_by_source.get(source_id) and not inactive_for_window and source_id not in satisfied_by:
            failures.append(f"{source_id} failed: {errors_by_source[source_id][0]}")
        if int(source_counts.get(source_id) or 0) <= 0 and not inactive_for_window and source_id not in satisfied_by:
            failures.append(f"{source_id} produced no rows")

    rows_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    latest_by_source: dict[str, datetime] = {}
    for row in rows:
        source_id = str(row.get("source_id") or "").strip()
        if not source_id:
            continue
        rows_by_source[source_id].append(row)
        published = parse_datetime(str(row.get("published_at") or ""))
        if published:
            published = published.astimezone(timezone.utc)
            if source_id not in latest_by_source or published > latest_by_source[source_id]:
                latest_by_source[source_id] = published

    now = datetime.now(timezone.utc)
    for source_id in required:
        latest = latest_by_source.get(source_id)
        if latest is None:
            if (
                not source_inactive_for_collection_window(report, str(report_observed_latest.get(source_id) or ""))
                and source_id not in satisfied_by
            ):
                failures.append(f"{source_id} has no parseable published_at")
            continue
        age_days = (now - latest).total_seconds() / 86400
        if age_days > args.max_source_age_days:
            failures.append(f"{source_id} latest published_at is stale: {latest.isoformat()} ({age_days:.1f}d)")

    reddit_sources = reddit_source_map(config)
    for source_id, expected_subreddit in reddit_sources.items():
        source_rows = rows_by_source.get(source_id) or []
        if not source_rows:
            continue
        for row in source_rows:
            raw = raw_payload(row)
            observed = normalize_subreddit_name(raw.get("normalized_subreddit") or raw.get("subreddit") or "")
            if not observed:
                failures.append(f"{source_id} row has no raw subreddit marker")
                break
            if observed.lower() != expected_subreddit.lower():
                failures.append(f"{source_id} subreddit mismatch: expected {expected_subreddit}, observed {observed}")
                break

    report_latest = report.get("source_latest_item_at") or {}
    if not isinstance(report_latest, dict):
        report_latest = {}
    summary = {
        "required_sources": {source_id: int(source_counts.get(source_id) or 0) for source_id in required},
        "required_latest_item_at": {
            source_id: (
                report_latest.get(source_id)
                or (latest_by_source[source_id].isoformat() if source_id in latest_by_source else "")
            )
            for source_id in required
        },
        "required_observed_latest_item_at": {
            source_id: str(report_observed_latest.get(source_id) or "")
            for source_id in required
        },
        "required_satisfied_by": {
            source_id: satisfied_by.get(source_id, "")
            for source_id in required
        },
        "required_live_current": {
            source_id: live_fresh_by_source.get(source_id, {})
            for source_id in required
            if source_id in live_fresh_by_source
        },
        "reddit_sources_checked": len(reddit_sources),
        "rows": len(rows),
        "failures": failures,
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
    return 1 if failures else 0


def load_json_or_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fp:
        return yaml.safe_load(fp) or {}


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fp:
        return json.load(fp)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_no}: invalid JSONL: {exc}") from exc
    return rows


def merge_required_sources(defaults: tuple[str, ...], csv_value: str, explicit: list[str]) -> tuple[str, ...]:
    values: list[str] = []
    values.extend(defaults)
    values.extend(split_csv(csv_value))
    values.extend(explicit)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = str(value or "").strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return tuple(out)


def split_csv(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def source_inactive_for_collection_window(report: dict[str, Any], observed_value: str) -> bool:
    observed = parse_datetime(observed_value)
    if observed is None:
        return False
    since_days = report.get("since_days")
    if since_days is None:
        return False
    try:
        since_days_float = float(since_days)
    except (TypeError, ValueError):
        return False
    if since_days_float < 0:
        return False
    started = parse_datetime(str(report.get("started_at") or "")) or datetime.now(timezone.utc)
    cutoff = started.astimezone(timezone.utc) - timedelta(days=since_days_float)
    return observed.astimezone(timezone.utc) < cutoff


def source_unavailable_error(messages: list[str]) -> bool:
    haystack = "\n".join(str(message or "").lower() for message in messages)
    return any(token in haystack for token in ("403", "blocked", "forbidden", "429", "too many", "503", "temporarily unavailable"))


def live_current_freshness(source_id: str, max_age_days: float) -> dict[str, Any]:
    try:
        rows = query_clickhouse_json_each_row(
            f"""
            SELECT
                count() AS rows,
                ifNull(formatDateTime(max(coalesce(original_published_at, collected_at)), '%Y-%m-%d %H:%i:%S', 'Asia/Seoul'), '') AS latest_source_at
            FROM Data_R_Community_Service.r_community_item_read_current
            WHERE source_id = {clickhouse_quote(source_id)}
              AND active = 1
              AND notEmpty(title)
              AND notEmpty(canonical_url)
            FORMAT JSONEachRow
            """
        )
    except RuntimeError as exc:
        return {"fresh": False, "reason": str(exc)}
    row = rows[0] if rows else {}
    latest_source_at = str(row.get("latest_source_at") or "")
    latest = parse_datetime(latest_source_at)
    if latest is None:
        return {"fresh": False, "rows": int(row.get("rows") or 0), "latest_source_at": latest_source_at, "reason": "no_parseable_live_current_timestamp"}
    age_days = (datetime.now(timezone.utc) - latest.astimezone(timezone.utc)).total_seconds() / 86400
    return {
        "fresh": int(row.get("rows") or 0) > 0 and age_days <= max_age_days,
        "rows": int(row.get("rows") or 0),
        "latest_source_at": latest_source_at,
        "age_days": round(age_days, 3),
    }


def query_clickhouse_json_each_row(sql: str) -> list[dict[str, Any]]:
    user = first_env("CH_USER", "CLICKHOUSE_USER")
    password = first_env("CH_PASSWORD", "CLICKHOUSE_PASSWORD")
    if not user:
        raise RuntimeError("clickhouse_user_missing")
    try:
        url = build_clickhouse_url(default_format="JSONEachRow", max_execution_time="30", max_threads="1")
    except RuntimeError as exc:
        raise RuntimeError("clickhouse_url_missing") from exc
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=35) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"clickhouse_http_{exc.code}") from exc
    except (TimeoutError, urllib.error.URLError, OSError) as exc:
        raise RuntimeError(f"clickhouse_{exc.__class__.__name__}") from exc
    out: list[dict[str, Any]] = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def first_env(*names: str) -> str:
    for name in names:
        value = str(os.environ.get(name, "") or "").strip()
        if value:
            return value
    return ""


def clickhouse_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def reddit_source_map(config: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for source in config.get("sources") or []:
        if not isinstance(source, dict) or source.get("type") != "reddit_subreddit":
            continue
        source_id = str(source.get("id") or "").strip()
        subreddit = normalize_subreddit_name(source.get("subreddit"))
        if source_id and subreddit:
            out[source_id] = subreddit
    return out


def raw_payload(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    if isinstance(raw, dict):
        return raw
    raw_json = row.get("raw_json")
    if isinstance(raw_json, dict):
        return raw_json
    if isinstance(raw_json, str) and raw_json.strip():
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


if __name__ == "__main__":
    sys.exit(main())
