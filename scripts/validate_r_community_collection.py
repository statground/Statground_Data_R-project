#!/usr/bin/env python3
"""Validate R Community collector output before publish/digest/CDN steps."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

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

    for source_id in required:
        if errors_by_source.get(source_id):
            failures.append(f"{source_id} failed: {errors_by_source[source_id][0]}")
        if int(source_counts.get(source_id) or 0) <= 0:
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

    summary = {
        "required_sources": {source_id: int(source_counts.get(source_id) or 0) for source_id in required},
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
