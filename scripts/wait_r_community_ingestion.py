#!/usr/bin/env python3
"""Wait for R Community source/digest rows to become visible in ClickHouse."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from clickhouse_http import build_clickhouse_url


KST = timezone(timedelta(hours=9), name="Asia/Seoul")
DEFAULT_REQUIRED_SOURCE_IDS = (
    "community:stackoverflow:r",
    "community:posit:latest-r-filtered",
    "reddit:r/rstats",
    "reddit:r/rprogramming",
)
DEFAULT_DIGEST_SOURCE_TYPES = (
    "community_forum",
    "qna_feed",
    "social_tag",
    "fediverse_group",
)
DEFAULT_EXCLUDED_DIGEST_SOURCE_IDS = ("mastodon:group:rstats",)
PUBMED_REDDIT_SOURCE_IDS = (
    "reddit:r/librarians",
    "reddit:r/research",
    "reddit:r/bioinformatics",
    "reddit:r/labrats",
    "reddit:r/AskAcademia",
    "reddit:r/medicine",
    "reddit:r/pharmacy",
    "reddit:r/DataHoarder",
    "reddit:r/healthIT",
)
PUBMED_DIGEST_REGEX = (
    "(?i)PubMed|MEDLINE|PubMed Central|\\bPMC\\b|\\bNCBI\\b|\\bNLM\\b|"
    "E[- ]utilities|\\bEntrez\\b|literature search|literature review|"
    "systematic review|scoping review|search strateg|evidence synthesis|"
    "biomedical literature|medical librarian|clinical literature|database search"
)
DIGEST_WRITE_RE = re.compile(r"\b(?:published|inserted)=(\d+)\b")


class ClickHouseQueryError(RuntimeError):
    """Retryable ClickHouse visibility-check failure."""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    source = subparsers.add_parser("source", help="wait for required source rows after collection")
    source.add_argument("--report", default="data/collected/r/latest_report.json")
    source.add_argument("--required-source", action="append", default=[])
    source.add_argument("--required-sources", default="")
    source.add_argument("--max-source-age-days", type=float, default=8)
    add_wait_args(source)

    digest = subparsers.add_parser("digest", help="wait for daily digest rows before CDN export")
    digest.add_argument("--digest-output", default="/tmp/r_community_digest.out")
    digest.add_argument("--plan", default="/tmp/r_community_digest_plan.json")
    digest.add_argument("--source-types", default=",".join(DEFAULT_DIGEST_SOURCE_TYPES))
    digest.add_argument("--exclude-sources", default=",".join(DEFAULT_EXCLUDED_DIGEST_SOURCE_IDS))
    digest.add_argument("--exclude-source", action="append", default=[])
    digest.add_argument("--since-days", type=int, default=14)
    add_wait_args(digest)

    args = parser.parse_args()
    if args.mode == "source":
        return wait_source(args)
    if args.mode == "digest":
        return wait_digest(args)
    raise AssertionError(args.mode)


def add_wait_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--attempts", type=int, default=36)
    parser.add_argument("--sleep", type=float, default=10)
    parser.add_argument("--query-timeout", type=float, default=12)


def wait_source(args: argparse.Namespace) -> int:
    report = load_json(Path(args.report))
    started_at = str(report.get("started_at") or "").strip()
    if not started_at:
        print("R Community report has no started_at; cannot verify source ingestion.", file=sys.stderr)
        return 1
    required = merge_required_sources(DEFAULT_REQUIRED_SOURCE_IDS, args.required_sources, args.required_source)
    if not required:
        print("No required R Community source ids were configured; cannot verify source ingestion.", file=sys.stderr)
        return 1

    collection = collection_source_summary(report)
    cutoff_sql = f"parseDateTime64BestEffort({quote(started_at)}, 3, 'Asia/Seoul') - INTERVAL 5 MINUTE"
    last_failures: list[str] = []
    for attempt in range(1, max(1, args.attempts) + 1):
        try:
            summary, failures = source_visibility(cutoff_sql, required, args.max_source_age_days, args.query_timeout, collection)
        except ClickHouseQueryError as exc:
            summary = {"clickhouse_query_error": str(exc)}
            failures = [str(exc)]
        print(
            json.dumps(
                {
                    "mode": "source",
                    "attempt": attempt,
                    "required_sources": summary,
                    "failures": failures,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        if not failures:
            return 0
        last_failures = failures
        if attempt < args.attempts:
            time.sleep(max(0, args.sleep))

    print("R Community required source rows did not become visible: " + "; ".join(last_failures), file=sys.stderr)
    return 1


def wait_digest(args: argparse.Namespace) -> int:
    published = parse_digest_write_count(Path(args.digest_output))
    planned = load_digest_plan(Path(args.plan))
    planned_ids = digest_plan_ids(planned)
    if not planned_ids:
        print(
            json.dumps(
                {
                    "mode": "digest",
                    "attempt": 1,
                    "published": published,
                    "planned_digest_count": 0,
                    "visible_digest_count": 0,
                    "missing_digest_count": 0,
                    "query_error": "",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        if published <= 0:
            return 0
        print("R Community daily digest publish reported events, but digest plan had no digest_id values.", file=sys.stderr)
        return 1

    last_missing: list[str] = []
    last_error = ""
    for attempt in range(1, max(1, args.attempts) + 1):
        query_error = ""
        try:
            visible, missing = digest_plan_visibility(planned_ids, args.query_timeout)
        except ClickHouseQueryError as exc:
            visible = {}
            missing = planned_ids
            query_error = str(exc)
            last_error = query_error
        print(
            json.dumps(
                {
                    "mode": "digest",
                    "attempt": attempt,
                    "published": published,
                    "planned_digest_count": len(planned_ids),
                    "visible_digest_count": len(visible),
                    "missing_digest_ids": missing[:20],
                    "missing_digest_count": len(missing),
                    "query_error": query_error,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        if query_error:
            if attempt < args.attempts:
                time.sleep(max(0, args.sleep))
                continue
            print(f"R Community daily digest visibility check did not complete: {query_error}", file=sys.stderr)
            return 1
        if not missing:
            return 0
        last_missing = missing
        if attempt < args.attempts and published > 0:
            time.sleep(max(0, args.sleep))
            continue
        break

    reason = "community-digest published no events" if published <= 0 else "digest rows did not become visible"
    if last_error:
        reason = last_error
    print(f"R Community planned daily digest rows did not become visible before CDN export ({reason}): {json.dumps(last_missing[:10], ensure_ascii=False)}", file=sys.stderr)
    return 1


def load_digest_plan(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        return {}
    return data


def digest_plan_ids(plan: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    raw_ids = plan.get("digest_ids")
    if isinstance(raw_ids, list):
        values.extend(raw_ids)
    raw_records = plan.get("records")
    if isinstance(raw_records, list):
        for record in raw_records:
            if isinstance(record, dict):
                values.append(record.get("digest_id"))
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        digest_id = str(value or "").strip()
        if digest_id and digest_id not in seen:
            seen.add(digest_id)
            out.append(digest_id)
    return out


def digest_plan_visibility(digest_ids: list[str], query_timeout: float) -> tuple[dict[str, dict[str, Any]], list[str]]:
    visible: dict[str, dict[str, Any]] = {}
    chunk_size = 200
    for start in range(0, len(digest_ids), chunk_size):
        chunk = digest_ids[start:start + chunk_size]
        for row in query_json_each_row(
            f"""
            SELECT
                digest_id,
                toString(digest_date) AS digest_date,
                source_id,
                source_name,
                generation_status,
                notEmpty(summary) AS has_summary
            FROM Data_R_Community_Service.v_r_community_daily_digest_latest
            WHERE digest_id IN ({quote_list(chunk)})
              AND notEmpty(summary)
            FORMAT JSONEachRow
            """,
            query_timeout,
        ):
            digest_id = str(row.get("digest_id") or "")
            if digest_id:
                visible[digest_id] = row
    missing = [digest_id for digest_id in digest_ids if digest_id not in visible]
    return visible, missing


def source_visibility(
    cutoff_sql: str,
    required: tuple[str, ...],
    max_source_age_days: float,
    query_timeout: float,
    collection: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    source_rows = {
        row.get("source_id", ""): row
        for row in query_json_each_row(
            f"""
            SELECT
                source_id,
                count() AS latest_rows,
                countIf(ingested_at >= {cutoff_sql}) AS latest_after_cutoff,
                ifNull(formatDateTime(max(coalesce(original_published_at, collected_at)), '%Y-%m-%d %H:%i:%S', 'Asia/Seoul'), '') AS latest_source_at,
                ifNull(formatDateTime(max(ingested_at), '%Y-%m-%d %H:%i:%S', 'Asia/Seoul'), '') AS latest_ingested_at
            FROM Data_R_Community_Service.r_community_item_read_current
            WHERE source_id IN ({quote_list(required)})
              AND active = 1
              AND notEmpty(title)
              AND notEmpty(canonical_url)
            GROUP BY source_id
            FORMAT JSONEachRow
            """,
            query_timeout,
        )
    }
    event_rows = {
        row.get("source_id", ""): row
        for row in query_json_each_row(
            f"""
            SELECT
                JSONExtractString(payload, 'source_id') AS source_id,
                count() AS raw_events_after_cutoff,
                ifNull(formatDateTime(max(ingested_at), '%Y-%m-%d %H:%i:%S', 'Asia/Seoul'), '') AS raw_event_ingested_at
            FROM Data_R_Community_Raw.r_community_event_raw
            WHERE event_type = 'r.community.item.v1'
              AND ingested_at >= {cutoff_sql}
              AND JSONExtractString(payload, 'source_id') IN ({quote_list(required)})
            GROUP BY source_id
            FORMAT JSONEachRow
            """,
            query_timeout,
        )
    }

    now = datetime.now(KST)
    failures: list[str] = []
    summary: dict[str, Any] = {}
    for source_id in required:
        source = source_rows.get(source_id) or {}
        events = event_rows.get(source_id) or {}
        collected = collection.get(source_id) or {}
        latest_after = int(source.get("latest_after_cutoff") or 0)
        raw_after = int(events.get("raw_events_after_cutoff") or 0)
        latest_source_at = str(source.get("latest_source_at") or "")
        latest_dt = parse_datetime_value(latest_source_at)
        collection_latest_at = str(collected.get("latest_item_at") or "")
        collection_latest_dt = parse_datetime_value(collection_latest_at)
        observed_latest_at = str(collected.get("observed_latest_item_at") or "")
        collected_rows = int(collected.get("rows") or 0)
        summary[source_id] = {
            "latest_after_cutoff": latest_after,
            "raw_events_after_cutoff": raw_after,
            "latest_source_at": latest_source_at,
            "latest_ingested_at": source.get("latest_ingested_at") or "",
            "raw_event_ingested_at": events.get("raw_event_ingested_at") or "",
            "collection_rows": collected_rows,
            "collection_latest_item_at": collection_latest_at,
            "collection_observed_latest_item_at": observed_latest_at,
        }
        if latest_after <= 0 and raw_after <= 0:
            if bool(collected.get("inactive_for_collection_window")):
                summary[source_id]["satisfied_by"] = "upstream_inactive_for_collection_window"
                continue
            existing_reason = fresh_existing_current_reason(now, latest_dt, collection_latest_dt, collected_rows, max_source_age_days)
            if existing_reason:
                summary[source_id]["satisfied_by"] = existing_reason
                continue
            failures.append(f"{source_id} missing from raw/current rows after collection start")
            continue
        if latest_dt is None:
            failures.append(f"{source_id} has no parseable latest source timestamp")
            continue
        age_days = (now - latest_dt).total_seconds() / 86400
        if age_days > max_source_age_days:
            failures.append(f"{source_id} latest source timestamp is stale: {latest_source_at} ({age_days:.1f}d)")
    return summary, failures


def collection_source_summary(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    counts = report.get("source_counts") or {}
    latest = report.get("source_latest_item_at") or {}
    observed_latest = report.get("source_observed_latest_item_at") or {}
    out: dict[str, dict[str, Any]] = {}
    if isinstance(counts, dict):
        for source_id, count in counts.items():
            key = str(source_id or "").strip()
            if not key:
                continue
            try:
                rows = int(count or 0)
            except (TypeError, ValueError):
                rows = 0
            out[key] = {"rows": rows, "latest_item_at": "", "observed_latest_item_at": "", "inactive_for_collection_window": False}
    if isinstance(latest, dict):
        for source_id, value in latest.items():
            key = str(source_id or "").strip()
            if not key:
                continue
            out.setdefault(key, {"rows": 0, "latest_item_at": "", "observed_latest_item_at": "", "inactive_for_collection_window": False})
            out[key]["latest_item_at"] = str(value or "").strip()
    if isinstance(observed_latest, dict):
        for source_id, value in observed_latest.items():
            key = str(source_id or "").strip()
            if not key:
                continue
            out.setdefault(key, {"rows": 0, "latest_item_at": "", "observed_latest_item_at": "", "inactive_for_collection_window": False})
            observed = str(value or "").strip()
            out[key]["observed_latest_item_at"] = observed
            out[key]["inactive_for_collection_window"] = source_inactive_for_collection_window(report, observed)
    return out


def source_inactive_for_collection_window(report: dict[str, Any], observed_value: str) -> bool:
    observed = parse_datetime_value(observed_value)
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
    started = parse_datetime_value(str(report.get("started_at") or "")) or datetime.now(KST)
    cutoff = started.astimezone(timezone.utc) - timedelta(days=since_days_float)
    return observed.astimezone(timezone.utc) < cutoff


def fresh_existing_current_reason(
    now: datetime,
    latest_dt: datetime | None,
    collection_latest_dt: datetime | None,
    collected_rows: int,
    max_source_age_days: float,
) -> str:
    if collected_rows <= 0 or latest_dt is None:
        return ""
    age_days = (now - latest_dt).total_seconds() / 86400
    if age_days > max_source_age_days:
        return ""
    if collection_latest_dt is not None and latest_dt + timedelta(minutes=5) < collection_latest_dt:
        return ""
    return "existing_current_fresh_after_skip_existing"


def digest_lag(source_types: tuple[str, ...], excluded_source_ids: tuple[str, ...], since_days: int, query_timeout: float) -> list[dict[str, str]]:
    since_where = ""
    if since_days >= 0:
        cutoff_date = (datetime.now(KST) - timedelta(days=since_days)).strftime("%Y-%m-%d")
        since_where = f"AND toDate(coalesce(l.original_published_at, l.collected_at)) >= toDate({quote(cutoff_date)})"
    exclude_where = ""
    if excluded_source_ids:
        exclude_where = f"AND l.source_id NOT IN ({quote_list(excluded_source_ids)})"

    rows = query_json_each_row(
        f"""
        WITH
        source_latest AS
        (
            SELECT
                l.source_id AS source_id,
                anyLast(l.source_name) AS source_name,
                anyLast(l.source_type) AS source_type,
                max(toDate(coalesce(l.original_published_at, l.collected_at))) AS latest_source_date
            FROM Data_R_Community_Service.r_community_item_read_current AS l
            WHERE l.source_type IN ({quote_list(source_types)})
              AND l.active = 1
              AND notEmpty(l.title)
              AND notEmpty(l.canonical_url)
              {exclude_where}
              {since_where}
              {pubmed_reddit_where("l.")}
            GROUP BY l.source_id
        ),
        digest_latest AS
        (
            SELECT source_id, max(digest_date) AS latest_digest_date
            FROM Data_R_Community_Service.v_r_community_daily_digest_latest
            WHERE source_type IN ({quote_list(source_types)})
              AND notEmpty(summary)
            GROUP BY source_id
        )
        SELECT
            source_latest.source_id AS source_id,
            source_latest.source_name AS source_name,
            source_latest.source_type AS source_type,
            toString(source_latest.latest_source_date) AS latest_source_date,
            ifNull(toString(digest_latest.latest_digest_date), '') AS latest_digest_date
        FROM source_latest
        LEFT JOIN digest_latest USING (source_id)
        WHERE ifNull(digest_latest.latest_digest_date, toDate('1970-01-01')) < source_latest.latest_source_date
        ORDER BY source_latest.latest_source_date DESC, source_latest.source_id ASC
        FORMAT JSONEachRow
        """,
        query_timeout,
    )
    return [{key: str(value) for key, value in row.items()} for row in rows]


def pubmed_reddit_where(prefix: str = "") -> str:
    return f"""
      AND (
          (
            positionCaseInsensitiveUTF8({prefix}source_name, 'PubMed') = 0
            AND {prefix}source_id NOT IN ({quote_list(PUBMED_REDDIT_SOURCE_IDS)})
          )
          OR positionUTF8(concat({prefix}title, '\\n', {prefix}summary, '\\n', {prefix}canonical_url), 'MeSH') > 0
          OR match(concat({prefix}title, '\\n', {prefix}summary, '\\n', {prefix}canonical_url), {quote(PUBMED_DIGEST_REGEX)})
      )
    """


def query_json_each_row(sql: str, query_timeout: float) -> list[dict[str, Any]]:
    body = clickhouse_request(sql, query_timeout).strip()
    if not body:
        return []
    rows: list[dict[str, Any]] = []
    for line in body.splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def clickhouse_request(sql: str, query_timeout: float) -> str:
    request = urllib.request.Request(clickhouse_url(query_timeout), data=sql.encode("utf-8"), method="POST")
    user = first_env("CH_USER", "CLICKHOUSE_USER")
    password = first_env("CH_PASSWORD", "CLICKHOUSE_PASSWORD")
    if not user:
        raise SystemExit("ClickHouse user secret is required")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=max(1, query_timeout)) as response:
            return response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read(300).decode("utf-8", errors="replace")
        raise ClickHouseQueryError(f"ClickHouse R Community ingestion check failed: HTTP {exc.code}: {redact_detail(detail)}") from exc
    except (TimeoutError, urllib.error.URLError, OSError) as exc:
        raise ClickHouseQueryError(f"ClickHouse R Community ingestion check failed: {exc.__class__.__name__}") from exc


def clickhouse_url(query_timeout: float) -> str:
    try:
        timeout = str(max(1, int(query_timeout)))
        return build_clickhouse_url(os.environ, default_format="JSONEachRow", max_execution_time=timeout, max_threads="1")
    except RuntimeError as exc:
        raise ClickHouseQueryError(str(exc)) from exc


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fp:
        return json.load(fp)


def parse_digest_write_count(path: Path) -> int:
    if not path.exists():
        return 0
    text = path.read_text(encoding="utf-8", errors="replace")
    matches = DIGEST_WRITE_RE.findall(text)
    if not matches:
        return 0
    return max(int(match) for match in matches)


def parse_datetime_value(value: str) -> datetime | None:
    value = str(value or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=KST)
        return parsed.astimezone(KST)
    except ValueError:
        pass
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    except ValueError:
        return None


def split_csv(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


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


def quote(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def quote_list(values: tuple[str, ...] | list[str]) -> str:
    return ",".join(quote(value) for value in values)


def first_env(*keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return ""


def redact_detail(value: str) -> str:
    password = first_env("CH_PASSWORD", "CLICKHOUSE_PASSWORD")
    if password:
        value = value.replace(password, "***")
    host = first_env("CH_HOST", "CLICKHOUSE_HOST")
    if host:
        value = value.replace(host, "<clickhouse-host>")
    return value


if __name__ == "__main__":
    sys.exit(main())
