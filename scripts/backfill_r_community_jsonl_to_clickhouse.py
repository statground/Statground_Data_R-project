#!/usr/bin/env python3
"""Insert collected R Community JSONL rows directly into ClickHouse raw table."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from clickhouse_http import build_clickhouse_url


KST = ZoneInfo("Asia/Seoul")
TARGET_TABLE = "Data_R_Community_Raw.r_community_item_raw"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="env file with ClickHouse credentials")
    parser.add_argument("--jsonl", default="data/collected/r/latest.jsonl", help="collector JSONL path")
    parser.add_argument("--dry-run", action="store_true", help="build rows but do not insert")
    args = parser.parse_args()

    env = load_env(Path(args.env))
    rows = load_jsonl(Path(args.jsonl))
    candidates = [to_raw_row(row) for row in rows if text(row.get("external_id"))]
    existing_external, existing_events = load_existing_keys(env, candidates)
    prepared = [
        row
        for row in candidates
        if row["external_id"] not in existing_external and row["event_id"] not in existing_events
    ]
    if args.dry_run:
        existing_count = len(candidates) - len(prepared)
        print(json.dumps({"input_rows": len(rows), "existing": existing_count, "insertable": len(prepared)}, ensure_ascii=False))
        return 0
    if prepared:
        insert_rows(env, prepared)
    existing_count = len(candidates) - len(prepared)
    print(json.dumps({"input_rows": len(rows), "existing": existing_count, "inserted": len(prepared)}, ensure_ascii=False))
    return 0


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_no}: invalid JSONL: {exc}") from exc
            if isinstance(row, dict):
                out.append(row)
    return out


def to_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    payload_json = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    external_id = text(row.get("external_id"))
    event_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"r-community-jsonl-backfill:{external_id}:{payload_hash}"))
    now_text = now_kst_text()
    collected_at = datetime_text(row.get("collected_at")) or now_text
    return {
        "uuid": event_uuid,
        "event_id": f"backfill:r-community-jsonl:{payload_hash}",
        "external_id": external_id,
        "source_id": text(row.get("source_id")),
        "source_name": text(row.get("source_name")),
        "source_type": text(row.get("source_type")),
        "platform": text(row.get("platform")),
        "source_url": text(row.get("source_url")),
        "canonical_url": text(row.get("canonical_url")),
        "title": text(row.get("title")),
        "summary": text(row.get("summary")),
        "author": text(row.get("author")),
        "language": text(row.get("language")),
        "tags_json": json.dumps(row.get("tags") if isinstance(row.get("tags"), list) else [], ensure_ascii=False, separators=(",", ":")),
        "raw_json": json.dumps(row.get("raw") if isinstance(row.get("raw"), dict) else {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        "published_at": datetime_text(row.get("published_at")),
        "collected_at": collected_at,
        "collection_status": "collected",
        "payload_hash": payload_hash,
        "payload_json": payload_json,
        "active": 1,
        "ingested_at": now_text,
    }


def load_existing_keys(env: dict[str, str], rows: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    external_ids = sorted({text(row.get("external_id")) for row in rows if text(row.get("external_id"))})
    event_ids = sorted({text(row.get("event_id")) for row in rows if text(row.get("event_id"))})
    if not external_ids and not event_ids:
        return set(), set()
    existing_external: set[str] = set()
    existing_events: set[str] = set()
    max_chunks = max((len(external_ids) + 199) // 200, (len(event_ids) + 199) // 200)
    for index in range(max_chunks):
        external_chunk = external_ids[index * 200 : index * 200 + 200]
        event_chunk = event_ids[index * 200 : index * 200 + 200]
        where_parts: list[str] = []
        if external_chunk:
            where_parts.append(f"external_id IN ({clickhouse_string_list(external_chunk)})")
        if event_chunk:
            where_parts.append(f"event_id IN ({clickhouse_string_list(event_chunk)})")
        query = f"""
SELECT external_id, event_id
  FROM {TARGET_TABLE}
 WHERE {" OR ".join(where_parts)}
 FORMAT JSONEachRow
"""
        for row in query_rows(env, query):
            existing = text(row.get("external_id"))
            if existing:
                existing_external.add(existing)
            event_id = text(row.get("event_id"))
            if event_id:
                existing_events.add(event_id)
    return existing_external, existing_events


def query_rows(env: dict[str, str], query: str) -> list[dict[str, Any]]:
    user, password = clickhouse_auth(env)
    request = urllib.request.Request(clickhouse_url(env, max_execution_time="60"), data=query.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=75) as response:
            body = response.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise SystemExit(f"ClickHouse query failed: {exc.__class__.__name__}") from exc
    rows: list[dict[str, Any]] = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def insert_rows(env: dict[str, str], rows: list[dict[str, Any]]) -> None:
    user, password = clickhouse_auth(env)
    body = f"INSERT INTO {TARGET_TABLE} SETTINGS insert_distributed_sync = 1 FORMAT JSONEachRow\n"
    body += "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows)
    body += "\n"
    request = urllib.request.Request(clickhouse_url(env, max_execution_time="120"), data=body.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            response.read()
    except urllib.error.URLError as exc:
        raise SystemExit(f"ClickHouse insert failed: {exc.__class__.__name__}") from exc


def clickhouse_url(env: dict[str, str], *, max_execution_time: str) -> str:
    return build_clickhouse_url(env, default_format="JSONEachRow", max_execution_time=max_execution_time, max_threads="2")


def clickhouse_auth(env: dict[str, str]) -> tuple[str, str]:
    user = env.get("CH_USER") or env.get("CLICKHOUSE_USER") or ""
    password = env.get("CH_PASSWORD") or env.get("CLICKHOUSE_PASSWORD") or ""
    if not user:
        raise SystemExit("ClickHouse user secret is required")
    return user, password


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    return {**env, **{key: value for key, value in os.environ.items() if value}}


def datetime_text(value: Any) -> str | None:
    raw = text(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def now_kst_text() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def clickhouse_string_list(values: list[str]) -> str:
    return ",".join("'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'" for value in values)


def chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def text(value: Any) -> str:
    return "" if value is None else str(value).strip()


if __name__ == "__main__":
    sys.exit(main())
