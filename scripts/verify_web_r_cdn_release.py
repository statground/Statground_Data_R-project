#!/usr/bin/env python3
"""Verify that a Web-R CDN2 release pointer and manifest are both visible."""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request

from clickhouse_http import build_clickhouse_url


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--language", default="ko")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--retries", type=int, default=int(os.environ.get("WEB_R_CDN_VERIFY_RETRIES", "12")))
    parser.add_argument("--retry-delay", type=float, default=float(os.environ.get("WEB_R_CDN_VERIFY_RETRY_DELAY", "10")))
    args = parser.parse_args()

    expected_sha = normalize_sha(args.commit_sha)
    if not expected_sha:
        raise SystemExit("valid --commit-sha is required")

    expected_base = f"https://cdn.jsdelivr.net/gh/{args.repo}@{expected_sha}"
    manifest_url = expected_base + "/" + args.manifest.lstrip("/")
    last_error = ""
    attempts = max(1, args.retries)
    for attempt in range(1, attempts + 1):
        row = latest_release_row(args.scope, args.language)
        actual_sha = normalize_sha(str(row.get("commit_sha", "")))
        actual_base = str(row.get("base_url", "")).rstrip("/")
        if actual_sha != expected_sha or actual_base != expected_base:
            last_error = (
                "CDN release pointer mismatch: "
                + json.dumps(
                    {
                        "scope": args.scope,
                        "language": args.language,
                        "expected_commit_sha": expected_sha,
                        "actual_commit_sha": actual_sha,
                        "expected_base_url": expected_base,
                        "actual_base_url": actual_base,
                        "attempt": attempt,
                        "attempts": attempts,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        else:
            status = head_status(manifest_url)
            if status == 200:
                print(json.dumps({"scope": args.scope, "commit_sha": expected_sha, "manifest": args.manifest, "http_status": status}, ensure_ascii=False))
                return 0
            last_error = f"CDN manifest is not visible yet: HTTP {status} {manifest_url} (attempt {attempt}/{attempts})"
        if attempt < attempts:
            time.sleep(max(0, args.retry_delay))

    raise SystemExit(last_error)


def latest_release_row(scope: str, language: str) -> dict[str, object]:
    sql = """
SELECT release_scope, language, commit_sha, base_url
  FROM Data_R_Community_Service.v_web_r_cdn_release_latest
 WHERE release_scope = {scope:String}
   AND language = {language:String}
   AND active = 1
 LIMIT 1
 FORMAT JSONEachRow
"""
    rows = clickhouse_json_each_row(sql, {"scope": scope, "language": language})
    if not rows:
        raise SystemExit(f"CDN release pointer is missing for {scope}/{language}")
    return rows[0]


def clickhouse_json_each_row(sql: str, params: dict[str, str]) -> list[dict[str, object]]:
    query = sql
    for key, value in params.items():
        query = query.replace("{" + key + ":String}", quote_clickhouse_string(value))
    request = urllib.request.Request(clickhouse_url(), data=query.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{first_env('CH_USER', 'CLICKHOUSE_USER')}:{first_env('CH_PASSWORD', 'CLICKHOUSE_PASSWORD')}".encode()).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read(300).decode("utf-8", errors="replace")
        raise SystemExit(f"ClickHouse release pointer verification failed: HTTP {exc.code}: {redact(detail)}") from exc
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def clickhouse_url() -> str:
    try:
        return build_clickhouse_url(os.environ, default_format="JSONEachRow", max_execution_time="30")
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc


def first_env(*keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    raise SystemExit(f"required environment variable is missing: {keys[0]}")


def head_status(url: str) -> int:
    request = urllib.request.Request(url, method="HEAD")
    request.add_header("User-Agent", "statground-cdn-release-verifier/1.0")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except urllib.error.URLError as exc:
        raise SystemExit(f"CDN manifest verification failed: {exc.__class__.__name__}") from exc


def normalize_sha(value: str) -> str:
    value = (value or "").strip().lower()
    if len(value) < 7 or len(value) > 40:
        return ""
    if not all(ch in "0123456789abcdef" for ch in value):
        return ""
    return value


def quote_clickhouse_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def redact(value: str) -> str:
    return value.replace(first_env("CH_PASSWORD", "CLICKHOUSE_PASSWORD"), "[redacted]")


if __name__ == "__main__":
    raise SystemExit(main())
