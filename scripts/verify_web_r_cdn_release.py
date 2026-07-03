#!/usr/bin/env python3
"""Verify that a Web-R CDN2 release pointer and manifest are both visible."""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.request

from clickhouse_http import build_clickhouse_url
from export_r_ecosystem_cdn import (
    clickhouse_error_category,
    env_bool,
    is_transient_clickhouse_export_failure,
)


class ClickHouseReleaseVerifyError(RuntimeError):
    def __init__(self, status_code: int, category: str, detail: str = "") -> None:
        self.status_code = int(status_code or 0)
        self.category = clickhouse_error_category(category or detail)
        self.detail = detail
        super().__init__(str(self))

    def __str__(self) -> str:
        if self.status_code:
            return f"ClickHouse release pointer verification failed: HTTP {self.status_code} {self.category}"
        return f"ClickHouse release pointer verification failed: {self.category}"


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
    last_pointer_mismatch: dict[str, object] = {}
    attempts = max(1, args.retries)
    for attempt in range(1, attempts + 1):
        try:
            row = latest_release_row(args.scope, args.language)
        except ClickHouseReleaseVerifyError as exc:
            if env_bool(os.environ, "WEB_R_CDN_RELEASE_VERIFY_TRANSIENT_FAIL_OPEN", True) and is_transient_clickhouse_export_failure(exc.status_code, exc.category, exc.detail):
                print(
                    f"[warn] Web-R CDN release verification deferred scope={args.scope} reason={exc.category}",
                    file=sys.stderr,
                )
                print(
                    json.dumps(
                        {
                            "scope": args.scope,
                            "commit_sha": expected_sha,
                            "manifest": args.manifest,
                            "verify_deferred": True,
                            "deferred_reason": exc.category,
                            "deferred_http_status": exc.status_code,
                        },
                        ensure_ascii=False,
                    )
                )
                return 0
            raise SystemExit(str(exc)) from exc
        actual_sha = normalize_sha(str(row.get("commit_sha", "")))
        actual_base = str(row.get("base_url", "")).rstrip("/")
        if actual_sha != expected_sha or actual_base != expected_base:
            last_pointer_mismatch = {
                "scope": args.scope,
                "language": args.language,
                "expected_commit_sha": expected_sha,
                "actual_commit_sha": actual_sha,
                "expected_base_url": expected_base,
                "actual_base_url": actual_base,
                "attempt": attempt,
                "attempts": attempts,
            }
            last_error = (
                "CDN release pointer mismatch: "
                + json.dumps(
                    last_pointer_mismatch,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        else:
            last_pointer_mismatch = {}
            status = head_status(manifest_url)
            if status == 200:
                print(json.dumps({"scope": args.scope, "commit_sha": expected_sha, "manifest": args.manifest, "http_status": status, "verify_deferred": False}, ensure_ascii=False))
                return 0
            last_error = f"CDN manifest is not visible yet: HTTP {status} {manifest_url} (attempt {attempt}/{attempts})"
        if attempt < attempts:
            time.sleep(max(0, args.retry_delay))

    if last_pointer_mismatch and env_bool(os.environ, "WEB_R_CDN_RELEASE_VERIFY_POINTER_MISMATCH_FAIL_OPEN", True):
        print(
            f"[warn] Web-R CDN release verification deferred scope={args.scope} reason=POINTER_MISMATCH",
            file=sys.stderr,
        )
        result = {
            "scope": args.scope,
            "commit_sha": expected_sha,
            "manifest": args.manifest,
            "verify_deferred": True,
            "deferred_reason": "POINTER_MISMATCH",
        }
        result.update(last_pointer_mismatch)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0

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
        with urllib.request.urlopen(request, timeout=int(os.environ.get("WEB_R_CDN_RELEASE_VERIFY_HTTP_TIMEOUT", "45"))) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read(800).decode("utf-8", errors="replace")
        raise ClickHouseReleaseVerifyError(exc.code, clickhouse_error_category(detail), detail) from exc
    except urllib.error.URLError as exc:
        raise ClickHouseReleaseVerifyError(0, "CLICKHOUSE_NETWORK", str(exc)) from exc
    except http.client.IncompleteRead as exc:
        raise ClickHouseReleaseVerifyError(0, "INCOMPLETE_READ", str(exc)) from exc
    except http.client.HTTPException as exc:
        raise ClickHouseReleaseVerifyError(0, "HTTP_CLIENT_ERROR", str(exc)) from exc
    except TimeoutError as exc:
        raise ClickHouseReleaseVerifyError(0, "TIMEOUT_EXCEEDED", str(exc)) from exc
    except OSError as exc:
        raise ClickHouseReleaseVerifyError(0, "CLICKHOUSE_NETWORK", str(exc)) from exc
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def clickhouse_url() -> str:
    try:
        return build_clickhouse_url(
            os.environ,
            default_format="JSONEachRow",
            max_execution_time=os.environ.get("WEB_R_CDN_RELEASE_VERIFY_CH_MAX_EXECUTION_TIME", "45"),
            max_threads=os.environ.get("WEB_R_CDN_RELEASE_VERIFY_CH_MAX_THREADS", "1"),
        )
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
