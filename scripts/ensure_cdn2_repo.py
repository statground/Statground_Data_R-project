#!/usr/bin/env python3
"""Ensure a public Statground CDN2 repository exists and is writable."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


API_ROOT = "https://api.github.com"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="owner/name repository slug")
    parser.add_argument("--token-env", default="STATGROUND_CDN2_ADMIN_TOKEN")
    parser.add_argument("--description", default="Statground Web-R CDN2 public assets")
    args = parser.parse_args()

    token = os.environ.get(args.token_env, "").strip()
    if not token:
        raise SystemExit(f"{args.token_env} secret is required")
    owner, name = parse_repo(args.repo)

    repo_payload = request_json("GET", f"/repos/{owner}/{name}", token, allow_404=True)
    if repo_payload is None:
        repo_payload = request_json(
            "POST",
            f"/orgs/{owner}/repos",
            token,
            {
                "name": name,
                "description": args.description,
                "private": False,
                "auto_init": False,
                "has_issues": False,
                "has_projects": False,
                "has_wiki": False,
            },
        )
        print(json.dumps({"repo": args.repo, "created": True, "private": repo_payload.get("private", None)}, ensure_ascii=False))
    else:
        permissions = repo_payload.get("permissions") or {}
        if repo_payload.get("private"):
            raise SystemExit(f"{args.repo} must be public for jsDelivr CDN use")
        if not permissions.get("push"):
            raise SystemExit(f"{args.token_env} must have contents write access to {args.repo}")
        print(json.dumps({"repo": args.repo, "created": False, "private": False}, ensure_ascii=False))
    return 0


def parse_repo(value: str) -> tuple[str, str]:
    parts = [part.strip() for part in value.strip().split("/", 1)]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise SystemExit("--repo must be owner/name")
    return parts[0], parts[1]


def request_json(method: str, path: str, token: str, payload: dict[str, object] | None = None, allow_404: bool = False) -> dict[str, object] | None:
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(API_ROOT + path, data=data, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if allow_404 and exc.code == 404:
            return None
        detail = exc.read(300).decode("utf-8", errors="replace")
        raise SystemExit(f"GitHub API {method} {path} failed: HTTP {exc.code}: {redact(detail)}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"GitHub API {method} {path} failed: {exc.__class__.__name__}") from exc


def redact(value: str) -> str:
    token = os.environ.get("STATGROUND_CDN2_ADMIN_TOKEN", "").strip()
    if token:
        value = value.replace(token, "***")
    return value


if __name__ == "__main__":
    sys.exit(main())
