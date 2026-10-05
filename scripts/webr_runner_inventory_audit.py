"""Manual read-only inventory; never infer absence from denied/incomplete reads."""
from __future__ import annotations

import json
import datetime
import os
import re
import signal
import time
import urllib.error
import urllib.request

REPOSITORY = "statground/Statground_Data_R-project"
ENDPOINT = "https://api.github.com/repos/" + REPOSITORY + "/actions/runners"
REQUIRED_LABELS = frozenset(("self-hosted", "linux", "x64", "webr-community-publisher"))
PAGE_SIZE, MAX_PAGES, MAX_BODY, WALL_SECONDS = 100, 10, 1 << 20, 90


class AuditFailure(Exception):
    def __init__(self, state: str, reason: str, status: int | None = None):
        self.state, self.reason, self.status = state, reason, status


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise AuditFailure("unavailable", "redirect_rejected")


def safe_name(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,59}", value):
        return "redacted"
    if re.search(r"(?:^|[-_])(?:\d{1,3}[-_]){3}\d{1,3}(?:$|[-_])", value) or re.match(r"(?:github_pat_|gh[pousr]_)", value):
        return "redacted"
    return value


def failure_result(state: str, reason: str, status: int | None = None) -> dict:
    return {"schema": "webr.runner.inventory.v1", "repository": REPOSITORY,
            "complete": False, "state": state, "reason": reason, "http_status": status,
            "total_count": None, "publisher_count": None, "publisher_online_count": None,
            "publisher_idle_count": None, "publishers": []}


def read_page(opener, token: str, page: int) -> dict:
    url = ENDPOINT + f"?per_page={PAGE_SIZE}&page={page}"
    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}, method="GET")
    with opener.open(request, timeout=8) as response:
        if response.status != 200 or response.geturl() != url:
            raise AuditFailure("unavailable", "response_identity_invalid")
        raw = response.read(MAX_BODY + 1)
    if len(raw) > MAX_BODY:
        raise AuditFailure("incomplete", "body_limit")
    try:
        data = json.loads(raw)
    except Exception:
        raise AuditFailure("incomplete", "json_invalid") from None
    if not isinstance(data, dict) or type(data.get("total_count")) is not int or data["total_count"] < 0 or not isinstance(data.get("runners"), list) or len(data["runners"]) > PAGE_SIZE:
        raise AuditFailure("incomplete", "inventory_shape_invalid")
    return data


def audit(token: str, opener=None, clock=time.monotonic) -> dict:
    if not token:
        return failure_result("unavailable", "designated_secret_missing")
    opener = opener or urllib.request.build_opener(NoRedirect())
    started, total, seen, publishers = clock(), None, set(), []
    try:
        for page in range(1, MAX_PAGES + 1):
            if clock() - started >= WALL_SECONDS:
                raise AuditFailure("unavailable", "wall_deadline")
            data = read_page(opener, token, page)
            if total is None:
                total = data["total_count"]
            if total != data["total_count"]:
                raise AuditFailure("incomplete", "inventory_changed")
            if total > PAGE_SIZE * MAX_PAGES:
                raise AuditFailure("incomplete", "inventory_limit")
            for runner in data["runners"]:
                if not isinstance(runner, dict) or type(runner.get("id")) is not int or runner["id"] <= 0 or runner["id"] in seen or runner.get("status") not in ("online", "offline") or type(runner.get("busy")) is not bool or not isinstance(runner.get("labels"), list):
                    raise AuditFailure("incomplete", "runner_shape_or_duplicate")
                seen.add(runner["id"])
                labels = runner["labels"]
                if any(not isinstance(label, dict) or not isinstance(label.get("name"), str) for label in labels):
                    raise AuditFailure("incomplete", "label_shape_invalid")
                if REQUIRED_LABELS.issubset(label["name"].lower() for label in labels):
                    publishers.append({"name": safe_name(runner.get("name")), "status": runner["status"], "busy": runner["busy"]})
            if len(seen) == total:
                return {"schema": "webr.runner.inventory.v1", "repository": REPOSITORY,
                    "complete": True, "state": "complete", "http_status": 200, "total_count": total,
                    "publisher_count": len(publishers),
                    "publisher_online_count": sum(p["status"] == "online" for p in publishers),
                    "publisher_idle_count": sum(p["status"] == "online" and not p["busy"] for p in publishers),
                    "publishers": publishers}
            if len(seen) > total or len(data["runners"]) != PAGE_SIZE:
                raise AuditFailure("incomplete", "inventory_count_mismatch")
        raise AuditFailure("incomplete", "pagination_limit")
    except urllib.error.HTTPError as error:
        status = int(error.code)
        return failure_result("access-denied" if status in (401, 403, 404) else "unavailable", "http_error", status)
    except AuditFailure as error:
        return failure_result(error.state, error.reason, error.status)
    except Exception:
        return failure_result("unavailable", "transport_or_parse_failed")


def write_outputs(result: dict, output_path: str) -> None:
    if not output_path:
        return
    values = {"inventory_state": result["state"]}
    for name in ("total_count", "publisher_count", "publisher_online_count", "publisher_idle_count"):
        value = result[name]
        values[name] = str(value) if result["complete"] and type(value) is int and 0 <= value <= PAGE_SIZE * MAX_PAGES else "unknown"
    with open(output_path, "a", encoding="ascii") as stream:
        for name, value in values.items():
            stream.write(name + "=" + value + "\n")


def main() -> int:
    # Hosted Linux timer bounds the complete audit even for a slow socket body.
    def deadline(_signum, _frame):
        raise AuditFailure("unavailable", "wall_deadline")
    previous = signal.signal(signal.SIGALRM, deadline)
    signal.alarm(WALL_SECONDS)
    try:
        result = audit(os.environ.get("RUNNER_AUDIT_TOKEN", ""))
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    result["observed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    source_sha = os.environ.get("GITHUB_SHA", "")
    result["source_sha"] = source_sha if re.fullmatch(r"[0-9a-f]{40}", source_sha) else None
    try:
        write_outputs(result, os.environ.get("GITHUB_OUTPUT", ""))
    except Exception:
        result = failure_result("unavailable", "safe_output_unavailable")
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
