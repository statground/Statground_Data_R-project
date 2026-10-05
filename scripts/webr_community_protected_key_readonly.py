"""Fixed protected-environment GETs; expose public encryption material, never secrets."""
from __future__ import annotations

import base64
import datetime
import json
import os
import re
import signal
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPOSITORY = "statground/Statground_Data_R-project"
ENVIRONMENT = "web-r-community-publication"
ORIGIN = "https://api.github.com"
REPO_URL = ORIGIN + "/repos/" + REPOSITORY
SECRETS_URL = REPO_URL + "/environments/" + ENVIRONMENT + "/secrets/"
SECRET_NAMES = (
    "WEBR_COMMUNITY_CLICKHOUSE_CONFIGS_B64",
    "WEBR_COMMUNITY_READER_INVENTORY_B64",
    "WEBR_COMMUNITY_READER_TOKENS_B64",
)
NONCE = "2dc637f61d5f92c34fc99b4ba39abd5b"
CREDENTIAL_CONTEXT = "repo-bootstrap-admin"
TAG = "webr-community-key-readonly-20261005-" + NONCE
WORKFLOW = ".github/workflows/webr-community-protected-key-readonly.yml"
MAX_BODY, REQUEST_SECONDS, READ_SECONDS, WALL_SECONDS = 8192, 12, 80, 100
SCHEMA = "webr.community.protected-key-readonly.v1"
GET_STAGES = ("repo", "key", "configs", "readers", "tokens")
REASONS = frozenset((
    "context_invalid", "checkout_identity_invalid", "designated_secret_missing",
    "authorization_header_invalid", "http_error", "redirect_rejected",
    "response_identity_invalid", "body_limit", "json_invalid", "repository_identity_invalid",
    "public_key_shape_invalid", "secret_metadata_identity_invalid", "wall_deadline",
    "transport_unavailable", "safe_output_unavailable", "public_material_collision",
))


class ReadFailure(Exception):
    def __init__(self, reason: str, status: int | None = None, stage: str | None = None):
        self.reason = reason if reason in REASONS else "transport_unavailable"
        self.status = status if type(status) is int and 100 <= status <= 599 else None
        self.stage = stage if stage in GET_STAGES else None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ReadFailure("redirect_rejected")


def checkout_head() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
        ).stdout.decode("ascii").strip()
    except Exception:
        raise ReadFailure("checkout_identity_invalid") from None


def workflow_context(env, head: str) -> dict:
    ref, event, sha = env.get("GITHUB_REF", ""), env.get("GITHUB_EVENT_NAME", ""), env.get("GITHUB_SHA", "")
    if not (
        env.get("GITHUB_ACTIONS") == "true"
        and env.get("GITHUB_REPOSITORY") == REPOSITORY
        and env.get("GITHUB_SERVER_URL") == "https://github.com"
        and env.get("GITHUB_API_URL") == ORIGIN
        and env.get("GITHUB_RUN_ATTEMPT") == "1"
        and env.get("PROTECTED_KEY_READONLY_NONCE") == NONCE
        and env.get("PROTECTED_KEY_CREDENTIAL_CONTEXT") == CREDENTIAL_CONTEXT
        and env.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and env.get("RUNNER_OS") == "Linux"
        and re.fullmatch(r"[0-9a-f]{40}", sha)
        and env.get("GITHUB_WORKFLOW_SHA") == sha
        and env.get("GITHUB_WORKFLOW_REF") == REPOSITORY + "/" + WORKFLOW + "@" + ref
        and re.fullmatch(r"[1-9][0-9]{0,19}", env.get("GITHUB_RUN_ID", ""))
        and re.fullmatch(r"[1-9][0-9]{0,19}", env.get("GITHUB_REPOSITORY_ID", ""))
        and ((event == "push" and ref == "refs/tags/" + TAG)
             or (event == "workflow_dispatch" and ref == "refs/heads/main"))
    ):
        raise ReadFailure("context_invalid")
    if head != sha:
        raise ReadFailure("checkout_identity_invalid")
    return {"source_sha": sha, "ref": ref, "run_id": env["GITHUB_RUN_ID"],
            "run_attempt": 1, "repository_id": int(env["GITHUB_REPOSITORY_ID"])}


def duplicate_free_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReadFailure("json_invalid")
        result[key] = value
    return result


def get_json(opener, token: str, url: str, remaining: float, missing_ok=False) -> dict | None:
    request = urllib.request.Request(url, method="GET", headers={
        "Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    try:
        with opener.open(request, timeout=min(REQUEST_SECONDS, remaining)) as response:
            if response.status != 200 or response.geturl() != url:
                raise ReadFailure("response_identity_invalid")
            raw = response.read(MAX_BODY + 1)
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()  # Never read or expose error bodies, URLs or headers.
        if missing_ok and status == 404:
            return None
        raise ReadFailure("http_error", status) from None
    if len(raw) > MAX_BODY:
        raise ReadFailure("body_limit")
    try:
        data = json.loads(raw, object_pairs_hook=duplicate_free_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ReadFailure("json_invalid")))
    except Exception:
        raise ReadFailure("json_invalid") from None
    if not isinstance(data, dict):
        raise ReadFailure("json_invalid")
    return data


def read_protected_key(token: str, context: dict, opener=None, clock=time.monotonic) -> dict:
    if not token:
        raise ReadFailure("designated_secret_missing")
    # Treat the existing credential as opaque; only reject unsafe HTTP header bytes.
    if len(token) > 4096 or any(not 33 <= ord(char) <= 126 for char in token):
        raise ReadFailure("authorization_header_invalid")
    opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    started = clock()

    def read(url, stage, missing_ok=False):
        try:
            remaining = READ_SECONDS - (clock() - started)
            if remaining <= 0:
                raise ReadFailure("wall_deadline")
            value = get_json(opener, token, url, remaining, missing_ok)
            if clock() - started >= READ_SECONDS:
                raise ReadFailure("wall_deadline")
            return value
        except ReadFailure as error:
            raise ReadFailure(error.reason, error.status, stage) from None
        except Exception:
            raise ReadFailure("transport_unavailable", stage=stage) from None

    repo = read(REPO_URL, "repo")
    if (type(repo.get("id")) is not int or repo["id"] != context["repository_id"]
            or repo.get("full_name") != REPOSITORY):
        raise ReadFailure("repository_identity_invalid", stage="repo")
    public = read(SECRETS_URL + "public-key", "key")
    key, key_id = public.get("key"), public.get("key_id")
    try:
        valid = (isinstance(key, str) and len(key) == 44
                 and len(base64.b64decode(key, validate=True)) == 32
                 and base64.b64encode(base64.b64decode(key, validate=True)).decode("ascii") == key
                 and isinstance(key_id, str) and re.fullmatch(r"[0-9]{1,32}", key_id))
    except Exception:
        valid = False
    if not valid:
        raise ReadFailure("public_key_shape_invalid", stage="key")
    if token in (key, key_id, str(repo["id"])):
        raise ReadFailure("public_material_collision", stage="key")
    presence = {}
    for stage, name in zip(GET_STAGES[2:], SECRET_NAMES):
        metadata = read(SECRETS_URL + name, stage, missing_ok=True)
        if metadata is not None and metadata.get("name") != name:
            raise ReadFailure("secret_metadata_identity_invalid", stage=stage)
        presence[name] = metadata is not None
    return {"complete": True, "state": "complete", "reason": None, "http_status": None, "get_stage": None,
            "key": key, "key_id": key_id, "metadata_get_200": presence}


def failure(reason: str, status=None, stage=None) -> dict:
    return {"complete": False, "state": "blocked", "reason": reason,
            "http_status": status, "get_stage": stage, "key": None, "key_id": None, "metadata_get_200": None}


def write_outputs(result: dict, env) -> None:
    # Only the existing runner command file receives finite public fields.
    try:
        path, temporary = Path(env["GITHUB_OUTPUT"]), Path(env["RUNNER_TEMP"])
        if (not path.is_absolute() or not temporary.is_absolute() or path.resolve() != path
                or temporary.resolve() != temporary or not path.is_relative_to(temporary)):
            raise ValueError()
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(fd, "a", encoding="ascii") as output:
            file_stat = os.fstat(output.fileno())
            if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_uid != os.geteuid():
                raise ValueError()
            values = {"complete": str(result["complete"]).lower(), "state": result["state"],
                      "reason": result["reason"] or "none", "http_status": str(result["http_status"] or "none"),
                      "get_stage": result["get_stage"] or "none", "credential_context": CREDENTIAL_CONTEXT,
                      "nonce": NONCE, "observed_at": result["observed_at"]}
            for name in ("source_sha", "run_id", "repository_id"):
                values[name] = str(result.get(name, "unknown"))
            values.update({"key": result["key"] or "unknown", "key_id": result["key_id"] or "unknown"})
            for label, name in zip(("configs", "readers", "tokens"), SECRET_NAMES):
                presence = result["metadata_get_200"]
                values[label] = str(presence[name]).lower() if presence is not None else "unknown"
            for name, value in values.items():
                output.write(name + "=" + value + "\n")
    except Exception:
        raise ReadFailure("safe_output_unavailable") from None


def main() -> int:
    def deadline(_signum, _frame):
        raise ReadFailure("wall_deadline")

    context = {}
    previous = signal.signal(signal.SIGALRM, deadline)
    signal.alarm(WALL_SECONDS)
    try:
        if len(sys.argv) != 1:
            raise ReadFailure("context_invalid")
        context = workflow_context(os.environ, checkout_head())
        result = read_protected_key(os.environ.get("RUNNER_BOOTSTRAP_TOKEN", ""), context)
    except ReadFailure as error:
        result = failure(error.reason, error.status, error.stage)
    except Exception:
        result = failure("transport_unavailable")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    result.update(context)
    result.update({"schema": SCHEMA, "repository": REPOSITORY, "environment": ENVIRONMENT,
                   "nonce": NONCE, "read_only": True,
                   "credential_context": CREDENTIAL_CONTEXT,
                   "observed_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
    try:
        write_outputs(result, os.environ)
    except ReadFailure:
        result.update(failure("safe_output_unavailable"))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
