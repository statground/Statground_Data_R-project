"""Reviewed enrollment token sealing; fixed repository, no runner/service start."""
from __future__ import annotations
import base64
import datetime as dt
import hashlib
import json
import os
import re
import signal
import subprocess
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

REPO = "statground/Statground_Data_R-project"
API = "https://api.github.com/repos/" + REPO
NAME = "webr-community-publisher-local"
LABELS = {"self-hosted", "linux", "x64", "webr-community-publisher"}
NONCE = "e78d49bc4cee984c1f33e895db1448d9"
KEY_SHA = "ea95594634d667621ea3e58cc9df2d711776613eb4b9b3176f8e28ad6b2386dc"
PUBLIC_KEY = b'-----BEGIN PUBLIC KEY-----\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAtoZWfZf765tIc0bbddrE\nWG9z9+IedEw5FJj9ulHFO9V1/29CPS0ZzJpDW+6NTwUm5XR4u2ox9AwQVL5OU8mx\nFXl184o5QS++eZ3+ko2BfZMSrGjorgMxuq7h+yZpEjAEnFZvdNg2wsOgg+iuCREX\nmRpfRJk7tqyWqt7jvwwP9UgKoWR0QJnaCCWkCSOtVkRqM0CbwdcQsGioS4OuixOU\nPxsYBSX904fujHdqPv+QLCgQBsgPExTlYtD6Qcy6inoK/4XHqttJDFnT9MAL/b5M\nxdr3UpUtVZklR1r8jDACqfdearataT2fPsUaufbcMd0mGomX3dQCzB2pqziqn5F5\nzt9gIJWOLUD1IMuG3uBEua1U3qXA2ecsOVHofPz5zUeZ7+sNit2RiiVB9DvgGPXB\nMf6YRxbL2ADMYU9AHS3M3TA/QjdKV1sFULHbFIgypf9cQQYUSbhAj9GV2u3sr3Bs\nsI47pO79gJOEkJMUdDtn/N7527prmIPAhUq0SOJtXB+ClE94CRCf4ZcO9Gl7ymQW\nIV5e+xEMjaGdO6LE19DQqywwnkOZMEOiIkf/BU6Kxd4hFeRABuZYLSxzgsFhF1Rm\nOTBqF0rj6RvEzdX3dD1WInmwJ4NcftYTULFAOBrpS9LbZKxEqxJ+P2PugQvhKuKF\nIZXY5znRRn+c+n22Jd/EseMCAwEAAQ==\n-----END PUBLIC KEY-----\n'
MAX_BODY, MAX_PAGES, WALL_SECONDS = 1 << 20, 10, 100

class Blocked(Exception):
    pass

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise Blocked("redirect_rejected")

class RepoAPI:
    def __init__(self, token):
        if not isinstance(token, str) or not token or len(token) > 4096 or not all(33 <= ord(c) <= 126 for c in token):
            raise Blocked("designated_secret_missing_or_invalid")
        self.token = token
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, method, suffix):
        if not ((method == "GET" and (suffix == "" or re.fullmatch(r"/actions/runners\?per_page=100&page=(?:[1-9]|10)", suffix))) or (method, suffix) == ("POST", "/actions/runners/registration-token")):
            raise Blocked("api_scope_rejected")
        url = API + suffix
        req = urllib.request.Request(url, data=b"{}" if method == "POST" else None, method=method,
            headers={"Authorization": "Bearer " + self.token, "Accept": "application/vnd.github+json",
                     "Content-Type": "application/json", "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with self.opener.open(req, timeout=8) as response:
                if response.status != (201 if method == "POST" else 200) or response.geturl() != url:
                    raise Blocked("response_identity_rejected")
                limit = 8192 if method == "POST" else MAX_BODY
                raw = response.read(limit + 1)
            if len(raw) > limit:
                raise Blocked("response_size_rejected")
            return json.loads(raw)
        except urllib.error.HTTPError as error:
            error.close()
            raise Blocked("github_permission_or_http_failure") from None
        except Exception as error:
            if isinstance(error, Blocked):
                raise
            raise Blocked("bounded_api_read_failed") from None

def inventory(api):
    repository = api.request("GET", "")
    if not isinstance(repository, dict) or str(repository.get("full_name", "")).casefold() != REPO.casefold():
        raise Blocked("repository_identity_rejected")
    rows, total, ids = [], None, set()
    for page in range(1, MAX_PAGES + 1):
        body = api.request("GET", f"/actions/runners?per_page=100&page={page}")
        if not isinstance(body, dict) or type(body.get("total_count")) is not int or not 0 <= body["total_count"] <= 1000 or not isinstance(body.get("runners"), list) or len(body["runners"]) > 100:
            raise Blocked("inventory_shape_rejected")
        if total is not None and total != body["total_count"]:
            raise Blocked("inventory_changed")
        total = body["total_count"]
        for row in body["runners"]:
            if not isinstance(row, dict) or type(row.get("id")) is not int or row["id"] <= 0 or row["id"] in ids or not isinstance(row.get("name"), str) or not row["name"] or row.get("status") not in ("online", "offline") or type(row.get("busy")) is not bool or not isinstance(row.get("labels"), list):
                raise Blocked("runner_shape_rejected")
            if any(not isinstance(label, dict) or not isinstance(label.get("name"), str) for label in row["labels"]):
                raise Blocked("runner_labels_rejected")
            ids.add(row["id"])
            rows.append(row)
        if len(rows) == total:
            return rows
        if len(rows) > total or len(body["runners"]) != 100:
            raise Blocked("inventory_incomplete")
    raise Blocked("inventory_page_limit")

def publisher(row):
    return LABELS <= {label["name"].casefold() for label in row["labels"]}

def require_empty(rows):
    if rows or any(publisher(row) or row["name"].casefold() == NAME.casefold() for row in rows):
        raise Blocked("inventory_not_empty_or_name_collision")

def context(env):
    if env.get("GITHUB_REPOSITORY", "").casefold() != REPO.casefold() or not re.fullmatch(r"[0-9a-f]{40}", env.get("GITHUB_SHA", "")) or not re.fullmatch(r"[1-9][0-9]{0,19}", env.get("GITHUB_RUN_ID", "")):
        raise Blocked("workflow_identity_rejected")
    ref = env.get("GITHUB_REF", "")
    if ref == "refs/tags/webr-runner-sealed-bootstrap-" + NONCE:
        if env.get("GITHUB_RUN_ATTEMPT") != "1":
            raise Blocked("mint_retry_rejected")
        return "mint", None
    match = re.fullmatch(r"refs/tags/webr-runner-sealed-readback-" + NONCE + r"-([1-9][0-9]{0,19})", ref)
    if match:
        return "readback", int(match[1])
    if env.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and env.get("BOOTSTRAP_MODE") == "readback" and re.fullmatch(r"[1-9][0-9]{0,19}", env.get("EXPECTED_RUNNER_ID", "")):
        return "readback", int(env["EXPECTED_RUNNER_ID"])
    raise Blocked("reviewed_tag_or_readback_required")

def seal(payload):
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    if len(raw) > 446 or hashlib.sha256(PUBLIC_KEY).hexdigest() != KEY_SHA:
        raise Blocked("sealed_input_or_public_key_rejected")
    with tempfile.TemporaryDirectory(prefix="webr-public-recipient-") as temp:
        public = Path(temp) / "recipient.pem"
        public.write_bytes(PUBLIC_KEY)
        try:
            result = subprocess.run(["openssl", "pkeyutl", "-encrypt", "-pubin", "-inkey", str(public),
                "-pkeyopt", "rsa_padding_mode:oaep", "-pkeyopt", "rsa_oaep_md:sha256", "-pkeyopt", "rsa_mgf1_md:sha256"],
                input=raw, capture_output=True, timeout=8)
        except Exception:
            raise Blocked("bounded_sealing_failed") from None
    if result.returncode or len(result.stdout) != 512:
        raise Blocked("sealing_failed")
    encoded = base64.urlsafe_b64encode(result.stdout).decode("ascii").rstrip("=")
    chunks = [encoded[index * 86:(index + 1) * 86] for index in range(8)]
    return [{"index": index, "chunk": chunk} for index, chunk in enumerate(chunks)]

def bootstrap(api, env, clock=lambda: int(dt.datetime.now(dt.timezone.utc).timestamp()), sealer=seal):
    mode, expected_id = context(env)
    rows = inventory(api)
    if mode == "readback":
        matches = [row for row in rows if publisher(row)]
        names = [row for row in rows if row["name"].casefold() == NAME.casefold()]
        found = [row for row in matches if row["id"] == expected_id and row["name"] == NAME]
        if len(matches) != 1 or len(names) != 1 or len(found) != 1 or found[0]["busy"]:
            raise Blocked("registered_runner_readback_failed")
        return {"state": "readback", "runner_id": expected_id, "name_match": "true", "labels_match": "true",
                "status": found[0]["status"], "busy": "false", "total": len(rows), "publishers": len(matches),
                "observed": clock(), "matrix": []}
    require_empty(rows)
    receipt = api.request("POST", "/actions/runners/registration-token")
    if not isinstance(receipt, dict) or not re.fullmatch(r"[A-Za-z0-9]{1,128}", str(receipt.get("token", ""))):
        raise Blocked("registration_token_shape_rejected")
    try:
        expiration = dt.datetime.fromisoformat(receipt["expires_at"].replace("Z", "+00:00"))
        if expiration.tzinfo is None:
            raise ValueError()
        expires = int(expiration.timestamp())
    except Exception:
        raise Blocked("registration_expiry_rejected") from None
    now = clock()
    if not now + 120 <= expires <= now + 3660:
        raise Blocked("registration_expiry_rejected")
    require_empty(inventory(api))
    now = clock()
    if expires < now + 120:
        raise Blocked("registration_expiry_rejected")
    payload = [1, REPO, receipt["token"], expires, now, env["GITHUB_SHA"], int(env["GITHUB_RUN_ID"]), NONCE, KEY_SHA]
    try:
        matrix = sealer(payload)
    finally:
        receipt.clear()
        payload.clear()
    return {"state": "sealed", "source": env["GITHUB_SHA"], "run": env["GITHUB_RUN_ID"], "nonce": NONCE,
            "key": KEY_SHA, "observed": now, "total": 0, "publishers": 0, "matrix": matrix}

def main():
    def deadline(*_args):
        raise Blocked("wall_deadline")
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(WALL_SECONDS)
    try:
        result = bootstrap(RepoAPI(os.environ.get("RUNNER_BOOTSTRAP_TOKEN", "")), os.environ)
        path = os.environ.get("GITHUB_OUTPUT", "")
        if not path:
            raise Blocked("safe_output_missing")
        with open(path, "a", encoding="ascii") as output:
            for key, value in result.items():
                output.write(key + "=" + (json.dumps(value, separators=(",", ":")) if key == "matrix" else str(value)) + "\n")
        print(json.dumps({"state": result["state"], "total": result["total"], "publishers": result["publishers"], "plaintext_output": False}, separators=(",", ":")))
        return 0
    except Exception as error:
        reason = str(error) if isinstance(error, Blocked) else "private_bootstrap_failed"
        print(json.dumps({"state": "unknown", "reason": reason, "plaintext_output": False}, separators=(",", ":")))
        return 1
    finally:
        signal.alarm(0)

if __name__ == "__main__":
    raise SystemExit(main())
