#!/usr/bin/env python3

from __future__ import annotations

import base64
import hashlib
import html as html_lib
import json
import os
import random
import re
import secrets
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer


def first_non_empty(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


INSTANCE = os.getenv("MASTODON_INSTANCE", "https://fosstodon.org").rstrip("/")
ACCT = os.getenv("MASTODON_ACCT", "R_Foundation").strip()
ACCOUNT_ID_ENV = first_non_empty(os.getenv("MASTODON_ACCOUNT_ID"), os.getenv("MASTODON_R_FOUNDATION_ACCOUNT_ID"))
MASTODON_TOKEN = os.getenv("MASTODON_TOKEN", "").strip()

KAFKA_BROKERS = first_non_empty(os.getenv("KAFKA_BROKERS"), os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "webr.events").strip()
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "").strip()
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip()
KAFKA_USERNAME = first_non_empty(os.getenv("KAFKA_USERNAME"), os.getenv("KAFKA_EXTERNAL_USER"))
KAFKA_PASSWORD = first_non_empty(os.getenv("KAFKA_PASSWORD"), os.getenv("KAFKA_EXTERNAL_PASSWORD"))
KAFKA_PRODUCER_MESSAGE_MAX_BYTES = int(os.getenv("KAFKA_PRODUCER_MESSAGE_MAX_BYTES", "12582912"))

STATE_FILE = Path(os.getenv("STATE_FILE", "data/mastodon/r_foundation/state.json"))
SYNC_MODE = os.getenv("SYNC_MODE", "incremental").strip().lower()
FORCE_REPUBLISH = os.getenv("FORCE_REPUBLISH", "false").lower() in {"1", "true", "yes", "y"}
TRANSLATE_ENABLED = os.getenv("TRANSLATE_ENABLED", os.getenv("MASTODON_TRANSLATE_ENABLED", "true")).lower() in {"1", "true", "yes", "y"}
FAIL_ON_TRANSLATION_ERROR = os.getenv("FAIL_ON_TRANSLATION_ERROR", os.getenv("MASTODON_FAIL_ON_TRANSLATION_ERROR", "false")).lower() in {
    "1",
    "true",
    "yes",
    "y",
}
TRANSLATION_MODEL = os.getenv("MASTODON_TRANSLATION_MODEL", "openai/gpt-oss-20b").strip()
AI_TIMEOUT_SECONDS = int(os.getenv("AI_TIMEOUT", "300"))
AI_PROVIDER_MAX_ATTEMPTS = int(os.getenv("AI_PROVIDER_MAX_ATTEMPTS", os.getenv("MASTODON_AI_PROVIDER_MAX_ATTEMPTS", "3")))
AI_RETRY_BASE_SECONDS = float(os.getenv("AI_RETRY_BASE_SECONDS", os.getenv("MASTODON_AI_RETRY_BASE_SECONDS", "2")))
AI_RETRY_MAX_SECONDS = float(os.getenv("AI_RETRY_MAX_SECONDS", os.getenv("MASTODON_AI_RETRY_MAX_SECONDS", "30")))
MAX_TRANSLATION_SOURCE_CHARS = int(os.getenv("MASTODON_MAX_TRANSLATION_SOURCE_CHARS", "5000"))
RETRY_MISSING_BOARD_TRANSLATIONS = os.getenv("RETRY_MISSING_BOARD_TRANSLATIONS", "true").lower() in {"1", "true", "yes", "y"}

EXCLUDE_REBLOGS = os.getenv("EXCLUDE_REBLOGS", "true").lower() in {"1", "true", "yes", "y"}
EXCLUDE_REPLIES = os.getenv("EXCLUDE_REPLIES", "false").lower() in {"1", "true", "yes", "y"}
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.4"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "10000"))

CRAWL_IMAGES_BASE64 = os.getenv("CRAWL_IMAGES_BASE64", "true").lower() in {"1", "true", "yes", "y"}
MAX_IMAGE_BYTES = int(os.getenv("MAX_IMAGE_BYTES", str(5 * 1024 * 1024)))
MAX_TOTAL_IMAGE_BYTES = int(os.getenv("MAX_TOTAL_IMAGE_BYTES", str(9 * 1024 * 1024)))
MAX_STATUS_IMAGES = int(os.getenv("MAX_STATUS_IMAGES", "4"))
MAX_KAFKA_EVENT_BYTES = int(os.getenv("MAX_KAFKA_EVENT_BYTES", "11534336"))

USER_AGENT = os.getenv(
    "USER_AGENT",
    "StatgroundBot/1.0 (+https://www.statground.net; Mastodon public feed mirror)",
)

RAW_EVENT_TYPE = "webr.mastodon.raw.v1"
LOG_EVENT_TYPE = "webr.mastodon.log.v1"
BOARD_EVENT_TYPE = "webr.mastodon.board.v1"
LINK_RE = re.compile(r"<([^>]+)>;\s*rel=\"([^\"]+)\"")
URL_RE = re.compile(r"(?i)\b(?:https?://|www\.)[^\s<>()\"']+")
MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((?:https?://|www\.)[^)]+\)")
TAG_RE = re.compile(r"(?is)</?\s*([a-zA-Z0-9]+)\b[^>]*>")
FIRST_ALLOWED_TAG_RE = re.compile(r"(?is)<\s*(h2|h3|p|ul|ol|li|strong|em|code|pre|blockquote)\b")
SPACE_RE = re.compile(r"[ \t\xA0]+")
TRY_AGAIN_IN_RE = re.compile(r"(?i)try again in\s+([0-9.]+)\s*s")
ALLOWED_CONTENT_TAGS = {"h2", "h3", "p", "ul", "ol", "li", "strong", "em", "code", "pre", "blockquote"}
BOARD_STATE_KEYS = {
    "board_payload_hash",
    "board_translation_status",
    "board_translated_at",
    "board_translation_attempted_at",
    "board_translation_attempts",
    "board_translation_error",
}


@dataclass
class ImageBudget:
    total_used: int = 0
    embedded_count: int = 0


def uuid7() -> str:
    """Generate an RFC 9562-style UUIDv7 string without external dependencies."""
    unix_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (unix_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0x2 << 62) | rand_b
    return str(uuid.UUID(int=value))


def now_kst_string() -> str:
    # GitHub runner usually runs UTC. We avoid zoneinfo dependency by formatting from UTC+9.
    ts = time.time() + 9 * 60 * 60
    dt = datetime.fromtimestamp(ts, timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_u64(value: Any) -> int:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def deterministic_uuid(seed: str) -> str:
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    value = bytearray(digest[:16])
    value[6] = (value[6] & 0x0F) | 0x50
    value[8] = (value[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(value)))


def instance_host() -> str:
    return INSTANCE.replace("https://", "").replace("http://", "").split("/")[0]


def status_stable_uuid(status_id: str) -> str:
    return deterministic_uuid(f"statground_mastodon:{instance_host()}:{ACCT}:{status_id}")


def source_language_code(language: str | None) -> str:
    value = (language or "").strip().lower()
    if not value or value in {"und", "unknown", "null"}:
        return "en"
    return value


def broker_host(raw: str) -> str:
    raw = raw.strip()
    parsed = urlparse(raw if "://" in raw else f"tcp://{raw}")
    if parsed.hostname:
        return parsed.hostname
    return raw.rsplit(":", 1)[0].strip("[]")


def is_loopback_host(host: str) -> bool:
    host = host.strip().strip("[]").lower()
    if host in {"", "localhost", "127.0.0.1", "0.0.0.0", "::1"}:
        return True
    try:
        ip = socket.getaddrinfo(host, None)[0][4][0]
    except socket.gaierror:
        return False
    parsed = socket.inet_pton(socket.AF_INET6 if ":" in ip else socket.AF_INET, ip)
    if len(parsed) == 4:
        return parsed[0] == 127 or parsed == b"\x00\x00\x00\x00"
    return ip == "::1" or ip == "::"


def validate_kafka_brokers() -> None:
    if not KAFKA_BROKERS:
        raise RuntimeError("KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS secret is required")
    for broker in KAFKA_BROKERS.split(","):
        broker = broker.strip()
        if not broker:
            continue
        host = broker_host(broker)
        if is_loopback_host(host):
            raise RuntimeError(
                f"KAFKA_BROKERS must use an externally reachable Kafka listener, not {broker!r}. "
                "Use the public Kafka listener address configured for this pipeline."
            )


def html_to_text(html: str | None) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)


def remove_urls(value: str) -> str:
    value = MARKDOWN_LINK_RE.sub(r"\1", value or "")
    value = URL_RE.sub("", value)
    return SPACE_RE.sub(" ", value).strip()


def truncate_chars(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit].rstrip()


def looks_korean(value: str, threshold: float = 0.2) -> bool:
    compact = "".join(value.split())
    if len(compact) < 10:
        return False
    hangul = sum(1 for char in compact if "가" <= char <= "힣")
    return bool(compact) and hangul / len(compact) >= threshold


def clean_title_output(value: str) -> str:
    value = (value or "").strip()
    value = value.removeprefix("```").removesuffix("```").strip()
    value = re.split(r"[\r\n]", value, maxsplit=1)[0]
    value = re.sub(r"(?i)^(translation|translated title|title|result|output|번역|번역문|제목|결과|출력)\s*[:\-]\s*", "", value)
    return remove_urls(value.strip(" \t\"'“”‘’"))


def remove_block_tag(value: str, tag: str) -> str:
    return re.sub(rf"(?is)<{tag}\b[^>]*>.*?</{tag}>", "", value)


def remove_void_tag(value: str, tag: str) -> str:
    return re.sub(rf"(?is)<{tag}\b[^>]*>", "", value)


def sanitize_html_fragment(value: str) -> str:
    value = (value or "").strip()
    value = value.removeprefix("```html").removeprefix("```").removesuffix("```").strip()
    if not value:
        return ""
    if not value.startswith("<"):
        match = FIRST_ALLOWED_TAG_RE.search(value)
        if match:
            value = value[match.start() :].strip()
        else:
            value = f"<p>{html_lib.escape(remove_urls(value))}</p>"

    for tag in ("script", "style", "iframe"):
        value = remove_block_tag(value, tag)
    value = remove_void_tag(value, "img")

    anchor_re = re.compile(r"(?is)<a\b[^>]*>(.*?)</a>")
    value = anchor_re.sub(lambda match: html_lib.escape(remove_urls(html_to_text(match.group(1)))), value)
    value = MARKDOWN_LINK_RE.sub(r"\1", value)
    value = URL_RE.sub("", value)

    def clean_tag(match: re.Match[str]) -> str:
        name = match.group(1).lower()
        if name not in ALLOWED_CONTENT_TAGS:
            return ""
        if match.group(0).lstrip().startswith("</"):
            return f"</{name}>"
        return f"<{name}>"

    value = TAG_RE.sub(clean_tag, value)
    value = re.sub(r"\s+</", "</", value).strip()
    value = re.sub(r">\s+", ">", value)
    if "<" not in value:
        value = f"<p>{html_lib.escape(remove_urls(value))}</p>"
    lower = value.lower()
    if "<a" in lower or "href=" in lower or URL_RE.search(value):
        raise ValueError("sanitized translation still contains a hyperlink or URL")
    return value


def html_fragment_blank(value: str) -> bool:
    return html_to_text(value).strip() == ""


def safe_board_content(title: str, content: str) -> str:
    content = (content or "").strip()
    if content and not html_fragment_blank(content):
        return content
    fallback = first_non_empty(title, "R Foundation Mastodon")
    return sanitize_html_fragment(f"<p>{html_lib.escape(remove_urls(fallback))}</p>")


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    if MASTODON_TOKEN:
        session.headers["Authorization"] = f"Bearer {MASTODON_TOKEN}"
    return session


SESSION = make_session()


class AIClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "User-Agent": USER_AGENT})
        self.timeout = AI_TIMEOUT_SECONDS
        self.keys = {
            "openrouter": os.getenv("OPENROUTER_API_KEY", "").strip(),
            "groq": os.getenv("GROQ_API_KEY", "").strip(),
            "cerebras": os.getenv("CEREBRAS_API_KEY", "").strip(),
            "gh_models": os.getenv("GH_MODELS_API_KEY", "").strip(),
        }
        self.providers = [provider for provider in ("openrouter", "groq", "cerebras", "gh_models") if self.keys[provider]]

    def enabled(self) -> bool:
        return bool(self.providers)

    def chat(self, prompt: str, model: str) -> str:
        providers = list(self.providers)
        random.shuffle(providers)
        errors: list[str] = []
        for provider in providers:
            try:
                result = self.call_provider_with_retries(provider, prompt, model)
            except Exception as exc:  # noqa: BLE001 - preserve provider fallback behavior
                errors.append(f"{provider}: {exc}")
                continue
            if result.strip():
                return result.strip()
        raise RuntimeError(" | ".join(errors) or "no AI provider returned content")

    def provider_model(self, provider: str, model: str) -> str:
        if provider == "openrouter":
            return first_non_empty(os.getenv("MASTODON_OPENROUTER_MODEL"), os.getenv("OPENROUTER_MODEL"), model)
        if provider == "groq":
            return first_non_empty(os.getenv("MASTODON_GROQ_MODEL"), os.getenv("GROQ_MODEL"), model)
        if provider == "cerebras":
            return first_non_empty(os.getenv("MASTODON_CEREBRAS_MODEL"), os.getenv("CEREBRAS_MODEL"), model)
        if provider == "gh_models":
            return first_non_empty(os.getenv("MASTODON_GH_MODELS_MODEL"), os.getenv("GH_MODELS_MODEL"), model)
        return model

    def call_provider_with_retries(self, provider: str, prompt: str, model: str) -> str:
        last_error: Exception | None = None
        attempts = max(1, AI_PROVIDER_MAX_ATTEMPTS)
        for attempt in range(1, attempts + 1):
            try:
                return self.call_provider(provider, prompt, model)
            except AIHTTPError as exc:
                last_error = exc
                if attempt >= attempts or not exc.retryable():
                    break
                delay = exc.retry_delay(attempt)
                print(f"{provider} returned HTTP {exc.status_code}. Retry {attempt}/{attempts} after {delay:.1f}s.", file=sys.stderr)
                time.sleep(delay)
            except Exception as exc:  # noqa: BLE001 - provider fallback path records the final provider error
                last_error = exc
                break
        raise RuntimeError(str(last_error) if last_error else "provider did not return content")

    def provider_request(self, provider: str, model: str) -> tuple[str, dict[str, str], str]:
        headers = {"Content-Type": "application/json"}
        requested_model = self.provider_model(provider, model)
        if provider == "openrouter":
            headers["Authorization"] = "Bearer " + self.keys[provider]
            return "https://openrouter.ai/api/v1/chat/completions", headers, normalize_openrouter_model(requested_model)
        if provider == "groq":
            headers["Authorization"] = "Bearer " + self.keys[provider]
            return "https://api.groq.com/openai/v1/chat/completions", headers, normalize_groq_model(requested_model)
        if provider == "cerebras":
            headers["Authorization"] = "Bearer " + self.keys[provider]
            return "https://api.cerebras.ai/v1/chat/completions", headers, normalize_cerebras_model(requested_model)
        if provider == "gh_models":
            headers["Authorization"] = "Bearer " + self.keys[provider]
            headers["Accept"] = "application/vnd.github+json"
            headers["X-GitHub-Api-Version"] = "2026-03-10"
            return "https://models.github.ai/inference/chat/completions", headers, normalize_gh_model(requested_model)
        raise ValueError(f"unsupported AI provider: {provider}")

    def call_provider(self, provider: str, prompt: str, model: str) -> str:
        endpoint, headers, used_model = self.provider_request(provider, model)
        body = {
            "model": used_model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
        }
        response = self.session.post(endpoint, headers=headers, json=body, timeout=self.timeout)
        if response.status_code // 100 != 2:
            raise AIHTTPError(response.status_code, response.text[:1000], response.headers.get("Retry-After"))
        decoded = response.json()
        choices = decoded.get("choices") or []
        if not choices:
            return ""
        first = choices[0] or {}
        message = first.get("message") or {}
        return str(message.get("content") or first.get("text") or "")


class AIHTTPError(RuntimeError):
    def __init__(self, status_code: int, body: str, retry_after: str | None = None) -> None:
        self.status_code = status_code
        self.body = body
        self.retry_after = retry_after
        super().__init__(f"HTTP {status_code}: {body}")

    def retryable(self) -> bool:
        return self.status_code in {408, 425, 429, 500, 502, 503, 504}

    def retry_delay(self, attempt: int) -> float:
        if self.retry_after:
            try:
                return min(AI_RETRY_MAX_SECONDS, max(0.5, float(self.retry_after)))
            except ValueError:
                pass
        match = TRY_AGAIN_IN_RE.search(self.body)
        if match:
            try:
                return min(AI_RETRY_MAX_SECONDS, max(0.5, float(match.group(1))))
            except ValueError:
                pass
        jitter = random.uniform(0, AI_RETRY_BASE_SECONDS)
        return min(AI_RETRY_MAX_SECONDS, AI_RETRY_BASE_SECONDS * (2 ** max(0, attempt - 1)) + jitter)


def normalize_openrouter_model(model: str) -> str:
    model = first_non_empty(model, "openai/gpt-oss-20b").removesuffix(":free")
    if model.startswith("google/gemini-2.0-flash-exp"):
        return "openai/gpt-oss-20b"
    return model


def normalize_groq_model(model: str) -> str:
    model = first_non_empty(model, "openai/gpt-oss-20b").removesuffix(":free")
    if model.startswith(("google/", "anthropic/", "x-ai/")):
        return "openai/gpt-oss-20b"
    return model


def normalize_cerebras_model(model: str) -> str:
    model = first_non_empty(model, "gpt-oss-120b").removesuffix(":free")
    if model.startswith(("google/", "anthropic/", "x-ai/")):
        return "gpt-oss-120b"
    if model in {"", "openai/gpt-oss-20b", "openai/gpt-oss-120b", "gpt-oss-20b"}:
        return "gpt-oss-120b"
    return model.replace("openai/", "")


def normalize_gh_model(model: str) -> str:
    model = first_non_empty(model, "openai/gpt-4.1-mini").removesuffix(":free")
    if model.startswith("google/"):
        return "openai/gpt-4.1-mini"
    return model


def parse_link_header(link_header: str | None, rel: str) -> str | None:
    if not link_header:
        return None
    for url, found_rel in LINK_RE.findall(link_header):
        if found_rel == rel:
            return url
    return None


def request_json(url: str, params: dict[str, Any] | None = None) -> tuple[Any, requests.structures.CaseInsensitiveDict[str]]:
    last_error: Exception | None = None
    for attempt in range(6):
        try:
            response = SESSION.get(url, params=params, timeout=30)
            if response.status_code == 429:
                wait_seconds = 60
                reset_at = response.headers.get("X-RateLimit-Reset")
                if reset_at:
                    try:
                        reset_dt = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                        wait_seconds = max(1, min(300, int((reset_dt - datetime.now(timezone.utc)).total_seconds()) + 1))
                    except ValueError:
                        pass
                print(f"Rate limited. Sleeping {wait_seconds}s.", file=sys.stderr)
                time.sleep(wait_seconds)
                continue
            if response.status_code >= 500 and attempt < 5:
                wait_seconds = 2 ** attempt
                print(f"Server error {response.status_code}. Retry after {wait_seconds}s.", file=sys.stderr)
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            return response.json(), response.headers
        except Exception as exc:  # noqa: BLE001 - CI retry wrapper
            last_error = exc
            if attempt < 5:
                wait_seconds = 2 ** attempt
                print(f"Request failed. Retry after {wait_seconds}s. {exc}", file=sys.stderr)
                time.sleep(wait_seconds)
                continue
    raise RuntimeError(f"Request failed after retries: {last_error}") from last_error


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"statuses": {}}
    with STATE_FILE.open("r", encoding="utf-8") as fp:
        state = json.load(fp)
    if "statuses" not in state or not isinstance(state["statuses"], dict):
        state["statuses"] = {}
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as fp:
        json.dump(state, fp, ensure_ascii=False, indent=2, sort_keys=True)
        fp.write("\n")


def lookup_account() -> dict[str, Any]:
    if ACCOUNT_ID_ENV:
        return {"id": ACCOUNT_ID_ENV, "acct": ACCT, "username": ACCT, "url": f"{INSTANCE}/@{ACCT}"}

    account, _headers = request_json(f"{INSTANCE}/api/v1/accounts/lookup", {"acct": ACCT})
    if not isinstance(account, dict) or not account.get("id"):
        raise RuntimeError("Mastodon account lookup did not return an account id")
    return account


def status_request_params() -> dict[str, Any]:
    params: dict[str, Any] = {"limit": 40}
    if EXCLUDE_REBLOGS:
        params["exclude_reblogs"] = "true"
    if EXCLUDE_REPLIES:
        params["exclude_replies"] = "true"
    return params


def fetch_statuses(account_id: str, known_ids: set[str], full_backfill: bool) -> list[dict[str, Any]]:
    url: str | None = f"{INSTANCE}/api/v1/accounts/{account_id}/statuses"
    params: dict[str, Any] | None = status_request_params()
    fetched: list[dict[str, Any]] = []

    for page_no in range(1, MAX_PAGES + 1):
        if not url:
            break
        page, headers = request_json(url, params)
        if not isinstance(page, list):
            raise TypeError(f"Unexpected statuses response type: {type(page)}")
        if not page:
            break

        fetched.extend(page)
        print(f"Fetched page={page_no}, items={len(page)}, total={len(fetched)}")

        if not full_backfill and any(str(item.get("id")) in known_ids for item in page):
            print("Reached already-known status. Incremental fetch finished.")
            break

        next_url = parse_link_header(headers.get("Link"), "next")
        if next_url:
            url = next_url
            params = None
        else:
            last_id = page[-1].get("id")
            if not last_id:
                break
            url = f"{INSTANCE}/api/v1/accounts/{account_id}/statuses"
            params = status_request_params()
            params["max_id"] = last_id

        time.sleep(REQUEST_SLEEP_SECONDS)

    return fetched


def fetch_image_as_base64(media: dict[str, Any], budget: ImageBudget) -> dict[str, Any]:
    result = dict(media)
    result.setdefault("image_base64", None)
    result.setdefault("image_data_uri", None)
    result.setdefault("image_content_type", None)
    result.setdefault("image_bytes", None)
    result.setdefault("image_sha256", None)
    result.setdefault("image_fetch_status", "not_attempted")

    if not CRAWL_IMAGES_BASE64:
        result["image_fetch_status"] = "disabled"
        return result

    if budget.embedded_count >= MAX_STATUS_IMAGES:
        result["image_fetch_status"] = "max_status_images_exceeded"
        return result

    if str(media.get("type") or "").lower() != "image":
        result["image_fetch_status"] = "not_image"
        return result

    media_url = media.get("url") or media.get("preview_url")
    if not media_url:
        result["image_fetch_status"] = "missing_url"
        return result

    try:
        with SESSION.get(str(media_url), stream=True, timeout=45, headers={"User-Agent": USER_AGENT}) as response:
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > MAX_IMAGE_BYTES:
                result["image_fetch_status"] = "image_too_large_by_header"
                result["image_bytes"] = int(content_length)
                return result

            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                size += len(chunk)
                if size > MAX_IMAGE_BYTES:
                    result["image_fetch_status"] = "image_too_large_by_stream"
                    result["image_bytes"] = size
                    return result
                if budget.total_used + size > MAX_TOTAL_IMAGE_BYTES:
                    result["image_fetch_status"] = "total_image_budget_exceeded"
                    result["image_bytes"] = size
                    return result
                chunks.append(chunk)

            data = b"".join(chunks)
            encoded = base64.b64encode(data).decode("ascii")
            sha256 = hashlib.sha256(data).hexdigest()
            result["image_base64"] = encoded
            result["image_data_uri"] = f"data:{content_type};base64,{encoded}"
            result["image_content_type"] = content_type
            result["image_bytes"] = len(data)
            result["image_sha256"] = sha256
            result["image_fetch_status"] = "embedded_base64"
            budget.total_used += len(data)
            budget.embedded_count += 1
            return result

    except Exception as exc:  # noqa: BLE001 - store error in raw payload
        result["image_fetch_status"] = f"fetch_error: {type(exc).__name__}"
        return result


def normalize_media_attachments(media_items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    budget = ImageBudget()
    normalized: list[dict[str, Any]] = []
    image_count = 0

    for item in media_items or []:
        base = {
            "id": item.get("id"),
            "type": item.get("type"),
            "url": item.get("url"),
            "preview_url": item.get("preview_url"),
            "remote_url": item.get("remote_url"),
            "description": item.get("description"),
            "blurhash": item.get("blurhash"),
            "meta": item.get("meta"),
        }
        if str(base.get("type") or "").lower() == "image":
            image_count += 1
        normalized.append(fetch_image_as_base64(base, budget))

    return normalized, image_count, budget.embedded_count


def normalize_status(status: dict[str, Any], account: dict[str, Any]) -> dict[str, Any]:
    media_attachments, image_count, image_base64_count = normalize_media_attachments(status.get("media_attachments") or [])
    reblog = status.get("reblog")
    raw_status_json = dict(status)
    raw_status_json["media_attachments"] = media_attachments

    account_obj = status.get("account") or account or {}
    status_id = str(status.get("id") or "")
    language_code = source_language_code(status.get("language"))

    payload = {
        "uuid": status_stable_uuid(status_id),
        "instance_host": instance_host(),
        "account_acct": ACCT,
        "account_id": str(account.get("id") or account_obj.get("id") or ""),
        "status_id": status_id,
        "status_uri": status.get("uri") or "",
        "status_url": status.get("url") or "",
        "status_created_at": status.get("created_at") or "",
        "status_edited_at": status.get("edited_at") or "",
        "visibility": status.get("visibility") or "unknown",
        "language": status.get("language") or "",
        "language_code": language_code,
        "sensitive": 1 if status.get("sensitive") else 0,
        "spoiler_text": status.get("spoiler_text") or "",
        "content_html": status.get("content") or "",
        "content_text": html_to_text(status.get("content")),
        "in_reply_to_id": str(status.get("in_reply_to_id") or ""),
        "in_reply_to_account_id": str(status.get("in_reply_to_account_id") or ""),
        "is_reblog": 1 if isinstance(reblog, dict) else 0,
        "reblog_status_id": str(reblog.get("id") if isinstance(reblog, dict) else ""),
        "replies_count": int(status.get("replies_count") or 0),
        "reblogs_count": int(status.get("reblogs_count") or 0),
        "favourites_count": int(status.get("favourites_count") or 0),
        "active": 1,
        "tags": status.get("tags") or [],
        "mentions": status.get("mentions") or [],
        "emojis": status.get("emojis") or [],
        "media_attachments": media_attachments,
        "card": status.get("card") or {},
        "poll": status.get("poll") or {},
        "raw_status_json": raw_status_json,
        "image_count": image_count,
        "image_base64_count": image_base64_count,
        "has_image_base64": 1 if image_base64_count > 0 else 0,
        "fetched_at": now_kst_string(),
    }
    payload["payload_hash"] = stable_u64({k: v for k, v in payload.items() if k not in {"uuid", "fetched_at", "payload_hash"}})
    return payload


def tombstone_payload(status_id: str, old_state: dict[str, Any], account: dict[str, Any]) -> dict[str, Any]:
    status_created_at = old_state.get("status_created_at") or now_kst_string()
    language_code = source_language_code(old_state.get("language_code") or old_state.get("language"))
    payload = {
        "uuid": old_state.get("uuid") or status_stable_uuid(status_id),
        "instance_host": instance_host(),
        "account_acct": ACCT,
        "account_id": str(account.get("id") or ""),
        "status_id": str(status_id),
        "status_uri": old_state.get("status_uri") or "",
        "status_url": old_state.get("status_url") or "",
        "status_created_at": status_created_at,
        "status_edited_at": "",
        "visibility": old_state.get("visibility") or "unknown",
        "language": old_state.get("language") or "",
        "language_code": language_code,
        "sensitive": 0,
        "spoiler_text": "",
        "content_html": "",
        "content_text": "",
        "in_reply_to_id": "",
        "in_reply_to_account_id": "",
        "is_reblog": 0,
        "reblog_status_id": "",
        "replies_count": 0,
        "reblogs_count": 0,
        "favourites_count": 0,
        "active": 0,
        "tags": [],
        "mentions": [],
        "emojis": [],
        "media_attachments": [],
        "card": {},
        "poll": {},
        "raw_status_json": {},
        "image_count": 0,
        "image_base64_count": 0,
        "has_image_base64": 0,
        "fetched_at": now_kst_string(),
    }
    payload["payload_hash"] = stable_u64({k: v for k, v in payload.items() if k not in {"uuid", "fetched_at", "payload_hash"}})
    return payload


def title_prompt(title: str) -> str:
    return f"""You are a professional Korean translator for R, statistics, and open-source community news titles.

Output rules:
- Return exactly one line: the Korean title only.
- Do not include explanations, labels, quotes, Markdown, HTML, URLs, or hyperlinks.
- Preserve meaning and keep the title concise.
- Preserve R, package names, version numbers, numeric values, acronyms, and proper nouns when appropriate.

Source title:
{title}"""


def content_prompt(title: str, content: str) -> str:
    return f"""You are an editorial Korean translator for R, statistics, data analysis, and open-source community posts.

Translate and lightly edit the source for a Korean Web-R community board post.

Output rules:
- Return only a compact HTML fragment. The first character must be "<".
- Never use <html>, <head>, or <body>.
- Allowed tags only: <h2>, <h3>, <p>, <ul>, <ol>, <li>, <strong>, <em>, <code>, <pre>, <blockquote>.
- Do not output hyperlinks, URLs, Markdown links, HTML <a> tags, href attributes, citations, source links, or "read more" links.
- If the source contains a link, keep only the human-readable text when it is useful, and omit the URL.
- Use polite formal Korean ending in ~합니다 or ~입니다.
- Preserve technical terms, code, package names, function names, numbers, and version strings unless a natural Korean rendering is obvious.
- Do not add an introduction, explanation, label, or meta-commentary.

Source title:
{title}

Source body:
{content}"""


def source_title(payload: dict[str, Any]) -> str:
    text = remove_urls(str(payload.get("content_text") or ""))
    spoiler = remove_urls(str(payload.get("spoiler_text") or ""))
    if spoiler:
        return truncate_chars(spoiler, 120)
    for sep in (". ", "\n", "! ", "? "):
        if sep in text:
            candidate = text.split(sep, 1)[0].strip()
            if len(candidate) >= 12:
                return truncate_chars(candidate, 120)
    return truncate_chars(first_non_empty(text, "R Foundation Mastodon"), 120)


def source_content(payload: dict[str, Any]) -> str:
    parts = []
    spoiler = remove_urls(str(payload.get("spoiler_text") or ""))
    text = remove_urls(str(payload.get("content_text") or ""))
    if spoiler:
        parts.append(spoiler)
    if text:
        parts.append(text)
    value = "\n\n".join(parts)
    return truncate_chars(value, MAX_TRANSLATION_SOURCE_CHARS)


def translate_payload(ai: AIClient, payload: dict[str, Any]) -> tuple[str, str]:
    src_title = source_title(payload)
    src_content = source_content(payload)
    translated_title = src_title
    translated_content = src_content

    if src_title and not looks_korean(src_title, 0.2):
        translated_title = ai.chat(title_prompt(src_title), TRANSLATION_MODEL)
    translated_title = clean_title_output(translated_title)
    if not translated_title:
        translated_title = clean_title_output(src_title)

    if src_content and not looks_korean(src_content, 0.25):
        translated_content = ai.chat(content_prompt(src_title, src_content), TRANSLATION_MODEL)
    translated_content = sanitize_html_fragment(translated_content)
    if not translated_content:
        translated_content = sanitize_html_fragment(f"<p>{html_lib.escape(remove_urls(first_non_empty(src_content, translated_title)))}</p>")

    return translated_title, safe_board_content(translated_title, translated_content)


def board_payload(
    raw_payload: dict[str, Any],
    title: str,
    content: str,
    *,
    updated_at: str | None = None,
    reason: str = "mastodon_board_translation",
) -> dict[str, Any]:
    active = int(raw_payload.get("active") or 0)
    created_at = first_non_empty(str(raw_payload.get("status_created_at") or ""), now_kst_string())
    payload: dict[str, Any] = {
        "uuid": raw_payload["uuid"],
        "status_url": raw_payload.get("status_url"),
        "title": clean_title_output(title) if active else "",
        "content": safe_board_content(title, content) if active else "",
        "active": active,
        "created_at": created_at,
        "updated_at": updated_at,
        "created_log": {
            "type": reason,
            "source": "Statground_Data_R-project_Mastodon",
            "raw_status_id": raw_payload.get("status_id"),
            "raw_status_url": raw_payload.get("status_url"),
            "raw_created_at": raw_payload.get("status_created_at"),
            "prompt_language": source_language_code(raw_payload.get("language_code")),
            "target_language": "ko",
            "hyperlinks": "removed",
            "content_fallback": "title_when_blank",
        },
        "updated_log": None,
        "language_code": "ko",
    }
    if updated_at:
        payload["updated_log"] = {
            "type": "mastodon_board_translation_update",
            "source": "Statground_Data_R-project_Mastodon",
            "updated_at": updated_at,
        }
    return payload


def carried_board_state(previous: dict[str, Any]) -> dict[str, Any]:
    return {key: previous[key] for key in BOARD_STATE_KEYS if key in previous}


def board_translation_due(previous: dict[str, Any], raw_payload: dict[str, Any]) -> bool:
    if not TRANSLATE_ENABLED:
        return False
    if FORCE_REPUBLISH:
        return True
    if str(previous.get("payload_hash") or "") != str(raw_payload.get("payload_hash") or ""):
        return True
    if not RETRY_MISSING_BOARD_TRANSLATIONS:
        return False
    return str(previous.get("board_payload_hash") or "") != str(raw_payload.get("payload_hash") or "")


def board_translation_attempt_count(previous: dict[str, Any]) -> int:
    try:
        return int(previous.get("board_translation_attempts") or 0) + 1
    except (TypeError, ValueError):
        return 1


def board_translation_success_state(raw_payload: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "board_payload_hash": raw_payload.get("payload_hash"),
        "board_translation_status": "published",
        "board_translated_at": now,
        "board_translation_attempted_at": now,
        "board_translation_attempts": board_translation_attempt_count(previous),
        "board_translation_error": "",
    }


def board_translation_failure_state(raw_payload: dict[str, Any], previous: dict[str, Any], exc: Exception) -> dict[str, Any]:
    now = utc_now_iso()
    state = carried_board_state(previous)
    state.update(
        {
            "board_translation_status": "failed",
            "board_translation_attempted_at": now,
            "board_translation_attempts": board_translation_attempt_count(previous),
            "board_translation_error": truncate_chars(str(exc), 1200),
        }
    )
    return state


def board_tombstone_state(raw_payload: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "board_payload_hash": raw_payload.get("payload_hash"),
        "board_translation_status": "tombstone_published",
        "board_translated_at": now,
        "board_translation_attempted_at": now,
        "board_translation_attempts": board_translation_attempt_count(previous),
        "board_translation_error": "",
    }


def log_payload(stage: str, created_log: dict[str, Any]) -> dict[str, Any]:
    now = now_kst_string()
    return {
        "uuid": uuid7(),
        "created_at": now,
        "created_log": {"type": "mastodon_pipeline", "stage": stage, **created_log},
        "language_code": "en",
    }


def make_producer() -> Producer:
    validate_kafka_brokers()

    conf: dict[str, Any] = {
        "bootstrap.servers": KAFKA_BROKERS,
        "client.id": "statground-r-foundation-mastodon-crawler",
        "compression.type": "zstd",
        "message.max.bytes": KAFKA_PRODUCER_MESSAGE_MAX_BYTES,
        "socket.timeout.ms": 30000,
        "retries": 5,
        "acks": "all",
    }

    if KAFKA_USERNAME and KAFKA_PASSWORD:
        conf["security.protocol"] = KAFKA_SECURITY_PROTOCOL or "SASL_PLAINTEXT"
        conf["sasl.mechanism"] = KAFKA_SASL_MECHANISM
        conf["sasl.username"] = KAFKA_USERNAME
        conf["sasl.password"] = KAFKA_PASSWORD
    elif KAFKA_SECURITY_PROTOCOL:
        conf["security.protocol"] = KAFKA_SECURITY_PROTOCOL

    return Producer(conf)


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        raise RuntimeError(f"Kafka delivery failed: {err}")
    print(f"Delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def make_event(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_uuid": uuid7(),
        "source": "actions",
        "host": os.getenv("RUNNER_NAME") or socket.gethostname() or "actions",
        "uuid_user": "",
        "ip": "",
        "url": payload.get("status_url") or f"{INSTANCE}/@{ACCT}",
        "event_type": event_type,
        "payload": canonical_json(payload),
        "created_at": now_kst_string(),
    }


def produce_events(producer: Producer, events: list[tuple[str, dict[str, Any]]]) -> None:
    delivery_errors: list[Any] = []

    def on_delivery(err: Any, msg: Any) -> None:
        if err is not None:
            delivery_errors.append(err)
            return
        delivery_report(err, msg)

    for event_type, payload in events:
        event = make_event(event_type, payload)
        value = canonical_json(event).encode("utf-8")
        if len(value) > MAX_KAFKA_EVENT_BYTES:
            raise RuntimeError(
                f"Kafka event type={event_type} for uuid={payload.get('uuid')} is {len(value)} bytes, "
                f"exceeds MAX_KAFKA_EVENT_BYTES={MAX_KAFKA_EVENT_BYTES}. "
                "Increase Kafka topic max.message.bytes or lower image limits."
            )
        key = str(payload.get("status_id") or payload.get("uuid") or event["event_uuid"]).encode("utf-8")
        producer.produce(KAFKA_TOPIC, key=key, value=value, callback=on_delivery)
        producer.poll(0)

    remaining = producer.flush(60)
    if remaining > 0:
        raise RuntimeError(f"Kafka producer flush timed out with {remaining} undelivered message(s)")
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")


def main() -> None:
    if SYNC_MODE not in {"incremental", "backfill"}:
        raise ValueError("SYNC_MODE must be incremental or backfill")

    ai = AIClient()
    if TRANSLATE_ENABLED and not ai.enabled():
        raise RuntimeError("TRANSLATE_ENABLED is true, but no AI provider key is configured")

    state = load_state()
    known_statuses: dict[str, Any] = state.get("statuses", {})
    known_ids = set(known_statuses.keys())

    account = lookup_account()
    account_id = str(account["id"])
    full_backfill = SYNC_MODE == "backfill" or not known_ids

    statuses = fetch_statuses(account_id=account_id, known_ids=known_ids, full_backfill=full_backfill)
    normalized_payloads = [normalize_status(status, account) for status in statuses]

    events_to_publish: list[tuple[str, dict[str, Any]]] = []
    run_started_at = now_kst_string()
    events_to_publish.append(
        (
            LOG_EVENT_TYPE,
            log_payload(
                "run_started",
                {
                    "sync_mode": SYNC_MODE,
                    "force_republish": FORCE_REPUBLISH,
                    "translate_enabled": TRANSLATE_ENABLED,
                    "translation_model": TRANSLATION_MODEL,
                    "retry_missing_board_translations": RETRY_MISSING_BOARD_TRANSLATIONS,
                    "ai_provider_max_attempts": AI_PROVIDER_MAX_ATTEMPTS,
                    "crawl_images_base64": CRAWL_IMAGES_BASE64,
                    "started_at": run_started_at,
                },
            ),
        )
    )
    seen_ids: set[str] = set()
    published_raw = 0
    published_board = 0
    attempted_board = 0
    translation_errors: list[str] = []

    for payload in normalized_payloads:
        status_id = str(payload["status_id"])
        seen_ids.add(status_id)
        previous = known_statuses.get(status_id) or {}
        previous_hash = previous.get("payload_hash")
        raw_changed = FORCE_REPUBLISH or previous_hash != payload["payload_hash"]
        board_state = carried_board_state(previous)
        if raw_changed:
            events_to_publish.append((RAW_EVENT_TYPE, payload))
            published_raw += 1

        if board_translation_due(previous, payload):
            attempted_board += 1
            try:
                title, content = translate_payload(ai, payload)
                events_to_publish.append((BOARD_EVENT_TYPE, board_payload(payload, title, content)))
                published_board += 1
                board_state = board_translation_success_state(payload, previous)
            except Exception as exc:  # noqa: BLE001 - log and optionally fail after run summary
                board_state = board_translation_failure_state(payload, previous, exc)
                translation_errors.append(f"{status_id}: {exc}")
                if FAIL_ON_TRANSLATION_ERROR:
                    raise

        status_state = {
            "uuid": payload.get("uuid"),
            "payload_hash": payload["payload_hash"],
            "status_created_at": payload.get("status_created_at"),
            "status_uri": payload.get("status_uri"),
            "status_url": payload.get("status_url"),
            "visibility": payload.get("visibility"),
            "language": payload.get("language"),
            "language_code": payload.get("language_code"),
            "last_seen_at": utc_now_iso(),
            "active": 1,
        }
        status_state.update(board_state)
        known_statuses[status_id] = status_state

    if full_backfill:
        missing_ids = known_ids - seen_ids
        for status_id in sorted(missing_ids):
            old = known_statuses.get(status_id) or {}
            if int(old.get("active", 1)) == 0:
                continue
            payload = tombstone_payload(status_id, old, account)
            events_to_publish.append((RAW_EVENT_TYPE, payload))
            events_to_publish.append((BOARD_EVENT_TYPE, board_payload(payload, "", "", reason="mastodon_board_tombstone")))
            published_raw += 1
            published_board += 1
            attempted_board += 1
            known_statuses[status_id] = {
                **old,
                "payload_hash": payload["payload_hash"],
                "last_seen_at": utc_now_iso(),
                "active": 0,
                **board_tombstone_state(payload, old),
            }

    done_at = now_kst_string()
    events_to_publish.append(
        (
            LOG_EVENT_TYPE,
            log_payload(
                "run_done",
                {
                    "sync_mode": SYNC_MODE,
                    "fetched_count": len(statuses),
                    "published_raw": published_raw,
                    "attempted_board": attempted_board,
                    "published_board": published_board,
                    "translation_errors": translation_errors[:50],
                    "prompt_language": "en",
                    "target_language": "ko",
                    "hyperlink_policy": "remove_urls_markdown_links_a_tags_and_href",
                    "retry_missing_board_translations": RETRY_MISSING_BOARD_TRANSLATIONS,
                    "done_at": done_at,
                },
            ),
        )
    )

    if events_to_publish:
        producer = make_producer()
        produce_events(producer, events_to_publish)
    else:
        print("No new or changed statuses to publish.")

    latest_payload = max(normalized_payloads, key=lambda item: item.get("status_created_at") or "", default=None)
    state.update(
        {
            "source": {
                "type": "mastodon_account_statuses_to_kafka",
                "instance": INSTANCE,
                "acct": ACCT,
                "account_id": account_id,
                "account_url": account.get("url") or f"{INSTANCE}/@{ACCT}",
                "exclude_reblogs": EXCLUDE_REBLOGS,
                "exclude_replies": EXCLUDE_REPLIES,
                "crawl_images_base64": CRAWL_IMAGES_BASE64,
            },
            "last_sync_completed_at": utc_now_iso(),
            "last_sync_mode": SYNC_MODE,
            "fetched_count": len(statuses),
            "published_raw_count": published_raw,
            "attempted_board_count": attempted_board,
            "published_board_count": published_board,
            "published_event_count": len(events_to_publish),
            "known_count": len(known_statuses),
            "latest_status_id": latest_payload.get("status_id") if latest_payload else state.get("latest_status_id"),
            "latest_status_created_at": latest_payload.get("status_created_at") if latest_payload else state.get("latest_status_created_at"),
            "statuses": known_statuses,
        }
    )
    save_state(state)
    print(f"State saved to {STATE_FILE}. Published {len(events_to_publish)} Kafka events.")


if __name__ == "__main__":
    main()
