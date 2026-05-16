"""Small ClickHouse HTTP helpers for GitHub Actions collectors.

The secret inventory is not perfectly uniform across repos: some jobs provide
CH_HOST as a bare host, while others provide a full HTTP URL. Keep URL assembly
centralized so workflows do not accidentally build malformed host:port strings.
"""

from __future__ import annotations

import os
import urllib.parse
from collections.abc import Mapping


def build_clickhouse_url(
    env: Mapping[str, str] | None = None,
    *,
    default_database: str = "Clickhouse_Statground",
    default_format: str = "JSONEachRow",
    max_execution_time: str = "30",
) -> str:
    values = env if env is not None else os.environ
    base = _first(values, "CH_URL", "CLICKHOUSE_URL", "CLICKHOUSE_HTTP_URL")
    host = _first(values, "CH_HOST", "CLICKHOUSE_HOST")
    port = _clean_port(_first(values, "CH_PORT", "CLICKHOUSE_PORT")) or "8123"
    secure = _first(values, "CH_SECURE", "CLICKHOUSE_SECURE").lower() in {"1", "true", "yes"}
    protocol = _first(values, "CH_PROTOCOL", "CLICKHOUSE_PROTOCOL") or ("https" if secure else "http")
    path = _first(values, "CH_HTTP_URL_PATH", "CLICKHOUSE_HTTP_URL_PATH")

    if base:
        root = _normalize_url_base(base, protocol, path)
    elif "://" in host:
        root = _normalize_url_base(host, protocol, path)
    else:
        root = _normalize_host_base(host, port, protocol, path)

    database = _first(values, "CH_DATABASE", "CLICKHOUSE_DEFAULT_DATABASE") or default_database
    query = urllib.parse.urlencode(
        {
            "database": database,
            "default_format": default_format,
            "max_execution_time": max_execution_time,
        }
    )
    separator = "&" if urllib.parse.urlsplit(root).query else "?"
    return f"{root}{separator}{query}"


def _first(values: Mapping[str, str], *keys: str) -> str:
    for key in keys:
        value = str(values.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _clean_port(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    if "://" in value:
        try:
            parsed = urllib.parse.urlsplit(value)
            return str(parsed.port or "")
        except ValueError:
            return ""
    if value.startswith(":"):
        value = value[1:]
    if value.isdigit() and 0 < int(value) <= 65535:
        return value
    return ""


def _normalize_path(path: str) -> str:
    path = str(path or "").strip()
    if not path:
        return "/"
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/")


def _normalize_url_base(raw: str, fallback_protocol: str, override_path: str) -> str:
    parsed = urllib.parse.urlsplit(str(raw or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return _normalize_host_base(raw, "8123", fallback_protocol, override_path)
    scheme = parsed.scheme or fallback_protocol
    path = _normalize_path(override_path) if str(override_path or "").strip() else _normalize_path(parsed.path)
    return urllib.parse.urlunsplit((scheme, parsed.netloc, path, "", ""))


def _normalize_host_base(host: str, port: str, protocol: str, path: str) -> str:
    host = str(host or "").strip().rstrip("/")
    if not host:
        raise RuntimeError("ClickHouse host secret is required")
    if "/" in host:
        host, host_path = host.split("/", 1)
        path = path or host_path
    netloc = host
    if not _has_port(netloc):
        netloc = f"{netloc}:{port}"
    return urllib.parse.urlunsplit((protocol, netloc, _normalize_path(path), "", ""))


def _has_port(netloc: str) -> bool:
    if netloc.startswith("["):
        return "]:" in netloc and _clean_port(netloc.rsplit(":", 1)[-1]) != ""
    if netloc.count(":") == 1:
        return _clean_port(netloc.rsplit(":", 1)[-1]) != ""
    return False
