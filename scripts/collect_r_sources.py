#!/usr/bin/env python3
"""
Statground R-language source collector.

No external API keys are required. The collector uses public RSS/Atom feeds,
public HTML pages, and public no-auth endpoints where providers expose them.

Output: JSON Lines, one normalized item per line.
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import hashlib
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

KST = dt.timezone(dt.timedelta(hours=9), name="Asia/Seoul")
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "spm",
    "ref_src",
    "ref",
    "source",
}
DEFAULT_USER_AGENT = "StatgroundRCollector/1.0 (+https://www.statground.net)"


@dataclass
class NormalizedItem:
    external_id: str
    source_id: str
    source_name: str
    source_type: str
    platform: str
    source_url: str
    canonical_url: str
    title: str
    summary: str | None = None
    author: str | None = None
    published_at: str | None = None
    collected_at: str = field(default_factory=lambda: dt.datetime.now(KST).isoformat(timespec="seconds"))
    language: str | None = None
    tags: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


class CollectorError(RuntimeError):
    pass


class RSourceCollector:
    def __init__(self, config: dict[str, Any], since_days: int | None, request_delay: float) -> None:
        self.config = config
        self.since_days = since_days
        self.request_delay = request_delay
        self.items: list[NormalizedItem] = []
        self.errors: list[dict[str, Any]] = []
        self.counts: dict[str, int] = defaultdict(int)
        self.seen: set[str] = set()
        self.seen_canonical: set[str] = set()
        self.started_at = dt.datetime.now(KST).isoformat(timespec="seconds")
        self.session = self._make_session()
        self.context = {
            "year": dt.datetime.now(KST).year,
            "prev_year": dt.datetime.now(KST).year - 1,
        }

    def _make_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            status=2,
            backoff_factor=1.2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {
                "User-Agent": os.environ.get("STATGROUND_USER_AGENT", DEFAULT_USER_AGENT),
                "Accept": "application/rss+xml, application/atom+xml, application/json, text/html;q=0.9, */*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        return session

    def run(self) -> list[NormalizedItem]:
        sources = self.config.get("sources", [])
        if not isinstance(sources, list):
            raise CollectorError("config.sources must be a list")

        for source in sources:
            if not source.get("enabled", True):
                logging.info("skip disabled source id=%s", source.get("id"))
                continue
            source_id = source.get("id") or source.get("name") or source.get("url")
            source_type = source.get("type")
            try:
                logging.info("collect source id=%s type=%s", source_id, source_type)
                if source_type == "rss":
                    self.collect_rss(source)
                elif source_type == "reddit_subreddit":
                    self.collect_reddit_subreddit(source)
                elif source_type == "mastodon_tag":
                    self.collect_mastodon_tag(source)
                elif source_type == "mastodon_account":
                    self.collect_mastodon_account(source)
                elif source_type == "dcinside_gallery":
                    self.collect_dcinside_gallery(source)
                elif source_type == "html_links":
                    self.collect_html_links(source)
                elif source_type == "html_release_notes":
                    self.collect_html_release_notes(source)
                else:
                    raise CollectorError(f"unknown source type: {source_type}")
            except Exception as exc:  # noqa: BLE001 - every source must be isolated
                logging.exception("source failed id=%s", source_id)
                self.errors.append(
                    {
                        "source_id": source_id,
                        "source_type": source_type,
                        "error_type": exc.__class__.__name__,
                        "message": str(exc),
                    }
                )
        return self.items

    def fetch(self, url: str, *, accept: str | None = None) -> requests.Response:
        if accept:
            headers = {"Accept": accept}
        else:
            headers = None
        time.sleep(self.request_delay)
        response = self.session.get(url, headers=headers, timeout=25)
        response.raise_for_status()
        return response

    def add_item(self, item: NormalizedItem) -> None:
        if not item.canonical_url and not item.external_id:
            return
        if item.external_id in self.seen:
            return
        canonical_key = canonical_dedupe_key(item.canonical_url)
        if canonical_key and canonical_key in self.seen_canonical:
            return
        if self.since_days is not None and item.published_at:
            parsed = parse_datetime(item.published_at)
            if parsed:
                cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=self.since_days)
                if parsed.astimezone(dt.timezone.utc) < cutoff:
                    return
        item.title = compact_text(item.title)[:500]
        item.summary = compact_text(item.summary or "")[:4000] or None
        item.tags = sorted({compact_text(tag).lstrip("#") for tag in item.tags if compact_text(tag)})
        self.seen.add(item.external_id)
        if canonical_key:
            self.seen_canonical.add(canonical_key)
        self.items.append(item)
        self.counts[item.source_id] += 1

    def make_item(
        self,
        *,
        source: dict[str, Any],
        source_id: str,
        source_url: str,
        canonical_url: str,
        title: str,
        native_id: str | None = None,
        summary: str | None = None,
        author: str | None = None,
        published_at: str | None = None,
        language: str | None = None,
        tags: Iterable[str] = (),
        raw: dict[str, Any] | None = None,
    ) -> NormalizedItem:
        canonical = canonicalize_url(canonical_url or source_url)
        external_id = make_external_id(source_id, native_id or canonical or title)
        source_tags = source.get("tags", []) or []
        return NormalizedItem(
            external_id=external_id,
            source_id=source_id,
            source_name=source.get("name") or source_id,
            source_type=source.get("source_type") or source.get("type") or "unknown",
            platform=source.get("platform") or infer_platform(source_url),
            source_url=source_url,
            canonical_url=canonical,
            title=title or canonical or source_id,
            summary=summary,
            author=author,
            published_at=published_at,
            language=language or source.get("language"),
            tags=list(tags) + list(source_tags),
            raw=raw or {},
        )

    def collect_rss(self, source: dict[str, Any]) -> None:
        url = format_url(source["url"], self.context)
        source_id = source.get("id") or f"rss:{url}"
        feed_items = self._parse_feed_url(url, source=source, source_id=source_id, source_url=source.get("source_url") or url)
        for item in feed_items:
            self.add_item(item)

    def _parse_feed_url(self, url: str, *, source: dict[str, Any], source_id: str, source_url: str) -> list[NormalizedItem]:
        response = self.fetch(url)
        parsed = feedparser.parse(response.content)
        if getattr(parsed, "bozo", False):
            logging.warning("feed parse warning source_id=%s url=%s error=%r", source_id, url, getattr(parsed, "bozo_exception", None))
        feed = parsed.feed
        feed_title = getattr(feed, "title", None)
        result: list[NormalizedItem] = []
        max_items = int(source.get("max_items", 200))
        for entry in parsed.entries[:max_items]:
            link = entry.get("link") or entry.get("id") or url
            title = html_to_text(entry.get("title") or link)
            summary = entry_summary(entry)
            haystack = f"{title}\n{summary or ''}\n{link}"
            if not source_item_allowed(source, haystack):
                continue
            native_id = entry.get("id") or entry.get("guid") or link
            published_at = entry_datetime(entry)
            tags = [tag.get("term") for tag in entry.get("tags", []) if tag.get("term")]
            result.append(
                self.make_item(
                    source=source,
                    source_id=source_id,
                    source_url=source_url,
                    canonical_url=link,
                    native_id=native_id,
                    title=title,
                    summary=summary,
                    author=entry.get("author"),
                    published_at=published_at,
                    language=entry.get("language") or source.get("language"),
                    tags=tags,
                    raw={
                        "collector": "rss",
                        "feed_url": url,
                        "feed_title": feed_title,
                        "feed_link": feed.get("link"),
                        "feed_subtitle": html_to_text(feed.get("subtitle")),
                        "feed_language": feed.get("language"),
                        "feed_updated": feed.get("updated"),
                        "feed_updated_parsed": struct_time_to_iso(feed.get("updated_parsed")) if feed.get("updated_parsed") else None,
                        "entry_id": native_id,
                        "link": link,
                        "published": entry.get("published"),
                        "updated": entry.get("updated"),
                        "author": entry.get("author"),
                        "author_detail": json_safe(entry.get("author_detail") or {}),
                        "tags": tags,
                        "title_detail": json_safe(entry.get("title_detail") or {}),
                        "summary_detail": json_safe(entry.get("summary_detail") or {}),
                        "links": json_safe(entry.get("links") or []),
                        "enclosures": json_safe(entry.get("enclosures") or []),
                        "media_content": json_safe(entry.get("media_content") or []),
                        "media_thumbnail": json_safe(entry.get("media_thumbnail") or []),
                        "content": json_safe(entry.get("content") or []),
                    },
                )
            )
        return result

    def collect_reddit_subreddit(self, source: dict[str, Any]) -> None:
        subreddit = source["subreddit"].strip().lstrip("r/")
        limit = int(source.get("limit", source.get("max_items", 100)))
        source_id = source.get("id") or f"reddit:r/{subreddit}"
        source_url = f"https://www.reddit.com/r/{subreddit}/"
        rss_urls = [
            f"https://www.reddit.com/r/{subreddit}/new/.rss?limit={limit}",
            f"https://old.reddit.com/r/{subreddit}/new/.rss?limit={limit}",
        ]
        collected = 0
        last_error: Exception | None = None
        if source.get("json_fallback", True):
            try:
                collected = self._collect_reddit_json(source, source_id, source_url, subreddit, limit)
                if collected:
                    return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logging.warning("reddit json failed subreddit=%s err=%s", subreddit, exc)

        for rss_url in rss_urls:
            try:
                feed_items = self._parse_feed_url(rss_url, source=source, source_id=source_id, source_url=source_url)
                for item in feed_items:
                    # Reddit Atom entries may include HTML tables; keep plain text only.
                    item.platform = "reddit"
                    item.tags.append(f"r/{subreddit}")
                    self.add_item(item)
                    collected += 1
                if collected:
                    return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logging.warning("reddit rss failed subreddit=%s url=%s err=%s", subreddit, rss_url, exc)

        if last_error:
            raise last_error

    def _collect_reddit_json(self, source: dict[str, Any], source_id: str, source_url: str, subreddit: str, limit: int) -> int:
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"
        response = self.fetch(url, accept="application/json")
        data = response.json()
        collected = 0
        for child in data.get("data", {}).get("children", []):
            post = child.get("data", {})
            permalink = post.get("permalink")
            canonical_url = urljoin("https://www.reddit.com", permalink) if permalink else post.get("url") or source_url
            haystack = "\n".join(
                str(value or "")
                for value in (
                    post.get("title"),
                    post.get("selftext"),
                    post.get("url"),
                    post.get("domain"),
                    post.get("link_flair_text"),
                    canonical_url,
                )
            )
            if not source_item_allowed(source, haystack):
                continue
            published = None
            if post.get("created_utc") is not None:
                published = dt.datetime.fromtimestamp(float(post["created_utc"]), tz=dt.timezone.utc).astimezone(KST).isoformat(timespec="seconds")
            item = self.make_item(
                source=source,
                source_id=source_id,
                source_url=source_url,
                canonical_url=canonical_url,
                native_id=post.get("name") or post.get("id") or canonical_url,
                title=post.get("title") or canonical_url,
                summary=post.get("selftext") or post.get("url"),
                author=post.get("author"),
                published_at=published,
                language=source.get("language"),
                tags=[f"r/{subreddit}", "reddit"],
                raw={
                    "collector": "reddit_json_noauth",
                    "subreddit": subreddit,
                    "reddit_id": post.get("id"),
                    "name": post.get("name"),
                    "permalink": permalink,
                    "url": post.get("url"),
                    "domain": post.get("domain"),
                    "created_utc": post.get("created_utc"),
                    "score": post.get("score"),
                    "ups": post.get("ups"),
                    "downs": post.get("downs"),
                    "upvote_ratio": post.get("upvote_ratio"),
                    "num_comments": post.get("num_comments"),
                    "over_18": post.get("over_18"),
                    "is_self": post.get("is_self"),
                    "stickied": post.get("stickied"),
                    "locked": post.get("locked"),
                    "link_flair_text": post.get("link_flair_text"),
                    "link_flair_css_class": post.get("link_flair_css_class"),
                    "author_flair_text": post.get("author_flair_text"),
                    "post_hint": post.get("post_hint"),
                    "thumbnail": post.get("thumbnail"),
                    "num_crossposts": post.get("num_crossposts"),
                    "selftext_html": post.get("selftext_html"),
                    "preview": post.get("preview") or {},
                    "media": post.get("media") or {},
                    "secure_media": post.get("secure_media") or {},
                    "gallery_data": post.get("gallery_data") or {},
                    "media_metadata": post.get("media_metadata") or {},
                    "raw_post": post,
                },
            )
            item.platform = "reddit"
            self.add_item(item)
            collected += 1
        return collected

    def collect_mastodon_tag(self, source: dict[str, Any]) -> None:
        tag = source["tag"].strip().lstrip("#")
        instances = source.get("instances") or [source.get("instance")]
        if not instances or not instances[0]:
            raise CollectorError("mastodon_tag requires instance or instances")
        limit = int(source.get("limit", source.get("max_items", 40)))
        for instance in instances:
            instance = str(instance).strip().removeprefix("https://").removeprefix("http://").strip("/")
            per_source = dict(source)
            per_source["name"] = f"{source.get('name', 'Mastodon tag')} #{tag} @ {instance}"
            source_id = f"{source.get('id', 'mastodon:tag')}:{tag}@{instance}"
            source_url = f"https://{instance}/tags/{tag}"
            rss_url = f"https://{instance}/tags/{tag}.rss"
            collected_before = len(self.items)
            if source.get("public_api_fallback", True):
                try:
                    self._collect_mastodon_tag_api(per_source, source_id, source_url, instance, tag, limit)
                except Exception as exc:  # noqa: BLE001
                    # Some instances disable public preview and legitimately return 401.
                    logging.info("mastodon no-auth tag api unavailable instance=%s tag=%s err=%s", instance, tag, exc)
            try:
                for item in self._parse_feed_url(rss_url, source=per_source, source_id=source_id, source_url=source_url):
                    item.platform = "mastodon"
                    item.tags.extend([tag, "mastodon"])
                    self.add_item(item)
            except Exception as exc:  # noqa: BLE001
                logging.info("mastodon tag rss unavailable instance=%s tag=%s err=%s", instance, tag, exc)

            if len(self.items) == collected_before:
                logging.info("mastodon tag yielded no rows instance=%s tag=%s", instance, tag)

    def _collect_mastodon_tag_api(
        self,
        source: dict[str, Any],
        source_id: str,
        source_url: str,
        instance: str,
        tag: str,
        limit: int,
    ) -> None:
        api_url = f"https://{instance}/api/v1/timelines/tag/{tag}?limit={limit}"
        response = self.fetch(api_url, accept="application/json")
        data = response.json()
        if not isinstance(data, list):
            raise CollectorError(f"unexpected Mastodon response: {type(data)!r}")
        for status in data:
            self.add_item(self._mastodon_status_item(source, source_id, source_url, instance, status, extra_tags=[tag], collector="mastodon_tag_api_noauth"))

    def collect_mastodon_account(self, source: dict[str, Any]) -> None:
        url = source.get("url")
        if not url:
            instance = source["instance"].strip().removeprefix("https://").removeprefix("http://").strip("/")
            username = source["username"].strip().lstrip("@")
            url = f"https://{instance}/@{username}.rss"
        source_id = source.get("id") or f"mastodon:account:{urlparse(url).netloc}:{Path(urlparse(url).path).stem}"
        source_url = source.get("source_url") or url.removesuffix(".rss")
        if source.get("public_api_fallback", True):
            try:
                instance, username = mastodon_instance_username(source, source_url or url)
                if instance and username:
                    self._collect_mastodon_account_api(source, source_id, source_url, instance, username, int(source.get("limit", source.get("max_items", 40))))
            except Exception as exc:  # noqa: BLE001
                logging.info("mastodon account no-auth api unavailable source=%s err=%s", source_id, exc)
        for item in self._parse_feed_url(url, source=source, source_id=source_id, source_url=source_url):
            item.platform = "mastodon"
            item.tags.append("mastodon")
            self.add_item(item)

    def _collect_mastodon_account_api(
        self,
        source: dict[str, Any],
        source_id: str,
        source_url: str,
        instance: str,
        username: str,
        limit: int,
    ) -> None:
        lookup_url = f"https://{instance}/api/v1/accounts/lookup?acct={username}"
        lookup_response = self.fetch(lookup_url, accept="application/json")
        account = lookup_response.json()
        account_id = str(account.get("id") or "")
        if not account_id:
            raise CollectorError(f"mastodon account lookup returned no id: {lookup_url}")
        statuses_url = f"https://{instance}/api/v1/accounts/{account_id}/statuses?limit={limit}&exclude_replies=false&exclude_reblogs=false"
        statuses_response = self.fetch(statuses_url, accept="application/json")
        statuses = statuses_response.json()
        if not isinstance(statuses, list):
            raise CollectorError(f"unexpected Mastodon account statuses response: {type(statuses)!r}")
        for status in statuses:
            self.add_item(self._mastodon_status_item(source, source_id, source_url, instance, status, extra_tags=["mastodon"], collector="mastodon_account_api_noauth"))

    def _mastodon_status_item(
        self,
        source: dict[str, Any],
        source_id: str,
        source_url: str,
        instance: str,
        status: dict[str, Any],
        *,
        extra_tags: list[str],
        collector: str,
    ) -> NormalizedItem:
        original_status = status
        status = status.get("reblog") or status
        account = status.get("account") or {}
        content_html = status.get("content") or ""
        content_text = html_to_text(content_html)
        title = content_text[:140] or f"Mastodon status {status.get('id')}"
        status_tags = [tag.get("name") for tag in status.get("tags", []) if tag.get("name")]
        status_url = status.get("url") or status.get("uri") or source_url
        item = self.make_item(
            source=source,
            source_id=source_id,
            source_url=source_url,
            canonical_url=status_url,
            native_id=status_url,
            title=title,
            summary=content_text,
            author=account.get("acct") or account.get("username"),
            published_at=parse_datetime_to_iso(status.get("created_at")),
            language=status.get("language") or source.get("language"),
            tags=[*extra_tags, "mastodon", *status_tags],
            raw={
                "collector": collector,
                "instance": instance,
                "instance_host": instance,
                "account_acct": account.get("acct") or account.get("username"),
                "account_id": str(account.get("id")) if account.get("id") is not None else None,
                "account": account,
                "status_id": str(status.get("id")) if status.get("id") is not None else None,
                "status_uri": status.get("uri"),
                "status_url": status_url,
                "url": status.get("url"),
                "visibility": status.get("visibility"),
                "language": status.get("language"),
                "sensitive": status.get("sensitive"),
                "spoiler_text": status.get("spoiler_text"),
                "content_html": content_html,
                "content_text": content_text,
                "favourites_count": status.get("favourites_count"),
                "reblogs_count": status.get("reblogs_count"),
                "replies_count": status.get("replies_count"),
                "tags": status.get("tags") or [],
                "mentions": status.get("mentions") or [],
                "media_attachments": status.get("media_attachments") or [],
                "card": status.get("card") or {},
                "poll": status.get("poll") or {},
                "status": status,
                "original_status": original_status,
            },
        )
        item.platform = "mastodon"
        if source_id == "mastodon:account:user-conf":
            enrich_user_conf_workshop_item(item)
        return item

    def collect_dcinside_gallery(self, source: dict[str, Any]) -> None:
        gallery_id = source["gallery_id"]
        pages = int(source.get("pages", 1))
        source_id = source.get("id") or f"dcinside:{gallery_id}"
        source_url = source.get("source_url") or f"https://m.dcinside.com/board/{gallery_id}"
        max_items = int(source.get("max_items", 200))
        fetch_detail = env_bool("DCINSIDE_FETCH_DETAIL", bool(source.get("fetch_detail", True)))
        detail_limit = int(source.get("detail_limit", os.environ.get("DCINSIDE_DETAIL_LIMIT", max_items)))
        emitted = 0
        for page in range(1, pages + 1):
            if emitted >= max_items:
                break
            url = f"https://m.dcinside.com/board/{gallery_id}?page={page}"
            response = self.fetch(url, accept="text/html")
            soup = BeautifulSoup(response.text, "html.parser")
            for anchor in soup.find_all("a", href=True):
                if emitted >= max_items:
                    break
                href = anchor.get("href") or ""
                match = re.search(rf"/board/{re.escape(gallery_id)}/(\d+)", href)
                if not match:
                    continue
                post_id = match.group(1)
                title = compact_text(anchor.get_text(" ", strip=True))
                if not title or len(title) < 2:
                    continue
                # Skip UI navigation labels that sometimes appear as anchors.
                if title in {"전체글", "개념글", "글쓰기", "본문 보기", "댓글닫기", "새로고침"}:
                    continue
                canonical_url = urljoin("https://m.dcinside.com", f"/board/{gallery_id}/{post_id}")
                detail: dict[str, Any] = {"detail_status": "not_requested"}
                if fetch_detail and emitted < detail_limit:
                    detail = self.fetch_dcinside_post_detail(canonical_url)
                detail_title = compact_text(detail.get("detail_title") or "")
                content_text = compact_text(detail.get("content_text") or "")
                item = self.make_item(
                    source=source,
                    source_id=source_id,
                    source_url=source_url,
                    canonical_url=canonical_url,
                    native_id=f"{gallery_id}:{post_id}",
                    title=detail_title or title,
                    summary=content_text or None,
                    author=detail.get("author"),
                    published_at=detail.get("published_at"),
                    language=source.get("language", "ko"),
                    tags=["dcinside", gallery_id],
                    raw={
                        "collector": "dcinside_mobile_html",
                        "gallery_id": gallery_id,
                        "post_id": post_id,
                        "page": page,
                        "list_title": title,
                        "detail_url": canonical_url,
                        **detail,
                    },
                )
                item.platform = "dcinside"
                self.add_item(item)
                emitted += 1

    def fetch_dcinside_post_detail(self, canonical_url: str) -> dict[str, Any]:
        collected_at = dt.datetime.now(KST).isoformat(timespec="seconds")
        try:
            response = self.fetch(canonical_url, accept="text/html")
        except Exception as exc:  # noqa: BLE001
            return {
                "detail_status": "failed",
                "detail_collected_at": collected_at,
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
            }
        soup = BeautifulSoup(response.text, "html.parser")
        page = html_page_snapshot(soup, canonical_url)
        text = compact_text(soup.get_text(" ", strip=True))
        detail_title = first_text_by_selectors(
            soup,
            [
                ".title_subject",
                ".gallview_head .title",
                ".view_subject",
                "h1",
                "h2",
                "title",
            ],
        )
        content_text = first_text_by_selectors(
            soup,
            [
                "#writeContents",
                ".write_div",
                ".gallview_contents",
                ".view_content_wrap",
                ".thum-txt",
                "article",
            ],
        )
        author = first_text_by_selectors(
            soup,
            [
                "[data-nick]",
                ".nickname",
                ".nick",
                ".gall_writer",
                ".writer",
                ".user_info",
            ],
        )
        published_at = parse_dcinside_datetime(text)
        return {
            "detail_status": "collected",
            "detail_collected_at": collected_at,
            "detail_title": detail_title or page.get("html_title"),
            "author": author,
            "published_at": published_at,
            "content_text": truncate_text(content_text, 8000),
            "comment_count": parse_count_near_label(text, "댓글"),
            "view_count": parse_count_near_label(text, "조회"),
            "recommend_count": parse_count_near_label(text, "추천"),
            "page_title": page.get("html_title"),
            "page_meta_description": page.get("meta_description"),
            "page_og_title": page.get("og_title"),
            "page_og_image": page.get("og_image"),
            "page_text_excerpt": page.get("text_excerpt"),
        }

    def collect_html_links(self, source: dict[str, Any]) -> None:
        url = format_url(source["url"], self.context)
        source_id = source.get("id") or f"html:{url}"
        source_url = source.get("source_url") or url
        root_html = self.fetch(url, accept="text/html").text
        root_soup = BeautifulSoup(root_html, "html.parser")
        page_title = html_page_title(root_soup) or source.get("name") or source_id
        page_meta = html_page_snapshot(root_soup, url)

        if source.get("emit_page_item", False):
            self.add_item(
                self.make_item(
                    source=source,
                    source_id=source_id,
                    source_url=source_url,
                    canonical_url=url,
                    native_id=url,
                    title=page_title,
                    summary=html_to_text(root_soup.get_text(" ", strip=True))[:1000],
                    tags=source.get("tags", []),
                    raw={"collector": "html_page", **page_meta},
                )
            )

        root_links = extract_links(
            root_soup,
            base_url=url,
            include_regex=source.get("include_regex"),
            exclude_regex=source.get("exclude_regex"),
        )
        self._emit_html_links(source, source_id, source_url, url, root_links, page_title, page_meta, source.get("max_items", 200))

        follow = source.get("follow") or {}
        if follow.get("enabled"):
            candidates = extract_links(
                root_soup,
                base_url=url,
                include_regex=follow.get("include_regex"),
                exclude_regex=follow.get("exclude_regex"),
            )
            followed = 0
            for link in unique_by_url(candidates):
                if followed >= int(follow.get("first_n", 3)):
                    break
                try:
                    follow_html = self.fetch(link["url"], accept="text/html").text
                    follow_soup = BeautifulSoup(follow_html, "html.parser")
                    follow_title = html_page_title(follow_soup) or link["title"]
                    follow_meta = html_page_snapshot(follow_soup, link["url"])
                    links = extract_links(
                        follow_soup,
                        base_url=link["url"],
                        include_regex=follow.get("extract_include_regex"),
                        exclude_regex=follow.get("extract_exclude_regex"),
                    )
                    self._emit_html_links(
                        source,
                        source_id,
                        source_url,
                        link["url"],
                        links,
                        follow_title,
                        follow_meta,
                        follow.get("max_items_per_page", 200),
                    )
                    followed += 1
                except Exception as exc:  # noqa: BLE001
                    logging.info("follow html failed source=%s url=%s err=%s", source_id, link["url"], exc)

    def _emit_html_links(
        self,
        source: dict[str, Any],
        source_id: str,
        source_url: str,
        page_url: str,
        links: list[dict[str, Any]],
        page_title: str,
        page_meta: dict[str, Any],
        max_items: int | str,
    ) -> None:
        emitted = 0
        fetch_target_meta = bool(source.get("fetch_target_meta", False))
        target_meta_limit = int(source.get("target_meta_limit", os.environ.get("HTML_TARGET_META_LIMIT", 20)))
        for link in unique_by_url(links):
            if emitted >= int(max_items):
                break
            target_meta: dict[str, Any] = {}
            if fetch_target_meta and emitted < target_meta_limit and same_scheme_http(link["url"]):
                try:
                    target_response = self.fetch(link["url"], accept="text/html")
                    if "html" in target_response.headers.get("Content-Type", "").lower():
                        target_soup = BeautifulSoup(target_response.text, "html.parser")
                        target_snapshot = html_page_snapshot(target_soup, link["url"])
                        target_meta = {
                            "target_title": target_snapshot.get("html_title"),
                            "target_meta_description": target_snapshot.get("meta_description"),
                            "target_og_image": target_snapshot.get("og_image"),
                            "target_content_type": target_response.headers.get("Content-Type", ""),
                        }
                except Exception as exc:  # noqa: BLE001
                    target_meta = {"target_fetch_error": str(exc)}
            raw = {
                "collector": "html_links",
                "page_url": page_url,
                "page_title": page_title,
                "page_host": page_meta.get("page_host"),
                "page_h1_title": page_meta.get("h1_title"),
                "page_meta_description": page_meta.get("meta_description"),
                "page_og_title": page_meta.get("og_title"),
                "page_og_description": page_meta.get("og_description"),
                "page_og_image": page_meta.get("og_image"),
                "page_canonical_url": page_meta.get("canonical_url"),
                "page_feed_urls": page_meta.get("feed_urls") or [],
                "link_text": link.get("link_text") or link.get("title"),
                "href_raw": link.get("href_raw"),
                "link_rel": link.get("rel"),
                "link_type": link.get("type"),
                "link_title_attr": link.get("title_attr"),
                "link_aria_label": link.get("aria_label"),
                "link_context": link.get("context_text"),
                "link_position": link.get("link_index"),
                **target_meta,
            }
            item = self.make_item(
                source=source,
                source_id=source_id,
                source_url=source_url,
                canonical_url=link["url"],
                native_id=link["url"],
                title=link["title"] or link["url"],
                summary=f"Discovered from {page_title}: {page_url}",
                author=None,
                published_at=parse_fuzzy_date_to_iso(" ".join([link.get("title", ""), link.get("context_text", "")])),
                language=source.get("language"),
                tags=source.get("tags", []),
                raw=raw,
            )
            self.add_item(item)
            emitted += 1

    def collect_html_release_notes(self, source: dict[str, Any]) -> None:
        url = format_url(source["url"], self.context)
        source_id = source.get("id") or f"release_notes:{url}"
        source_url = source.get("source_url") or url
        heading_regex = re.compile(source.get("heading_regex", r"Changes in R"), re.IGNORECASE)
        max_items = int(source.get("max_items", 20))
        response = self.fetch(url, accept="text/html")
        soup = BeautifulSoup(response.text, "html.parser")
        page_meta = html_page_snapshot(soup, url)
        emitted = 0
        for heading in soup.find_all(re.compile(r"^h[1-6]$")):
            if emitted >= max_items:
                break
            title = compact_text(heading.get_text(" ", strip=True))
            if not heading_regex.search(title):
                continue
            summary_parts: list[str] = []
            html_parts: list[str] = []
            heading_level = int(heading.name[1]) if heading.name and heading.name[1].isdigit() else 6
            for sibling in heading.find_next_siblings():
                if sibling.name and re.match(r"^h[1-6]$", sibling.name):
                    sibling_level = int(sibling.name[1])
                    if sibling_level <= heading_level:
                        break
                text = compact_text(sibling.get_text(" ", strip=True)) if hasattr(sibling, "get_text") else compact_text(str(sibling))
                if text:
                    summary_parts.append(text)
                html_parts.append(str(sibling)[:3000])
                if sum(len(part) for part in summary_parts) > int(source.get("summary_limit", 2500)):
                    break
            slug = slugify(title)
            section_text = "\n".join(summary_parts)[: int(source.get("summary_limit", 2500))]
            section_html = "\n".join(html_parts)[: int(source.get("html_limit", 12000))]
            item = self.make_item(
                source=source,
                source_id=source_id,
                source_url=source_url,
                canonical_url=f"{url}#{slug}",
                native_id=title,
                title=title,
                summary=section_text or None,
                author=source.get("author"),
                published_at=parse_fuzzy_date_to_iso(title),
                language=source.get("language"),
                tags=source.get("tags", []),
                raw={
                    "collector": "html_release_notes",
                    "page_url": url,
                    "heading": title,
                    "heading_level": heading_level,
                    "heading_id": heading.get("id") or slug,
                    "section_index": emitted,
                    "version_text": release_version_text(title),
                    "section_text": section_text,
                    "section_html": section_html,
                    "section_word_count": len(section_text.split()),
                    "page_title": page_meta.get("html_title"),
                    "page_meta_description": page_meta.get("meta_description"),
                    "page_og_title": page_meta.get("og_title"),
                    "page_canonical_url": page_meta.get("canonical_url"),
                },
            )
            self.add_item(item)
            emitted += 1

    def write_outputs(self, out_dir: Path) -> tuple[Path, Path]:
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = dt.datetime.now(KST).strftime("%Y%m%d_%H%M%S")
        jsonl_path = out_dir / f"r_sources_{stamp}.jsonl"
        latest_path = out_dir / "latest.jsonl"
        rows = [asdict(item) for item in sorted(self.items, key=lambda x: (x.published_at or "", x.source_id, x.canonical_url), reverse=True)]
        for path in (jsonl_path, latest_path):
            with path.open("w", encoding="utf-8") as fp:
                for row in rows:
                    fp.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        report = {
            "started_at": self.started_at,
            "finished_at": dt.datetime.now(KST).isoformat(timespec="seconds"),
            "since_days": self.since_days,
            "item_count": len(rows),
            "source_counts": dict(sorted(self.counts.items())),
            "error_count": len(self.errors),
            "errors": self.errors,
        }
        report_path = out_dir / f"r_sources_report_{stamp}.json"
        latest_report_path = out_dir / "latest_report.json"
        for path in (report_path, latest_report_path):
            path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return jsonl_path, report_path


# ----------------------------- utility functions -----------------------------


def format_url(url: str, context: dict[str, Any]) -> str:
    try:
        return url.format(**context)
    except KeyError:
        return url


def enrich_user_conf_workshop_item(item: NormalizedItem) -> None:
    text = " ".join(
        str(value or "")
        for value in (
            item.title,
            item.summary,
            item.canonical_url,
            item.raw.get("content_text"),
            item.raw.get("status_url"),
        )
    )
    workshop_key = classify_r_conference_workshop_key(text) or "rconf-user-2026"
    title = conference_title_from_workshop_key(workshop_key)
    item.tags = unique_preserve_order([*item.tags, "workshop_board", workshop_key, title.replace(" ", "")])
    item.raw.update(
        {
            "related_workshop_key": workshop_key,
            "related_conference_title": title,
            "related_conference_source_id": "official:r:conferences",
            "related_conference_canonical_url": conference_url_from_workshop_key(workshop_key),
            "workshop_board_role": "post",
            "workshop_board_author_email": "rproject@web-r.org",
            "workshop_board_author_kind": "bot",
        }
    )


def classify_r_conference_workshop_key(text: str) -> str | None:
    lowered = text.lower()
    if "r/basel" in lowered or "r-basel" in lowered:
        return "rconf-r-basel-2023"
    match = re.search(r"r\s+summit\s*([12][0-9]{3})", lowered)
    if match:
        return f"rconf-r-summit-{match.group(1)}"
    match = re.search(r"user!?\s*([12][0-9]{3})", lowered)
    if match:
        return f"rconf-user-{match.group(1)}"
    match = re.search(r"user([12][0-9]{3})", lowered)
    if match:
        return f"rconf-user-{match.group(1)}"
    match = re.search(r"dsc[-/\s]*([12][0-9]{3})", lowered)
    if match:
        return f"rconf-dsc-{match.group(1)}"
    return None


def conference_title_from_workshop_key(workshop_key: str) -> str:
    match = re.match(r"rconf-user-([12][0-9]{3})$", workshop_key)
    if match:
        return f"useR! {match.group(1)}"
    match = re.match(r"rconf-dsc-([12][0-9]{3})$", workshop_key)
    if match:
        return f"DSC {match.group(1)}"
    if workshop_key == "rconf-r-basel-2023":
        return "R/Basel 2023"
    match = re.match(r"rconf-r-summit-([12][0-9]{3})$", workshop_key)
    if match:
        return f"R Summit {match.group(1)}"
    return workshop_key


def conference_url_from_workshop_key(workshop_key: str) -> str:
    known = {
        "rconf-user-2026": "https://user2026.r-project.org",
        "rconf-r-basel-2023": "https://user-regional-2023.gitlab.io/basel/",
        "rconf-r-summit-2015": "https://www.r-project.org/conferences/rsummit-2015/rsummit2015/",
    }
    if workshop_key in known:
        return known[workshop_key]
    match = re.match(r"rconf-user-([12][0-9]{3})$", workshop_key)
    if match:
        return f"https://user{match.group(1)}.r-project.org/"
    match = re.match(r"rconf-dsc-([12][0-9]{3})$", workshop_key)
    if match:
        return f"https://www.r-project.org/dsc/{match.group(1)}"
    return "https://www.r-project.org/conferences/"


def unique_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        value = str(value or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def compact_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def truncate_text(value: str | None, limit: int) -> str:
    text = compact_text(value)
    if limit <= 0:
        return text
    return text[:limit]


def html_to_text(value: str | None) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(str(value), "html.parser")
    return compact_text(soup.get_text(" ", strip=True))


def html_page_title(soup: BeautifulSoup) -> str | None:
    if soup.title and soup.title.string:
        return compact_text(soup.title.string)
    h1 = soup.find("h1")
    if h1:
        return compact_text(h1.get_text(" ", strip=True))
    return None


def html_page_snapshot(soup: BeautifulSoup, page_url: str) -> dict[str, Any]:
    h1 = soup.find("h1")
    canonical = rel_href(soup, "canonical", page_url)
    og_image = meta_content(soup, property_name="og:image")
    return {
        "page_url": page_url,
        "page_host": urlparse(page_url).netloc.lower(),
        "html_title": html_page_title(soup) or "",
        "h1_title": compact_text(h1.get_text(" ", strip=True)) if h1 else "",
        "meta_description": meta_content(soup, name="description"),
        "meta_keywords": meta_content(soup, name="keywords"),
        "canonical_url": canonicalize_url(canonical or page_url),
        "og_title": meta_content(soup, property_name="og:title"),
        "og_description": meta_content(soup, property_name="og:description"),
        "og_image": canonicalize_url(urljoin(page_url, og_image)) if og_image else "",
        "twitter_title": meta_content(soup, name="twitter:title"),
        "twitter_description": meta_content(soup, name="twitter:description"),
        "feed_urls": extract_feed_urls(soup, page_url),
        "link_count": len(soup.find_all("a", href=True)),
        "text_excerpt": truncate_text(soup.get_text(" ", strip=True), 4000),
    }


def meta_content(soup: BeautifulSoup, *, name: str | None = None, property_name: str | None = None) -> str:
    attrs: dict[str, str] = {}
    if name:
        attrs["name"] = name
    if property_name:
        attrs["property"] = property_name
    tag = soup.find("meta", attrs=attrs)
    if tag and tag.get("content"):
        return compact_text(str(tag.get("content")))
    return ""


def rel_href(soup: BeautifulSoup, rel: str, base_url: str) -> str:
    rel_lower = rel.lower()
    for tag in soup.find_all("link", href=True):
        rels = [str(value).lower() for value in (tag.get("rel") or [])]
        if rel_lower in rels:
            return urljoin(base_url, str(tag.get("href")))
    return ""


def extract_feed_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    feed_urls: list[str] = []
    for tag in soup.find_all("link", href=True):
        rels = " ".join(str(value).lower() for value in (tag.get("rel") or []))
        type_value = str(tag.get("type") or "").lower()
        if "alternate" not in rels and "feed" not in rels and "rss" not in type_value and "atom" not in type_value:
            continue
        href = str(tag.get("href") or "")
        if href:
            feed_urls.append(canonicalize_url(urljoin(base_url, href)))
    return sorted({url for url in feed_urls if url})


def canonicalize_url(url: str | None) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower in TRACKING_QUERY_KEYS or any(key_lower.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        query.append((key, value))
    normalized = parsed._replace(query=urlencode(query, doseq=True), fragment=parsed.fragment)
    return urlunparse(normalized)


def make_external_id(source_id: str, native_id: str) -> str:
    digest = hashlib.sha256(f"{source_id}\n{native_id}".encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def canonical_dedupe_key(raw_url: str | None) -> str:
    canonical = canonicalize_url(raw_url)
    if not canonical:
        return ""
    parsed = urlparse(canonical)
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
    )
    path = normalized.path
    if path != "/" and path.endswith("/"):
        normalized = normalized._replace(path=path.rstrip("/"))
    return urlunparse(normalized)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, time.struct_time):
        return struct_time_to_iso(value)
    return value


def infer_platform(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "reddit.com" in host:
        return "reddit"
    if "dcinside.com" in host:
        return "dcinside"
    if any(host.endswith(domain) for domain in ("mastodon.social", "fosstodon.org", "hachyderm.io", "vis.social", "mathstodon.xyz")):
        return "mastodon"
    if "r-project.org" in host or "cran.r-project.org" in host:
        return "r-project"
    return host or "web"


def mastodon_instance_username(source: dict[str, Any], fallback_url: str) -> tuple[str | None, str | None]:
    instance = str(source.get("instance") or "").strip().removeprefix("https://").removeprefix("http://").strip("/")
    username = str(source.get("username") or "").strip().lstrip("@")
    if instance and username:
        return instance, username
    parsed = urlparse(fallback_url)
    host = parsed.netloc
    path = parsed.path.strip("/")
    if path.endswith(".rss"):
        path = path[:-4]
    if path.startswith("@"):
        path = path[1:]
    if "@" in path:
        path = path.split("@", 1)[0]
    return host or None, path or None


def entry_summary(entry: Any) -> str | None:
    if entry.get("summary"):
        return html_to_text(entry.get("summary"))
    content = entry.get("content") or []
    if content and isinstance(content, list):
        return html_to_text(content[0].get("value"))
    return None


def entry_datetime(entry: Any) -> str | None:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = entry.get(key)
        if parsed:
            return struct_time_to_iso(parsed)
    for key in ("published", "updated", "created"):
        value = entry.get(key)
        parsed = parse_datetime_to_iso(value)
        if parsed:
            return parsed
    return None


def struct_time_to_iso(value: time.struct_time) -> str:
    seconds = calendar.timegm(value)
    return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).astimezone(KST).isoformat(timespec="seconds")


def first_text_by_selectors(soup: BeautifulSoup, selectors: list[str]) -> str:
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        if node.has_attr("data-nick"):
            value = compact_text(str(node.get("data-nick")))
            if value:
                return value
        text = compact_text(node.get_text(" ", strip=True))
        if text:
            return text
    return ""


def parse_dcinside_datetime(text: str) -> str | None:
    patterns = [
        r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)",
        r"(\d{2}[./-]\d{1,2}[./-]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        raw = match.group(1).replace(".", "-").replace("/", "-")
        if re.match(r"^\d{2}-", raw):
            raw = "20" + raw
        parsed = parse_datetime_to_iso(raw)
        if parsed:
            return parsed
    return None


def parse_count_near_label(text: str, label: str) -> int:
    patterns = [
        rf"{re.escape(label)}\s*[:：]?\s*([0-9,]+)",
        rf"([0-9,]+)\s*{re.escape(label)}",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1).replace(",", ""))
    return 0


def same_scheme_http(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def release_version_text(title: str) -> str:
    match = re.search(r"R\s+(?:version\s+)?([0-9]+(?:\.[0-9]+){1,3}(?:\s+\S+)?)", title, re.IGNORECASE)
    if not match:
        return ""
    return compact_text(match.group(0))


def source_item_allowed(source: dict[str, Any], haystack: str) -> bool:
    include_regex = source.get("item_include_regex")
    exclude_regex = source.get("item_exclude_regex")
    if include_regex and not re.search(include_regex, haystack, re.IGNORECASE):
        return False
    if exclude_regex and re.search(exclude_regex, haystack, re.IGNORECASE):
        return False
    return True


def parse_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dateparser.parse(str(value))
    except (ValueError, TypeError, OverflowError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed


def parse_datetime_to_iso(value: str | None) -> str | None:
    parsed = parse_datetime(value)
    if not parsed:
        return None
    return parsed.astimezone(KST).isoformat(timespec="seconds")


def parse_fuzzy_date_to_iso(text: str | None) -> str | None:
    if not text or not re.search(r"(?:19|20)\d{2}", text):
        return None
    try:
        parsed = dateparser.parse(text, fuzzy=True, default=dt.datetime(1, 1, 1, tzinfo=KST))
    except (ValueError, TypeError, OverflowError):
        return None
    if parsed is None or parsed.year < 1900:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST).isoformat(timespec="seconds")


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9가-힣._-]+", "-", text.lower()).strip("-")
    return slug[:120] or hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def extract_links(
    soup: BeautifulSoup,
    *,
    base_url: str,
    include_regex: str | None = None,
    exclude_regex: str | None = None,
) -> list[dict[str, Any]]:
    include = re.compile(include_regex, re.IGNORECASE) if include_regex else None
    exclude = re.compile(exclude_regex, re.IGNORECASE) if exclude_regex else None
    links: list[dict[str, Any]] = []
    for index, anchor in enumerate(soup.find_all("a", href=True)):
        link_text = compact_text(anchor.get_text(" ", strip=True))
        href = anchor.get("href") or ""
        title = link_text or compact_text(str(anchor.get("title") or anchor.get("aria-label") or ""))
        url = canonicalize_url(urljoin(base_url, href))
        if not title or not url or url.startswith("mailto:") or url.startswith("javascript:"):
            continue
        context_text = compact_text(anchor.parent.get_text(" ", strip=True)) if anchor.parent else title
        haystack = f"{title}\n{url}\n{context_text}"
        if include and not include.search(haystack):
            continue
        if exclude and exclude.search(haystack):
            continue
        rel = anchor.get("rel") or []
        class_attr = anchor.get("class") or []
        links.append(
            {
                "title": title,
                "url": url,
                "href_raw": href,
                "link_text": link_text,
                "title_attr": compact_text(str(anchor.get("title") or "")),
                "aria_label": compact_text(str(anchor.get("aria-label") or "")),
                "rel": " ".join(str(value) for value in rel),
                "type": compact_text(str(anchor.get("type") or "")),
                "id": compact_text(str(anchor.get("id") or "")),
                "class": " ".join(str(value) for value in class_attr),
                "context_text": truncate_text(context_text, 600),
                "link_index": index,
            }
        )
    return links


def unique_by_url(links: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for link in links:
        url = link.get("url", "")
        if not url or url in seen:
            continue
        seen.add(url)
        result.append(link)
    return result


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect R language official/community sources without external API keys.")
    parser.add_argument("--config", default="config/r_sources.yaml", help="YAML source configuration path")
    parser.add_argument("--out-dir", default="data/collected/r", help="Output directory")
    parser.add_argument("--since-days", type=int, default=14, help="Keep items newer than N days when published_at exists. Use -1 for no date filtering.")
    parser.add_argument("--request-delay", type=float, default=0.8, help="Delay between HTTP requests in seconds")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    config_path = Path(args.config)
    if not config_path.exists():
        logging.error("config not found: %s", config_path)
        return 2
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    since_days = None if args.since_days is not None and args.since_days < 0 else args.since_days
    collector = RSourceCollector(config, since_days=since_days, request_delay=args.request_delay)
    collector.run()
    jsonl_path, report_path = collector.write_outputs(Path(args.out_dir))
    logging.info("wrote %s items to %s", len(collector.items), jsonl_path)
    logging.info("wrote report to %s", report_path)
    if collector.errors:
        logging.warning("completed with %s source errors; see report", len(collector.errors))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
