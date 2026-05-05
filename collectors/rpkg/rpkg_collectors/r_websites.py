from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer
from .youtube.url_parser import parse_youtube_url


DEFAULT_WEBSITES = [
    "https://www.r-project.org/",
    "https://cran.r-project.org/",
    "https://cran.r-project.org/web/views/",
    "https://www.bioconductor.org/",
    "https://r-universe.dev/",
    "https://www.r-bloggers.com/",
    "https://rweekly.org/",
    "https://posit.co/blog/",
    "https://www.tidyverse.org/blog/",
    "https://ropensci.org/blog/",
    "https://www.r-consortium.org/news/blogs",
]


def configured_urls() -> list[str]:
    raw = os.getenv("RPKG_R_WEBSITE_URLS", "").strip()
    if not raw:
        return DEFAULT_WEBSITES
    return [part.strip() for part in raw.split(",") if part.strip()]


def configured_mentions() -> list[str]:
    raw = os.getenv("RPKG_WEBSITE_MENTION_PACKAGES", "").strip()
    return [part.strip() for part in raw.split(",") if part.strip()]


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def collect(producer: KafkaEventProducer, *, limit: int = 0) -> int:
    urls = configured_urls()
    mention_packages = configured_mentions()
    count = 0
    for target_url in urls:
        if limit and count >= limit:
            break
        fetched_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        payload = fetch_website(target_url)
        payload["fetched_at"] = fetched_at
        producer.produce(
            RPackageEvent.create(
                event_type="rpkg.r_website.fetch_snapshot.v1",
                source="r_website_seed_fetcher",
                source_url=target_url,
                repository="R-Web",
                observed_at=fetched_at,
                payload=payload,
            )
        )
        count += 1

        page_text = payload.get("page_text", "")
        if isinstance(page_text, str):
            for package_name in mention_packages:
                if package_name and package_name.lower() in page_text.lower():
                    mention_payload = {
                        "source_url": target_url,
                        "package": package_name,
                        "repository": "CRAN",
                        "mention_context": package_name,
                        "confidence": 0.4,
                        "detected_at": fetched_at,
                        "source": "r_website_seed_fetcher",
                    }
                    producer.produce(
                        RPackageEvent.create(
                            event_type="rpkg.r_website.package_mention_snapshot.v1",
                            source="r_website_seed_fetcher",
                            source_url=target_url,
                            repository="R-Web",
                            package_name=package_name,
                            observed_at=fetched_at,
                            payload=mention_payload,
                        )
                    )
                    count += 1

    producer.flush()
    return count


def fetch_website(target_url: str) -> dict[str, object]:
    try:
        response = requests.get(target_url, timeout=30, headers={"User-Agent": "StatgroundBot/1.0"})
        status_code = response.status_code
        content_type = response.headers.get("Content-Type", "")
        html = response.text if "html" in content_type.lower() else ""
        soup = BeautifulSoup(html, "html.parser") if html else BeautifulSoup("", "html.parser")
        title_node = soup.find(["title", "h1"])
        description_node = soup.find("meta", attrs={"name": "description"})
        canonical_node = soup.find("link", rel=lambda value: value and "canonical" in value)
        feed_links = []
        for link in soup.find_all("link"):
            rel = " ".join(link.get("rel") or []).lower()
            link_type = str(link.get("type") or "").lower()
            href = str(link.get("href") or "")
            if href and ("alternate" in rel or "rss" in link_type or "atom" in link_type or "json" in link_type):
                feed_links.append(urljoin(target_url, href))
        youtube_urls = sorted(find_youtube_urls(target_url, soup.get_text(" ", strip=True), [str(a.get("href") or "") for a in soup.find_all("a")]))
        return {
            "target_url": target_url,
            "url_hash": sha256_hex(target_url),
            "host": urlparse(target_url).hostname or "",
            "status_code": status_code,
            "content_type": content_type,
            "title": title_node.get_text(" ", strip=True) if title_node else "",
            "description": str(description_node.get("content") or "") if description_node else "",
            "canonical_url": urljoin(target_url, str(canonical_node.get("href") or "")) if canonical_node else target_url,
            "feed_urls": feed_links[:20],
            "youtube_urls": youtube_urls[:50],
            "error_code": "",
            "page_text": soup.get_text(" ", strip=True)[:20000] if soup else "",
        }
    except requests.RequestException as exc:
        return {
            "target_url": target_url,
            "url_hash": sha256_hex(target_url),
            "host": urlparse(target_url).hostname or "",
            "status_code": 0,
            "content_type": "",
            "title": "",
            "description": "",
            "canonical_url": target_url,
            "feed_urls": [],
            "youtube_urls": [],
            "error_code": exc.__class__.__name__,
            "page_text": "",
        }


def find_youtube_urls(base_url: str, page_text: str, hrefs: list[str]) -> set[str]:
    candidates: set[str] = set()
    for href in hrefs:
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if is_youtube_url(absolute):
            candidates.add(parse_youtube_url(absolute).canonical_url or absolute)
    for match in re_youtube_url().finditer(page_text):
        raw = match.group(0).rstrip(").,;]")
        if is_youtube_url(raw):
            candidates.add(parse_youtube_url(raw).canonical_url or raw)
    return candidates


def is_youtube_url(value: str) -> bool:
    host = urlparse(value if "://" in value else f"https://{value}").hostname or ""
    return host.endswith("youtube.com") or host.endswith("youtu.be")


def re_youtube_url():
    import re

    return re.compile(r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s\"'<>()]+", re.IGNORECASE)
