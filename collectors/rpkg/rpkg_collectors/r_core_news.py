from __future__ import annotations

import os

import requests
from bs4 import BeautifulSoup

from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer


R_CORE_NEWS_URL = os.getenv("RPKG_R_CORE_NEWS_URL", "https://cran.r-project.org/doc/manuals/r-release/NEWS.html")


def collect(producer: KafkaEventProducer) -> int:
    response = requests.get(R_CORE_NEWS_URL, timeout=60, headers={"User-Agent": "StatgroundBot/1.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.find(["h1", "title"])
    headings = [heading.get_text(" ", strip=True) for heading in soup.find_all(["h2", "h3"], limit=50)]
    payload = {
        "title": title.get_text(" ", strip=True) if title else "R NEWS",
        "headings": headings,
        "html_sha256_source": R_CORE_NEWS_URL,
        "content_length": len(response.text),
    }
    producer.produce(
        RPackageEvent.create(
            event_type="rpkg.r_core.news_snapshot.v1",
            source="r_core_news_html",
            source_url=R_CORE_NEWS_URL,
            repository="R-Core",
            payload=payload,
        )
    )
    producer.flush()
    return 1
