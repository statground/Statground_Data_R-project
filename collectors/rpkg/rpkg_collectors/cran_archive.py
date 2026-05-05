from __future__ import annotations

import os
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer


CRAN_ARCHIVE_INDEX_URL = os.getenv("RPKG_CRAN_ARCHIVE_INDEX_URL", "https://cran.r-project.org/src/contrib/Archive/")


def collect(producer: KafkaEventProducer, *, limit: int = 0) -> int:
    response = requests.get(CRAN_ARCHIVE_INDEX_URL, timeout=90, headers={"User-Agent": "StatgroundBot/1.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    observed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    count = 0
    for link in soup.find_all("a"):
        href = str(link.get("href") or "")
        if not href.endswith("/") or href.startswith("?") or href == "../":
            continue
        package_name = href.strip("/")
        if not package_name:
            continue
        archive_url = f"{CRAN_ARCHIVE_INDEX_URL.rstrip('/')}/{package_name}/"
        payload = {
            "package": package_name,
            "archive_url": archive_url,
            "is_archived": True,
            "source": "CRAN Archive index",
        }
        producer.produce(
            RPackageEvent.create(
                event_type="rpkg.cran.archive_snapshot.v1",
                source="cran_archive_index_html",
                source_url=CRAN_ARCHIVE_INDEX_URL,
                repository="CRAN",
                package_name=package_name,
                observed_at=observed_at,
                payload=payload,
            )
        )
        count += 1
        if limit and count >= limit:
            break
    producer.flush()
    return count
