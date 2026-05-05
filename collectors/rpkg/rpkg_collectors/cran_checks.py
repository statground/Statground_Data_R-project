from __future__ import annotations

import os
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer


CRAN_CHECK_SUMMARY_URL = os.getenv(
    "RPKG_CRAN_CHECK_SUMMARY_URL",
    "https://cran.r-project.org/web/checks/check_summary.html",
)
STATUS_ORDER = ("ERROR", "WARNING", "NOTE", "OK")


def worst_status(cells: list[str]) -> str:
    upper_cells = " ".join(cells).upper()
    for status in STATUS_ORDER:
        if status in upper_cells:
            return status
    return "UNKNOWN"


def collect(producer: KafkaEventProducer, *, limit: int = 0) -> int:
    response = requests.get(CRAN_CHECK_SUMMARY_URL, timeout=90, headers={"User-Agent": "StatgroundBot/1.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    collected_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    count = 0
    for row in soup.select("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        if cells[0].lower() in {"package", "name"}:
            continue
        package_name = cells[0]
        if not package_name or package_name.lower().startswith("summary"):
            continue
        payload = {
            "package": package_name,
            "version": "",
            "flavor": "summary",
            "status": worst_status(cells[1:]),
            "raw_cells": cells,
            "checked_at": collected_at,
            "source": "CRAN check summary",
        }
        producer.produce(
            RPackageEvent.create(
                event_type="rpkg.cran.check_snapshot.v1",
                source="cran_check_summary_html",
                source_url=CRAN_CHECK_SUMMARY_URL,
                repository="CRAN",
                package_name=package_name,
                observed_at=collected_at,
                payload=payload,
            )
        )
        count += 1
        if limit and count >= limit:
            break
    producer.flush()
    return count
