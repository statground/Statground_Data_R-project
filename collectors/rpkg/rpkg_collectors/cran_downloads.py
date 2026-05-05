from __future__ import annotations

import os
from typing import Iterable

import requests

from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer


CRANLOGS_BASE_URL = os.getenv("RPKG_CRANLOGS_BASE_URL", "https://cranlogs.r-pkg.org")


def _json(url: str) -> object:
    response = requests.get(url, timeout=45, headers={"User-Agent": "StatgroundBot/1.0"})
    response.raise_for_status()
    return response.json()


def top_packages(limit: int) -> list[str]:
    data = _json(f"{CRANLOGS_BASE_URL}/top/last-week/{limit}")
    rows: Iterable[object]
    if isinstance(data, dict):
        rows = data.get("downloads", [])  # type: ignore[assignment]
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    packages: list[str] = []
    for row in rows:
        if isinstance(row, dict) and row.get("package"):
            packages.append(str(row["package"]))
    return packages[:limit]


def collect_package_downloads(producer: KafkaEventProducer, *, package_name: str, period: str) -> int:
    url = f"{CRANLOGS_BASE_URL}/downloads/daily/{period}/{package_name}"
    data = _json(url)
    rows = data.get("downloads", []) if isinstance(data, dict) else []
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        payload = {
            "package": str(row.get("package") or package_name),
            "download_date": str(row.get("day") or row.get("date") or ""),
            "downloads": int(row.get("downloads") or 0),
            "period": period,
            "source": "cranlogs",
        }
        producer.produce(
            RPackageEvent.create(
                event_type="rpkg.cran.download_daily.v1",
                source="cranlogs",
                source_url=url,
                repository="CRAN",
                package_name=payload["package"],
                payload=payload,
            )
        )
        count += 1
    return count


def collect(producer: KafkaEventProducer, *, packages: list[str], top: int = 0, period: str = "last-month") -> int:
    selected = [package.strip() for package in packages if package.strip()]
    if top:
        selected.extend(package for package in top_packages(top) if package not in selected)
    count = 0
    for package_name in selected:
        count += collect_package_downloads(producer, package_name=package_name, period=period)
    producer.flush()
    return count
