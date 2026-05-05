from __future__ import annotations

import gzip
import os
from typing import Iterable

import requests

from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer


CRAN_PACKAGES_URL = os.getenv("RPKG_CRAN_PACKAGES_URL", "https://cran.r-project.org/src/contrib/PACKAGES.gz")


def parse_dcf(text: str) -> Iterable[dict[str, str]]:
    record: dict[str, str] = {}
    current_key = ""
    for line in text.splitlines():
        if not line.strip():
            if record:
                yield record
                record = {}
                current_key = ""
            continue
        if line[0].isspace() and current_key:
            record[current_key] = f"{record[current_key]}\n{line.strip()}"
            continue
        key, sep, value = line.partition(":")
        if sep:
            current_key = key.strip()
            record[current_key] = value.strip()
    if record:
        yield record


def fetch_cran_packages() -> list[dict[str, str]]:
    response = requests.get(CRAN_PACKAGES_URL, timeout=60, headers={"User-Agent": "StatgroundBot/1.0"})
    response.raise_for_status()
    return list(parse_dcf(gzip.decompress(response.content).decode("utf-8", errors="replace")))


def collect(producer: KafkaEventProducer, *, limit: int = 0) -> int:
    records = fetch_cran_packages()
    count = 0
    for record in records:
        if limit and count >= limit:
            break
        package_name = record.get("Package", "")
        package_version = record.get("Version", "")
        payload = {
            "package": package_name,
            "version": package_version,
            "title": record.get("Title", ""),
            "description": record.get("Description", ""),
            "license": record.get("License", ""),
            "maintainer": record.get("Maintainer", ""),
            "author": record.get("Author", ""),
            "authors_at_r": record.get("Authors@R", ""),
            "depends": record.get("Depends", ""),
            "imports": record.get("Imports", ""),
            "suggests": record.get("Suggests", ""),
            "linking_to": record.get("LinkingTo", ""),
            "enhances": record.get("Enhances", ""),
            "needs_compilation": record.get("NeedsCompilation", ""),
            "date_publication": record.get("Date/Publication", ""),
            "repository": record.get("Repository", "CRAN"),
            "url": record.get("URL", ""),
            "bug_reports": record.get("BugReports", ""),
            "md5sum": record.get("MD5sum", ""),
        }
        producer.produce(
            RPackageEvent.create(
                event_type="rpkg.cran.package_snapshot.v1",
                source="cran_packages_gz",
                source_url=CRAN_PACKAGES_URL,
                repository="CRAN",
                package_name=package_name,
                package_version=package_version,
                payload=payload,
            )
        )
        count += 1
    producer.flush()
    return count
