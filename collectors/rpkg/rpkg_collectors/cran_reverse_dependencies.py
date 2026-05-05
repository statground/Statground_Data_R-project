from __future__ import annotations

import re
from datetime import datetime, timezone

from . import cran_metadata
from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer


DEPENDENCY_FIELDS = ("Depends", "Imports", "Suggests", "LinkingTo", "Enhances")
VERSION_SPEC_RE = re.compile(r"\s*\(.*?\)\s*")


def snapshot_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def parse_dependency_names(value: str) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for raw_part in value.split(","):
        dependency_spec = raw_part.strip()
        if not dependency_spec:
            continue
        package_name = VERSION_SPEC_RE.sub("", dependency_spec).strip()
        package_name = package_name.split()[0] if package_name.split() else ""
        if package_name and package_name != "R":
            edges.append((package_name, dependency_spec))
    return edges


def collect(producer: KafkaEventProducer, *, limit: int = 0) -> int:
    records = cran_metadata.fetch_cran_packages()
    count = 0
    observed_date = snapshot_date()
    for record in records:
        if limit and count >= limit:
            break
        from_package = record.get("Package", "")
        from_version = record.get("Version", "")
        if not from_package:
            continue
        for field_name in DEPENDENCY_FIELDS:
            for to_package, dependency_spec in parse_dependency_names(record.get(field_name, "")):
                if limit and count >= limit:
                    break
                payload = {
                    "snapshot_date": observed_date,
                    "source": "CRAN",
                    "from_repository": "CRAN",
                    "from_package": from_package,
                    "from_version": from_version,
                    "to_package": to_package,
                    "dependency_type": field_name,
                    "dependency_spec": dependency_spec,
                }
                producer.produce(
                    RPackageEvent.create(
                        event_type="rpkg.cran.dependency_edge_snapshot.v1",
                        source="cran_packages_gz_dependency_parser",
                        source_url=cran_metadata.CRAN_PACKAGES_URL,
                        repository="CRAN",
                        package_name=from_package,
                        package_version=from_version,
                        observed_at=f"{observed_date}T00:00:00.000Z",
                        payload=payload,
                    )
                )
                count += 1
    producer.flush()
    return count
