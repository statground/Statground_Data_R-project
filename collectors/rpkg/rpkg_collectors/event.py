from __future__ import annotations

import hashlib
import json
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def uuid7() -> str:
    unix_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (unix_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0x2 << 62) | rand_b
    return str(uuid.UUID(int=value))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RPackageEvent:
    event_id: str
    event_type: str
    schema_version: int
    source: str
    source_url: str
    repository: str
    package_name: str
    package_version: str
    observed_at: str
    collected_at: str
    payload_hash: str
    payload: dict[str, Any]

    @classmethod
    def create(
        cls,
        *,
        event_type: str,
        source: str,
        source_url: str,
        repository: str,
        package_name: str = "",
        package_version: str = "",
        observed_at: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> "RPackageEvent":
        payload = payload or {}
        collected_at = utc_now_iso()
        return cls(
            event_id=uuid7(),
            event_type=event_type,
            schema_version=1,
            source=source,
            source_url=source_url,
            repository=repository,
            package_name=package_name,
            package_version=package_version,
            observed_at=observed_at or collected_at,
            collected_at=collected_at,
            payload_hash=payload_hash(payload),
            payload=payload,
        )

    def key(self) -> str:
        parts = [self.repository, self.package_name, self.package_version, self.event_type]
        return ":".join(part for part in parts if part)

    def as_kafka_record(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "schema_version": self.schema_version,
            "source": self.source,
            "source_url": self.source_url,
            "repository": self.repository,
            "package_name": self.package_name,
            "package_version": self.package_version,
            "observed_at": self.observed_at,
            "collected_at": self.collected_at,
            "payload_hash": self.payload_hash,
            "payload": canonical_json(self.payload),
        }

    def as_json_line(self) -> str:
        return canonical_json(self.as_kafka_record())
