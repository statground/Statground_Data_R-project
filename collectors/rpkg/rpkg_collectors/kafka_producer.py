from __future__ import annotations

import os
import socket
import sys
from urllib.parse import urlparse

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - GitHub Actions installs requirements.
    Producer = None  # type: ignore[assignment]

from .event import RPackageEvent


def first_non_empty(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


def broker_host(raw: str) -> str:
    parsed = urlparse(raw if "://" in raw else f"tcp://{raw}")
    return parsed.hostname or raw.rsplit(":", 1)[0].strip("[]")


def is_loopback_host(host: str) -> bool:
    host = host.strip().strip("[]").lower()
    if host in {"", "localhost", "127.0.0.1", "0.0.0.0", "::1"}:
        return True
    try:
        ip = socket.getaddrinfo(host, None)[0][4][0]
    except socket.gaierror:
        return False
    return ip.startswith("127.") or ip in {"0.0.0.0", "::1", "::"}


def validate_brokers(brokers: str) -> None:
    if not brokers:
        raise RuntimeError("KAFKA_BROKERS or KAFKA_BOOTSTRAP_SERVERS is required")
    for broker in brokers.split(","):
        broker = broker.strip()
        if broker and is_loopback_host(broker_host(broker)):
            raise RuntimeError(f"Kafka broker {broker!r} is not reachable from GitHub Actions")


class KafkaEventProducer:
    def __init__(self, *, topic: str | None = None, dry_run: bool = False) -> None:
        self.topic = topic or first_non_empty(os.getenv("RPKG_KAFKA_TOPIC"), os.getenv("KAFKA_TOPIC"), "rpkg.events")
        self.dry_run = dry_run or os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes", "y"}
        self._producer: object | None = None

        if self.dry_run:
            return

        if Producer is None:
            raise RuntimeError("confluent-kafka is required unless --dry-run is used")

        brokers = first_non_empty(os.getenv("KAFKA_BROKERS"), os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        validate_brokers(brokers)
        config: dict[str, object] = {
            "bootstrap.servers": brokers,
            "client.id": first_non_empty(os.getenv("KAFKA_CLIENT_ID"), "statground-rpkg-collector"),
            "message.max.bytes": int(os.getenv("KAFKA_PRODUCER_MESSAGE_MAX_BYTES", "12582912")),
            "queue.buffering.max.messages": int(os.getenv("KAFKA_QUEUE_BUFFERING_MAX_MESSAGES", "100000")),
        }

        security_protocol = first_non_empty(os.getenv("KAFKA_SECURITY_PROTOCOL"), "SASL_PLAINTEXT")
        username = first_non_empty(os.getenv("KAFKA_USERNAME"), os.getenv("KAFKA_EXTERNAL_USER"))
        password = first_non_empty(os.getenv("KAFKA_PASSWORD"), os.getenv("KAFKA_EXTERNAL_PASSWORD"))
        if username or password:
            config.update(
                {
                    "security.protocol": security_protocol,
                    "sasl.mechanism": first_non_empty(os.getenv("KAFKA_SASL_MECHANISM"), "PLAIN"),
                    "sasl.username": username,
                    "sasl.password": password,
                }
            )
        self._producer = Producer(config)

    def produce(self, event: RPackageEvent) -> None:
        if self.dry_run:
            print(event.as_json_line())
            return

        assert self._producer is not None
        self._producer.produce(
            self.topic,
            key=event.key().encode("utf-8"),
            value=event.as_json_line().encode("utf-8"),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        if self._producer is not None:
            remaining = self._producer.flush(float(os.getenv("KAFKA_FLUSH_TIMEOUT", "30")))
            if remaining:
                raise RuntimeError(f"Kafka flush timed out with {remaining} message(s) still queued")

    @staticmethod
    def _delivery_report(err: object, msg: object) -> None:
        if err is not None:
            print(f"Kafka delivery failed: {err}", file=sys.stderr)
