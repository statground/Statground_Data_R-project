#!/usr/bin/env python3
"""Verify that all four publication endpoints resolve only to private routes."""

from __future__ import annotations

import argparse
import ipaddress
import json
import socket
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


ENDPOINTS = ("s1r1", "s1r2", "s2r1", "s2r2")
PRIVATE_ROUTES = tuple(ipaddress.ip_network(cidr) for cidr in (
    "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "fc00::/7",
))


class NetworkPreflightError(RuntimeError):
    pass


def private_address(address: str) -> bool:
    candidate = ipaddress.ip_address(address)
    return any(candidate in route for route in PRIVATE_ROUTES)


def verify_endpoint(path: Path, label: str, timeout: float = 5) -> None:
    try:
        root = ET.fromstring(path.read_bytes())
        host = (root.findtext("host") or "").strip()
        secure = (root.findtext("secure") or "").strip().lower() in {"1", "true", "yes"}
        port = int((root.findtext("port") or str(9440 if secure else 9000)).strip())
        if root.tag != "clickhouse" or not host or port < 1 or port > 65535:
            raise ValueError("invalid endpoint")
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        if not addresses or any(not private_address(address[4][0]) for address in addresses):
            raise ValueError("endpoint is not private")
        for family, kind, protocol, _, sockaddr in addresses:
            with socket.socket(family, kind, protocol) as connection:
                connection.settimeout(timeout)
                connection.connect(sockaddr)
                break
    except (OSError, ValueError, ET.ParseError) as exc:
        raise NetworkPreflightError(f"private publication endpoint {label} is unavailable") from exc


def verify_network(runtime_dir: Path) -> None:
    for label in ENDPOINTS:
        verify_endpoint(runtime_dir / "endpoints" / f"{label}.xml", label)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        verify_network(args.runtime_dir)
    except NetworkPreflightError as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps({"status": "private_endpoints_reachable", "endpoint_count": len(ENDPOINTS)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
