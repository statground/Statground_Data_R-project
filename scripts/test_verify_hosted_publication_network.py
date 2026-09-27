from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from verify_hosted_publication_network import (
    NetworkPreflightError,
    private_address,
    verify_endpoint,
    verify_network,
)


class HostedPublicationNetworkTest(unittest.TestCase):
    def test_private_routes_only(self) -> None:
        for address in ("10.1.2.3", "172.16.1.1", "192.168.0.15", "100.101.102.103", "fd7a:115c:a1e0::1"):
            self.assertTrue(private_address(address))
        for address in ("127.0.0.1", "8.8.8.8", "169.254.1.1", "::1"):
            self.assertFalse(private_address(address))

    def test_public_or_unreachable_endpoint_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            endpoint = Path(temporary) / "s1r1.xml"
            endpoint.write_text("<clickhouse><host>db.example</host><port>9000</port></clickhouse>")
            with patch("verify_hosted_publication_network.socket.getaddrinfo", return_value=[(2, 1, 6, "", ("8.8.8.8", 9000))]):
                with self.assertRaises(NetworkPreflightError):
                    verify_endpoint(endpoint, "s1r1")
            with patch("verify_hosted_publication_network.socket.getaddrinfo", return_value=[
                (2, 1, 6, "", ("192.168.0.15", 9000)),
                (2, 1, 6, "", ("8.8.8.8", 9000)),
            ]):
                with self.assertRaises(NetworkPreflightError):
                    verify_endpoint(endpoint, "s1r1")
            with patch("verify_hosted_publication_network.socket.getaddrinfo", side_effect=OSError("offline")):
                with self.assertRaises(NetworkPreflightError):
                    verify_endpoint(endpoint, "s1r1")

    def test_private_endpoint_uses_clickhouse_default_port(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            endpoint = Path(temporary) / "s1r1.xml"
            endpoint.write_text("<clickhouse><host>db.internal</host></clickhouse>")
            with patch("verify_hosted_publication_network.socket.getaddrinfo", return_value=[(2, 1, 6, "", ("192.168.0.15", 9000))]) as resolve:
                with patch("verify_hosted_publication_network.socket.socket") as connection:
                    verify_endpoint(endpoint, "s1r1")
                    connection.return_value.__enter__.return_value.connect.assert_called_once_with(("192.168.0.15", 9000))
                resolve.assert_called_once_with("db.internal", 9000, type=1)

    def test_all_four_endpoint_configs_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(NetworkPreflightError):
                verify_network(Path(temporary))


if __name__ == "__main__":
    unittest.main()
