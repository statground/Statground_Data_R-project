from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from urllib import error as urllib_error
from urllib import parse as urllib_parse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import webr_community_internal_audit as audit


class FakeResponse:
    status = 200

    def __init__(self, url: str, payload: bytes):
        self.url = url
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def geturl(self):
        return self.url

    def read(self, limit):
        return self.payload[:limit]


class InternalCommunityAuditTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.endpoints: list[str] = []
        for index, name in enumerate(audit.ENDPOINT_NAMES):
            path = self.root / f"{name}.xml"
            path.write_text(
                f"<clickhouse><host>db.local</host><port>{9000 + index}</port>"
                f"<password>xml-secret-{name}</password></clickhouse>",
                encoding="utf-8",
            )
            path.chmod(0o600)
            self.endpoints.append(f"{name}={path}")
        self.token = self.root / "reader.token"
        self.token.write_text("reader-secret-0123456789-abcdefgh", encoding="utf-8")
        self.token.chmod(0o600)
        self.inventory = self.root / "inventory.json"
        self.inventory.write_text(json.dumps({
            "schema": audit.INVENTORY_SCHEMA,
            "reader_inventory_revision": 1,
            "readers": [{
                "app_service": "web-r",
                "instance_id": "web-r-1",
                "inventory_endpoint": "http://127.0.0.1:8080/internal/book-publication/inventory",
                "url": "http://127.0.0.1:8080/internal/community-publication/transition",
                "token_file": str(self.token),
            }],
        }), encoding="utf-8")
        self.inventory.chmod(0o600)

    def args(self, *extra: str) -> list[str]:
        result = ["--clickhouse-client", sys.executable]
        if "--db-only" not in extra:
            result.extend(("--reader-inventory", str(self.inventory)))
        for endpoint in self.endpoints:
            result.extend(("--endpoint", endpoint))
        return [*result, *extra]

    def run_main(self, *extra: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = audit.main(self.args(*extra))
        return result, stdout.getvalue(), stderr.getvalue()

    @staticmethod
    def db_success(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, "1\n", "")

    def reader_success(self, request, **_kwargs):
        self.assertEqual(request.get_method(), "GET")
        self.assertIsNone(request.data)
        self.assertEqual(request.get_header("Authorization"), "Bearer reader-secret-0123456789-abcdefgh")
        nonce = urllib_parse.parse_qs(urllib_parse.urlsplit(request.full_url).query)["inventory_nonce"][0]
        payload = json.dumps({
            "format": audit.TRANSITION_INVENTORY_FORMAT,
            "app_service": "web-r",
            "domain": "community",
            "reader_instance": "web-r-1",
            "inventory_nonce": nonce,
            "phase": "steady",
            "admission_open": True,
            "inflight": 0,
        }).encode("utf-8")
        return FakeResponse(request.full_url, payload)

    def test_default_checks_four_constant_selects_and_one_authenticated_get(self) -> None:
        with mock.patch.object(audit.subprocess, "run", side_effect=self.db_success) as db, mock.patch.object(
            audit.HTTP_OPENER, "open", side_effect=self.reader_success
        ) as http:
            code, out, err = self.run_main()
        self.assertEqual(code, 0)
        self.assertFalse(err)
        self.assertEqual(json.loads(out), {
            "status": "audit_pass", "database_endpoint_count": 4,
            "reader_count": 1, "reader_get_checked": True, "publication_ready": False,
        })
        self.assertEqual(db.call_count, 4)
        self.assertEqual(http.call_count, 1)
        for call in db.call_args_list:
            command = call.args[0]
            self.assertEqual(command[1], "--config-file")
            self.assertEqual(command[-2:], ["--query", "SELECT 1 FORMAT TSVRaw"])
            self.assertEqual(call.kwargs["stderr"], subprocess.DEVNULL)
            self.assertNotIn("INSERT", " ".join(command))
            self.assertNotIn("SYSTEM SYNC", " ".join(command))

    def test_db_only_is_explicit_and_not_reported_as_full_audit(self) -> None:
        with mock.patch.object(audit.subprocess, "run", side_effect=self.db_success) as db, mock.patch.object(
            audit.HTTP_OPENER, "open"
        ) as http, mock.patch.object(audit, "reader_inventory") as inventory:
            code, out, err = self.run_main("--db-only")
        self.assertEqual(code, 0)
        self.assertFalse(err)
        self.assertEqual(json.loads(out)["status"], "db_connectivity_only")
        self.assertFalse(json.loads(out)["reader_get_checked"])
        self.assertEqual(json.loads(out)["reader_count"], 0)
        self.assertFalse(json.loads(out)["publication_ready"])
        self.assertEqual(db.call_count, 4)
        http.assert_not_called()
        inventory.assert_not_called()

    def test_db_only_ignores_even_a_supplied_missing_reader_inventory(self) -> None:
        missing = self.root / "missing-reader-inventory.json"
        with mock.patch.object(audit.subprocess, "run", side_effect=self.db_success) as db, mock.patch.object(
            audit, "reader_inventory"
        ) as inventory:
            code, out, err = self.run_main("--db-only", "--reader-inventory", str(missing))
        self.assertEqual(code, 0)
        self.assertFalse(err)
        self.assertEqual(json.loads(out)["status"], "db_connectivity_only")
        self.assertEqual(db.call_count, 4)
        inventory.assert_not_called()

    def test_full_audit_requires_reader_inventory_before_any_query(self) -> None:
        args = ["--clickhouse-client", sys.executable]
        for endpoint in self.endpoints:
            args.extend(("--endpoint", endpoint))
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), mock.patch.object(audit.subprocess, "run") as db:
            code = audit.main(args)
        self.assertEqual(code, 2)
        self.assertIn("reader inventory is required", stderr.getvalue())
        db.assert_not_called()

    def test_duplicate_xml_network_endpoint_blocks_before_any_query(self) -> None:
        second = self.root / "s1r2.xml"
        second.write_text(
            "<clickhouse><host>db.local</host><port>9000</port><password>xml-secret</password></clickhouse>",
            encoding="utf-8",
        )
        second.chmod(0o600)
        with mock.patch.object(audit.subprocess, "run") as db:
            code, out, err = self.run_main("--db-only")
        self.assertEqual(code, 2)
        self.assertFalse(out)
        self.assertIn("duplicate network endpoints", err)
        self.assertNotIn("xml-secret", err)
        db.assert_not_called()

    def test_unowned_mode_and_symlink_block_before_any_query(self) -> None:
        path = self.root / "s1r1.xml"
        path.chmod(0o644)
        with mock.patch.object(audit.subprocess, "run") as db:
            code, _out, err = self.run_main("--db-only")
        self.assertEqual(code, 2)
        self.assertIn("owner-only", err)
        db.assert_not_called()

        path.chmod(0o600)
        link = self.root / "linked.xml"
        link.symlink_to(path)
        self.endpoints[0] = f"s1r1={link}"
        with mock.patch.object(audit.subprocess, "run") as db:
            code, _out, err = self.run_main("--db-only")
        self.assertEqual(code, 2)
        self.assertIn("symlink", err)
        db.assert_not_called()

    def test_reader_token_permission_blocks_before_db_query(self) -> None:
        self.token.chmod(0o640)
        with mock.patch.object(audit.subprocess, "run") as db:
            code, _out, err = self.run_main()
        self.assertEqual(code, 2)
        self.assertIn("owner-only", err)
        db.assert_not_called()

    def test_reader_token_control_character_blocks_before_db_query(self) -> None:
        self.token.write_text("reader-secret-0123456789-abcdefgh\x01", encoding="utf-8")
        self.token.chmod(0o600)
        with mock.patch.object(audit.subprocess, "run") as db:
            code, _out, err = self.run_main()
        self.assertEqual(code, 2)
        self.assertIn("reader token is invalid", err)
        db.assert_not_called()

    def test_db_error_discards_raw_stderr_and_secret_values(self) -> None:
        def failed(command, **_kwargs):
            return subprocess.CompletedProcess(command, 1, "", "xml-secret-s1r1 reader-secret")

        with mock.patch.object(audit.subprocess, "run", side_effect=failed):
            code, out, err = self.run_main("--db-only")
        self.assertEqual(code, 2)
        self.assertFalse(out)
        self.assertIn("DB read failed", err)
        self.assertNotIn("xml-secret", err)
        self.assertNotIn("reader-secret", err)

    def test_reader_404_or_redirect_is_blocked_without_raw_response(self) -> None:
        def not_found(request, **_kwargs):
            raise urllib_error.HTTPError(request.full_url, 404, "reader-secret", {}, None)

        with mock.patch.object(audit.subprocess, "run", side_effect=self.db_success), mock.patch.object(
            audit.HTTP_OPENER, "open", side_effect=not_found
        ):
            code, out, err = self.run_main()
        self.assertEqual(code, 2)
        self.assertFalse(out)
        self.assertIn("reader inventory GET failed", err)
        self.assertNotIn("reader-secret", err)

        def redirected(request, **_kwargs):
            return FakeResponse("https://unexpected.example/redirect", b"reader-secret")

        with mock.patch.object(audit.subprocess, "run", side_effect=self.db_success), mock.patch.object(
            audit.HTTP_OPENER, "open", side_effect=redirected
        ):
            code, out, err = self.run_main()
        self.assertEqual(code, 2)
        self.assertFalse(out)
        self.assertIn("not a direct HTTP 200", err)
        self.assertNotIn("reader-secret", err)

    def test_reader_identity_nonce_and_steady_state_are_required(self) -> None:
        def mismatched(request, **_kwargs):
            payload = json.dumps({
                "format": audit.TRANSITION_INVENTORY_FORMAT,
                "app_service": "web-r", "domain": "community",
                "reader_instance": "web-r-1", "inventory_nonce": "wrong",
                "phase": "recovery_hold", "admission_open": False, "inflight": 0,
            }).encode("utf-8")
            return FakeResponse(request.full_url, payload)

        with mock.patch.object(audit.subprocess, "run", side_effect=self.db_success), mock.patch.object(
            audit.HTTP_OPENER, "open", side_effect=mismatched
        ):
            code, out, err = self.run_main()
        self.assertEqual(code, 2)
        self.assertFalse(out)
        self.assertIn("steady reader", err)


if __name__ == "__main__":
    unittest.main()
