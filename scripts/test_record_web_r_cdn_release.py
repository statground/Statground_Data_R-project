#!/usr/bin/env python3

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from pathlib import Path
from unittest import mock

import record_web_r_cdn_release as record


COMMIT_SHA = "485a550671b7e91802a2b093a83da6811e9a8480"
RELEASE_ID = f"web-r-content:ko:{COMMIT_SHA}"


class RecordWebRCDNReleaseTest(unittest.TestCase):
    def run_main_with_insert(
        self,
        insert_side_effect: object,
        *,
        attempts: int = 3,
        fail_open: bool = True,
    ) -> tuple[int, dict[str, object], str, mock.Mock]:
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "manifest_paths": ["contents/ko/index.json"],
                        "item_count": 3145,
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            argv = [
                "record_web_r_cdn_release.py",
                "--report",
                str(report_path),
                "--commit-sha",
                COMMIT_SHA,
                "--repo",
                "statground/web-r_CDN2_contents",
                "--scope",
                "web-r-content",
                "--language",
                "ko",
            ]
            env = {
                "WEB_R_CDN_RELEASE_RECORD_ATTEMPTS": str(attempts),
                "WEB_R_CDN_RELEASE_RECORD_BACKOFF_SECONDS": "0",
                "WEB_R_CDN_RELEASE_RECORD_TRANSIENT_FAIL_OPEN": (
                    "true" if fail_open else "false"
                ),
            }
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(
                    record,
                    "insert_json_each_row",
                    side_effect=insert_side_effect,
                ) as insert,
                mock.patch.object(record.time, "sleep") as sleep,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                result = record.main()
            sleep.assert_not_called()
            payload = json.loads(stdout.getvalue().strip().splitlines()[-1])
            return result, payload, stderr.getvalue(), insert

    def test_first_transient_then_success_reuses_payload_and_token(self) -> None:
        rows: list[dict[str, object]] = []
        tokens: list[str] = []

        def insert(
            _table: str,
            row: dict[str, object],
            *,
            deduplication_token: str,
        ) -> None:
            rows.append(deepcopy(row))
            tokens.append(deduplication_token)
            if len(rows) == 1:
                raise record.ClickHouseReleaseRecordError(
                    0,
                    "TIMEOUT_EXCEEDED",
                    "request timed out",
                )

        result, payload, stderr, mocked_insert = self.run_main_with_insert(insert)

        self.assertEqual(result, 0)
        self.assertFalse(payload["record_deferred"])
        self.assertEqual(payload["record_attempts"], 2)
        self.assertEqual(mocked_insert.call_count, 2)
        self.assertEqual(rows[0], rows[1])
        self.assertEqual(rows[0]["published_at"], rows[1]["published_at"])
        self.assertEqual(rows[0]["version"], rows[1]["version"])
        self.assertEqual(tokens, [record.release_record_deduplication_token(RELEASE_ID)] * 2)
        self.assertIn("reason=TIMEOUT_EXCEEDED attempt=1/3", stderr)

    def test_all_transient_attempts_defer_only_after_bound(self) -> None:
        transient = record.ClickHouseReleaseRecordError(
            0,
            "TIMEOUT_EXCEEDED",
            "request timed out",
        )

        result, payload, stderr, insert = self.run_main_with_insert(
            transient,
            attempts=3,
        )

        self.assertEqual(result, 0)
        self.assertTrue(payload["record_deferred"])
        self.assertEqual(payload["record_attempts"], 3)
        self.assertEqual(payload["deferred_reason"], "TIMEOUT_EXCEEDED")
        self.assertEqual(insert.call_count, 3)
        self.assertEqual(stderr.count("release record retry"), 2)
        self.assertIn("record deferred", stderr)
        self.assertIn("attempts=3", stderr)

    def test_fatal_contract_error_is_not_retried_or_deferred(self) -> None:
        for status, category in (
            (403, "ACCESS_DENIED"),
            (500, "UNKNOWN_TABLE"),
            (500, "UNKNOWN_IDENTIFIER"),
            (400, "SYNTAX_ERROR"),
        ):
            with self.subTest(category=category):
                fatal = record.ClickHouseReleaseRecordError(
                    status,
                    category,
                    category,
                )
                row = {"release_id": RELEASE_ID}

                with (
                    mock.patch.object(
                        record,
                        "insert_json_each_row",
                        side_effect=fatal,
                    ) as insert,
                    mock.patch.object(record.time, "sleep") as sleep,
                    redirect_stderr(io.StringIO()),
                    self.assertRaises(record.ClickHouseReleaseRecordError),
                ):
                    record.insert_release_with_retry(
                        record.RELEASE_LOG_TABLE,
                        row,
                        scope="web-r-content",
                        attempts=3,
                        backoff_seconds=0,
                    )
                self.assertEqual(insert.call_count, 1)
                sleep.assert_not_called()

    def test_insert_uses_stable_release_deduplication_token(self) -> None:
        row = {
            "release_id": RELEASE_ID,
            "published_at": "2026-08-08 00:00:00.000",
            "version": 1,
        }
        later_process_row = {
            "release_id": RELEASE_ID,
            "published_at": "2026-08-08 00:05:00.000",
            "version": 2,
        }
        token = record.release_record_deduplication_token(RELEASE_ID)
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.read.return_value = b""
        env = {
            "CH_HOST": "clickhouse.example.invalid",
            "CH_USER": "test-user",
            "CH_PASSWORD": "test-password",
        }

        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch.object(
                record.urllib.request,
                "urlopen",
                return_value=response,
            ) as urlopen,
        ):
            record.insert_json_each_row(
                record.RELEASE_LOG_TABLE,
                row,
                deduplication_token=token,
            )

        request = urlopen.call_args.args[0]
        sql = request.data.decode("utf-8")
        self.assertIn("SETTINGS insert_distributed_sync = 1, insert_deduplicate = 1", sql)
        self.assertIn(f"insert_deduplication_token = '{token}'", sql)
        self.assertEqual(
            token,
            record.release_record_deduplication_token(
                str(later_process_row["release_id"])
            ),
        )
        self.assertNotEqual(
            token,
            record.release_record_deduplication_token(
                f"web-r-content:ko:{'a' * 40}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
