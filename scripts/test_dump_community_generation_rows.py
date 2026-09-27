import json
import os
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest import mock

from dump_community_generation_rows import DumpError, dump_rows


GENERATION = "2026-09-27 01:02:03.123456"
PUBLIC_ROW = {
    "uuid": "00000000-0000-0000-0000-000000000001",
    "source": "rcommunity", "category": "R Community", "category_url": "rcommunity",
    "category_url_sub": "", "source_type": "rss", "source_id": "source",
    "source_name": "name", "platform": "R", "title": "Public title",
    "content": "Public content", "user_uuid": "00000000-0000-0000-0000-000000000002",
    "user_nickname": "Public author", "user_role": "Bot", "url": "/community/read/x/",
    "deduped_item_count": 0, "published_at": "2026-09-27 10:02:03",
    "updated_at": "", "withdrawal_revision": 0,
    "account_authority_revision": 1, "category_authority_revision": 1,
}


class DumpCommunityGenerationRowsTest(unittest.TestCase):
    def test_native_query_writes_only_exact_public_rows_and_preserves_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            client = root / "clickhouse-client"
            config = root / "config.xml"
            output = root / "rows.jsonl"
            client.write_bytes(b"native client")
            config.write_bytes(b"private config")
            config.chmod(0o600)

            def native_query(argv, *, stdout, stderr, timeout, check):  # noqa: ANN001
                self.assertEqual(argv[:3], [str(client), "--config-file", str(config)])
                self.assertIn("mart_webr.community_feed_snapshot_v1_local", argv[4])
                self.assertIn(GENERATION, argv[4])
                self.assertIn("category_url IN ('rcommunity', 'notebook')", argv[4])
                stdout.write(json.dumps(PUBLIC_ROW).encode() + b"\n")
                return CompletedProcess(argv, 0)

            with mock.patch("dump_community_generation_rows.subprocess.run", side_effect=native_query):
                result = dump_rows(client, config, GENERATION, output)
            self.assertEqual(result["status"], "candidate_rows_dumped")
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(os.stat(output).st_mode & 0o777, 0o600)
            original = output.read_bytes()
            with self.assertRaisesRegex(DumpError, "unavailable or invalid"):
                dump_rows(client, config, GENERATION, output)
            self.assertEqual(output.read_bytes(), original)

    def test_native_result_with_private_column_is_deleted_before_export(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            client = root / "clickhouse-client"
            config = root / "config.xml"
            output = root / "rows.jsonl"
            client.write_bytes(b"native client")
            config.write_bytes(b"private config")
            config.chmod(0o600)

            def native_query(argv, *, stdout, stderr, timeout, check):  # noqa: ANN001
                stdout.write(json.dumps({**PUBLIC_ROW, "email": "private@example.test"}).encode() + b"\n")
                return CompletedProcess(argv, 0)

            with mock.patch("dump_community_generation_rows.subprocess.run", side_effect=native_query):
                with self.assertRaisesRegex(DumpError, "unavailable or invalid"):
                    dump_rows(client, config, GENERATION, output)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
