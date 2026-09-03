import pathlib
import sys
import unittest
from unittest import mock


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

import backfill_r_packages_korean_fields as backfill


class BackfillRPackagesKoreanFieldsTest(unittest.TestCase):
    def test_insert_confirms_distributed_delivery_and_deduplicates(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b""
        with mock.patch.object(backfill.urllib.request, "urlopen", return_value=response) as urlopen:
            backfill.insert_rows(
                {
                    "CLICKHOUSE_HOST": "clickhouse.test",
                    "CLICKHOUSE_PORT": "8443",
                    "CLICKHOUSE_PROTOCOL": "https",
                    "CLICKHOUSE_USER": "test",
                    "CLICKHOUSE_PASSWORD": "test",
                },
                [{"event_id": "event-1"}],
            )

        request = urlopen.call_args.args[0]
        sql = request.data.decode("utf-8")
        self.assertIn(
            "SETTINGS insert_distributed_sync = 1, insert_deduplicate = 1 FORMAT JSONEachRow",
            sql,
        )


if __name__ == "__main__":
    unittest.main()
