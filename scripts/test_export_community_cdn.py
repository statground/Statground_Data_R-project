import unittest

from export_community_cdn import digest_sql


class CommunityCDNExportTest(unittest.TestCase):
    def test_digest_export_uses_generation_timestamp_as_publication_time(self) -> None:
        query = digest_sql(10)

        self.assertIn(
            "formatDateTime(d.created_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS published_at",
            query,
        )
        self.assertNotIn(
            "concat(toString(digest_date), ' 23:59:00') AS published_at",
            query,
        )


if __name__ == "__main__":
    unittest.main()
