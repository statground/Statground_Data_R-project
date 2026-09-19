import unittest

from export_community_cdn import (
    POSIT_COMMUNITY_EVENTS_ID,
    digest_sql,
    is_posit_workshop_event,
    require_posit_workshop_coverage,
    workshop_event_item,
    workshop_event_sql,
)


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

    def test_workshop_query_keeps_semantic_posit_event_after_canonical_dedup(self) -> None:
        query = workshop_event_sql(120)

        self.assertIn("tags_json", query)
        self.assertIn("platform = 'posit-community'", query)
        self.assertIn("has(JSONExtract(tags_json, 'Array(String)'), 'Conferences & Events')", query)

    def test_semantic_posit_event_is_exported_under_surviving_source_identity(self) -> None:
        row = {
            "external_id": "topic-217314",
            "source_id": "community:posit:latest-r-filtered",
            "source_name": "Posit Community latest topics filtered for R terms",
            "source_type": "community_forum",
            "platform": "posit-community",
            "canonical_url": "https://forum.posit.co/t/example/217314",
            "title": "EMBL symposium on statistical computing",
            "summary": "Conferences & Events. August 19, 2026.",
            "tags_json": '["Conferences & Events","Posit Community","R","forum"]',
            "published_at": "2026-08-19 00:00:00",
            "collected_at": "2026-08-20 13:27:00",
        }

        self.assertTrue(is_posit_workshop_event(row))
        item = workshop_event_item(row, "ko")
        self.assertTrue(item["uuid"].startswith("posit-event-"))
        self.assertEqual(item["source_id"], "community:posit:latest-r-filtered")
        self.assertEqual(item["source_note"], "Posit Community Conferences & Events category")

    def test_unrelated_posit_topic_is_not_exported_as_workshop(self) -> None:
        row = {
            "external_id": "topic-other",
            "source_id": "community:posit:latest-r-filtered",
            "platform": "posit-community",
            "canonical_url": "https://forum.posit.co/t/ordinary-r-question/1",
            "title": "Ordinary R question",
            "summary": "A regular forum topic",
            "tags_json": '["R","forum"]',
        }

        self.assertFalse(is_posit_workshop_event(row))
        self.assertEqual(workshop_event_item(row, "ko"), {})

    def test_exact_posit_event_source_remains_supported_without_tags(self) -> None:
        self.assertTrue(is_posit_workshop_event({"source_id": POSIT_COMMUNITY_EVENTS_ID}))

    def test_full_export_rejects_missing_or_dropped_posit_lane(self) -> None:
        with self.assertRaisesRegex(SystemExit, "missing the Posit"):
            require_posit_workshop_coverage(0, 0, 0)
        with self.assertRaisesRegex(SystemExit, "dropped every Posit"):
            require_posit_workshop_coverage(0, 3, 0)
        require_posit_workshop_coverage(0, 3, 3)
        require_posit_workshop_coverage(1, 0, 0)


if __name__ == "__main__":
    unittest.main()
