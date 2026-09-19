import json
import tempfile
import unittest
from pathlib import Path

from export_community_cdn import (
    POSIT_COMMUNITY_EVENTS_ID,
    WORKSHOP_AUTHORITY_REGISTRY_SCHEMA,
    WORKSHOP_WITHDRAWAL_AUTHORITY_SCHEMA,
    add_workshop_source_representation,
    authorize_empty_workshop_source,
    digest_sql,
    is_posit_workshop_event,
    load_workshop_authority_registry,
    require_workshop_source_parity,
    workshop_posit_source_identity_set,
    workshop_represented_source_identity_set,
    workshop_source_identity_digest,
    workshop_source_parity_receipt,
    workshop_source_scope,
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

    def test_catalog_dedupe_represents_every_source_identity(self) -> None:
        rows = [self.posit_row(f"topic-{index}") for index in range(3)]
        source = workshop_posit_source_identity_set(rows)
        catalog_item: dict[str, object] = {"uuid": "shared-catalog-event"}
        for row in rows:
            add_workshop_source_representation(catalog_item, next(iter(workshop_posit_source_identity_set([row]))))

        represented = workshop_represented_source_identity_set({"shared-catalog-event": catalog_item})
        require_workshop_source_parity(source, represented)
        self.assertEqual(len(source), 3)
        self.assertEqual(catalog_item["represented_source_identities"], sorted(source))

    def test_partial_three_to_one_export_is_rejected(self) -> None:
        rows = [self.posit_row(f"topic-{index}") for index in range(3)]
        source = workshop_posit_source_identity_set(rows)
        represented = {sorted(source)[0]}

        with self.assertRaisesRegex(SystemExit, r"source=3 represented=1 missing=2"):
            require_workshop_source_parity(source, represented)

    def test_source_identity_drift_is_rejected_even_when_counts_match(self) -> None:
        source = workshop_posit_source_identity_set([self.posit_row("topic-stable")])
        drifted = workshop_posit_source_identity_set([self.posit_row("topic-drifted")])

        with self.assertRaisesRegex(SystemExit, r"missing=1 unexpected=1"):
            require_workshop_source_parity(source, drifted)

    def test_approved_empty_withdrawal_uses_newer_durable_revision(self) -> None:
        scope = workshop_source_scope("ko")
        empty: set[str] = set()
        previous = workshop_posit_source_identity_set([self.posit_row("previous")])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry_path = root / "registry.json"
            authority_path = root / "authority.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "schema": WORKSHOP_AUTHORITY_REGISTRY_SCHEMA,
                        "scope": scope,
                        "authority_revision": 7,
                        "source_identity_count": len(previous),
                        "source_identity_sha256": workshop_source_identity_digest(previous),
                        "represented_identity_count": len(previous),
                        "represented_identity_sha256": workshop_source_identity_digest(previous),
                        "exact": True,
                        "empty_withdrawal_authorized": False,
                    }
                ),
                encoding="utf-8",
            )
            authority_path.write_text(
                json.dumps(
                    {
                        "schema": WORKSHOP_WITHDRAWAL_AUTHORITY_SCHEMA,
                        "scope": scope,
                        "authority_revision": 8,
                        "source_identity_count": 0,
                        "source_identity_sha256": workshop_source_identity_digest(empty),
                        "allow_empty_withdrawal": True,
                    }
                ),
                encoding="utf-8",
            )

            registry = load_workshop_authority_registry(registry_path, scope)
            revision, approved = authorize_empty_workshop_source(
                source_identities=empty,
                scope=scope,
                current_registry=registry,
                authority_manifest_path=authority_path,
            )
            stale_registry = dict(registry or {}, authority_revision=8)
            with self.assertRaisesRegex(SystemExit, "must be greater"):
                authorize_empty_workshop_source(
                    source_identities=empty,
                    scope=scope,
                    current_registry=stale_registry,
                    authority_manifest_path=authority_path,
                )

        receipt = workshop_source_parity_receipt(
            source_identities=empty,
            represented_identities=empty,
            scope=scope,
            authority_revision=revision,
            empty_withdrawal_authorized=approved,
        )
        self.assertEqual(revision, 8)
        self.assertTrue(approved)
        self.assertTrue(receipt["exact"])
        self.assertEqual(receipt["source_identity_count"], 0)

    def test_unapproved_empty_source_is_rejected(self) -> None:
        scope = workshop_source_scope("ko")
        previous = workshop_posit_source_identity_set([self.posit_row("previous")])
        current_registry = {
            "schema": WORKSHOP_AUTHORITY_REGISTRY_SCHEMA,
            "scope": scope,
            "authority_revision": 7,
            "source_identity_count": len(previous),
            "source_identity_sha256": workshop_source_identity_digest(previous),
            "represented_identity_count": len(previous),
            "represented_identity_sha256": workshop_source_identity_digest(previous),
            "exact": True,
            "empty_withdrawal_authorized": False,
        }

        with self.assertRaisesRegex(SystemExit, "without a durable withdrawal authority"):
            authorize_empty_workshop_source(
                source_identities=set(),
                scope=scope,
                current_registry=current_registry,
                authority_manifest_path=None,
            )

    @staticmethod
    def posit_row(external_id: str) -> dict[str, object]:
        return {
            "external_id": external_id,
            "source_id": "community:posit:latest-r-filtered",
            "source_name": "Posit Community latest topics filtered for R terms",
            "source_type": "community_forum",
            "platform": "posit-community",
            "canonical_url": f"https://forum.posit.co/t/example/{external_id}",
            "title": "Example conference",
            "summary": "Conferences & Events",
            "tags_json": '["Conferences & Events","R"]',
        }


if __name__ == "__main__":
    unittest.main()
