import hashlib
import unittest

from export_community_cdn import (
    GENERATION_PROOF_SCHEMA,
    community_generation_proof,
    digest_sql,
    generation_community_item,
    generation_community_sql,
    go_canonical_json_bytes,
    normalize_generation,
    workshop_export_proof,
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

    def test_generation_requires_exact_utc_microseconds(self) -> None:
        exact = "2026-09-20 14:20:30.123456"
        self.assertEqual(normalize_generation(exact), exact)
        for invalid in (
            "2026-09-20 14:20:30",
            "2026-09-20 14:20:30.1",
            "2026-09-20T14:20:30.123456Z",
            "2026-02-30 14:20:30.123456",
            "2026-09-20 14:20:30.123456' OR 1=1",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(SystemExit):
                normalize_generation(invalid)

    def test_generation_query_reads_only_the_exact_completed_snapshot_subset(self) -> None:
        generation = "2026-09-20 14:20:30.123456"
        query = generation_community_sql(generation, 25)

        self.assertIn("FROM mart_webr.community_feed_snapshot_v1_local", query)
        self.assertIn(
            f"generation_id = toDateTime64('{generation}', 6, 'UTC')", query
        )
        for predicate in (
            "tombstone = 0",
            "article_active = 1",
            "user_blocked = 0",
            "user_active = 1",
            "language_code = 'ko'",
            "category_url IN ('rcommunity', 'notebook')",
        ):
            self.assertIn(predicate, query)
        self.assertIn("ORDER BY uuid\nLIMIT 25\nFORMAT JSONEachRow", query)
        self.assertNotIn("v_r_community_daily_digest_latest", query)
        self.assertNotIn("v_d1_notebook", query)

    def test_generation_item_has_the_exact_go_manifest_shape(self) -> None:
        item = generation_community_item(self.generation_row(), "ko")

        self.assertEqual(
            set(item),
            {
                "uuid", "kind", "language", "published_at", "updated_at", "path",
                "base_url", "source", "user_uuid", "user_nickname", "user_role",
                "category", "category_url", "category_url_sub", "source_type",
                "source_id", "source_name", "platform", "title", "summary",
                "content", "source_items_json", "url", "deduped_item_count",
            },
        )
        self.assertEqual(item["kind"], "rcommunity")
        self.assertEqual(item["summary"], "내용")
        self.assertEqual(item["content"], "내용")
        self.assertEqual(item["deduped_item_count"], 3)

    def test_generation_proof_matches_go_canonical_json_golden(self) -> None:
        generation = "2026-09-20 14:20:30.123456"
        row = self.generation_row()
        item = generation_community_item(row, "ko")
        item["path"] = "community/ko/rcommunity/2026/09/x.json"
        items = {item["uuid"]: item}

        proof = community_generation_proof(generation, items, [row])

        self.assertEqual(
            proof,
            {
                "schema": GENERATION_PROOF_SCHEMA,
                "generation": generation,
                "complete": True,
                "item_count": 1,
                "identity_hash": "7ac1b8d7010bb6cd3a3e84e7f90136b880bbc899e428ece49333372911ab9052",
                "content_hash": "3de6d2d936b33a65c279792a489cdec673e48ed4e0f49b4524fd3de3bae9ed60",
                "withdrawal_revision": 700,
                "account_authority_revision": 701,
                "category_authority_revision": 702,
            },
        )
        encoded = go_canonical_json_bytes(items)
        self.assertEqual(hashlib.sha256(encoded).hexdigest(), proof["content_hash"])
        self.assertIn("<테스트>&".encode(), encoded)
        escaped = go_canonical_json_bytes({"value": "a\u2028b\u2029c"})
        self.assertIn(b"\\u2028", escaped)
        self.assertIn(b"\\u2029", escaped)
        self.assertNotIn("\u2028".encode(), escaped)

    def test_generation_proof_rejects_partial_or_mixed_authority(self) -> None:
        row = self.generation_row()
        item = generation_community_item(row, "ko")
        item["path"] = "community/ko/rcommunity/2026/09/x.json"
        items = {item["uuid"]: item}
        generation = "2026-09-20 14:20:30.123456"

        with self.assertRaisesRegex(SystemExit, "empty or contains invalid"):
            community_generation_proof(generation, {}, [row])
        second_row = {
            **row,
            "uuid": "00000000-0000-0000-0000-000000000002",
            "account_authority_revision": 999,
        }
        second_item = generation_community_item(second_row, "ko")
        second_item["path"] = "community/ko/rcommunity/2026/09/y.json"
        mixed = [row, second_row]
        with self.assertRaisesRegex(SystemExit, "mixes authority revisions"):
            community_generation_proof(
                generation, {**items, second_item["uuid"]: second_item}, mixed
            )

    def test_workshop_export_proof_is_complete_and_order_independent(self) -> None:
        items = {
            "workshop-b": {"uuid": "workshop-b", "title": "B", "board_key": "board"},
            "workshop-a": {"uuid": "workshop-a", "title": "A", "board_key": "board"},
        }
        first = {
            "board": [{"uuid": "post-b"}, {"uuid": "post-a"}],
            "unreferenced": [{"uuid": "must-not-affect-proof"}],
        }
        second = {"board": list(reversed(first["board"]))}

        proof = workshop_export_proof(items, first)

        self.assertTrue(proof["complete"])
        self.assertEqual(proof["item_count"], 2)
        self.assertEqual(proof["post_count"], 2)
        self.assertEqual(proof["content_hash"], proof["catalog_token"])
        self.assertEqual(proof, workshop_export_proof(items, second))

    def test_workshop_export_proof_rejects_empty_items_or_posts(self) -> None:
        with self.assertRaisesRegex(SystemExit, "workshop export is empty"):
            workshop_export_proof({}, {"board": [{"uuid": "post"}]})
        with self.assertRaisesRegex(SystemExit, "no source posts"):
            workshop_export_proof(
                {"workshop": {"uuid": "workshop", "board_key": "board"}}, {}
            )

    @staticmethod
    def generation_row() -> dict[str, object]:
        return {
            "uuid": "00000000-0000-0000-0000-000000000001",
            "source": "rcommunity",
            "category": "R Community",
            "category_url": "rcommunity",
            "category_url_sub": "",
            "source_type": "rss",
            "source_id": "source",
            "source_name": "name",
            "platform": "R",
            "title": "<테스트>&\u2028",
            "content": "내용",
            "user_uuid": "u",
            "user_nickname": "R Community",
            "user_role": "Bot",
            "url": "/community/read/x/",
            "deduped_item_count": 3,
            "published_at": "2026-09-20 14:20:30",
            "updated_at": "",
            "withdrawal_revision": 700,
            "account_authority_revision": 701,
            "category_authority_revision": 702,
        }


if __name__ == "__main__":
    unittest.main()
