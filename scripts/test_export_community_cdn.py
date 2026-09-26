import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from export_community_cdn import (
    GENERATION_PROOF_SCHEMA,
    CANDIDATE_PROJECTION_SCHEMA,
    candidate_projection_item,
    candidate_projection_json_bytes,
    community_generation_proof,
    digest_sql,
    generation_community_item,
    generation_community_sql,
    go_canonical_json_bytes,
    load_candidate_json,
    normalize_generation,
    verify_candidate_evidence,
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

    def test_generation_export_requires_exact_loaded_candidate_content(self) -> None:
        row = self.generation_row()
        item = generation_community_item(row, "ko")
        item["path"] = "community/ko/rcommunity/2026/09/x.json"
        proof = community_generation_proof("2026-09-20 14:20:30.123456", {item["uuid"]: item}, [row])
        receipt = {
            "status": "candidate_loaded",
            "generation": proof["generation"],
            "source_sha256": "a" * 64,
            "row_count": 2,
            "visible_row_count": 1,
            "cdn_item_count": proof["item_count"],
            "cdn_identity_hash": proof["identity_hash"],
            "withdrawal_revision": proof["withdrawal_revision"],
            "account_authority_revision": proof["account_authority_revision"],
            "category_authority_revision": proof["category_authority_revision"],
        }
        projection = {
            "schema": CANDIDATE_PROJECTION_SCHEMA,
            "generation": proof["generation"],
            "source_sha256": receipt["source_sha256"],
            "item_count": proof["item_count"],
            "identity_hash": proof["identity_hash"],
            "withdrawal_revision": proof["withdrawal_revision"],
            "account_authority_revision": proof["account_authority_revision"],
            "category_authority_revision": proof["category_authority_revision"],
            "items": {
                row["uuid"]: hashlib.sha256(
                    candidate_projection_json_bytes(candidate_projection_item(row))
                ).hexdigest(),
            },
        }

        def seal() -> None:
            projection["projection_sha256"] = hashlib.sha256(
                candidate_projection_json_bytes({key: value for key, value in projection.items() if key != "projection_sha256"})
            ).hexdigest()

        seal()
        receipt["cdn_projection_sha256"] = projection["projection_sha256"]
        verify_candidate_evidence(proof, [row], receipt, projection)

        changed_row = {**row, "title": "A changed title"}
        with self.assertRaisesRegex(SystemExit, "source row differs"):
            verify_candidate_evidence(proof, [changed_row], receipt, projection)

        projection["withdrawal_revision"] += 1
        seal()
        with self.assertRaisesRegex(SystemExit, "authority revision differs"):
            verify_candidate_evidence(proof, [row], receipt, projection)

        projection["withdrawal_revision"] -= 1
        seal()
        receipt["cdn_identity_hash"] = "b" * 64
        with self.assertRaisesRegex(SystemExit, "identities differ"):
            verify_candidate_evidence(proof, [row], receipt, projection)

        receipt["cdn_identity_hash"] = proof["identity_hash"]
        receipt["cdn_projection_sha256"] = "c" * 64
        with self.assertRaisesRegex(SystemExit, "loader receipt"):
            verify_candidate_evidence(proof, [row], receipt, projection)

    def test_candidate_projection_file_is_owner_only_and_rejects_duplicate_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = root / "candidate.json"
            candidate.write_text('{"schema":"x","schema":"y"}', encoding="utf-8")
            candidate.chmod(0o600)
            with self.assertRaisesRegex(SystemExit, "duplicate JSON key"):
                load_candidate_json(candidate, 1024, owner_only=True)
            candidate.write_text(json.dumps({"schema": "x"}), encoding="utf-8")
            candidate.chmod(0o644)
            with self.assertRaisesRegex(SystemExit, "invalid owner, mode"):
                load_candidate_json(candidate, 1024, owner_only=True)
            candidate.chmod(0o600)
            alias = root / "alias.json"
            os.symlink(candidate, alias)
            with self.assertRaisesRegex(SystemExit, "unavailable or invalid"):
                load_candidate_json(alias, 1024, owner_only=True)

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
