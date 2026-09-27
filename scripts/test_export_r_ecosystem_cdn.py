#!/usr/bin/env python3

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from export_r_ecosystem_cdn import (
    MANIFEST_SCHEMA,
    SUPPORTED_LANGUAGES,
    approved_workflow_locales,
    article_sql,
    community_sql,
    community_source_sha256,
    derive_key,
    encrypt_document,
    korean_community_authority,
    main,
    native_english_row_authorized,
    normalize_language,
    official_mastodon_sql,
    write_json_atomic,
)


def native_english_community_row() -> dict[str, object]:
    uuid = "11111111-2222-3333-4444-555555555555"
    return {
        "item_uuid": uuid, "external_id": "source:1", "source_id": "official:r",
        "source_name": "R Project", "source_type": "official_blog", "platform": "R Project",
        "source_url": "https://example.org/news/1", "canonical_url": "https://example.org/news/1",
        "title": "Original English title", "summary": "Original English summary", "author": "R Project",
        "language": "en", "source_language": "en", "source_title": "Original English title",
        "source_summary": "Original English summary", "source_version": "123", "active": 1,
        "tags_json": "[]", "raw_json": '{"title_ko":"한국어 제목"}',
        "payload_json": '{"summary_ko":"한국어 요약"}',
        "published_at_text": "2026-09-27 12:00:00", "collected_at_text": "2026-09-27 12:00:00",
    }


class REcosystemLocaleExportTests(unittest.TestCase):
    def test_locales_match_public_menu_and_keep_source_case(self) -> None:
        self.assertEqual(len(SUPPORTED_LANGUAGES), 23)
        self.assertEqual(normalize_language("ZH-HANS"), "zh-hans")
        self.assertEqual(normalize_language("pt-BR"), "pt-br")
        with self.assertRaisesRegex(SystemExit, "unsupported"):
            article_sql(1, "ko' OR 1=1")

    def test_korean_query_preserves_existing_sources(self) -> None:
        query = article_sql(10, "ko")
        self.assertIn("a.source IN ('rblogger', 'rproject')", query)
        self.assertIn("a.language_code = 'ko'", query)
        self.assertIn("coalesce(a.title, '') AS title", query)
        self.assertNotIn("AS verified_locale", query)

    def test_native_english_community_uses_latest_active_source_text(self) -> None:
        query = community_sql(10, "en")
        self.assertLess(query.index("LIMIT 1 BY item_uuid"), query.index("WHERE r.active = 1"))
        self.assertIn("r.language = 'en'", query)
        self.assertIn("r.title AS source_title, r.summary AS source_summary", query)
        self.assertIn("toString(r.version) AS source_version", query)
        self.assertIn("title, summary, author, language, tags_json", query)
        self.assertNotIn("'en' AS language", query)
        self.assertIn("'' AS raw_json, '' AS payload_json", query)
        self.assertIn("NOT match(title, '[가-힣]')", query)
        self.assertIn("NOT match(summary, '[가-힣]')", query)
        self.assertIn("LIMIT 10", query)
        with self.assertRaisesRegex(ValueError, "only for ko and en"):
            community_sql(10, "ja")
        korean_query = community_sql(10, "ko")
        self.assertIn("r.title AS source_title", korean_query)
        self.assertIn("r.summary AS source_summary", korean_query)
        self.assertIn("toString(r.version) AS source_version", korean_query)

    def test_native_english_requires_verified_korean_publication(self) -> None:
        row = native_english_community_row()
        uuid = row["item_uuid"]
        row["source_sha256"] = community_source_sha256(row)
        item = {
            "uuid": uuid, "kind": "community", "language": "ko", "path": f"contents/ko/2026/09/{uuid}.json",
            "canonical_url": row["canonical_url"], "source_id": row["source_id"],
            "source_type": row["source_type"], "source_version": row["source_version"],
            "source_sha256": row["source_sha256"],
        }
        key = derive_key("test-key")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(SystemExit, "authority could not be verified"):
                korean_community_authority(root, key)
            manifest = {"schema": MANIFEST_SCHEMA, "language": "ko", "items": {uuid: {k: v for k, v in item.items() if k != "source_sha256"}}}
            rel_path = "contents/ko/index.json"
            write_json_atomic(root / rel_path, encrypt_document(manifest, key, rel_path, "ko", ""))
            with self.assertRaisesRegex(SystemExit, "lacks source revision/hash"):
                korean_community_authority(root, key)
            manifest["items"][uuid] = item
            write_json_atomic(root / rel_path, encrypt_document(manifest, key, rel_path, "ko", ""))
            authority = korean_community_authority(root, key)
            self.assertTrue(native_english_row_authorized(row, authority))
            self.assertFalse(native_english_row_authorized({**row, "canonical_url": "https://example.org/withdrawn"}, authority))
            self.assertFalse(native_english_row_authorized({**row, "language": "ko"}, authority))
            self.assertFalse(native_english_row_authorized({**row, "summary": "한국어 요약"}, authority))
            self.assertFalse(native_english_row_authorized({**row, "active": 0}, authority))
            self.assertFalse(native_english_row_authorized({**row, "source_type": "newsletter"}, authority))
            changed = {**row, "source_version": "124", "source_summary": "Edited source summary"}
            changed["source_sha256"] = community_source_sha256(changed)
            self.assertFalse(native_english_row_authorized(changed, authority))
            self.assertNotEqual(row["source_sha256"], community_source_sha256({**row, "source_summary": "Original English summary "}))
            self.assertFalse(native_english_row_authorized(row, {uuid: {**item, "source_type": "newsletter"}}))
            self.assertFalse(native_english_row_authorized(row, {uuid: {**item, "kind": "article"}}))
            self.assertFalse(native_english_row_authorized(row, {}))
            with self.assertRaisesRegex(SystemExit, "authority could not be verified"):
                korean_community_authority(root, derive_key("wrong-key"))

    def test_other_locale_requires_latest_active_source_hash_and_translation(self) -> None:
        query = article_sql(10, "ZH-HANS")
        self.assertIn("a.source = 'rblogger'", query)
        self.assertIn("a.language_code = 'zh-Hans'", query)
        self.assertIn("coalesce(verified_locale.title, '') AS title", query)
        self.assertIn("coalesce(verified_locale.content, '') AS content", query)
        self.assertIn("AS verified_locale ON verified_locale.uuid = a.uuid", query)
        self.assertIn("WHERE b.rn = 1 AND r.rn = 1", query)
        self.assertIn("coalesce(b.active, 0) = 1 AND coalesce(r.active, 0) = 1", query)
        self.assertIn("JSONExtractString(toString(b.created_log), 'target_language') = 'zh-Hans'", query)
        self.assertIn("JSONExtractString(toString(b.created_log), 'source_sha256') =", query)
        self.assertIn("SHA256(concat(ifNull(r.title, ''), unhex('0A'), ifNull(r.content, '')))", query)

    def test_official_locale_requires_latest_active_source_and_exact_hash(self) -> None:
        query = official_mastodon_sql(2, "ZH-HANS")
        self.assertIn("WHERE language_code = 'zh-Hans'", query)
        self.assertIn("PARTITION BY uuid ORDER BY fetched_at DESC, ingested_at DESC, event_uuid DESC", query)
        self.assertIn("b.rn = 1 AND r.rn = 1", query)
        self.assertIn("coalesce(b.active, 0) = 1 AND r.active = 1", query)
        self.assertIn("r.visibility IN ('public', 'unlisted')", query)
        self.assertIn("JSONExtractString(toString(b.created_log), 'target_language') = 'zh-Hans'", query)
        self.assertIn("JSONExtractString(toString(b.created_log), 'source_sha256') = r.source_sha256", query)
        self.assertIn("LIMIT 2", query)
        with self.assertRaisesRegex(ValueError, "Korean official"):
            official_mastodon_sql(1, "ko")

    def test_untranslated_new_locale_does_not_publish_empty_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            argv = ["export_r_ecosystem_cdn.py", "--cdn-root", tmp, "--language", "ja"]
            with mock.patch.object(sys, "argv", argv), mock.patch(
                "export_r_ecosystem_cdn.load_env", return_value={"R_ECOSYSTEM_CONTENT_KEY": "test-key"}
            ), mock.patch("export_r_ecosystem_cdn.fetch_json_rows", return_value=[]) as fetch, mock.patch(
                "builtins.print"
            ) as output:
                self.assertEqual(main(), 0)
            self.assertEqual(fetch.call_count, 2)
            self.assertEqual(json.loads(output.call_args.args[0])["skipped"], "no_verified_locale_rows")
            self.assertFalse((root / "contents/ja/index.json").exists())

    def test_verified_official_locale_enters_dry_run_export(self) -> None:
        row = {
            "source": "rproject", "uuid": "11111111-2222-3333-4444-555555555555",
            "title": "R Foundation news", "content": "<p>Official news.</p>",
            "url": "https://fosstodon.org/@R_Foundation/1", "created_at": "2026-09-27 12:00:00",
            "author": "R Project", "platform": "R Project", "category": "Official announcement", "language": "en",
        }
        with tempfile.TemporaryDirectory() as tmp:
            argv = ["export_r_ecosystem_cdn.py", "--cdn-root", tmp, "--language", "en", "--dry-run"]
            with mock.patch.object(sys, "argv", argv), mock.patch(
                "export_r_ecosystem_cdn.load_env", return_value={"R_ECOSYSTEM_CONTENT_KEY": "test-key"}
            ), mock.patch("export_r_ecosystem_cdn.fetch_json_rows", side_effect=[[], [], [row]]) as fetch, mock.patch(
                "builtins.print"
            ) as output:
                self.assertEqual(main(), 0)
            self.assertEqual(fetch.call_count, 3)
            report = json.loads(output.call_args.args[0])
            self.assertEqual(report["article"], 1)
            self.assertEqual(report["export"], 1)

    def test_native_english_payload_keeps_original_without_korean_overrides(self) -> None:
        row = native_english_community_row()
        uuid = row["item_uuid"]
        authority = {uuid: {
            "uuid": uuid, "kind": "community", "language": "ko", "path": f"contents/ko/2026/09/{uuid}.json",
            "canonical_url": row["canonical_url"], "source_id": row["source_id"],
            "source_type": row["source_type"], "source_version": row["source_version"],
            "source_sha256": community_source_sha256(row),
        }}
        with tempfile.TemporaryDirectory() as tmp:
            argv = ["export_r_ecosystem_cdn.py", "--cdn-root", tmp, "--language", "en"]
            with mock.patch.object(sys, "argv", argv), mock.patch(
                "export_r_ecosystem_cdn.load_env", return_value={"R_ECOSYSTEM_CONTENT_KEY": "test-key"}
            ), mock.patch("export_r_ecosystem_cdn.fetch_json_rows", side_effect=[[row], [], []]), mock.patch(
                "export_r_ecosystem_cdn.korean_community_authority", return_value=authority
            ), mock.patch("export_r_ecosystem_cdn.encrypt_document", return_value={}) as encrypt, mock.patch(
                "builtins.print"
            ) as output:
                self.assertEqual(main(), 0)
            payload = encrypt.call_args_list[0].args[0]["community_item"]
            self.assertEqual(payload["title"], row["title"])
            self.assertEqual(payload["summary"], row["summary"])
            self.assertEqual(payload["raw_json"], "")
            self.assertEqual(payload["payload_json"], "")
            self.assertEqual(json.loads(output.call_args.args[0])["community"], 1)

    def test_korean_manifest_records_exact_source_revision(self) -> None:
        row = native_english_community_row()
        row["title"] = "한국어 게시 제목"
        row["summary"] = "한국어 게시 요약"
        row["language"] = "ko"
        with tempfile.TemporaryDirectory() as tmp:
            argv = ["export_r_ecosystem_cdn.py", "--cdn-root", tmp, "--language", "ko"]
            with mock.patch.object(sys, "argv", argv), mock.patch(
                "export_r_ecosystem_cdn.load_env", return_value={"R_ECOSYSTEM_CONTENT_KEY": "test-key"}
            ), mock.patch("export_r_ecosystem_cdn.fetch_json_rows", side_effect=[[row], []]), mock.patch(
                "export_r_ecosystem_cdn.encrypt_document", return_value={}
            ) as encrypt, mock.patch("builtins.print"):
                self.assertEqual(main(), 0)
            item = encrypt.call_args_list[-1].args[0]["items"][row["item_uuid"]]
            self.assertEqual(item["source_type"], row["source_type"])
            self.assertEqual(item["source_version"], row["source_version"])
            self.assertEqual(item["source_sha256"], community_source_sha256(row))

    def test_workflow_native_english_publication_is_default_off(self) -> None:
        self.assertEqual(approved_workflow_locales("en,ja", False), ["ja"])
        self.assertEqual(approved_workflow_locales("en,ja", True), ["en", "ja"])
        self.assertEqual(approved_workflow_locales("zh-Hant", False), ["zh-hant"])
        with self.assertRaisesRegex(SystemExit, "one or two"):
            approved_workflow_locales("en,ja,fr", True)


if __name__ == "__main__":
    unittest.main()
