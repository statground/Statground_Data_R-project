#!/usr/bin/env python3

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from export_r_ecosystem_cdn import (
    SUPPORTED_LANGUAGES,
    article_sql,
    main,
    normalize_language,
    official_mastodon_sql,
)


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
            ), mock.patch("export_r_ecosystem_cdn.fetch_json_rows", side_effect=[[], [row]]) as fetch, mock.patch(
                "builtins.print"
            ) as output:
                self.assertEqual(main(), 0)
            self.assertEqual(fetch.call_count, 2)
            report = json.loads(output.call_args.args[0])
            self.assertEqual(report["article"], 1)
            self.assertEqual(report["export"], 1)


if __name__ == "__main__":
    unittest.main()
