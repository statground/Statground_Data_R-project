from __future__ import annotations

import unittest
from datetime import date, timedelta
from pathlib import Path

from r_ecosystem_locale_batch import TARGET_LANGUAGES, select_batch


class LocaleBatchTest(unittest.TestCase):
    def test_eleven_daily_batches_cover_every_non_korean_locale_once(self) -> None:
        start = date(2026, 9, 28)
        batches = [select_batch("", start + timedelta(days=offset)) for offset in range(11)]
        self.assertTrue(all(len(batch) == 2 for batch in batches))
        self.assertEqual(set(language for batch in batches for language in batch), set(TARGET_LANGUAGES))
        self.assertEqual(len({language for batch in batches for language in batch}), 22)
        self.assertEqual(select_batch("", start), select_batch("", start + timedelta(days=11)))

    def test_explicit_batch_is_canonical_and_bounded(self) -> None:
        self.assertEqual(select_batch("ZH-hant,PT-br", date(2026, 9, 28)), ("zh-Hant", "pt-BR"))
        for requested in ("ko", "en,ja,fr", "unknown", ",", "en,"):
            with self.subTest(requested=requested), self.assertRaises(ValueError):
                select_batch(requested, date(2026, 9, 28))

    def test_social_rotation_does_not_limit_cdn_withdrawal_refresh(self) -> None:
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/r-project-all.yml").read_text()
        self.assertIn("RBLOGGER_EXTRA_LOCALES: ${{ vars.RBLOGGER_LOCALE_BACKFILL_APPROVED == 'true' && steps.opts.outputs.locale_batch || '' }}", workflow)
        self.assertIn("MASTODON_EXTRA_LOCALES: ${{ vars.MASTODON_LOCALE_BACKFILL_APPROVED == 'true' && steps.opts.outputs.locale_batch || '' }}", workflow)
        self.assertIn("&& steps.opts.outputs.all_locales || ''", workflow)
        self.assertIn("requested + existing", workflow)
        self.assertIn("len(requested) > 22", workflow)


if __name__ == "__main__":
    unittest.main()
