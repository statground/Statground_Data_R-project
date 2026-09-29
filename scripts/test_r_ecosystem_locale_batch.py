#!/usr/bin/env python3

import unittest

from r_ecosystem_locale_batch import (
    BATCH_SIZE,
    EXTRA_LANGUAGES,
    MAX_ROWS_PER_SOURCE_AND_LANGUAGE,
    locales_for_run,
)


class LocaleBatchTests(unittest.TestCase):
    def test_all_public_locales_are_selected_once_per_cycle(self) -> None:
        batches = [locales_for_run(run_number, True) for run_number in range(1, 12)]
        self.assertEqual(len(EXTRA_LANGUAGES), 22)
        self.assertTrue(all(len(batch) == BATCH_SIZE for batch in batches))
        self.assertEqual(set(language for batch in batches for language in batch), set(EXTRA_LANGUAGES))
        self.assertEqual(len([language for batch in batches for language in batch]), len(EXTRA_LANGUAGES))
        self.assertEqual(locales_for_run(12, True), batches[0])
        self.assertEqual(MAX_ROWS_PER_SOURCE_AND_LANGUAGE, 1)

    def test_retry_is_stable_and_approval_is_required(self) -> None:
        self.assertEqual(locales_for_run(6, True), locales_for_run(6, True))
        self.assertEqual(locales_for_run(6, False), ())
        with self.assertRaisesRegex(ValueError, "positive"):
            locales_for_run(0, True)


if __name__ == "__main__":
    unittest.main()
