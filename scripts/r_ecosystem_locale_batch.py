#!/usr/bin/env python3
"""Select a bounded, repeatable batch from Web-R's public language menu."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo


SUPPORTED_LANGUAGES = (
    "ko", "en", "ja", "zh-Hans", "zh-Hant", "es", "fr", "de", "pt-BR",
    "ru", "id", "vi", "th", "ms", "fil", "hi", "ar", "it", "nl",
    "pl", "sv", "tr", "uk",
)
TARGET_LANGUAGES = SUPPORTED_LANGUAGES[1:]


def canonical_language(raw: str) -> str:
    for language in TARGET_LANGUAGES:
        if language.casefold() == raw.strip().casefold():
            return language
    raise ValueError("unsupported or Korean locale in translation batch")


def select_batch(requested: str, day: date, size: int = 2) -> tuple[str, ...]:
    if size < 1 or size > 2:
        raise ValueError("locale batch size must be one or two")
    if requested.strip():
        chosen = tuple(dict.fromkeys(canonical_language(part) for part in requested.split(",")))
        if not chosen or len(chosen) > size:
            raise ValueError("manual locale batch must contain one or two distinct locales")
        return chosen
    batches = tuple(TARGET_LANGUAGES[index:index + size] for index in range(0, len(TARGET_LANGUAGES), size))
    return batches[day.toordinal() % len(batches)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requested", default="", help="optional one or two target locales")
    parser.add_argument("--date", default=datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat())
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--all-codes", action="store_true", help="list all non-Korean locales for withdrawal-safe CDN refresh")
    args = parser.parse_args()
    if args.all_codes:
        print(",".join(TARGET_LANGUAGES))
        return 0
    try:
        chosen = select_batch(args.requested, date.fromisoformat(args.date), args.batch_size)
    except ValueError as exc:
        parser.error(str(exc))
    print(",".join(chosen))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
