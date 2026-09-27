#!/usr/bin/env python3
"""Choose a small, repeatable R ecosystem translation batch for one workflow run."""

from __future__ import annotations

import argparse

from r_ecosystem_locales import SUPPORTED_LANGUAGES


EXTRA_LANGUAGES = tuple(language for language in SUPPORTED_LANGUAGES if language != "ko")
BATCH_SIZE = 2
MAX_ROWS_PER_SOURCE_AND_LANGUAGE = 1


def locales_for_run(run_number: int, approved: bool) -> tuple[str, ...]:
    if not approved:
        return ()
    if run_number < 1:
        raise ValueError("workflow run number must be positive")
    start = ((run_number - 1) * BATCH_SIZE) % len(EXTRA_LANGUAGES)
    return tuple(EXTRA_LANGUAGES[(start + index) % len(EXTRA_LANGUAGES)] for index in range(BATCH_SIZE))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-number", type=int, required=True)
    parser.add_argument("--approved", choices=("true", "false"), required=True)
    args = parser.parse_args()
    try:
        batch = locales_for_run(args.run_number, args.approved == "true")
    except ValueError as exc:
        parser.error(str(exc))
    print("locales=" + ",".join(batch))
    print(f"max_rows_per_source_and_language={MAX_ROWS_PER_SOURCE_AND_LANGUAGE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
