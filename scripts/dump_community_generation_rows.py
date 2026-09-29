#!/usr/bin/env python3
"""Read exact public community candidate rows with the offline native principal."""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

from export_community_cdn import generation_community_sql, load_generation_rows_file, normalize_generation


class DumpError(RuntimeError):
    pass


def owner_only_file(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise DumpError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise DumpError(f"{label} must be an owner-only regular file")


def dump_rows(client: Path, config: Path, generation: str, output: Path) -> dict[str, object]:
    generation = normalize_generation(generation)
    owner_only_file(config, "native client config")
    if not client.is_file():
        raise DumpError("native ClickHouse client is unavailable")
    try:
        directory = output.parent.lstat()
    except OSError as exc:
        raise DumpError("candidate output directory is unavailable") from exc
    if not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid() or directory.st_mode & 0o077:
        raise DumpError("candidate output directory must be owner-only")
    descriptor = -1
    created = False
    try:
        descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        created = True
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            result = subprocess.run(
                [str(client), "--config-file", str(config), "--query", generation_community_sql(generation, 0)],
                stdout=stream, stderr=subprocess.DEVNULL, timeout=180, check=False,
            )
            stream.flush()
            os.fsync(stream.fileno())
        if result.returncode != 0:
            raise DumpError("native candidate query failed")
        rows = load_generation_rows_file(output)
        return {"status": "candidate_rows_dumped", "generation": generation, "row_count": len(rows)}
    except (OSError, subprocess.TimeoutExpired, SystemExit):
        if created:
            output.unlink(missing_ok=True)
        raise DumpError("native candidate rows are unavailable or invalid") from None
    except BaseException:
        if created:
            output.unlink(missing_ok=True)
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = dump_rows(args.client, args.config, args.generation, args.output)
    except (DumpError, SystemExit) as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
