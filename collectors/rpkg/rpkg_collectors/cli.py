from __future__ import annotations

import argparse
import os
from collections.abc import Callable

from . import cran_archive, cran_checks, cran_downloads, cran_metadata, cran_reverse_dependencies, r_core_news, r_websites
from .event import RPackageEvent
from .kafka_producer import KafkaEventProducer
from .youtube import collectors as youtube_collectors


CollectorFn = Callable[[KafkaEventProducer], int]


def env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else default


def split_packages(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def run_recorded(job_type: str, source_code: str, producer: KafkaEventProducer, fn: CollectorFn) -> int:
    del job_type, source_code
    return fn(producer)


def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--topic", default=None, help="Kafka topic. Defaults to RPKG_KAFKA_TOPIC or rpkg.events.")
    parser.add_argument("--dry-run", action="store_true", help="Print JSON events instead of publishing to Kafka.")


def make_producer(args: argparse.Namespace) -> KafkaEventProducer:
    return KafkaEventProducer(topic=args.topic, dry_run=args.dry_run)


def cmd_cran_metadata(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("RPKG_CRAN_METADATA_LIMIT", 0)
    count = run_recorded(
        "cran_metadata",
        "cran",
        producer,
        lambda p: cran_metadata.collect(p, limit=limit),
    )
    print(f"published={count}")
    return 0


def cmd_cran_downloads(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    packages = args.package or split_packages(os.getenv("RPKG_DOWNLOAD_PACKAGES", ""))
    top = args.top if args.top is not None else env_int("RPKG_DOWNLOAD_TOP", 100)
    period = args.period or os.getenv("RPKG_DOWNLOAD_PERIOD", "last-month")
    count = run_recorded(
        "cran_downloads",
        "cranlogs",
        producer,
        lambda p: cran_downloads.collect(p, packages=packages, top=top, period=period),
    )
    print(f"published={count}")
    return 0


def cmd_r_core_news(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    count = run_recorded("r_core_news", "r_core", producer, r_core_news.collect)
    print(f"published={count}")
    return 0


def cmd_cran_reverse_dependencies(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("RPKG_REVERSE_DEPENDENCY_LIMIT", 0)
    count = run_recorded(
        "cran_reverse_dependencies",
        "cran",
        producer,
        lambda p: cran_reverse_dependencies.collect(p, limit=limit),
    )
    print(f"published={count}")
    return 0


def cmd_cran_checks(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("RPKG_CRAN_CHECK_LIMIT", 0)
    count = run_recorded(
        "cran_checks",
        "cran",
        producer,
        lambda p: cran_checks.collect(p, limit=limit),
    )
    print(f"published={count}")
    return 0


def cmd_cran_archive(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("RPKG_CRAN_ARCHIVE_LIMIT", 0)
    count = run_recorded(
        "cran_archive",
        "cran",
        producer,
        lambda p: cran_archive.collect(p, limit=limit),
    )
    print(f"published={count}")
    return 0


def cmd_r_websites(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("RPKG_R_WEBSITE_LIMIT", 0)
    count = run_recorded(
        "r_websites",
        "r_web",
        producer,
        lambda p: r_websites.collect(p, limit=limit),
    )
    print(f"published={count}")
    return 0


def cmd_youtube_source_seeds(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("R_YOUTUBE_SEED_LIMIT", 0)
    count = run_recorded(
        "youtube_source_seeds",
        "r_youtube_seed",
        producer,
        lambda p: youtube_collectors.collect_source_seeds(p, limit=limit, seed_path=args.seed_path),
    )
    print(f"published={count}")
    return 0


def cmd_youtube_video_snapshots(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("R_YOUTUBE_VIDEO_LIMIT", 50)
    video_ids = args.video_id or split_packages(os.getenv("R_YOUTUBE_VIDEO_IDS", ""))
    count = run_recorded(
        "youtube_video_snapshots",
        "youtube_api",
        producer,
        lambda p: youtube_collectors.collect_video_snapshots(p, video_ids=video_ids, limit=limit),
    )
    print(f"published={count}")
    return 0


def cmd_youtube_public_transcripts(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    limit = args.limit if args.limit is not None else env_int("R_YOUTUBE_TRANSCRIPT_LIMIT", 20)
    video_ids = args.video_id or split_packages(os.getenv("R_YOUTUBE_VIDEO_IDS", ""))
    count = run_recorded(
        "youtube_public_transcripts",
        "public_transcript_api",
        producer,
        lambda p: youtube_collectors.collect_public_transcripts(p, video_ids=video_ids, limit=limit),
    )
    print(f"published={count}")
    return 0


def cmd_run_due_jobs(args: argparse.Namespace) -> int:
    total = 0
    producer = make_producer(args)
    metadata_limit = args.metadata_limit if args.metadata_limit is not None else env_int("RPKG_CRAN_METADATA_LIMIT", 0)
    download_top = args.download_top if args.download_top is not None else env_int("RPKG_DOWNLOAD_TOP", 100)
    period = args.period or os.getenv("RPKG_DOWNLOAD_PERIOD", "last-month")

    total += run_recorded("cran_metadata", "cran", producer, lambda p: cran_metadata.collect(p, limit=metadata_limit))
    total += run_recorded(
        "cran_downloads",
        "cranlogs",
        producer,
        lambda p: cran_downloads.collect(p, packages=[], top=download_top, period=period),
    )
    total += run_recorded("r_core_news", "r_core", producer, r_core_news.collect)
    total += run_recorded("cran_reverse_dependencies", "cran", producer, cran_reverse_dependencies.collect)
    total += run_recorded("cran_checks", "cran", producer, cran_checks.collect)
    total += run_recorded("cran_archive", "cran", producer, cran_archive.collect)
    total += run_recorded("r_websites", "r_web", producer, r_websites.collect)
    print(f"published={total}")
    return 0


def cmd_emit_sample(args: argparse.Namespace) -> int:
    producer = make_producer(args)
    producer.produce(
        RPackageEvent.create(
            event_type="rpkg.sample.v1",
            source="rpkg_collector_cli",
            source_url="local",
            repository="CRAN",
            package_name="ggplot2",
            payload={"package": "ggplot2", "sample": True},
        )
    )
    producer.flush()
    print("published=1")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rpkg-collector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cran_metadata_parser = subparsers.add_parser("cran-metadata")
    add_common(cran_metadata_parser)
    cran_metadata_parser.add_argument("--limit", type=int, default=None)
    cran_metadata_parser.set_defaults(func=cmd_cran_metadata)

    cran_downloads_parser = subparsers.add_parser("cran-downloads")
    add_common(cran_downloads_parser)
    cran_downloads_parser.add_argument("--package", action="append", default=[])
    cran_downloads_parser.add_argument("--top", type=int, default=None)
    cran_downloads_parser.add_argument("--period", default=None)
    cran_downloads_parser.set_defaults(func=cmd_cran_downloads)

    r_core_parser = subparsers.add_parser("r-core-news")
    add_common(r_core_parser)
    r_core_parser.set_defaults(func=cmd_r_core_news)

    reverse_parser = subparsers.add_parser("cran-reverse-dependencies")
    add_common(reverse_parser)
    reverse_parser.add_argument("--limit", type=int, default=None)
    reverse_parser.set_defaults(func=cmd_cran_reverse_dependencies)

    checks_parser = subparsers.add_parser("cran-checks")
    add_common(checks_parser)
    checks_parser.add_argument("--limit", type=int, default=None)
    checks_parser.set_defaults(func=cmd_cran_checks)

    archive_parser = subparsers.add_parser("cran-archive")
    add_common(archive_parser)
    archive_parser.add_argument("--limit", type=int, default=None)
    archive_parser.set_defaults(func=cmd_cran_archive)

    websites_parser = subparsers.add_parser("r-websites")
    add_common(websites_parser)
    websites_parser.add_argument("--limit", type=int, default=None)
    websites_parser.set_defaults(func=cmd_r_websites)

    youtube_seeds_parser = subparsers.add_parser("youtube-source-seeds")
    add_common(youtube_seeds_parser)
    youtube_seeds_parser.add_argument("--limit", type=int, default=None)
    youtube_seeds_parser.add_argument("--seed-path", default=None)
    youtube_seeds_parser.set_defaults(func=cmd_youtube_source_seeds)

    youtube_videos_parser = subparsers.add_parser("youtube-video-snapshots")
    add_common(youtube_videos_parser)
    youtube_videos_parser.add_argument("--video-id", action="append", default=[])
    youtube_videos_parser.add_argument("--limit", type=int, default=None)
    youtube_videos_parser.set_defaults(func=cmd_youtube_video_snapshots)

    youtube_transcripts_parser = subparsers.add_parser("youtube-public-transcripts")
    add_common(youtube_transcripts_parser)
    youtube_transcripts_parser.add_argument("--video-id", action="append", default=[])
    youtube_transcripts_parser.add_argument("--limit", type=int, default=None)
    youtube_transcripts_parser.set_defaults(func=cmd_youtube_public_transcripts)

    run_due_parser = subparsers.add_parser("run-due-jobs")
    add_common(run_due_parser)
    run_due_parser.add_argument("--metadata-limit", type=int, default=None)
    run_due_parser.add_argument("--download-top", type=int, default=None)
    run_due_parser.add_argument("--period", default=None)
    run_due_parser.set_defaults(func=cmd_run_due_jobs)

    sample_parser = subparsers.add_parser("emit-sample")
    add_common(sample_parser)
    sample_parser.set_defaults(func=cmd_emit_sample)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
