from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from ..event import RPackageEvent
from ..kafka_producer import KafkaEventProducer
from .mention_extractor import extract_mentions
from .seed_loader import YouTubeSeed, load_seeds
from .url_parser import parse_youtube_url
from .youtube_api_client import YouTubeAPIClient, iso8601_duration_to_seconds


def collect_source_seeds(producer: KafkaEventProducer, *, limit: int = 0, seed_path: str | None = None) -> int:
    count = 0
    for seed in load_seeds(seed_path):
        if limit and count >= limit:
            break
        producer.produce(
            RPackageEvent.create(
                event_type="r.youtube.source.seed.v1",
                source="r_youtube_seed_loader",
                source_url=seed.url,
                repository="R-YouTube",
                payload=seed.payload(),
            )
        )
        count += 1
    producer.flush()
    return count


def collect_video_snapshots(producer: KafkaEventProducer, *, video_ids: list[str] | None = None, limit: int = 50) -> int:
    video_ids = video_ids or configured_video_ids()
    video_ids = dedupe(video_ids)[:limit]
    if not video_ids:
        return 0

    client = YouTubeAPIClient()
    count = 0
    items = client.videos(video_ids)
    producer.produce(
        RPackageEvent.create(
            event_type="r.youtube.quota.usage.v1",
            source="youtube_videos_list",
            source_url="https://www.googleapis.com/youtube/v3/videos",
            repository="R-YouTube",
            payload={
                "quota_date": current_iso()[:10],
                "api_key_alias": os.getenv("YOUTUBE_API_KEY_ALIAS", "default"),
                "method_name": "videos.list",
                "quota_cost": "1",
                "request_count": "1",
                "quota_units_used": "1",
                "source_method": "youtube_data_api",
                "collection_status": "collected",
            },
        )
    )
    count += 1
    for item in items:
        payload = video_snapshot_payload(item)
        if not payload.get("youtube_video_id"):
            continue
        producer.produce(
            RPackageEvent.create(
                event_type="r.youtube.video.snapshot.v1",
                source="youtube_videos_list",
                source_url=str(payload["canonical_url"]),
                repository="R-YouTube",
                observed_at=str(payload.get("published_at") or current_iso()),
                payload=payload,
            )
        )
        count += 1
        for mention in extract_mentions(f"{payload.get('video_title', '')} {payload.get('video_description', '')}"):
            producer.produce(
                RPackageEvent.create(
                    event_type="r.youtube.package.mention.v1",
                    source="youtube_metadata_mention_extractor",
                    source_url=str(payload["canonical_url"]),
                    repository="CRAN",
                    package_name=mention.package_name,
                    payload={
                        "youtube_video_id": str(payload["youtube_video_id"]),
                        "mention_source": "metadata",
                        "language_code": str(payload.get("language_code") or "und"),
                        "segment_start_ms": "0",
                        "segment_end_ms": "0",
                        "match_text": mention.match_text,
                        "confidence": mention.confidence,
                        "confidence_score": f"{mention.confidence_score:.2f}",
                        "extractor_version": "rpkg-youtube-mention-v1",
                        "source_method": "metadata_regex",
                        "collection_status": "collected",
                    },
                )
            )
            count += 1
    producer.flush()
    return count


def collect_public_transcripts(producer: KafkaEventProducer, *, video_ids: list[str] | None = None, limit: int = 20) -> int:
    video_ids = dedupe(video_ids or configured_video_ids())[:limit]
    if not video_ids:
        return 0
    try:
        from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi
    except ImportError:
        print("youtube-transcript-api is not installed; skipping transcript collection")
        return 0

    count = 0
    for video_id in video_ids:
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            transcript = None
            for language_code in ["ko", "en", "ja", "es", "fr", "de", "pt"]:
                try:
                    transcript = transcript_list.find_manually_created_transcript([language_code])
                    break
                except NoTranscriptFound:
                    pass
            if transcript is None:
                for language_code in ["ko", "en", "ja", "es", "fr", "de", "pt"]:
                    try:
                        transcript = transcript_list.find_generated_transcript([language_code])
                        break
                    except NoTranscriptFound:
                        pass
            if transcript is None:
                transcript = next(iter(transcript_list))

            language_code = getattr(transcript, "language_code", "") or "und"
            is_generated = bool(getattr(transcript, "is_generated", False))
            for index, segment in enumerate(transcript.fetch()):
                text = str(segment.get("text", "")).strip()
                if not text:
                    continue
                start_ms = int(float(segment.get("start", 0)) * 1000)
                duration_ms = int(float(segment.get("duration", 0)) * 1000)
                end_ms = start_ms + duration_ms
                payload = {
                    "youtube_video_id": video_id,
                    "caption_track_key": f"public:{language_code}:{'auto' if is_generated else 'manual'}",
                    "language_code": language_code,
                    "segment_index": str(index),
                    "start_ms": str(start_ms),
                    "end_ms": str(end_ms),
                    "duration_ms": str(duration_ms),
                    "text_raw": text,
                    "text_normalized": normalize_text(text),
                    "is_auto_generated": "1" if is_generated else "0",
                    "source_method": "public_transcript_api",
                    "collection_status": "collected",
                    "retention_policy_code": "retain_public_caption_best_effort",
                }
                producer.produce(
                    RPackageEvent.create(
                        event_type="r.youtube.transcript.segment.v1",
                        source="youtube_public_transcript",
                        source_url=f"https://www.youtube.com/watch?v={video_id}",
                        repository="R-YouTube",
                        payload=payload,
                    )
                )
                count += 1
                for mention in extract_mentions(text):
                    producer.produce(
                        RPackageEvent.create(
                            event_type="r.youtube.package.mention.v1",
                            source="youtube_transcript_mention_extractor",
                            source_url=f"https://www.youtube.com/watch?v={video_id}&t={start_ms // 1000}s",
                            repository="CRAN",
                            package_name=mention.package_name,
                            payload={
                                "youtube_video_id": video_id,
                                "mention_source": "transcript",
                                "language_code": language_code,
                                "segment_start_ms": str(start_ms),
                                "segment_end_ms": str(end_ms),
                                "match_text": mention.match_text,
                                "confidence": mention.confidence,
                                "confidence_score": f"{mention.confidence_score:.2f}",
                                "extractor_version": "rpkg-youtube-mention-v1",
                                "source_method": "transcript_regex",
                                "collection_status": "collected",
                            },
                        )
                    )
                    count += 1
        except (NoTranscriptFound, TranscriptsDisabled) as exc:
            producer.produce(collection_failure(video_id, exc.__class__.__name__))
            count += 1
    producer.flush()
    return count


def configured_video_ids() -> list[str]:
    raw = os.getenv("R_YOUTUBE_VIDEO_IDS", "").strip()
    if raw:
        return [part.strip() for part in raw.split(",") if part.strip()]
    ids: list[str] = []
    for seed in load_seeds():
        ref = parse_youtube_url(seed.url)
        if ref.video_id:
            ids.append(ref.video_id)
    return ids


def video_snapshot_payload(item: dict[str, Any]) -> dict[str, str]:
    video_id = str(item.get("id") or "")
    snippet = as_dict(item.get("snippet"))
    content = as_dict(item.get("contentDetails"))
    statistics = as_dict(item.get("statistics"))
    status = as_dict(item.get("status"))
    thumbnails = as_dict(snippet.get("thumbnails"))
    thumbnail_url = best_thumbnail_url(thumbnails)
    tags = snippet.get("tags") if isinstance(snippet.get("tags"), list) else []
    return {
        "youtube_video_id": video_id,
        "youtube_channel_id": str(snippet.get("channelId") or ""),
        "playlist_ids_json": "[]",
        "video_title": str(snippet.get("title") or ""),
        "video_description": str(snippet.get("description") or ""),
        "canonical_url": f"https://www.youtube.com/watch?v={video_id}",
        "thumbnail_url": thumbnail_url,
        "published_at": str(snippet.get("publishedAt") or ""),
        "duration_seconds": str(iso8601_duration_to_seconds(str(content.get("duration") or ""))),
        "view_count": str(statistics.get("viewCount") or "0"),
        "like_count": str(statistics.get("likeCount") or "0"),
        "comment_count": str(statistics.get("commentCount") or "0"),
        "favorite_count": str(statistics.get("favoriteCount") or "0"),
        "caption_available": "1" if str(content.get("caption") or "").lower() == "true" else "0",
        "default_audio_language": str(snippet.get("defaultAudioLanguage") or ""),
        "default_language": str(snippet.get("defaultLanguage") or ""),
        "language_code": str(snippet.get("defaultAudioLanguage") or snippet.get("defaultLanguage") or "und"),
        "tags_json": json.dumps(tags, ensure_ascii=False, sort_keys=True),
        "thumbnail_urls_json": json.dumps(thumbnails, ensure_ascii=False, sort_keys=True),
        "channel_title": str(snippet.get("channelTitle") or ""),
        "privacy_status": str(status.get("privacyStatus") or ""),
        "source_method": "videos.list",
        "source_tag": "r_project_ecosystem_youtube",
        "source_category": classify_video(snippet),
        "source_confidence": "api_refreshed",
        "collection_status": "collected",
    }


def collection_failure(video_id: str, error_code: str) -> RPackageEvent:
    return RPackageEvent.create(
        event_type="r.youtube.collection.failure.v1",
        source="youtube_public_transcript",
        source_url=f"https://www.youtube.com/watch?v={video_id}",
        repository="R-YouTube",
        payload={
            "youtube_video_id": video_id,
            "source_method": "public_transcript_api",
            "collection_status": "failed",
            "error_code": error_code,
        },
    )


def classify_video(snippet: dict[str, Any]) -> str:
    text = f"{snippet.get('title') or ''} {snippet.get('description') or ''}".lower()
    if any(token in text for token in ["conference", "posit::conf", "user!", "rstudio::conf", "talk"]):
        return "conference"
    if any(token in text for token in ["tutorial", "workshop", "course", "lesson", "lecture"]):
        return "tutorial"
    return "r_project_ecosystem_video"


def best_thumbnail_url(thumbnails: dict[str, Any]) -> str:
    for key in ["maxres", "standard", "high", "medium", "default"]:
        value = as_dict(thumbnails.get(key))
        if value.get("url"):
            return str(value["url"])
    return ""


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def current_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
