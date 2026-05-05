from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qs, urlparse


RefType = Literal["video", "playlist", "channel", "handle", "shorts", "live", "query", "unknown"]


@dataclass(frozen=True)
class YouTubeRef:
    ref_type: RefType
    video_id: str = ""
    playlist_id: str = ""
    channel_id: str = ""
    handle: str = ""
    canonical_url: str = ""


def parse_youtube_url(raw_url: str) -> YouTubeRef:
    raw_url = raw_url.strip()
    if not raw_url:
        return YouTubeRef(ref_type="unknown")

    parsed = urlparse(raw_url if "://" in raw_url else f"https://{raw_url}")
    host = (parsed.hostname or "").lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    query = parse_qs(parsed.query)

    if "youtube.com/results" in raw_url or "search_query" in query:
        return YouTubeRef(ref_type="query", canonical_url=raw_url)

    if host.endswith("youtu.be") and path_parts:
        video_id = path_parts[0]
        return YouTubeRef(ref_type="video", video_id=video_id, canonical_url=f"https://www.youtube.com/watch?v={video_id}")

    if not host.endswith("youtube.com"):
        return YouTubeRef(ref_type="unknown", canonical_url=raw_url)

    if parsed.path == "/watch" and query.get("v"):
        video_id = query["v"][0]
        return YouTubeRef(ref_type="video", video_id=video_id, canonical_url=f"https://www.youtube.com/watch?v={video_id}")

    if path_parts and path_parts[0] == "playlist" and query.get("list"):
        playlist_id = query["list"][0]
        return YouTubeRef(
            ref_type="playlist",
            playlist_id=playlist_id,
            canonical_url=f"https://www.youtube.com/playlist?list={playlist_id}",
        )

    if len(path_parts) >= 2 and path_parts[0] == "channel":
        channel_id = path_parts[1]
        return YouTubeRef(ref_type="channel", channel_id=channel_id, canonical_url=f"https://www.youtube.com/channel/{channel_id}")

    if path_parts and path_parts[0].startswith("@"):
        handle = path_parts[0].lstrip("@")
        return YouTubeRef(ref_type="handle", handle=handle, canonical_url=f"https://www.youtube.com/@{handle}")

    if len(path_parts) >= 2 and path_parts[0] in {"c", "user"}:
        handle = f"{path_parts[0]}/{path_parts[1]}"
        return YouTubeRef(ref_type="handle", handle=handle, canonical_url=f"https://www.youtube.com/{handle}")

    if len(path_parts) >= 2 and path_parts[0] in {"shorts", "live"}:
        video_id = path_parts[1]
        ref_type: RefType = "shorts" if path_parts[0] == "shorts" else "live"
        return YouTubeRef(ref_type=ref_type, video_id=video_id, canonical_url=f"https://www.youtube.com/watch?v={video_id}")

    return YouTubeRef(ref_type="unknown", canonical_url=raw_url)

