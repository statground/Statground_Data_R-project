from __future__ import annotations

import os
import re
from typing import Any

import requests


YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


class YouTubeAPIClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = (api_key or os.getenv("YOUTUBE_API_KEY", "")).strip()
        if not self.api_key:
            raise RuntimeError("YOUTUBE_API_KEY is required for YouTube Data API collectors")

    def get_json(self, resource: str, params: dict[str, str]) -> dict[str, Any]:
        request_params = dict(params)
        request_params["key"] = self.api_key
        response = requests.get(f"{YOUTUBE_API_BASE}/{resource}", params=request_params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"YouTube API {resource} returned a non-object payload")
        return payload

    def videos(self, video_ids: list[str]) -> list[dict[str, Any]]:
        if not video_ids:
            return []
        payload = self.get_json(
            "videos",
            {
                "part": "snippet,contentDetails,statistics,status",
                "id": ",".join(video_ids[:50]),
                "maxResults": str(min(len(video_ids), 50)),
            },
        )
        items = payload.get("items", [])
        if not isinstance(items, list):
            return []
        return [item for item in items if isinstance(item, dict)]


def iso8601_duration_to_seconds(value: str) -> int:
    match = re.fullmatch(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", value or "")
    if not match:
        return 0
    days, hours, minutes, seconds = (int(part or 0) for part in match.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + seconds

