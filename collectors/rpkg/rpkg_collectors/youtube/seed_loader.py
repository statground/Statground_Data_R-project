from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .url_parser import parse_youtube_url


DEFAULT_SEED_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "r_youtube_seed.yml"


@dataclass(frozen=True)
class YouTubeSeed:
    source_code: str
    title: str
    source_type: str
    url: str
    category: str
    language_hint: str
    source_confidence: str
    priority: str
    notes: str = ""

    def payload(self) -> dict[str, str]:
        ref = parse_youtube_url(self.url)
        return {
            "source_code": self.source_code,
            "title": self.title,
            "source_type": self.source_type,
            "url": self.url,
            "category": self.category,
            "language_hint": self.language_hint,
            "source_confidence": self.source_confidence,
            "priority": self.priority,
            "notes": self.notes,
            "parsed_ref_type": ref.ref_type,
            "parsed_video_id": ref.video_id,
            "parsed_playlist_id": ref.playlist_id,
            "parsed_channel_id": ref.channel_id,
            "parsed_handle": ref.handle,
            "canonical_url": ref.canonical_url,
            "source_method": "fixture_seed_catalog",
            "collection_status": "seeded",
        }


def load_seeds(path: str | Path | None = None) -> list[YouTubeSeed]:
    seed_path = Path(path) if path else DEFAULT_SEED_PATH
    raw = seed_path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"{seed_path} must contain a JSON/YAML list of seed objects")
    return [_seed_from_dict(item) for item in data]


def _seed_from_dict(item: Any) -> YouTubeSeed:
    if not isinstance(item, dict):
        raise ValueError("Each YouTube seed must be an object")
    return YouTubeSeed(
        source_code=str(item["source_code"]),
        title=str(item["title"]),
        source_type=str(item["source_type"]),
        url=str(item["url"]),
        category=str(item["category"]),
        language_hint=str(item.get("language_hint", "und")),
        source_confidence=str(item.get("source_confidence", "candidate")),
        priority=str(item.get("priority", "P2")),
        notes=str(item.get("notes", "")),
    )

