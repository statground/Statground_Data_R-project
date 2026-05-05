from __future__ import annotations

import os
import re
from dataclasses import dataclass


DEFAULT_PACKAGES = [
    "dplyr",
    "ggplot2",
    "tidyr",
    "shiny",
    "data.table",
    "tidymodels",
    "sf",
    "forecast",
    "brms",
    "rstan",
    "BiocManager",
    "Seurat",
    "renv",
    "quarto",
    "knitr",
    "rmarkdown",
]


@dataclass(frozen=True)
class PackageMention:
    package_name: str
    match_text: str
    confidence: str
    confidence_score: float


def configured_packages() -> list[str]:
    raw = os.getenv("R_YOUTUBE_MENTION_PACKAGES", "").strip()
    if not raw:
        return DEFAULT_PACKAGES
    return [part.strip() for part in raw.split(",") if part.strip()]


def extract_mentions(text: str, packages: list[str] | None = None) -> list[PackageMention]:
    packages = packages or configured_packages()
    mentions: list[PackageMention] = []
    for package_name in packages:
        pattern = re.compile(rf"(?<![A-Za-z0-9_.]){re.escape(package_name)}(?:::[A-Za-z0-9_.]+)?(?![A-Za-z0-9_.])", re.IGNORECASE)
        match = pattern.search(text)
        if not match:
            continue
        start = max(match.start() - 60, 0)
        end = min(match.end() + 60, len(text))
        mentions.append(
            PackageMention(
                package_name=package_name,
                match_text=text[start:end].strip(),
                confidence="high" if "::" in match.group(0) else "medium",
                confidence_score=0.9 if "::" in match.group(0) else 0.7,
            )
        )
    return mentions

