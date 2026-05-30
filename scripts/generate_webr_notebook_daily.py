#!/usr/bin/env python3
"""Generate one public Web-R Notebook series post and insert it into ClickHouse."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from clickhouse_http import build_clickhouse_url


NOTEBOOK_BOT_UUID = "7b1c9fc4-7216-44cb-81b8-5fe17f2158bc"
KST = ZoneInfo("Asia/Seoul")
RECENT_CONTENT_LIMIT = 16
MAX_CANDIDATE_ATTEMPTS = 12
RECENT_STYLE_LOOKBACK = 4
RECENT_TOPIC_LOOKBACK = 6
SIMILARITY_THRESHOLD = 0.50
STOP_TOKENS = {
    "action",
    "active",
    "analysis",
    "below",
    "cell",
    "code",
    "color",
    "daily",
    "data",
    "date",
    "days",
    "format",
    "from",
    "generated",
    "github",
    "label",
    "length",
    "main",
    "notebook",
    "output",
    "plot",
    "producer",
    "result",
    "round",
    "same",
    "schema",
    "seed",
    "series",
    "source",
    "stats",
    "style",
    "topic",
    "value",
    "webr",
    "with",
    "가상의",
    "같은",
    "구성했습니다",
    "데이터는",
    "분석합니다",
    "아래",
    "오늘은",
    "오늘의",
    "입니다",
    "자동",
}


@dataclass(frozen=True)
class Topic:
    key: str
    title: str
    metric: str
    entity: str
    question: str
    source_note: str
    background: str
    color: str
    accent: str
    base: int
    slope: float
    amplitude: float
    noise: float
    threshold: float


@dataclass(frozen=True)
class NotebookStyle:
    key: str
    label: str
    title_prefix: str
    question_prefix: str
    title_template: str
    method_note: str
    formula_note: str
    closing: str


TOPICS = [
    Topic(
        key="package-download-pulse",
        title="R package download pulse",
        metric="simulated package downloads",
        entity="R 패키지 다운로드",
        question="최근 흐름에서 평소보다 강하게 튀는 날이 있었는가?",
        source_note="가상의 일별 패키지 다운로드 수",
        background="패키지 다운로드는 실제 수요뿐 아니라 강의, 문서 공개, CI 재설치, release 직후의 반짝 관심이 함께 섞인 신호입니다. 그래서 하루 값 하나보다 기준선과 이탈 정도를 같이 보는 편이 안전합니다.",
        color="#2563eb",
        accent="#dc2626",
        base=92,
        slope=0.72,
        amplitude=13,
        noise=7.5,
        threshold=1.45,
    ),
    Topic(
        key="workshop-signup-rhythm",
        title="Workshop signup rhythm",
        metric="simulated workshop signups",
        entity="워크숍 신청",
        question="홍보 이후 신청 흐름이 실제로 기준선을 넘어섰는가?",
        source_note="가상의 워크숍 일별 신청 수",
        background="워크숍 신청은 공지 직후에 급하게 몰리고, 이후에는 일정과 주제 적합성에 따라 서서히 빠지는 경우가 많습니다. 총 신청자 수만 보면 어느 채널이나 시점이 변화를 만들었는지 놓치기 쉽습니다.",
        color="#0f766e",
        accent="#f97316",
        base=24,
        slope=0.36,
        amplitude=6,
        noise=3.2,
        threshold=1.35,
    ),
    Topic(
        key="render-latency-band",
        title="Render latency stability band",
        metric="simulated render latency",
        entity="Notebook 렌더링 지연",
        question="일별 중앙값이 단기 안정 구간 밖으로 벗어났는가?",
        source_note="가상의 렌더링 지연 시간",
        background="렌더링 지연은 서버 부하, 브라우저 자원, 패키지 로딩, 그래프 출력 크기가 한꺼번에 섞입니다. 평균 하나로 보면 일부 무거운 실행이 전체 경험을 가리는지 알기 어렵습니다.",
        color="#7c3aed",
        accent="#ea580c",
        base=430,
        slope=-1.4,
        amplitude=21,
        noise=13.0,
        threshold=1.30,
    ),
    Topic(
        key="community-reply-momentum",
        title="Community reply momentum",
        metric="simulated community replies",
        entity="커뮤니티 답변",
        question="답변 속도가 어느 시점부터 빨라졌는가?",
        source_note="가상의 커뮤니티 일별 답변 수",
        background="커뮤니티 답변은 질문량뿐 아니라 답변 가능한 사람이 접속하는 시간대와 주제 난이도의 영향을 받습니다. 흐름을 보면 단순한 활발함과 실제 응답성의 변화를 분리해 볼 수 있습니다.",
        color="#1d4ed8",
        accent="#be123c",
        base=38,
        slope=0.18,
        amplitude=9,
        noise=4.2,
        threshold=1.40,
    ),
    Topic(
        key="search-interest-shift",
        title="Search interest shift",
        metric="simulated search events",
        entity="R 검색 이벤트",
        question="관심도가 주중 패턴을 넘어 구조적으로 이동했는가?",
        source_note="가상의 검색 이벤트 수",
        background="검색 이벤트는 사용자가 문서를 읽기 전 남기는 아주 이른 신호입니다. 요일 효과가 강하기 때문에, 같은 요일의 반복과 새로운 관심 이동을 구분해서 보는 일이 중요합니다.",
        color="#166534",
        accent="#b45309",
        base=61,
        slope=0.44,
        amplitude=11,
        noise=5.8,
        threshold=1.42,
    ),
    Topic(
        key="documentation-click-through",
        title="Documentation click-through drift",
        metric="simulated documentation clicks",
        entity="문서 클릭",
        question="새 문서 묶음 이후 클릭 패턴이 완만히 이동했는가?",
        source_note="가상의 문서 클릭 수",
        background="문서 클릭은 학습자가 어디에서 막히는지 보여주는 간접 지표입니다. 클릭이 늘었다고 곧바로 좋은 문서라고 말할 수는 없지만, 어떤 경로가 더 자주 확인되는지는 개선 순서를 정하는 데 도움이 됩니다.",
        color="#0369a1",
        accent="#c2410c",
        base=75,
        slope=0.55,
        amplitude=10,
        noise=6.0,
        threshold=1.38,
    ),
]

STYLES = [
    NotebookStyle(
        key="signal-brief",
        label="Signal brief",
        title_prefix="Signal brief",
        question_prefix="오늘은 작은 시계열 신호를 빠르게 훑습니다.",
        title_template="{entity}의 단기 신호를 기준선과 비교하기",
        method_note="짧은 시계열에서는 이동평균이 빠른 기준선 역할을 합니다. 오늘은 관측값에서 7일 이동평균을 빼고, 그 잔차를 표준화해 평소보다 큰 날을 찾습니다.",
        formula_note="핵심 식은 `z_t = (x_t - m_t) / sd(x_t - m_t)` 입니다. 여기서 `m_t`는 최근 7일 평균이고, `z_t`가 클수록 평소 흐름에서 더 멀어진 날입니다.",
        closing="하루짜리 브리프처럼 읽되, 같은 코드로 plot과 요약 수치를 다시 만들 수 있게 남겼습니다.",
    ),
    NotebookStyle(
        key="ranked-audit",
        label="Ranked audit",
        title_prefix="Ranked audit",
        question_prefix="오늘은 여러 항목을 순위와 누적 비중으로 훑어봅니다.",
        title_template="{entity} 경로별 기여도를 순위로 읽기",
        method_note="여러 경로가 함께 움직일 때는 가장 큰 항목만 보는 대신, 각 항목의 비중과 누적 비중을 같이 봅니다. 그러면 상위 몇 개가 전체 변화를 거의 설명하는지 바로 확인할 수 있습니다.",
        formula_note="비중은 `p_i = x_i / sum(x_i)`로 두고, 누적 비중은 `P_k = sum_{i <= k} p_i`로 계산합니다. 절반을 넘기는 지점이 집중도의 실마리입니다.",
        closing="막대 순위와 누적 비중을 같이 보면 작은 항목들이 전체 해석에 남기는 그림자까지 볼 수 있습니다.",
    ),
    NotebookStyle(
        key="bootstrap-lab",
        label="Bootstrap lab",
        title_prefix="Bootstrap lab",
        question_prefix="오늘은 차이가 있어 보이는 두 집단을 재표본추출로 다시 의심해봅니다.",
        title_template="{entity} 차이를 부트스트랩으로 의심하기",
        method_note="두 집단의 평균 차이는 표본이 조금만 달라도 흔들릴 수 있습니다. 그래서 관측된 표본을 여러 번 다시 뽑아 차이의 분포를 만들고, 결론의 폭을 먼저 봅니다.",
        formula_note="관심 값은 `Delta = mean(treatment) - mean(control)` 입니다. 부트스트랩은 이 `Delta`를 반복 계산해 한 숫자가 아니라 가능한 범위로 읽게 해줍니다.",
        closing="점추정 하나로 결론내리지 않고, 같은 데이터를 여러 번 다시 뽑아 불확실성의 폭을 같이 봅니다.",
    ),
    NotebookStyle(
        key="cohort-map",
        label="Cohort map",
        title_prefix="Cohort map",
        question_prefix="오늘은 시간에 따라 남는 비율을 cohort heatmap으로 읽습니다.",
        title_template="{entity} 유지율을 코호트 지도로 읽기",
        method_note="시작 시점이 다른 집단을 한 줄에 놓고 같은 경과 주차끼리 비교하면, 절대 날짜의 계절성보다 경험 경과에 따른 이탈을 더 잘 볼 수 있습니다.",
        formula_note="각 칸은 `r_{c,w} = n_{c,w} / n_{c,0}` 입니다. `c`는 시작 cohort, `w`는 경과 주차이며 값이 낮을수록 빨리 빠져나간 cohort입니다.",
        closing="행은 시작 cohort, 열은 경과 주차입니다. 색이 빨리 옅어지는 곳이 개선 후보입니다.",
    ),
    NotebookStyle(
        key="scatter-diagnostics",
        label="Scatter diagnostics",
        title_prefix="Scatter diagnostics",
        question_prefix="오늘은 두 지표의 관계를 회귀선과 잔차로 점검합니다.",
        title_template="{entity} 관계를 잔차로 점검하기",
        method_note="두 지표가 함께 움직인다고 해서 모든 점이 같은 규칙을 따르는 것은 아닙니다. 회귀선을 기준으로 멀리 떨어진 점을 찾으면 다음에 따로 봐야 할 segment가 보입니다.",
        formula_note="기본 모형은 `y = beta0 + beta1 x + error` 입니다. 잔차 `error`가 큰 점은 평균적 관계가 설명하지 못한 관측치입니다.",
        closing="선에서 멀리 떨어진 점은 실패가 아니라 다음 질문을 만드는 관측치입니다.",
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="optional env file path")
    parser.add_argument("--date", default="", help="series date in YYYY-MM-DD, default: today in Asia/Seoul")
    parser.add_argument("--runner", default="scripts/webr_notebook_runner.mjs", help="Node webR runner path")
    parser.add_argument("--output", default="", help="write generated notebook JSON summary to this path")
    parser.add_argument("--dry-run", action="store_true", help="run WebR and build the row without inserting it")
    parser.add_argument("--force-new", action="store_true", help="allow more than one generated post for the same date")
    args = parser.parse_args()

    repo_root = Path.cwd()
    env = load_env(repo_root / args.env)
    series_date = parse_series_date(args.date)
    existing_titles = existing_notebook_titles(env)
    recent_rows = recent_notebook_content_rows(env)

    if not args.force_new and daily_post_exists(env, series_date):
        result = {
            "schema": "web-r.notebook.daily-result.v1",
            "inserted": False,
            "skipped": True,
            "reason": "daily_post_exists",
            "series_date": series_date,
        }
        emit_result(result, args.output)
        return 0

    spec = build_notebook_spec(series_date, existing_titles, recent_rows, force_new=args.force_new)
    runner_result = run_webr_runner(repo_root / args.runner, spec)
    row = build_clickhouse_row(spec, runner_result)

    if not args.dry_run:
        insert_json_each_row(env, "webr_webr.notebook", row)

    result = {
        "schema": "web-r.notebook.daily-result.v1",
        "inserted": not args.dry_run,
        "dry_run": args.dry_run,
        "skipped": False,
        "notebook_uuid": row["uuid"],
        "share_uuid": row["uuid_share"],
        "title": row["title"],
        "series_date": series_date,
        "url": f"/webr/notebook/view/{row['uuid_share']}/",
        "topic": spec["topic"]["key"],
        "style": spec["style"]["key"],
        "similarity_score": spec.get("similarity_guard", {}).get("max_similarity"),
        "similarity_matched_title": spec.get("similarity_guard", {}).get("matched_title", ""),
        "r_cell_count": len([cell for cell in spec["cells"] if cell["mode"] == "r"]),
    }
    emit_result(result, args.output)
    return 0


def load_env(path: Path) -> dict[str, str]:
    env = dict(os.environ)
    if path.exists():
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return env


def parse_series_date(value: str) -> str:
    if value:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    return datetime.now(KST).strftime("%Y-%m-%d")


def recent_notebook_style_keys(rows: list[dict[str, Any]]) -> list[str]:
    styles: list[str] = []
    for row in rows[:RECENT_STYLE_LOOKBACK]:
        meta = parse_json_maybe(row.get("data_meta"))
        style = meta.get("style") if isinstance(meta, dict) else ""
        if isinstance(style, dict):
            style_key = str(style.get("key", "")).strip()
        else:
            style_key = str(style or "").strip()
        if style_key:
            styles.append(style_key)
    return styles


def recent_notebook_topic_keys(rows: list[dict[str, Any]]) -> list[str]:
    topics: list[str] = []
    for row in rows[:RECENT_TOPIC_LOOKBACK]:
        meta = parse_json_maybe(row.get("data_meta"))
        topic = meta.get("topic") if isinstance(meta, dict) else ""
        if isinstance(topic, dict):
            topic_key = str(topic.get("key", "")).strip()
        else:
            topic_key = str(topic or "").strip()
        if not topic_key:
            topic_key = topic_key_from_title(str(row.get("title", "")))
        if topic_key:
            topics.append(topic_key)
    return topics


def topic_key_from_title(title: str) -> str:
    normalized = title.lower()
    for topic in TOPICS:
        if topic.key in normalized or topic.title.lower() in normalized:
            return topic.key
    return ""


def build_public_title(topic: Topic, style: NotebookStyle, existing_titles: set[str], seed: int) -> str:
    base_title = style.title_template.format(entity=topic.entity, topic=topic.title)
    candidates = [
        base_title,
        f"{base_title}: 민감도 점검",
        f"{base_title}: 다른 가정으로 다시 보기",
        f"{base_title}: 작은 표본 실험",
        f"{base_title}: 해석 프레임 바꾸기",
    ]
    start = seed % len(candidates)
    for offset in range(len(candidates)):
        candidate = sanitize_public_title(candidates[(start + offset) % len(candidates)])
        if candidate not in existing_titles:
            return candidate
    return candidates[0]


def sanitize_public_title(title: str) -> str:
    cleaned = str(title or "").strip()
    cleaned = re.sub(r"(?i)^daily\s+webr\s+notebook\s*:\s*", "", cleaned)
    cleaned = re.sub(r"\s*\((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{1,2}\)\s*", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*#[0-9a-fA-F]{4,12}\s*$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:")
    return cleaned or "Web-R Notebook 데이터 분석 노트"


def parse_json_maybe(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def notebook_content_text_from_spec(spec: dict[str, Any]) -> str:
    parts = [
        str(spec.get("title", "")),
        str(spec.get("description", "")),
        json.dumps(spec.get("topic", {}), ensure_ascii=False, sort_keys=True),
        json.dumps(spec.get("style", {}), ensure_ascii=False, sort_keys=True),
    ]
    for cell in spec.get("cells", []):
        if isinstance(cell, dict):
            parts.append(str(cell.get("mode", "")))
            parts.append(str(cell.get("source", "")))
    return "\n".join(part for part in parts if part)


def notebook_content_text_from_row(row: dict[str, Any]) -> str:
    parts = [str(row.get("title", "")), str(row.get("description", ""))]
    for key in ("data_markdown", "data_rcode"):
        payload = parse_json_maybe(row.get(key))
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    parts.append(str(item.get("source", "")))
                else:
                    parts.append(str(item))
        elif payload is not None:
            parts.append(str(payload))
    meta = parse_json_maybe(row.get("data_meta"))
    if isinstance(meta, dict):
        for key in ("series_date", "topic", "style"):
            if key in meta:
                parts.append(json.dumps(meta[key], ensure_ascii=False, sort_keys=True))
    return "\n".join(part for part in parts if part)


def content_tokens(text: str) -> set[str]:
    normalized = text.lower()
    normalized = re.sub(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}\b", " ", normalized)
    normalized = re.sub(r"#[0-9a-f]{4,}\b", " ", normalized)
    raw_tokens = re.findall(r"[0-9a-z_가-힣]{3,}", normalized)
    return {
        token
        for token in raw_tokens
        if not token.isdigit() and token not in STOP_TOKENS
    }


def content_similarity(left: str, right: str) -> float:
    left_tokens = content_tokens(left)
    right_tokens = content_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    smaller = min(len(left_tokens), len(right_tokens))
    jaccard = overlap / union
    containment = overlap / smaller
    return (0.65 * jaccard) + (0.35 * containment)


def max_recent_similarity(spec: dict[str, Any], recent_rows: list[dict[str, Any]]) -> tuple[float, str]:
    candidate_text = notebook_content_text_from_spec(spec)
    max_score = 0.0
    matched_title = ""
    for row in recent_rows:
        score = content_similarity(candidate_text, notebook_content_text_from_row(row))
        if score > max_score:
            max_score = score
            matched_title = str(row.get("title", ""))
    return max_score, matched_title


def build_notebook_spec(series_date: str, existing_titles: set[str], recent_rows: list[dict[str, Any]], *, force_new: bool) -> dict[str, Any]:
    base_seed = int(hashlib.sha256(f"webr-notebook:{series_date}".encode("utf-8")).hexdigest()[:8], 16)
    recent_styles = recent_notebook_style_keys(recent_rows)
    recent_topics = recent_notebook_topic_keys(recent_rows)
    best_spec: dict[str, Any] | None = None
    best_similarity = 1.0

    for attempt in range(MAX_CANDIDATE_ATTEMPTS):
        seed = (base_seed + attempt * 104729) & 0xFFFFFFFF
        spec = build_candidate_notebook_spec(
            series_date=series_date,
            existing_titles=existing_titles,
            recent_styles=recent_styles,
            recent_topics=recent_topics,
            seed=seed,
            attempt=attempt,
            force_new=force_new,
        )
        similarity, matched_title = max_recent_similarity(spec, recent_rows)
        spec["similarity_guard"] = {
            "threshold": SIMILARITY_THRESHOLD,
            "max_similarity": round(similarity, 4),
            "matched_title": matched_title,
            "compared_recent_count": len(recent_rows),
            "attempt": attempt + 1,
            "max_attempts": MAX_CANDIDATE_ATTEMPTS,
            "accepted": similarity <= SIMILARITY_THRESHOLD or not recent_rows,
        }
        if best_spec is None or similarity < best_similarity:
            best_spec = spec
            best_similarity = similarity
        if spec["similarity_guard"]["accepted"]:
            return spec

    if best_spec is None:
        raise RuntimeError("no Web-R Notebook candidate could be generated")
    best_spec["similarity_guard"]["fallback"] = "least_similar_after_retry"
    return best_spec


def build_candidate_notebook_spec(
    *,
    series_date: str,
    existing_titles: set[str],
    recent_styles: list[str],
    recent_topics: list[str],
    seed: int,
    attempt: int,
    force_new: bool,
) -> dict[str, Any]:
    style = choose_style(seed + attempt * 2, recent_styles)
    topic = choose_topic(seed, recent_topics, attempt)
    title = build_public_title(topic, style, existing_titles, seed + attempt)
    description = (
        f"{series_date} Web-R Notebook 자동 연재 글입니다. "
        f"{topic.source_note}를 만들고, `{style.label}` 형식으로 {topic.entity} 데이터를 분석합니다."
    )

    start_date = (datetime.strptime(series_date, "%Y-%m-%d") - timedelta(days=59)).strftime("%Y-%m-%d")
    n = 60
    change_point = 34 + (seed % 10)
    effect = 10 + (seed % 11)
    cells = build_style_cells(
        style=style,
        topic=topic,
        title=title,
        seed=seed,
        start_date=start_date,
        n=n,
        change_point=change_point,
        effect=effect,
    )
    return {
        "schema": "web-r.notebook.daily-spec.v1",
        "series_date": series_date,
        "notebook_uuid": str(uuid.uuid4()),
        "share_uuid": str(uuid.uuid4()),
        "title": title,
        "description": description,
        "topic": topic.__dict__,
        "style": style.__dict__,
        "seed": seed,
        "candidate_attempt": attempt,
        "cells": cells,
    }


def choose_style(seed: int, recent_styles: list[str]) -> NotebookStyle:
    recent = {style for style in recent_styles[:3] if style}
    for offset in range(len(STYLES)):
        candidate = STYLES[(seed + offset) % len(STYLES)]
        if candidate.key not in recent:
            return candidate
    return STYLES[seed % len(STYLES)]


def choose_topic(seed: int, recent_topics: list[str], attempt: int) -> Topic:
    recent = {topic for topic in recent_topics[:4] if topic}
    start = ((seed // max(1, len(STYLES))) + attempt * 2) % len(TOPICS)
    for offset in range(len(TOPICS)):
        candidate = TOPICS[(start + offset) % len(TOPICS)]
        if candidate.key not in recent:
            return candidate
    return TOPICS[start]


def build_style_cells(
    *,
    style: NotebookStyle,
    topic: Topic,
    title: str,
    seed: int,
    start_date: str,
    n: int,
    change_point: int,
    effect: int,
) -> list[dict[str, Any]]:
    plot_path = f"/tmp/webr_daily_{style.key}.svg"
    if style.key == "ranked-audit":
        r_setup = build_ranked_audit_r_code(seed=seed, topic=topic, plot_path=plot_path)
        r_summary = build_ranked_audit_summary_r_code(topic)
    elif style.key == "bootstrap-lab":
        r_setup = build_bootstrap_lab_r_code(seed=seed, topic=topic, plot_path=plot_path)
        r_summary = build_bootstrap_lab_summary_r_code(topic)
    elif style.key == "cohort-map":
        r_setup = build_cohort_map_r_code(seed=seed, topic=topic, plot_path=plot_path)
        r_summary = build_cohort_map_summary_r_code(topic)
    elif style.key == "scatter-diagnostics":
        r_setup = build_scatter_diagnostics_r_code(seed=seed, topic=topic, plot_path=plot_path)
        r_summary = build_scatter_diagnostics_summary_r_code(topic)
    else:
        r_setup = build_analysis_r_code(
            seed=seed,
            start_date=start_date,
            n=n,
            change_point=change_point,
            effect=effect,
            topic=topic,
            plot_path=plot_path,
        )
        r_summary = build_summary_r_code(topic)

    return [
        {
            "id": 1,
            "mode": "markdown",
            "source": (
                f"### {title}\n\n"
                f"{topic.background}\n\n"
                f"{style.question_prefix} 오늘의 질문은 **{topic.question}** 입니다.\n\n"
                f"{style.formula_note}\n\n"
                f"아래에서는 {topic.source_note}를 만든 뒤, WebR 안에서 그대로 다시 실행할 수 있는 base R 코드로 작은 분석을 진행합니다."
            ),
        },
        {"id": 2, "mode": "r", "source": r_setup, "plot_path": plot_path},
        {
            "id": 3,
            "mode": "markdown",
            "source": (
                "### 코드가 하고 있는 일\n\n"
                f"{style.method_note} 위 cell은 데이터 생성과 시각화를 같이 담았고, 아래 cell은 같은 객체에서 숫자 요약만 분리해 확인합니다."
            ),
        },
        {"id": 4, "mode": "r", "source": r_summary},
        {
            "id": 5,
            "mode": "markdown",
            "source": (
                "### 읽는 포인트\n\n"
                f"{style.closing} 오늘의 수치는 실제 운영 지표가 아니라 재현 가능한 예제 데이터이지만, 같은 접근은 실제 로그를 볼 때도 그대로 옮겨갈 수 있습니다."
            ),
        },
    ]


def build_analysis_r_code(*, seed: int, start_date: str, n: int, change_point: int, effect: int, topic: Topic, plot_path: str) -> str:
    return f"""# WebR data analysis note: {topic.key}
set.seed({seed})
day <- seq.Date(as.Date({r_string(start_date)}), by = "day", length.out = {n})
phase <- seq_along(day)
baseline <- {topic.base} + ({topic.slope}) * phase + {topic.amplitude} * sin(2 * pi * phase / 7)
intervention <- ifelse(phase >= {change_point}, {effect}, 0)
noise <- rnorm(length(day), mean = 0, sd = {topic.noise})
value <- round(pmax(1, baseline + intervention + noise))

daily <- data.frame(day = day, phase = phase, value = value)
daily$roll7 <- as.numeric(stats::filter(daily$value, rep(1 / 7, 7), sides = 1))
daily$residual <- daily$value - daily$roll7
daily$z <- as.numeric(scale(daily$residual))
daily$z[is.na(daily$z)] <- 0
daily$flag <- daily$z > {topic.threshold}

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(4.4, 4.7, 3.2, 1.2), bg = "white")
plot(
  daily$day, daily$value,
  type = "h", lwd = 4, lend = "round",
  col = ifelse(daily$flag, {r_string(topic.accent)}, {r_string(topic.color)}),
  xlab = "day", ylab = {r_string(topic.metric)},
  main = {r_string(topic.title)}
)
points(
  daily$day, daily$value,
  pch = 21,
  bg = ifelse(daily$flag, {r_string(topic.accent)}, {r_string(topic.color)}),
  col = "white", cex = 1.05
)
lines(daily$day, daily$roll7, col = "#111827", lwd = 3)
legend(
  "topleft",
  legend = c("daily value", "7-day moving average", "positive residual flag"),
  col = c({r_string(topic.color)}, "#111827", {r_string(topic.accent)}),
  lwd = c(4, 3, 0), pch = c(21, NA, 21),
  pt.bg = c({r_string(topic.color)}, NA, {r_string(topic.accent)}),
  bty = "n"
)
par(op)
grDevices::dev.off()
"""


def build_summary_r_code(topic: Topic) -> str:
    return f"""# Text summary from the generated data
fit <- stats::lm(value ~ phase, data = daily)
top <- daily[which.max(daily$value), ]
flag_days <- daily$day[daily$flag]
flag_text <- if (length(flag_days)) paste(format(flag_days, "%Y-%m-%d"), collapse = ", ") else "none"
cat({r_string(topic.entity)}, "analysis\\n")
cat("date range:", format(min(daily$day), "%Y-%m-%d"), "to", format(max(daily$day), "%Y-%m-%d"), "\\n")
cat("peak day:", format(top$day, "%Y-%m-%d"), "with", top$value, "\\n")
cat("linear slope per day:", round(unname(coef(fit)[["phase"]]), 3), "\\n")
cat("last 7-day moving average:", round(tail(stats::na.omit(daily$roll7), 1), 1), "\\n")
cat("flagged days:", flag_text, "\\n")
"""


def build_ranked_audit_r_code(*, seed: int, topic: Topic, plot_path: str) -> str:
    return f"""# Ranked audit: {topic.key}
set.seed({seed})
channel <- c("docs", "search", "examples", "forum", "video", "newsletter", "package page", "workshop")
raw_score <- {topic.base} + stats::rpois(length(channel), lambda = seq(12, 34, length.out = length(channel)))
tilt <- round(seq(length(channel), 1) * runif(length(channel), 0.5, 2.2))
value <- raw_score + sample(tilt)
audit <- data.frame(channel = channel, value = value)
audit <- audit[order(audit$value, decreasing = TRUE), ]
audit$share <- audit$value / sum(audit$value)
audit$cumulative <- cumsum(audit$share)
top3 <- head(audit, 3)

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(6.2, 4.7, 3.2, 4.2), bg = "white")
bar_mid <- barplot(
  audit$value,
  names.arg = audit$channel,
  las = 2,
  col = {r_string(topic.color)},
  border = "white",
  ylab = {r_string(topic.metric)},
  main = {r_string(topic.title)}
)
points(bar_mid, audit$value, pch = 21, bg = {r_string(topic.accent)}, col = "white", cex = 1.1)
par(new = TRUE)
plot(bar_mid, audit$cumulative, type = "b", pch = 19, axes = FALSE, xlab = "", ylab = "", col = "#111827", ylim = c(0, 1))
axis(4, at = seq(0, 1, by = 0.25), labels = paste0(seq(0, 100, by = 25), "%"))
mtext("cumulative share", side = 4, line = 2.7)
legend("bottomright", legend = c("value", "cumulative share"), fill = c({r_string(topic.color)}, NA), border = c("white", NA), lty = c(NA, 1), pch = c(NA, 19), col = c(NA, "#111827"), bty = "n")
par(op)
grDevices::dev.off()
"""


def build_ranked_audit_summary_r_code(topic: Topic) -> str:
    return f"""# Ranked contribution summary
top_names <- paste(top3$channel, collapse = ", ")
top_share <- sum(top3$share)
halfway <- audit$channel[which(audit$cumulative >= 0.5)[1]]
cat({r_string(topic.entity)}, "ranked audit\\n")
cat("top three:", top_names, "\\n")
cat("top-three share:", paste0(round(100 * top_share, 1), "%"), "\\n")
cat("first channel crossing 50% cumulative share:", halfway, "\\n")
cat("smallest channel:", tail(audit$channel, 1), "with", tail(audit$value, 1), "\\n")
"""


def build_bootstrap_lab_r_code(*, seed: int, topic: Topic, plot_path: str) -> str:
    return f"""# Bootstrap lab: {topic.key}
set.seed({seed})
n_control <- 180
n_treatment <- 176
p_control <- pmin(0.82, pmax(0.08, {topic.base} / ({topic.base} + 180)))
p_treatment <- pmin(0.9, p_control + runif(1, 0.015, 0.075))
control <- stats::rbinom(n_control, size = 1, prob = p_control)
treatment <- stats::rbinom(n_treatment, size = 1, prob = p_treatment)
observed_lift <- mean(treatment) - mean(control)
boot_lift <- replicate(
  1000,
  mean(sample(treatment, replace = TRUE)) - mean(sample(control, replace = TRUE))
)
interval <- stats::quantile(boot_lift, c(0.05, 0.5, 0.95))
prob_positive <- mean(boot_lift > 0)

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(4.5, 4.7, 3.2, 1.2), bg = "white")
hist(
  100 * boot_lift,
  breaks = 34,
  col = {r_string(topic.color)},
  border = "white",
  xlab = "bootstrap lift, percentage points",
  main = {r_string(topic.title)}
)
abline(v = 100 * observed_lift, col = {r_string(topic.accent)}, lwd = 3)
abline(v = 100 * interval[c(1, 3)], col = "#111827", lwd = 2, lty = 2)
legend("topright", legend = c("observed lift", "90% interval"), col = c({r_string(topic.accent)}, "#111827"), lwd = c(3, 2), lty = c(1, 2), bty = "n")
par(op)
grDevices::dev.off()
"""


def build_bootstrap_lab_summary_r_code(topic: Topic) -> str:
    return f"""# Bootstrap uncertainty summary
label <- if (prob_positive > 0.8) "mostly positive" else if (prob_positive < 0.2) "mostly negative" else "uncertain"
cat({r_string(topic.entity)}, "bootstrap lab\\n")
cat("control mean:", round(mean(control), 3), "\\n")
cat("treatment mean:", round(mean(treatment), 3), "\\n")
cat("observed lift:", paste0(round(100 * observed_lift, 2), " percentage points"), "\\n")
cat("90% bootstrap interval:", paste(round(100 * interval[c(1, 3)], 2), collapse = " to "), "\\n")
cat("Pr(lift > 0):", round(prob_positive, 3), "=>", label, "\\n")
"""


def build_cohort_map_r_code(*, seed: int, topic: Topic, plot_path: str) -> str:
    return f"""# Cohort map: {topic.key}
set.seed({seed})
cohort_labels <- paste("cohort", LETTERS[1:6])
week <- 0:5
start_level <- runif(length(cohort_labels), 0.74, 0.93)
decay <- runif(length(cohort_labels), 0.055, 0.14)
retention <- sapply(
  seq_along(week),
  function(w) pmax(0.04, start_level * exp(-decay * w) + rnorm(length(cohort_labels), 0, 0.018))
)
retention <- pmin(1, retention)
rownames(retention) <- cohort_labels
colnames(retention) <- paste0("week ", week)
drop_to_week5 <- retention[, 1] - retention[, 6]

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(4.6, 5.5, 3.2, 4.2), bg = "white")
palette <- grDevices::colorRampPalette(c("#f8fafc", {r_string(topic.color)}, {r_string(topic.accent)}))(30)
image(
  x = week,
  y = seq_along(cohort_labels),
  z = t(retention),
  col = palette,
  axes = FALSE,
  xlab = "weeks since start",
  ylab = "",
  main = {r_string(topic.title)}
)
axis(1, at = week, labels = paste0("w", week))
axis(2, at = seq_along(cohort_labels), labels = cohort_labels, las = 2)
box()
legend_values <- seq(0.2, 1.0, length.out = 5)
axis(4, at = seq(1, length(cohort_labels), length.out = 5), labels = paste0(round(100 * legend_values), "%"))
mtext("retention", side = 4, line = 2.7)
par(op)
grDevices::dev.off()
"""


def build_cohort_map_summary_r_code(topic: Topic) -> str:
    return f"""# Cohort retention summary
last_week <- retention[, ncol(retention)]
best <- names(which.max(last_week))
fastest_drop <- names(which.max(drop_to_week5))
cat({r_string(topic.entity)}, "cohort map\\n")
cat("best final cohort:", best, "at", paste0(round(100 * max(last_week), 1), "%"), "\\n")
cat("fastest drop:", fastest_drop, "lost", paste0(round(100 * max(drop_to_week5), 1), " points"), "\\n")
cat("median final retention:", paste0(round(100 * stats::median(last_week), 1), "%"), "\\n")
cat("matrix size:", nrow(retention), "cohorts x", ncol(retention), "weeks\\n")
"""


def build_scatter_diagnostics_r_code(*, seed: int, topic: Topic, plot_path: str) -> str:
    return f"""# Scatter diagnostics: {topic.key}
set.seed({seed})
n <- 72
exposure <- round(stats::runif(n, 15, 100), 1)
segment <- sample(c("small", "medium", "large"), n, replace = TRUE, prob = c(0.38, 0.42, 0.20))
segment_effect <- ifelse(segment == "large", 18, ifelse(segment == "medium", 8, 0))
outlier_boost <- ifelse(seq_len(n) %% 23 == 0, sample(c(-22, 28), n, replace = TRUE), 0)
response <- 18 + 0.74 * exposure + segment_effect + stats::rnorm(n, 0, 9) + outlier_boost
points_df <- data.frame(exposure = exposure, response = response, segment = segment)
fit <- stats::lm(response ~ exposure + segment, data = points_df)
resid_z <- as.numeric(scale(stats::resid(fit)))
points_df$flag <- abs(resid_z) > 1.6

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(4.5, 4.7, 3.2, 1.2), bg = "white")
plot(
  points_df$exposure, points_df$response,
  pch = 21,
  bg = ifelse(points_df$flag, {r_string(topic.accent)}, {r_string(topic.color)}),
  col = "white",
  cex = ifelse(points_df$flag, 1.35, 1.0),
  xlab = "synthetic exposure",
  ylab = {r_string(topic.metric)},
  main = {r_string(topic.title)}
)
simple_fit <- stats::lm(response ~ exposure, data = points_df)
abline(simple_fit, col = "#111827", lwd = 3)
legend("topleft", legend = c("regular point", "large residual", "simple trend"), pt.bg = c({r_string(topic.color)}, {r_string(topic.accent)}, NA), pch = c(21, 21, NA), lty = c(NA, NA, 1), lwd = c(NA, NA, 3), col = c("white", "white", "#111827"), bty = "n")
par(op)
grDevices::dev.off()
"""


def build_scatter_diagnostics_summary_r_code(topic: Topic) -> str:
    return f"""# Regression diagnostics summary
fit_summary <- summary(fit)
flagged <- sum(points_df$flag)
cat({r_string(topic.entity)}, "scatter diagnostics\\n")
cat("observations:", nrow(points_df), "\\n")
cat("model R-squared:", round(fit_summary$r.squared, 3), "\\n")
cat("exposure coefficient:", round(unname(stats::coef(fit)[["exposure"]]), 3), "\\n")
cat("large residual points:", flagged, "\\n")
cat("segment mix:", paste(names(table(points_df$segment)), as.integer(table(points_df$segment)), collapse = ", "), "\\n")
"""


def r_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def run_webr_runner(runner: Path, spec: dict[str, Any]) -> dict[str, Any]:
    if not runner.exists():
        raise SystemExit(f"webR runner is missing: {runner}")
    with tempfile.TemporaryDirectory(prefix="webr-notebook-") as tmp:
        spec_path = Path(tmp) / "spec.json"
        result_path = Path(tmp) / "result.json"
        spec_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")
        subprocess.run(
            ["node", str(runner), "--input", str(spec_path), "--output", str(result_path)],
            check=True,
        )
        return json.loads(result_path.read_text(encoding="utf-8"))


def build_clickhouse_row(spec: dict[str, Any], runner_result: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    cell_order = [cell["id"] for cell in spec["cells"]]
    cell_mode = {str(cell["id"]): cell["mode"] for cell in spec["cells"]}
    data_markdown = [
        {"id": cell["id"], "source": cell["source"]}
        for cell in spec["cells"]
        if cell["mode"] == "markdown"
    ]
    data_rcode = [
        {"id": cell["id"], "source": cell["source"]}
        for cell in spec["cells"]
        if cell["mode"] == "r"
    ]
    result_by_id = {int(item["id"]): item["output"] for item in runner_result.get("r_results", [])}
    data_rcode_result = [
        {"id": cell["id"], "output": result_by_id.get(cell["id"], {"type": "text", "text": "No output"})}
        for cell in spec["cells"]
        if cell["mode"] == "r"
    ]
    meta = {
        "version": "split-v1",
        "activeCellId": 2,
        "cell_order": cell_order,
        "cell_mode": cell_mode,
        "cell_mdPreview": {str(cell["id"]): cell["mode"] == "markdown" for cell in spec["cells"]},
        "cell_showCode": {str(cell["id"]): True for cell in spec["cells"]},
        "cell_showOutput": {str(cell["id"]): True for cell in spec["cells"]},
        "timestamp": datetime.now(ZoneInfo("UTC")).isoformat(timespec="seconds"),
        "runtime_sessionInfo": runner_result.get("runtime_sessionInfo", ""),
        "producer": "Statground_Data_R-project/scripts/generate_webr_notebook_daily.py",
        "source_repo": "Statground_Data_R-project",
        "series_date": spec["series_date"],
        "topic": spec["topic"]["key"],
        "style": spec["style"]["key"],
        "candidate_attempt": spec.get("candidate_attempt", 0),
        "similarity_guard": spec.get("similarity_guard", {}),
        "input_hash": hashlib.sha256(json.dumps(spec, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest(),
    }
    created_log = {
        "action": "publish_webr_notebook_daily",
        "operation_uuid": str(uuid.uuid4()),
        "producer": "github_actions_webr_notebook_daily",
        "source_repo": "Statground_Data_R-project",
        "uuid_user": NOTEBOOK_BOT_UUID,
        "created_by": "Web-R Notebook",
        "created_at_kst": now,
        "series_date": spec["series_date"],
        "topic": spec["topic"]["key"],
        "style": spec["style"]["key"],
        "candidate_attempt": spec.get("candidate_attempt", 0),
        "similarity_guard": spec.get("similarity_guard", {}),
        "title": spec["title"],
    }
    updated_log = {
        "action": "publish",
        "operation_uuid": str(uuid.uuid4()),
        "producer": "github_actions_webr_notebook_daily",
        "share": 1,
    }
    return {
        "uuid": spec["notebook_uuid"],
        "uuid_share": spec["share_uuid"],
        "uuid_user": NOTEBOOK_BOT_UUID,
        "active": 1,
        "share": 1,
        "title": spec["title"],
        "description": spec["description"],
        "created_at": now,
        "created_log": json.dumps(created_log, ensure_ascii=False, separators=(",", ":")),
        "updated_at": now,
        "updated_log": json.dumps(updated_log, ensure_ascii=False, separators=(",", ":")),
        "favoriate": 0,
        "favoriate_at": None,
        "data_markdown": json.dumps(data_markdown, ensure_ascii=False, separators=(",", ":")),
        "data_rcode": json.dumps(data_rcode, ensure_ascii=False, separators=(",", ":")),
        "data_rcode_result": json.dumps(data_rcode_result, ensure_ascii=False, separators=(",", ":")),
        "data_data": "[]",
        "data_rpackage": "[]",
        "data_meta": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def existing_notebook_titles(env: dict[str, str]) -> set[str]:
    sql = f"""
SELECT DISTINCT title
  FROM webr_webr.v_d1_notebook
 WHERE uuid_user = toUUID('{NOTEBOOK_BOT_UUID}')
   AND coalesce(active, 0) = 1
 FORMAT JSONEachRow
"""
    return {str(row.get("title", "")).strip() for row in clickhouse_json_each_row(env, sql) if str(row.get("title", "")).strip()}


def recent_notebook_content_rows(env: dict[str, str]) -> list[dict[str, Any]]:
    sql = f"""
SELECT
    ifNull(title, '') AS title,
    ifNull(description, '') AS description,
    ifNull(data_markdown, '') AS data_markdown,
    ifNull(data_rcode, '') AS data_rcode,
    ifNull(data_meta, '') AS data_meta,
    toString(created_at) AS created_at
  FROM webr_webr.v_d1_notebook
 WHERE uuid_user = toUUID('{NOTEBOOK_BOT_UUID}')
   AND coalesce(active, 0) = 1
 ORDER BY created_at DESC
 LIMIT {RECENT_CONTENT_LIMIT}
 FORMAT JSONEachRow
"""
    return clickhouse_json_each_row(env, sql)


def daily_post_exists(env: dict[str, str], series_date: str) -> bool:
    marker = json.dumps({"series_date": series_date}, separators=(",", ":"))[1:-1]
    sql = f"""
SELECT count() AS count
  FROM webr_webr.notebook
 WHERE uuid_user = toUUID('{NOTEBOOK_BOT_UUID}')
   AND coalesce(active, 0) = 1
   AND position(ifNull(created_log, ''), {quote_clickhouse_string(marker)}) > 0
 FORMAT JSONEachRow
"""
    rows = clickhouse_json_each_row(env, sql)
    return bool(rows and int(rows[0].get("count") or 0) > 0)


def clickhouse_json_each_row(env: dict[str, str], sql: str) -> list[dict[str, Any]]:
    request = urllib.request.Request(clickhouse_url(env), data=sql.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{first_env(env, 'CH_USER', 'CLICKHOUSE_USER')}:{first_env(env, 'CH_PASSWORD', 'CLICKHOUSE_PASSWORD')}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=40) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read(300).decode("utf-8", errors="replace")
        raise SystemExit(f"ClickHouse query failed: HTTP {exc.code}: {redact_detail(env, detail)}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"ClickHouse query failed: {exc.__class__.__name__}") from exc
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def insert_json_each_row(env: dict[str, str], table: str, row: dict[str, Any]) -> None:
    sql = f"INSERT INTO {table} FORMAT JSONEachRow\n" + json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
    request = urllib.request.Request(clickhouse_url(env), data=sql.encode("utf-8"), method="POST")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{first_env(env, 'CH_USER', 'CLICKHOUSE_USER')}:{first_env(env, 'CH_PASSWORD', 'CLICKHOUSE_PASSWORD')}".encode("utf-8")).decode("ascii"))
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(request, timeout=40) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read(300).decode("utf-8", errors="replace")
        raise SystemExit(f"ClickHouse insert failed: HTTP {exc.code}: {redact_detail(env, detail)}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"ClickHouse insert failed: {exc.__class__.__name__}") from exc


def clickhouse_url(env: dict[str, str]) -> str:
    try:
        return build_clickhouse_url(env, default_format="JSONEachRow", max_execution_time="60")
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc


def first_env(env: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = str(env.get(key, "") or "").strip()
        if value:
            return value
    raise SystemExit(f"required environment variable is missing: {keys[0]}")


def quote_clickhouse_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def redact_detail(env: dict[str, str], value: str) -> str:
    password = env.get("CH_PASSWORD") or env.get("CLICKHOUSE_PASSWORD") or ""
    if password:
        value = value.replace(password, "***")
    for key in ("CH_HOST", "CLICKHOUSE_HOST", "CH_URL", "CLICKHOUSE_URL", "CLICKHOUSE_HTTP_URL"):
        raw = str(env.get(key, "") or "").strip()
        if not raw:
            continue
        value = value.replace(raw, "<clickhouse-host>")
        try:
            parsed = urllib.parse.urlsplit(raw)
            if parsed.netloc:
                value = value.replace(parsed.netloc, "<clickhouse-host>")
            if parsed.hostname:
                value = value.replace(parsed.hostname, "<clickhouse-host>")
        except ValueError:
            pass
    return value


def emit_result(result: dict[str, Any], output_path: str) -> None:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output_path:
        Path(output_path).write_text(text + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    started = time.time()
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(json.dumps({"schema": "web-r.notebook.daily-result.v1", "inserted": False, "error": "webr_runner_failed", "returncode": exc.returncode, "elapsed_seconds": round(time.time() - started, 3)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(exc.returncode) from exc
