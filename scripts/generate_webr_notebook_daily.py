#!/usr/bin/env python3
"""Generate one public Web-R Notebook series post and insert it into ClickHouse."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
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


@dataclass(frozen=True)
class Topic:
    key: str
    title: str
    metric: str
    entity: str
    question: str
    source_note: str
    color: str
    accent: str
    base: int
    slope: float
    amplitude: float
    noise: float
    threshold: float


TOPICS = [
    Topic(
        key="package-download-pulse",
        title="R package download pulse",
        metric="simulated package downloads",
        entity="R 패키지 다운로드",
        question="최근 흐름에서 평소보다 강하게 튀는 날이 있었는가?",
        source_note="가상의 일별 패키지 다운로드 수",
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
        color="#0369a1",
        accent="#c2410c",
        base=75,
        slope=0.55,
        amplitude=10,
        noise=6.0,
        threshold=1.38,
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

    spec = build_notebook_spec(series_date, existing_titles, force_new=args.force_new)
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


def build_notebook_spec(series_date: str, existing_titles: set[str], *, force_new: bool) -> dict[str, Any]:
    seed = int(hashlib.sha256(f"webr-notebook:{series_date}".encode("utf-8")).hexdigest()[:8], 16)
    topic = TOPICS[seed % len(TOPICS)]
    suffix = datetime.strptime(series_date, "%Y-%m-%d").strftime("%b %d")
    title = f"Daily WebR Notebook: {topic.title} ({suffix})"
    if title in existing_titles or force_new:
        nonce = uuid.uuid4().hex[:6]
        title = f"{title} #{nonce}"
    description = (
        f"{series_date} Web-R Notebook 자동 연재 글입니다. "
        f"{topic.source_note}를 만들고, 이동평균과 잔차 z-score로 {topic.entity} 신호의 변화를 분석합니다."
    )

    start_date = (datetime.strptime(series_date, "%Y-%m-%d") - timedelta(days=59)).strftime("%Y-%m-%d")
    n = 60
    change_point = 34 + (seed % 10)
    effect = 10 + (seed % 11)
    source_plot = "/tmp/webr_daily_plot.svg"

    r_setup = build_analysis_r_code(
        seed=seed,
        start_date=start_date,
        n=n,
        change_point=change_point,
        effect=effect,
        topic=topic,
        plot_path=source_plot,
    )
    r_summary = build_summary_r_code(topic)
    cells = [
        {
            "id": 1,
            "mode": "markdown",
            "source": (
                f"### {title}\n\n"
                f"오늘의 질문은 **{topic.question}** 입니다. "
                f"데이터는 {topic.source_note}이며, WebR 안에서 바로 재현할 수 있도록 base R만 사용합니다."
            ),
        },
        {"id": 2, "mode": "r", "source": r_setup, "plot_path": source_plot},
        {"id": 3, "mode": "r", "source": r_summary},
        {
            "id": 4,
            "mode": "markdown",
            "source": (
                "### 읽는 포인트\n\n"
                "검은 선은 7일 이동평균이고, 강조된 점은 단기 기준선에서 크게 벗어난 날입니다. "
                "이 연재는 매일 다른 seed와 주제로 작은 데이터 분석 흐름을 만들고, 같은 R 코드를 Notebook에서 다시 실행할 수 있게 남깁니다."
            ),
        },
    ]
    return {
        "schema": "web-r.notebook.daily-spec.v1",
        "series_date": series_date,
        "notebook_uuid": str(uuid.uuid4()),
        "share_uuid": str(uuid.uuid4()),
        "title": title,
        "description": description,
        "topic": topic.__dict__,
        "seed": seed,
        "cells": cells,
    }


def build_analysis_r_code(*, seed: int, start_date: str, n: int, change_point: int, effect: int, topic: Topic, plot_path: str) -> str:
    return f"""# Daily WebR Notebook analysis: {topic.key}
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
