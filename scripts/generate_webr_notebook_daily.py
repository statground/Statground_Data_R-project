#!/usr/bin/env python3
"""Generate one public Web-R Notebook series post and insert it into ClickHouse."""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
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
RECENT_CONTENT_LIMIT = 36
MAX_CANDIDATE_ATTEMPTS = 512
RECENT_STYLE_LOOKBACK = 5
RECENT_TOPIC_LOOKBACK = 8
RECENT_PAIR_LOOKBACK = 16
# Full Notebook text includes a reusable base-R template, so scores around
# 0.58-0.61 are normal even when the source topic and validation experiment
# differ.  The previous 0.42 threshold was never attainable and silently fell
# back to the least-similar candidate.  The new hard threshold has no fallback;
# blueprint, title, recent topic/style and this text score must all pass.
SIMILARITY_THRESHOLD = 0.64
R_SET_SEED_MAX = 2_147_483_647
DEFAULT_SOURCE_CONTEXT_LOOKBACK_DAYS = 21
DEFAULT_SOURCE_CONTEXT_LIMIT = 24
SOURCE_CONTEXT_TOPIC_PREFIX = "source-context-"
SOURCE_CONTEXT_COLORS = [
    ("#2563eb", "#dc2626"),
    ("#0f766e", "#f97316"),
    ("#166534", "#b45309"),
    ("#0369a1", "#c2410c"),
    ("#4f46e5", "#db2777"),
    ("#0e7490", "#a21caf"),
]
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
    source_context: dict[str, str] | None = None


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


@dataclass(frozen=True)
class DiversityDimension:
    key: str
    label: str
    note: str
    title_suffix: str = ""


@dataclass(frozen=True)
class WebRPackageProfile:
    key: str
    package: str
    label: str
    note: str


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
    Topic(
        key="learning-path-completion",
        title="Learning path completion",
        metric="simulated lesson completions",
        entity="학습 경로 완료",
        question="어떤 구간에서 학습 완료가 부드럽게 이어지고, 어디에서 끊기는가?",
        source_note="가상의 학습 경로 단계별 완료 수",
        background="학습 경로 완료는 콘텐츠 난이도, 예제 데이터 품질, 다음 단계 안내가 함께 만든 결과입니다. 마지막 완료율만 보면 어느 단계에서 사용자가 멈췄는지 놓치기 쉽습니다.",
        color="#0e7490",
        accent="#a21caf",
        base=68,
        slope=0.28,
        amplitude=8,
        noise=4.9,
        threshold=1.33,
    ),
    Topic(
        key="question-tag-mix",
        title="Question tag mix",
        metric="simulated tagged questions",
        entity="질문 태그 구성",
        question="최근 질문이 특정 주제에 과도하게 몰리고 있는가?",
        source_note="가상의 질문 태그별 발생 수",
        background="질문 태그는 커뮤니티가 지금 어디에서 막히는지 보여주는 빠른 표식입니다. 하나의 인기 태그보다 태그 조합의 균형을 보면 문서 보강이나 예제 추가 우선순위를 정하기 쉽습니다.",
        color="#4f46e5",
        accent="#db2777",
        base=44,
        slope=0.22,
        amplitude=7,
        noise=4.1,
        threshold=1.36,
    ),
    Topic(
        key="example-copy-error-rate",
        title="Example copy error rate",
        metric="simulated example error rate",
        entity="예제 실행 오류율",
        question="예제를 따라 치는 과정의 오류율이 어떤 조건에서 높아지는가?",
        source_note="가상의 예제 실행 오류율",
        background="예제 실행 오류는 문법 자체보다 복사 과정, 패키지 설치 상태, 환경 차이에서 자주 생깁니다. 오류율의 분포를 보면 어떤 예제를 더 친절하게 바꿔야 할지 감이 생깁니다.",
        color="#b45309",
        accent="#0f766e",
        base=18,
        slope=-0.08,
        amplitude=5,
        noise=2.7,
        threshold=1.32,
    ),
    Topic(
        key="package-update-cadence",
        title="Package update cadence",
        metric="simulated package update intervals",
        entity="패키지 업데이트 간격",
        question="업데이트 간격이 안정적인지, 특정 구간에서 길어지는지 볼 수 있는가?",
        source_note="가상의 패키지 업데이트 간격",
        background="패키지 업데이트 간격은 유지보수 활력과 release 부담을 함께 반영합니다. 간격이 조금 길어졌다고 바로 위험은 아니지만, 분포가 넓어지면 관리해야 할 변동성이 커졌다는 신호일 수 있습니다.",
        color="#15803d",
        accent="#7c2d12",
        base=32,
        slope=0.12,
        amplitude=6,
        noise=4.4,
        threshold=1.34,
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
    NotebookStyle(
        key="seasonality-scan",
        label="Seasonality scan",
        title_prefix="Seasonality scan",
        question_prefix="오늘은 요일처럼 반복되는 패턴과 새로운 변화 신호를 분리해봅니다.",
        title_template="{entity}의 반복 패턴을 요일별로 나눠 보기",
        method_note="반복 패턴이 강한 지표는 전체 평균보다 요일별 기준선을 먼저 보는 편이 좋습니다. 같은 요일끼리 평균을 만든 뒤, 최근 값이 그 기준에서 얼마나 벗어났는지 확인합니다.",
        formula_note="요일별 기준선은 `b_d = mean(x_t | weekday(t)=d)`로 둡니다. 관측값과 `b_d`의 차이를 보면 반복과 변화가 조금 더 분리됩니다.",
        closing="요일별 기준선을 따로 세우면 평소 반복과 새 변화가 한 그림 안에서 덜 섞입니다.",
    ),
    NotebookStyle(
        key="distribution-shift",
        label="Distribution shift",
        title_prefix="Distribution shift",
        question_prefix="오늘은 앞부분과 뒷부분의 분포가 같은지 조심스럽게 비교합니다.",
        title_template="{entity} 분포가 최근에 이동했는지 비교하기",
        method_note="평균만 비교하면 꼬리나 산포 변화가 사라집니다. 그래서 기간을 둘로 나누고 boxplot과 분위수를 같이 보면서 분포 전체가 움직였는지 확인합니다.",
        formula_note="분포 이동은 `Q_recent(p) - Q_early(p)`처럼 같은 분위수의 차이로 읽습니다. 중앙값과 꼬리 분위수가 함께 움직이면 변화 해석에 힘이 실립니다.",
        closing="두 기간의 상자와 분위수 차이를 함께 보면 평균 하나보다 더 입체적인 변화가 보입니다.",
    ),
    NotebookStyle(
        key="threshold-lens",
        label="Threshold lens",
        title_prefix="Threshold lens",
        question_prefix="오늘은 임계값을 바꾸면 판단이 얼마나 달라지는지 살펴봅니다.",
        title_template="{entity} 판단 기준을 임계값 곡선으로 점검하기",
        method_note="어떤 기준 이상을 표시할지 정할 때는 임계값 하나를 고정하기 전에 precision과 recall의 균형을 봅니다. 임계값을 여러 개 움직이면 민감한 구간이 드러납니다.",
        formula_note="기본 지표는 `precision = TP / (TP + FP)`와 `recall = TP / (TP + FN)`입니다. 둘 중 하나만 보면 실제 판단 비용을 놓칠 수 있습니다.",
        closing="임계값 곡선은 정답 하나를 주기보다, 운영자가 감당할 오탐과 미탐의 균형점을 찾게 해줍니다.",
    ),
]

# The primary style is only one dimension of a Notebook.  These independent
# dimensions are materialized in the generated markdown and in a second WebR
# validation experiment.  Even with only the curated topics, the Cartesian
# product is comfortably larger than one daily post for 100 years.
DATA_DESIGNS = [
    DiversityDimension("longitudinal-block", "반복 측정 시계열", "같은 단위를 여러 시점에서 관측해 시간 의존성을 보존합니다."),
    DiversityDimension("overdispersed-count", "과산포 카운트", "평균보다 분산이 큰 횟수 자료를 만들어 단순 Poisson 가정을 의심합니다."),
    DiversityDimension("zero-inflated", "0이 많은 사건 자료", "사건이 전혀 없는 관측과 양의 관측이 섞인 구조를 따로 봅니다."),
    DiversityDimension("bounded-rate", "0과 1 사이 비율", "상한과 하한이 있는 비율 자료의 비대칭성을 보존합니다."),
    DiversityDimension("paired-change", "대응 전후 비교", "같은 대상의 전후 차이를 사용해 대상 간 이질성을 제거합니다."),
    DiversityDimension("clustered-sample", "군집 표본", "source나 cohort 안에서 서로 닮은 관측이 생기는 구조를 반영합니다."),
    DiversityDimension("heavy-tail", "꼬리가 두꺼운 자료", "소수의 큰 값이 평균을 흔드는 상황을 만들어 강건 통계를 비교합니다."),
    DiversityDimension("censored-time", "검열된 시간 자료", "관측 종료 전에 사건이 없었던 항목을 검열 표시와 함께 보존합니다."),
    DiversityDimension("compositional-share", "합이 1인 구성비", "여러 경로의 비중이 서로 독립적이지 않은 구성비 자료를 만듭니다."),
    DiversityDimension("irregular-interval", "불규칙 관측 간격", "관측 사이 간격이 일정하지 않은 로그 자료를 재현합니다."),
]

VALIDATION_LENSES = [
    DiversityDimension("temporal-holdout", "시간 순서 holdout", "앞 구간에서 세운 해석이 뒤 구간에서도 유지되는지 확인합니다.", "시간 순서 검증"),
    DiversityDimension("bootstrap-stability", "부트스트랩 안정성", "재표본추출마다 핵심 추정량이 얼마나 흔들리는지 확인합니다.", "재표본 안정성"),
    DiversityDimension("permutation-placebo", "순열 placebo", "집단표시를 섞었을 때도 같은 크기의 차이가 흔한지 비교합니다.", "placebo 점검"),
    DiversityDimension("leave-one-out", "leave-one-out 영향도", "관측 하나를 뺄 때 결론이 뒤집히는지 확인합니다.", "영향도 점검"),
    DiversityDimension("subgroup-consistency", "하위집단 일관성", "서로 다른 segment에서도 효과 방향이 같은지 비교합니다.", "집단별 일관성"),
    DiversityDimension("missingness-stress", "결측 민감도", "값이 선택적으로 빠지는 상황을 만들어 결론의 민감도를 봅니다.", "결측 민감도"),
    DiversityDimension("robust-estimator", "강건 추정 비교", "평균과 절사평균, 중앙값을 나란히 두어 극단값 의존도를 봅니다.", "강건성 비교"),
    DiversityDimension("threshold-sensitivity", "판정선 민감도", "판정선을 이동시키며 선택 비율과 결론 변화를 확인합니다.", "판정선 민감도"),
    DiversityDimension("sample-size-stability", "표본크기 안정성", "작은 표본부터 관측을 늘리며 추정량의 수렴을 확인합니다.", "표본크기 점검"),
    DiversityDimension("noise-stress", "잡음 stress test", "추가 잡음의 크기를 늘려 신호가 언제 사라지는지 확인합니다.", "잡음 stress test"),
    DiversityDimension("negative-control", "negative control", "관계가 없어야 하는 대조 변수를 넣어 가짜 신호 가능성을 봅니다.", "대조 변수 점검"),
    DiversityDimension("calibration-check", "보정도 점검", "예측된 수준과 실제 관측 수준이 구간별로 맞는지 비교합니다.", "보정도 점검"),
]

VISUAL_GRAMMARS = [
    DiversityDimension("ecdf", "누적분포 곡선", "개별 bin 선택에 덜 민감한 ECDF로 전체 분포를 비교합니다."),
    DiversityDimension("histogram", "분포 히스토그램", "추정량 또는 관측값의 분포 모양을 직접 확인합니다."),
    DiversityDimension("boxplot", "강건 요약 상자그림", "중앙값, 사분위 범위와 극단 관측을 함께 봅니다."),
    DiversityDimension("running-estimate", "누적 추정 곡선", "표본이 늘어날 때 추정량이 안정되는 과정을 선으로 봅니다."),
    DiversityDimension("rank-dot", "순위 점도표", "큰 관측부터 작은 관측까지 영향 순서를 비교합니다."),
    DiversityDimension("interval-forest", "구간 forest", "segment별 점추정과 불확실성 구간을 같은 축에 둡니다."),
    DiversityDimension("residual-map", "잔차 지도", "관측 순서와 모형 오차를 함께 그려 구조적 패턴을 찾습니다."),
    DiversityDimension("calibration-curve", "보정 곡선", "기대 수준과 관측 수준이 일치하는지 대각선과 비교합니다."),
]

NARRATIVE_FRAMES = [
    DiversityDimension("operator-decision", "운영 의사결정", "결과가 어떤 운영 선택을 바꾸는지부터 읽습니다.", "운영 선택"),
    DiversityDimension("teaching-first", "개념 학습", "처음 접하는 독자가 식과 그림을 연결할 수 있게 설명합니다.", "개념부터 읽기"),
    DiversityDimension("debugging", "분석 디버깅", "결론보다 먼저 어떤 가정이 깨질 수 있는지 추적합니다.", "가정 디버깅"),
    DiversityDimension("resource-allocation", "자원 배분", "한정된 시간과 자원을 어디에 먼저 투입할지 비교합니다.", "우선순위 결정"),
    DiversityDimension("reproducibility", "재현성", "같은 입력에서 결과가 다시 만들어지는지와 seed 의존성을 봅니다.", "재현성 점검"),
    DiversityDimension("risk", "위험 관리", "오탐, 미탐과 극단 상황의 비용을 중심으로 해석합니다.", "위험 관점"),
    DiversityDimension("counterfactual", "반사실 질문", "조건이 달랐다면 결과가 어떻게 달라졌을지 비교합니다.", "다른 조건 상상하기"),
    DiversityDimension("communication", "결과 커뮤니케이션", "숫자를 과장하지 않고 핵심 불확실성을 전달하는 방식을 봅니다.", "설명 방식 바꾸기"),
]

# Curated from the official repo.r-wasm.org R 4.6 package index.  The Node
# runner still installs and loads the selected package before executing any
# cell, so a stale catalog entry cannot produce a falsely successful post.
WEBR_PACKAGE_PROFILES = [
    WebRPackageProfile("matrix-sparse", "Matrix", "희소행렬", "Matrix::sparseMatrix로 드문 사건의 행렬 표현을 계산합니다."),
    WebRPackageProfile("boot-resample", "boot", "재표본추출", "boot::boot로 추정량의 재표본 분포를 계산합니다."),
    WebRPackageProfile("mass-robust", "MASS", "강건 회귀", "MASS::rlm으로 극단값에 덜 민감한 기울기를 비교합니다."),
    WebRPackageProfile("survival-time", "survival", "생존 시간", "survival::Surv와 survfit으로 사건까지의 시간을 요약합니다."),
    WebRPackageProfile("cluster-pam", "cluster", "군집 구조", "cluster::pam으로 관측 패턴을 세 군집으로 나눕니다."),
    WebRPackageProfile("mgcv-smooth", "mgcv", "비선형 평활", "mgcv::gam으로 시간에 따른 완만한 비선형 변화를 추정합니다."),
    WebRPackageProfile("nlme-grouped", "nlme", "군집별 모형", "nlme::lme로 segment별 절편 차이를 반영합니다."),
    WebRPackageProfile("data-table-group", "data.table", "고속 그룹 집계", "data.table 문법으로 segment별 요약을 계산합니다."),
    WebRPackageProfile("dplyr-summary", "dplyr", "파이프형 요약", "dplyr::summarise로 그룹별 중심과 산포를 계산합니다."),
    WebRPackageProfile("ggplot-build", "ggplot2", "그래프 문법", "ggplot2 객체를 만들고 ggplot_build 결과를 점검합니다."),
    WebRPackageProfile("tidyr-reshape", "tidyr", "긴 자료 변환", "tidyr::pivot_longer로 여러 측정값을 tidy long 형식으로 바꿉니다."),
    WebRPackageProfile("purrr-map", "purrr", "함수형 반복", "purrr::map_dbl로 segment별 강건 요약을 계산합니다."),
    WebRPackageProfile("stringr-token", "stringr", "문자 패턴", "stringr 함수로 분석 label의 토큰 패턴을 점검합니다."),
    WebRPackageProfile("broom-tidy", "broom", "모형 표 정리", "broom::tidy로 회귀 계수를 재사용 가능한 표로 만듭니다."),
    WebRPackageProfile("zoo-roll", "zoo", "이동 창", "zoo::rollmean으로 불규칙 신호의 이동 기준선을 계산합니다."),
    WebRPackageProfile("jsonlite-roundtrip", "jsonlite", "JSON 재현", "jsonlite로 요약 결과를 JSON 왕복 변환해 재현성을 확인합니다."),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env", help="optional env file path")
    parser.add_argument("--date", default="", help="series date in YYYY-MM-DD, default: today in Asia/Seoul")
    parser.add_argument("--runner", default="scripts/webr_notebook_runner.mjs", help="Node webR runner path")
    parser.add_argument("--output", default="", help="write generated notebook JSON summary to this path")
    parser.add_argument(
        "--batch-history",
        default="",
        help="optional JSONL results from earlier posts in the same batch; excludes them before DB visibility catches up",
    )
    parser.add_argument("--dry-run", action="store_true", help="run WebR and build the row without inserting it")
    parser.add_argument("--force-new", action="store_true", help="allow more than one generated post for the same date")
    parser.add_argument("--validate-style-templates", action="store_true", help="run every Notebook style template through webR and exit")
    args = parser.parse_args()

    repo_root = Path.cwd()
    if args.validate_style_templates:
        result = validate_style_templates(repo_root / args.runner)
        emit_result(result, args.output)
        return 0

    env = load_env(repo_root / args.env)
    series_date = parse_series_date(args.date)
    existing_titles = existing_notebook_titles(env)
    recent_rows = recent_notebook_content_rows(env)
    published_blueprints = published_notebook_blueprint_fingerprints(env)
    if args.batch_history:
        merge_batch_history_exclusions(
            Path(args.batch_history),
            existing_titles=existing_titles,
            recent_rows=recent_rows,
            published_blueprints=published_blueprints,
        )

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

    topic_pool = build_notebook_topic_pool(env, series_date)
    spec = build_notebook_spec(
        series_date,
        existing_titles,
        recent_rows,
        topic_pool=topic_pool,
        force_new=args.force_new,
        published_blueprints=published_blueprints,
    )
    runner_result = run_webr_runner(repo_root / args.runner, spec)
    row = build_clickhouse_row(spec, runner_result)

    insert_state = {
        "inserted": not args.dry_run,
        "insert_deferred": False,
        "insert_failure": "",
    }
    if not args.dry_run:
        insert_state = insert_json_each_row(env, "webr_webr.notebook", row)

    topic_source_context = spec.get("topic", {}).get("source_context") or {}
    result = {
        "schema": "web-r.notebook.daily-result.v1",
        "inserted": bool(insert_state.get("inserted")),
        "insert_deferred": bool(insert_state.get("insert_deferred")),
        "insert_failure": insert_state.get("insert_failure", ""),
        "dry_run": args.dry_run,
        "skipped": False,
        "notebook_uuid": row["uuid"],
        "share_uuid": row["uuid_share"],
        "title": row["title"],
        "series_date": series_date,
        "url": f"/webr/notebook/view/{row['uuid_share']}/",
        "topic": spec["topic"]["key"],
        "topic_source": topic_source_context.get("context_kind", "curated_static"),
        "style": spec["style"]["key"],
        "blueprint_fingerprint": spec["blueprint"]["fingerprint"],
        "data_design": spec["blueprint"]["data_design"]["key"],
        "validation_lens": spec["blueprint"]["validation_lens"]["key"],
        "visual_grammar": spec["blueprint"]["visual_grammar"]["key"],
        "narrative_frame": spec["blueprint"]["narrative_frame"]["key"],
        "webr_package": spec["blueprint"]["package_profile"]["package"],
        "webr_package_profile": spec["blueprint"]["package_profile"]["key"],
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


def merge_batch_history_exclusions(
    path: Path,
    *,
    existing_titles: set[str],
    recent_rows: list[dict[str, Any]],
    published_blueprints: set[str],
) -> int:
    """Exclude earlier batch results without waiting for Distributed-table visibility."""
    if not path.exists():
        return 0

    added_rows: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        result = json.loads(line)
        title = str(result.get("title", "")).strip()
        fingerprint = str(result.get("blueprint_fingerprint", "")).strip().lower()
        if title:
            existing_titles.add(title)
        if re.fullmatch(r"[0-9a-f]{64}", fingerprint):
            published_blueprints.add(fingerprint)

        blueprint = {
            "fingerprint": fingerprint,
            "data_design": {"key": str(result.get("data_design", ""))},
            "validation_lens": {"key": str(result.get("validation_lens", ""))},
            "visual_grammar": {"key": str(result.get("visual_grammar", ""))},
            "narrative_frame": {"key": str(result.get("narrative_frame", ""))},
            "package_profile": {
                "key": str(result.get("webr_package_profile") or result.get("webr_package", "")),
            },
        }
        added_rows.append(
            {
                "title": title,
                "description": "",
                "data_markdown": "",
                "data_rcode": "",
                "data_meta": json.dumps({"blueprint": blueprint}, ensure_ascii=False, separators=(",", ":")),
                "created_at": str(result.get("series_date", "")),
            }
        )

    if added_rows:
        recent_rows[:0] = reversed(added_rows)
    return len(added_rows)


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


def recent_notebook_pairs(rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for row in rows[:RECENT_PAIR_LOOKBACK]:
        meta = parse_json_maybe(row.get("data_meta"))
        topic_key = ""
        style_key = ""
        if isinstance(meta, dict):
            topic = meta.get("topic")
            style = meta.get("style")
            topic_key = str(topic.get("key", "") if isinstance(topic, dict) else topic or "").strip()
            style_key = str(style.get("key", "") if isinstance(style, dict) else style or "").strip()
        if not topic_key:
            topic_key = topic_key_from_title(str(row.get("title", "")))
        if topic_key and style_key:
            pairs.add((topic_key, style_key))
    return pairs


def recent_blueprint_dimension_keys(rows: list[dict[str, Any]], dimension: str, lookback: int) -> set[str]:
    keys: set[str] = set()
    for row in rows[:lookback]:
        meta = parse_json_maybe(row.get("data_meta"))
        blueprint = meta.get("blueprint") if isinstance(meta, dict) else None
        value = blueprint.get(dimension) if isinstance(blueprint, dict) else None
        key = str(value.get("key", "") if isinstance(value, dict) else value or "").strip()
        if key:
            keys.add(key)
    return keys


def topic_key_from_title(title: str) -> str:
    normalized = title.lower()
    for topic in TOPICS:
        if topic.key in normalized or topic.title.lower() in normalized:
            return topic.key
    return ""


def build_diversity_blueprint(topic: Topic, style: NotebookStyle, seed: int, attempt: int) -> dict[str, Any]:
    # Mixed-radix selection makes every dimension advance at a different pace,
    # while the persisted fingerprint remains the final authority on reuse.
    cursor = (int(seed) + attempt * 1_000_003) & 0xFFFFFFFFFFFFFFFF
    data_design = DATA_DESIGNS[cursor % len(DATA_DESIGNS)]
    cursor //= len(DATA_DESIGNS)
    validation_lens = VALIDATION_LENSES[cursor % len(VALIDATION_LENSES)]
    cursor //= len(VALIDATION_LENSES)
    visual_grammar = VISUAL_GRAMMARS[cursor % len(VISUAL_GRAMMARS)]
    cursor //= len(VISUAL_GRAMMARS)
    narrative_frame = NARRATIVE_FRAMES[cursor % len(NARRATIVE_FRAMES)]
    cursor //= len(NARRATIVE_FRAMES)
    package_profile = WEBR_PACKAGE_PROFILES[cursor % len(WEBR_PACKAGE_PROFILES)]
    components = {
        "topic": topic.key,
        "style": style.key,
        "data_design": data_design.key,
        "validation_lens": validation_lens.key,
        "visual_grammar": visual_grammar.key,
        "narrative_frame": narrative_frame.key,
        "package_profile": package_profile.key,
    }
    canonical = json.dumps(components, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        **components,
        "data_design": data_design.__dict__,
        "validation_lens": validation_lens.__dict__,
        "visual_grammar": visual_grammar.__dict__,
        "narrative_frame": narrative_frame.__dict__,
        "package_profile": package_profile.__dict__,
        "fingerprint": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "space_size_per_topic": len(STYLES) * len(DATA_DESIGNS) * len(VALIDATION_LENSES) * len(VISUAL_GRAMMARS) * len(NARRATIVE_FRAMES) * len(WEBR_PACKAGE_PROFILES),
    }


def build_public_title(topic: Topic, style: NotebookStyle, blueprint: dict[str, Any], existing_titles: set[str], seed: int) -> str:
    base_title = style.title_template.format(entity=topic.entity, topic=topic.title)
    validation_suffix = str(blueprint["validation_lens"].get("title_suffix") or blueprint["validation_lens"]["label"])
    narrative_suffix = str(blueprint["narrative_frame"].get("title_suffix") or blueprint["narrative_frame"]["label"])
    visual_label = str(blueprint["visual_grammar"]["label"])
    data_design_label = str(blueprint["data_design"]["label"])
    candidates = [
        f"{base_title}: {validation_suffix}",
        f"{base_title}: {narrative_suffix}",
        f"{base_title}: {visual_label}로 확인하기",
        f"{base_title}: {data_design_label}{korean_instrumental_particle(data_design_label)} 다시 보기",
        f"{base_title}: {narrative_suffix}에서 {validation_suffix}",
        f"{base_title}: {visual_label}과 {validation_suffix}",
    ]
    existing_keys = {title_identity_key(title) for title in existing_titles if title_identity_key(title)}
    start = seed % len(candidates)
    for offset in range(len(candidates)):
        candidate = sanitize_public_title(candidates[(start + offset) % len(candidates)])
        if candidate not in existing_titles and title_identity_key(candidate) not in existing_keys:
            return candidate
    short_fingerprint = str(blueprint["fingerprint"])[:8]
    return sanitize_public_title(f"{base_title}: {validation_suffix} {short_fingerprint}")


def korean_instrumental_particle(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "으로"
    code = ord(text[-1])
    if 0xAC00 <= code <= 0xD7A3:
        final_consonant = (code - 0xAC00) % 28
        return "로" if final_consonant in (0, 8) else "으로"
    return "로"


def sanitize_public_title(title: str) -> str:
    cleaned = str(title or "").strip()
    cleaned = re.sub(r"(?i)^daily\s+webr\s+notebook\s*:\s*", "", cleaned)
    cleaned = re.sub(r"\s*\((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{1,2}\)\s*", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*#[0-9a-fA-F]{4,12}\s*$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:")
    return cleaned or "Web-R Notebook 데이터 분석 노트"


def title_identity_key(title: str) -> str:
    cleaned = sanitize_public_title(title).lower()
    cleaned = re.sub(r"[:：]\s*(민감도 점검|다른 가정으로 다시 보기|작은 표본 실험|해석 프레임 바꾸기|기준선 바꿔 보기|분포까지 함께 보기|운영 질문으로 다시 읽기|실험 노트 \d+)\s*$", "", cleaned)
    cleaned = re.sub(r"[^0-9a-z가-힣]+", "", cleaned)
    return cleaned


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
        json.dumps(spec.get("blueprint", {}), ensure_ascii=False, sort_keys=True),
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
        for key in ("series_date", "topic", "style", "blueprint"):
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


def build_notebook_topic_pool(env: dict[str, str], series_date: str) -> list[Topic]:
    if not env_bool(env, "WEBR_NOTEBOOK_SOURCE_CONTEXT_ENABLED", True):
        return TOPICS

    dynamic_topics: list[Topic] = []
    seen_keys: set[str] = set()
    for row in load_notebook_source_context_rows(env, series_date):
        topic = topic_from_source_context(row)
        if topic is None or topic.key in seen_keys:
            continue
        dynamic_topics.append(topic)
        seen_keys.add(topic.key)

    if dynamic_topics:
        print(f"[notebook] source context topic candidates={len(dynamic_topics)}", file=sys.stderr)
        return dynamic_topics + TOPICS

    print("[notebook] source context topic candidates=0 fallback=curated_static", file=sys.stderr)
    return TOPICS


def load_notebook_source_context_rows(env: dict[str, str], series_date: str) -> list[dict[str, Any]]:
    lookback_days = max(1, min(120, env_int(env, "WEBR_NOTEBOOK_SOURCE_LOOKBACK_DAYS", DEFAULT_SOURCE_CONTEXT_LOOKBACK_DAYS)))
    limit = max(0, min(80, env_int(env, "WEBR_NOTEBOOK_SOURCE_LIMIT", DEFAULT_SOURCE_CONTEXT_LIMIT)))
    if limit == 0:
        return []

    cutoff_date = (datetime.strptime(series_date, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    cutoff_dt = f"{cutoff_date} 00:00:00"
    per_source_limit = max(4, limit)

    digest_sql = f"""
SELECT
    'community_digest' AS context_kind,
    source_type,
    source_id,
    source_name,
    platform,
    source_url,
    title,
    summary,
    toString(digest_uuid) AS context_id,
    toString(digest_date) AS published_at,
    toString(item_count) AS item_count,
    toString(deduped_item_count) AS deduped_item_count
  FROM Data_R_Community_Service.v_r_community_daily_digest_latest
 WHERE digest_date >= toDate({quote_clickhouse_string(cutoff_date)})
   AND notEmpty(title)
 ORDER BY digest_date DESC, updated_at DESC
 LIMIT {per_source_limit}
 FORMAT JSONEachRow
"""
    item_sql = f"""
SELECT
    'community_item' AS context_kind,
    source_type,
    source_id,
    source_name,
    platform,
    source_url,
    canonical_url AS canonical_url,
    title,
    summary,
    toString(item_uuid) AS context_id,
    toString(coalesce(original_published_at, published_at, collected_at)) AS published_at,
    '' AS item_count,
    '' AS deduped_item_count
  FROM Data_R_Community_Service.r_community_item_read_current
 WHERE active = 1
   AND collected_at >= toDateTime64({quote_clickhouse_string(cutoff_dt)}, 3, 'Asia/Seoul')
   AND notEmpty(title)
 ORDER BY collected_at DESC
 LIMIT {per_source_limit}
 FORMAT JSONEachRow
"""
    package_sql = f"""
SELECT
    'package_profile' AS context_kind,
    'package_index' AS source_type,
    package_name AS source_id,
    repository AS source_name,
    'r-package' AS platform,
    '' AS source_url,
    concat(package_name, ' ', latest_version) AS title,
    if(notEmpty(description), description, title) AS summary,
    package_name AS context_id,
    toString(last_observed_at) AS published_at,
    '' AS item_count,
    '' AS deduped_item_count
 FROM Data_R_Package_Service.package_current
 WHERE last_observed_at >= toDateTime64({quote_clickhouse_string(cutoff_dt)}, 3, 'Asia/Seoul')
   AND notEmpty(package_name)
 ORDER BY last_observed_at DESC, cityHash64(package_name, {quote_clickhouse_string(series_date)}) ASC
 LIMIT {max(4, min(12, per_source_limit))}
 FORMAT JSONEachRow
"""
    groups = [
        optional_clickhouse_json_each_row(env, digest_sql, "community-digest"),
        optional_clickhouse_json_each_row(env, item_sql, "community-item"),
        optional_clickhouse_json_each_row(env, package_sql, "package-profile"),
    ]
    return interleave_context_rows(groups, limit)


def optional_clickhouse_json_each_row(env: dict[str, str], sql: str, label: str) -> list[dict[str, Any]]:
    try:
        return clickhouse_json_each_row(env, sql)
    except SystemExit:
        print(f"[notebook] source context skipped={label} reason=clickhouse-query-error", file=sys.stderr)
        return []


def interleave_context_rows(groups: list[list[dict[str, Any]]], limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    max_len = max((len(group) for group in groups), default=0)
    for index in range(max_len):
        for group in groups:
            if index >= len(group):
                continue
            row = group[index]
            identity = source_context_identity(row)
            if identity in seen:
                continue
            seen.add(identity)
            rows.append(row)
            if len(rows) >= limit:
                return rows
    return rows


def topic_from_source_context(row: dict[str, Any]) -> Topic | None:
    title = clean_source_text(row.get("title"), 120)
    if not title:
        return None
    summary = clean_source_text(row.get("summary"), 180)
    source_name = clean_source_text(row.get("source_name"), 48) or clean_source_text(row.get("platform"), 32) or "R community"
    source_type = clean_source_text(row.get("source_type"), 48)
    platform = clean_source_text(row.get("platform"), 32)
    context_kind = clean_source_text(row.get("context_kind"), 32) or "community_item"
    context_id = clean_source_text(row.get("context_id"), 80)
    source_id = clean_source_text(row.get("source_id"), 80)
    published_at = clean_source_text(row.get("published_at"), 32)
    source_url = clean_source_url(row)
    identity = source_context_identity(row)
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    seed = int(digest[:12], 16)
    color, accent = SOURCE_CONTEXT_COLORS[seed % len(SOURCE_CONTEXT_COLORS)]
    context_label = source_context_label(context_kind, source_type, platform)
    entity = source_context_entity(context_kind, source_type, source_name, title, source_id)
    metric = source_context_metric(context_kind, source_type)
    question = source_context_question(context_kind, source_type)
    title_hint = shorten_text(title, 56)
    source_phrase = f"{source_name}의 공개 글감" if source_name else "최근 R 공개 글감"
    background = (
        f"최근 수집된 {source_phrase} `{title_hint}`은 {context_label}을 작은 데이터 질문으로 바꿔 볼 만한 단서입니다. "
        "이 Notebook은 원문을 복제하지 않고, 그 맥락에서 보이는 관심 방향을 재현용 예제 데이터로 바꿔 분석합니다."
    )
    if summary:
        background += " 수집 요약은 주제 선택의 힌트로만 사용하고, 아래 분석 값은 모두 재현 가능한 예제 데이터입니다."

    return Topic(
        key=f"{SOURCE_CONTEXT_TOPIC_PREFIX}{digest[:14]}",
        title=f"R ecosystem source pulse {digest[:4]}",
        metric=metric,
        entity=entity,
        question=question,
        source_note=f"최근 R ecosystem/community 수집 컨텍스트 `{title_hint}`에서 얻은 재현용 관심도 지표",
        background=background,
        color=color,
        accent=accent,
        base=34 + (seed % 96),
        slope=round((((seed >> 8) % 90) - 25) / 100, 2),
        amplitude=5 + ((seed >> 16) % 16),
        noise=round(2.4 + ((seed >> 24) % 72) / 10, 1),
        threshold=round(1.24 + ((seed >> 32) % 28) / 100, 2),
        source_context={
            "context_kind": context_kind,
            "source_type": source_type,
            "source_id": source_id,
            "source_name": source_name,
            "platform": platform,
            "context_id": context_id,
            "source_title": title,
            "source_url": source_url,
            "published_at": published_at,
        },
    )


def source_context_identity(row: dict[str, Any]) -> str:
    return "|".join(
        clean_source_text(row.get(key), 160)
        for key in ("context_kind", "source_type", "source_id", "context_id", "title")
    )


def source_context_label(context_kind: str, source_type: str, platform: str) -> str:
    combined = f"{context_kind} {source_type} {platform}".lower()
    if "package" in combined:
        return "패키지 업데이트와 유지보수 신호"
    if "qna" in combined or "stackoverflow" in combined or "question" in combined:
        return "질문과 답변의 관심 흐름"
    if "digest" in combined:
        return "커뮤니티 요약에서 보이는 반복 주제"
    if "forum" in combined or "community" in combined:
        return "커뮤니티 토론의 방향"
    if "social" in combined or "fediverse" in combined or "mastodon" in combined:
        return "소셜 커뮤니티 반응"
    return "R 에코시스템 공개 글감"


def source_context_entity(context_kind: str, source_type: str, source_name: str, title: str, source_id: str) -> str:
    combined = f"{context_kind} {source_type}".lower()
    if "package" in combined:
        package = source_id or (title.split()[0] if title.split() else title)
        package = shorten_text(package, 26)
        return f"{package} 패키지 업데이트"
    if "qna" in combined or "question" in combined:
        return f"{shorten_text(source_name, 24)} 질문 흐름"
    if "digest" in combined:
        return f"{shorten_text(source_name, 24)} 커뮤니티 요약"
    if "social" in combined or "fediverse" in combined:
        return "R 커뮤니티 반응"
    return f"{shorten_text(source_name, 24)} 에코시스템 글감"


def source_context_metric(context_kind: str, source_type: str) -> str:
    combined = f"{context_kind} {source_type}".lower()
    if "package" in combined:
        return "simulated package update attention"
    if "qna" in combined or "question" in combined:
        return "simulated question attention"
    if "digest" in combined:
        return "simulated community digest attention"
    if "social" in combined or "fediverse" in combined:
        return "simulated community reaction"
    return "simulated ecosystem attention"


def source_context_question(context_kind: str, source_type: str) -> str:
    combined = f"{context_kind} {source_type}".lower()
    if "package" in combined:
        return "최근 패키지 업데이트 신호가 기준선에서 벗어났는가?"
    if "qna" in combined or "question" in combined:
        return "질문과 답변 흐름이 특정 주제에 집중되고 있는가?"
    if "digest" in combined:
        return "커뮤니티 요약에서 반복되는 관심이 실제 신호처럼 보이는가?"
    if "social" in combined or "fediverse" in combined:
        return "소셜 반응이 짧은 관심 급등으로 이어질 수 있는가?"
    return "최근 공개 글감이 R 에코시스템 관심 이동으로 이어질 수 있는가?"


def clean_source_url(row: dict[str, Any]) -> str:
    for key in ("canonical_url", "source_url"):
        value = clean_source_text(row.get(key), 260)
        if value.startswith(("http://", "https://")):
            return value
    return ""


def clean_source_text(value: Any, max_len: int) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("`", "'")
    return shorten_text(text, max_len)


def shorten_text(value: str, max_len: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 1)].rstrip() + "…"


def build_notebook_spec(
    series_date: str,
    existing_titles: set[str],
    recent_rows: list[dict[str, Any]],
    *,
    topic_pool: list[Topic],
    force_new: bool,
    published_blueprints: set[str] | None = None,
) -> dict[str, Any]:
    base_seed = int(hashlib.sha256(f"webr-notebook:{series_date}".encode("utf-8")).hexdigest()[:8], 16)
    recent_styles = recent_notebook_style_keys(recent_rows)
    recent_topics = recent_notebook_topic_keys(recent_rows)
    recent_pairs = recent_notebook_pairs(recent_rows)
    recent_blueprint_dimensions = {
        "data_design": recent_blueprint_dimension_keys(recent_rows, "data_design", 5),
        "validation_lens": recent_blueprint_dimension_keys(recent_rows, "validation_lens", 5),
        "visual_grammar": recent_blueprint_dimension_keys(recent_rows, "visual_grammar", 5),
        "narrative_frame": recent_blueprint_dimension_keys(recent_rows, "narrative_frame", 5),
        "package_profile": recent_blueprint_dimension_keys(recent_rows, "package_profile", 8),
    }
    existing_title_keys = {title_identity_key(title) for title in existing_titles if title_identity_key(title)}
    published_blueprints = set(published_blueprints or set())

    for attempt in range(MAX_CANDIDATE_ATTEMPTS):
        seed = (base_seed + attempt * 104729) & 0xFFFFFFFF
        spec = build_candidate_notebook_spec(
            series_date=series_date,
            existing_titles=existing_titles,
            recent_styles=recent_styles,
            recent_topics=recent_topics,
            topic_pool=topic_pool,
            seed=seed,
            attempt=attempt,
            force_new=force_new,
        )
        similarity, matched_title = max_recent_similarity(spec, recent_rows)
        topic_key = str(spec.get("topic", {}).get("key", ""))
        style_key = str(spec.get("style", {}).get("key", ""))
        title_key = title_identity_key(str(spec.get("title", "")))
        blueprint_fingerprint = str(spec.get("blueprint", {}).get("fingerprint", ""))
        title_duplicate = bool(title_key and title_key in existing_title_keys)
        blueprint_duplicate = not blueprint_fingerprint or blueprint_fingerprint in published_blueprints
        repeated_blueprint_dimensions = {
            dimension: str(spec.get("blueprint", {}).get(dimension, {}).get("key", "")) in recent_keys
            for dimension, recent_keys in recent_blueprint_dimensions.items()
        }
        pair_duplicate = bool(topic_key and style_key and (topic_key, style_key) in recent_pairs)
        topic_recent = topic_key in set(recent_topics[:4])
        style_recent = style_key in set(recent_styles[:3])
        accepted = (
            (similarity <= SIMILARITY_THRESHOLD or not recent_rows)
            and not title_duplicate
            and not blueprint_duplicate
            and not any(repeated_blueprint_dimensions.values())
            and not pair_duplicate
            and not (topic_recent and style_recent)
        )
        spec["similarity_guard"] = {
            "threshold": SIMILARITY_THRESHOLD,
            "max_similarity": round(similarity, 4),
            "matched_title": matched_title,
            "compared_recent_count": len(recent_rows),
            "title_duplicate": title_duplicate,
            "blueprint_duplicate": blueprint_duplicate,
            "blueprint_fingerprint": blueprint_fingerprint,
            "repeated_blueprint_dimensions": repeated_blueprint_dimensions,
            "published_blueprint_count": len(published_blueprints),
            "pair_duplicate": pair_duplicate,
            "topic_recent": topic_recent,
            "style_recent": style_recent,
            "attempt": attempt + 1,
            "max_attempts": MAX_CANDIDATE_ATTEMPTS,
            "accepted": accepted,
        }
        if accepted:
            return spec

    raise RuntimeError(
        "no novel Web-R Notebook blueprint passed the title, history, and similarity guards; refusing to publish a repeated design"
    )


def build_candidate_notebook_spec(
    *,
    series_date: str,
    existing_titles: set[str],
    recent_styles: list[str],
    recent_topics: list[str],
    topic_pool: list[Topic],
    seed: int,
    attempt: int,
    force_new: bool,
) -> dict[str, Any]:
    style = choose_style(seed + attempt * 2, recent_styles)
    topic = choose_topic(seed, recent_topics, attempt, topic_pool)
    blueprint = build_diversity_blueprint(topic, style, seed, attempt)
    title = build_public_title(topic, style, blueprint, existing_titles, seed + attempt)
    description = (
        f"{topic.source_note}를 {blueprint['data_design']['label']} 구조로 만들고, "
        f"`{style.label}` 분석 뒤 {blueprint['validation_lens']['label']}과 "
        f"{blueprint['visual_grammar']['label']}을 사용해 결론을 다시 점검합니다."
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
        blueprint=blueprint,
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
        "blueprint": blueprint,
        "required_packages": [blueprint["package_profile"]["package"]],
        "seed": seed,
        "r_seed": r_seed_value(seed),
        "candidate_attempt": attempt,
        "cells": cells,
    }


def choose_style(seed: int, recent_styles: list[str]) -> NotebookStyle:
    recent = {style for style in recent_styles[:RECENT_STYLE_LOOKBACK] if style}
    for offset in range(len(STYLES)):
        candidate = STYLES[(seed + offset) % len(STYLES)]
        if candidate.key not in recent:
            return candidate
    return STYLES[seed % len(STYLES)]


def choose_topic(seed: int, recent_topics: list[str], attempt: int, topic_pool: list[Topic]) -> Topic:
    recent = {topic for topic in recent_topics[:RECENT_TOPIC_LOOKBACK] if topic}
    pool = topic_pool or TOPICS
    source_topics = [topic for topic in pool if topic.source_context]
    curated_topics = [topic for topic in pool if not topic.source_context]
    if source_topics:
        source_start = ((seed // max(1, len(STYLES))) + attempt * 2) % len(source_topics)
        for offset in range(len(source_topics)):
            candidate = source_topics[(source_start + offset) % len(source_topics)]
            if candidate.key not in recent:
                return candidate

    fallback_pool = curated_topics or pool
    start = ((seed // max(1, len(STYLES))) + attempt * 2) % len(fallback_pool)
    for offset in range(len(fallback_pool)):
        candidate = fallback_pool[(start + offset) % len(fallback_pool)]
        if candidate.key not in recent:
            return candidate
    return fallback_pool[start]


def source_context_markdown(topic: Topic) -> str:
    context = topic.source_context or {}
    title = str(context.get("source_title", "") or "").strip()
    source_name = str(context.get("source_name", "") or "").strip()
    published_at = str(context.get("published_at", "") or "").strip()
    if not title:
        return ""
    published_note = f" ({published_at[:10]})" if published_at else ""
    source_label = f"{source_name}의 " if source_name else ""
    return (
        f"참고 컨텍스트는 {source_label}`{shorten_text(title, 72)}`{published_note}입니다. "
        "원문 링크나 본문을 복제하지 않고, 주제 신호만 작은 분석 프레임으로 가져옵니다."
    )


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
    blueprint: dict[str, Any],
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
    elif style.key == "seasonality-scan":
        r_setup = build_seasonality_scan_r_code(seed=seed, start_date=start_date, n=n, topic=topic, plot_path=plot_path)
        r_summary = build_seasonality_scan_summary_r_code(topic)
    elif style.key == "distribution-shift":
        r_setup = build_distribution_shift_r_code(seed=seed, start_date=start_date, n=n, topic=topic, plot_path=plot_path)
        r_summary = build_distribution_shift_summary_r_code(topic)
    elif style.key == "threshold-lens":
        r_setup = build_threshold_lens_r_code(seed=seed, topic=topic, plot_path=plot_path)
        r_summary = build_threshold_lens_summary_r_code(topic)
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

    context_note = source_context_markdown(topic)
    context_block = f"{context_note}\n\n" if context_note else ""
    data_design = blueprint["data_design"]
    validation_lens = blueprint["validation_lens"]
    visual_grammar = blueprint["visual_grammar"]
    narrative_frame = blueprint["narrative_frame"]
    package_profile = blueprint["package_profile"]
    validation_plot_path = f"/tmp/webr_daily_validation_{blueprint['fingerprint'][:12]}.svg"
    validation_code = build_blueprint_validation_r_code(
        seed=seed,
        topic=topic,
        blueprint=blueprint,
        plot_path=validation_plot_path,
    )
    return [
        {
            "id": 1,
            "mode": "markdown",
            "source": (
                f"### {title}\n\n"
                f"{topic.background}\n\n"
                f"{context_block}"
                f"{style.question_prefix} 오늘의 질문은 **{topic.question}** 입니다.\n\n"
                f"이번 글은 **{data_design['label']}** 구조와 **{narrative_frame['label']}** 관점을 사용합니다. "
                f"{data_design['note']} {narrative_frame['note']}\n\n"
                f"{style.formula_note}\n\n"
                f"아래에서는 {topic.source_note}를 만든 뒤, 브라우저 WebR에서 실행 가능한 base R 코드로 작은 분석을 진행합니다."
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
                "### 결론을 한 번 더 흔들어 보기\n\n"
                f"주 분석 다음에는 **{validation_lens['label']}**을 적용합니다. {validation_lens['note']} "
                f"결과는 **{visual_grammar['label']}**으로 그립니다. {visual_grammar['note']} "
                f"또한 WebAssembly 지원 패키지 **{package_profile['package']}**를 실제로 불러 "
                f"{package_profile['note']}"
            ),
        },
        {"id": 6, "mode": "r", "source": validation_code, "plot_path": validation_plot_path},
        {
            "id": 7,
            "mode": "markdown",
            "source": (
                "### 읽는 포인트\n\n"
                f"{style.closing} {narrative_frame['note']} 오늘의 수치는 실제 운영 지표가 아니라 재현 가능한 예제 데이터이지만, "
                "같은 분석 blueprint는 실제 로그에서도 데이터 구조와 검증 질문을 명시한 뒤 재사용할 수 있습니다."
            ),
        },
    ]


def build_blueprint_validation_r_code(*, seed: int, topic: Topic, blueprint: dict[str, Any], plot_path: str) -> str:
    design_key = str(blueprint["data_design"]["key"])
    lens_key = str(blueprint["validation_lens"]["key"])
    visual_key = str(blueprint["visual_grammar"]["key"])
    package_key = str(blueprint["package_profile"]["key"])
    design_code = {
        "longitudinal-block": f"""subject <- rep(seq_len(30), each = 6)
time_index <- rep(seq_len(6), times = 30)
segment <- rep(c("A", "B", "C"), length.out = length(subject))
probe <- {topic.base} + rep(rnorm(30, 0, {max(topic.noise, 1.0)}), each = 6) + {topic.slope} * time_index + rnorm(length(subject), 0, {max(topic.noise / 2, 0.5)})
observed <- rep(TRUE, length(probe))""",
        "overdispersed-count": f"""time_index <- seq_len(180)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
mu <- pmax(2, {topic.base} + {topic.slope} * time_index / 4)
probe <- rnbinom(length(time_index), mu = mu, size = 2.5)
observed <- rep(TRUE, length(probe))""",
        "zero-inflated": f"""time_index <- seq_len(180)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
positive <- rpois(length(time_index), lambda = pmax(1, {topic.base} / 8))
probe <- ifelse(rbinom(length(time_index), 1, 0.38) == 1, 0, positive)
observed <- rep(TRUE, length(probe))""",
        "bounded-rate": """time_index <- seq_len(180)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
shape1 <- 2.2 + 0.35 * (segment == "C")
shape2 <- 3.4 - 0.25 * (segment == "C")
probe <- rbeta(length(time_index), shape1 = shape1, shape2 = shape2)
observed <- rep(TRUE, length(probe))""",
        "paired-change": f"""time_index <- seq_len(180)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
before <- rnorm(length(time_index), {topic.base}, {max(topic.noise, 1.0)})
after <- before + rnorm(length(time_index), {max(abs(topic.slope) * 3, 1.0)}, {max(topic.noise / 2, 0.5)})
probe <- after - before
observed <- rep(TRUE, length(probe))""",
        "clustered-sample": f"""cluster <- rep(seq_len(24), each = 8)
time_index <- seq_along(cluster)
segment <- rep(c("A", "B", "C"), length.out = length(cluster))
cluster_effect <- rep(rnorm(24, 0, {max(topic.noise, 1.0)}), each = 8)
probe <- {topic.base} + cluster_effect + rnorm(length(cluster), 0, {max(topic.noise / 2, 0.5)})
observed <- rep(TRUE, length(probe))""",
        "heavy-tail": f"""time_index <- seq_len(180)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
probe <- {topic.base} + {max(topic.noise, 1.0)} * rt(length(time_index), df = 3)
observed <- rep(TRUE, length(probe))""",
        "censored-time": """time_index <- seq_len(180)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
event_time <- rexp(length(time_index), rate = 0.08 + 0.02 * (segment == "C"))
censor_time <- runif(length(time_index), 4, 22)
observed <- event_time <= censor_time
probe <- pmin(event_time, censor_time)""",
        "compositional-share": """time_index <- seq_len(180)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
parts <- matrix(rgamma(length(time_index) * 4, shape = rep(c(2, 3, 4, 5), each = length(time_index))), ncol = 4)
parts <- parts / rowSums(parts)
probe <- parts[, 1]
observed <- rep(TRUE, length(probe))""",
        "irregular-interval": f"""gap <- rexp(180, rate = 0.7)
time_index <- cumsum(gap)
segment <- rep(c("A", "B", "C"), length.out = length(time_index))
probe <- {topic.base} + {topic.slope} * time_index + {topic.amplitude} * sin(time_index / 3) + rnorm(length(time_index), 0, {max(topic.noise, 1.0)})
observed <- rep(TRUE, length(probe))""",
    }[design_key]
    lens_code = {
        "temporal-holdout": "lens_values <- cumsum(probe) / seq_along(probe)\nlens_axis <- seq_along(lens_values)\nlens_summary <- tail(lens_values, 1) - lens_values[max(2, floor(length(lens_values) * 0.67))]",
        "bootstrap-stability": "lens_values <- replicate(320, median(sample(probe, replace = TRUE)))\nlens_axis <- seq_along(lens_values)\nlens_summary <- stats::sd(lens_values)",
        "permutation-placebo": "observed_gap <- mean(probe[segment == 'C']) - mean(probe[segment == 'A'])\nlens_values <- replicate(320, { shuffled <- sample(segment); mean(probe[shuffled == 'C']) - mean(probe[shuffled == 'A']) })\nlens_axis <- seq_along(lens_values)\nlens_summary <- mean(abs(lens_values) >= abs(observed_gap))",
        "leave-one-out": "lens_values <- vapply(seq_along(probe), function(i) mean(probe[-i]), numeric(1))\nlens_axis <- seq_along(lens_values)\nlens_summary <- diff(range(lens_values))",
        "subgroup-consistency": "lens_values <- as.numeric(tapply(probe, segment, median))\nlens_axis <- seq_along(lens_values)\nlens_summary <- diff(range(lens_values))",
        "missingness-stress": "drop_rate <- seq(0, 0.45, length.out = 30)\nlens_values <- vapply(drop_rate, function(p) mean(sample(probe, max(8, floor(length(probe) * (1 - p))))), numeric(1))\nlens_axis <- drop_rate\nlens_summary <- tail(lens_values, 1) - lens_values[1]",
        "robust-estimator": "lens_values <- c(mean = mean(probe), trimmed = mean(probe, trim = 0.1), median = median(probe))\nlens_axis <- seq_along(lens_values)\nlens_summary <- max(lens_values) - min(lens_values)",
        "threshold-sensitivity": "lens_axis <- as.numeric(quantile(probe, probs = seq(0.1, 0.9, by = 0.05)))\nlens_values <- vapply(lens_axis, function(x) mean(probe >= x), numeric(1))\nlens_summary <- max(abs(diff(lens_values)))",
        "sample-size-stability": "lens_axis <- unique(round(seq(12, length(probe), length.out = 28)))\nlens_values <- vapply(lens_axis, function(n) mean(probe[seq_len(n)]), numeric(1))\nlens_summary <- tail(lens_values, 1) - lens_values[1]",
        "noise-stress": "lens_axis <- seq(0, stats::sd(probe), length.out = 28)\nlens_values <- vapply(lens_axis, function(s) suppressWarnings(cor(probe, probe + rnorm(length(probe), 0, s))), numeric(1))\nlens_summary <- tail(lens_values, 1)",
        "negative-control": "lens_values <- replicate(320, suppressWarnings(cor(probe, sample(time_index))))\nlens_axis <- seq_along(lens_values)\nlens_summary <- mean(abs(lens_values) > 0.2, na.rm = TRUE)",
        "calibration-check": "expected <- rank(probe, ties.method = 'average') / (length(probe) + 1)\nbin <- cut(expected, breaks = seq(0, 1, by = 0.1), include.lowest = TRUE)\nlens_axis <- as.numeric(tapply(expected, bin, mean))\nlens_values <- as.numeric(tapply(observed * 1, bin, mean))\nlens_summary <- mean(abs(lens_values - lens_axis), na.rm = TRUE)",
    }[lens_key]
    visual_code = {
        "ecdf": "plot(stats::ecdf(lens_values), verticals = TRUE, do.points = FALSE, col = topic_color, lwd = 3, main = visual_title, xlab = 'validation value', ylab = 'cumulative probability')",
        "histogram": "hist(lens_values, breaks = 'FD', col = topic_color, border = 'white', main = visual_title, xlab = 'validation value')",
        "boxplot": "boxplot(lens_values, horizontal = TRUE, col = topic_color, border = '#111827', main = visual_title, xlab = 'validation value')",
        "running-estimate": "plot(lens_axis, lens_values, type = 'l', col = topic_color, lwd = 3, main = visual_title, xlab = 'validation step', ylab = 'estimate'); points(lens_axis, lens_values, pch = 21, bg = topic_accent, col = 'white')",
        "rank-dot": "ranked_values <- sort(lens_values, decreasing = TRUE); plot(seq_along(ranked_values), ranked_values, pch = 21, bg = topic_color, col = 'white', main = visual_title, xlab = 'rank', ylab = 'validation value')",
        "interval-forest": "groups <- cut(seq_along(lens_values), breaks = min(6, length(lens_values)), labels = FALSE); centers <- as.numeric(tapply(lens_values, groups, mean)); spreads <- as.numeric(tapply(lens_values, groups, sd)); spreads[!is.finite(spreads)] <- 0; plot(centers, seq_along(centers), xlim = range(c(centers - spreads, centers + spreads)), pch = 19, col = topic_color, main = visual_title, xlab = 'estimate and one SD', ylab = 'block'); segments(centers - spreads, seq_along(centers), centers + spreads, seq_along(centers), col = topic_accent, lwd = 2)",
        "residual-map": "fit_probe <- lm(probe ~ time_index); plot(time_index, resid(fit_probe), pch = 21, bg = ifelse(abs(scale(resid(fit_probe))) > 1.5, topic_accent, topic_color), col = 'white', main = visual_title, xlab = 'observation order', ylab = 'residual'); abline(h = 0, lty = 2, lwd = 2)",
        "calibration-curve": "scaled_values <- rank(lens_values, ties.method = 'average') / (length(lens_values) + 1); expected_values <- seq_along(scaled_values) / (length(scaled_values) + 1); plot(expected_values, sort(scaled_values), type = 'b', pch = 21, bg = topic_color, col = topic_color, main = visual_title, xlab = 'expected quantile', ylab = 'observed quantile'); abline(0, 1, lty = 2, col = topic_accent, lwd = 2)",
    }[visual_key]
    package_code = {
        "matrix-sparse": "pkg_object <- Matrix::sparseMatrix(i = seq_along(probe), j = rep(1:3, length.out = length(probe)), x = probe); package_result <- mean(Matrix::rowSums(pkg_object))",
        "boot-resample": "pkg_object <- boot::boot(probe, statistic = function(d, i) mean(d[i]), R = 120); package_result <- stats::sd(as.numeric(pkg_object$t))",
        "mass-robust": "pkg_object <- MASS::rlm(probe ~ time_index); package_result <- unname(stats::coef(pkg_object)[['time_index']])",
        "survival-time": "pkg_object <- survival::survfit(survival::Surv(abs(probe) + 0.01, observed) ~ 1); package_result <- unname(summary(pkg_object)$table[['median']])",
        "cluster-pam": "pkg_object <- cluster::pam(scale(cbind(probe, time_index)), k = 3); package_result <- pkg_object$silinfo$avg.width",
        "mgcv-smooth": "pkg_object <- mgcv::gam(probe ~ s(time_index, k = 6)); package_result <- summary(pkg_object)$r.sq",
        "nlme-grouped": "pkg_object <- nlme::lme(probe ~ time_index, random = ~1 | segment, control = nlme::lmeControl(returnObject = TRUE)); package_result <- unname(nlme::fixef(pkg_object)[['time_index']])",
        "data-table-group": "pkg_object <- data.table::data.table(probe = probe, segment = segment)[, .(center = median(probe)), by = segment]; package_result <- mean(pkg_object$center)",
        "dplyr-summary": "pkg_object <- dplyr::summarise(dplyr::group_by(data.frame(probe = probe, segment = segment), segment), center = mean(probe), spread = stats::sd(probe), .groups = 'drop'); package_result <- mean(pkg_object$center)",
        "ggplot-build": "pkg_plot <- ggplot2::ggplot(data.frame(time_index = time_index, probe = probe), ggplot2::aes(time_index, probe)) + ggplot2::geom_point() + ggplot2::geom_smooth(method = 'lm', se = FALSE); pkg_object <- ggplot2::ggplot_build(pkg_plot); package_result <- length(pkg_object$data[[1]]$x)",
        "tidyr-reshape": "pkg_object <- tidyr::pivot_longer(data.frame(id = seq_along(probe), observed = probe, centered = probe - mean(probe)), cols = c('observed', 'centered'), names_to = 'measure', values_to = 'value'); package_result <- nrow(pkg_object)",
        "purrr-map": "pkg_object <- purrr::map_dbl(split(probe, segment), stats::median); package_result <- mean(pkg_object)",
        "stringr-token": f"pkg_object <- stringr::str_count(c({r_string(topic.entity)}, {r_string(topic.question)}), stringr::boundary('word')); package_result <- sum(pkg_object)",
        "broom-tidy": "pkg_object <- broom::tidy(stats::lm(probe ~ time_index)); package_result <- pkg_object$estimate[pkg_object$term == 'time_index']",
        "zoo-roll": "pkg_object <- zoo::rollmean(probe, k = 7, fill = NA, align = 'right'); package_result <- tail(stats::na.omit(pkg_object), 1)",
        "jsonlite-roundtrip": "pkg_json <- jsonlite::toJSON(list(center = mean(probe), spread = stats::sd(probe)), auto_unbox = TRUE); pkg_object <- jsonlite::fromJSON(pkg_json); package_result <- pkg_object$center",
    }[package_key]
    return "\n".join(
        [
            f"# Blueprint validation: {blueprint['fingerprint']}",
            f"set.seed({r_seed_value(seed + 7919)})",
            design_code,
            lens_code,
            "lens_values <- as.numeric(lens_values)",
            "lens_values <- lens_values[is.finite(lens_values)]",
            "if (!length(lens_values)) lens_values <- 0",
            f"topic_color <- {r_string(topic.color)}",
            f"topic_accent <- {r_string(topic.accent)}",
            f"visual_title <- {r_string(blueprint['validation_lens']['label'] + ' · ' + blueprint['visual_grammar']['label'])}",
            f"grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = 'white')",
            "op <- par(mar = c(4.6, 4.8, 3.2, 1.2), bg = 'white')",
            visual_code,
            "par(op)",
            "grDevices::dev.off()",
            package_code,
            f"cat('WebAssembly package:', {r_string(blueprint['package_profile']['package'])}, as.character(utils::packageVersion({r_string(blueprint['package_profile']['package'])})), '\\n')",
            "cat('package calculation:', round(as.numeric(package_result)[1], 4), '\\n')",
            f"cat('data design:', {r_string(blueprint['data_design']['label'])}, '\\n')",
            f"cat('validation lens:', {r_string(blueprint['validation_lens']['label'])}, '\\n')",
            f"cat('visual grammar:', {r_string(blueprint['visual_grammar']['label'])}, '\\n')",
            "cat('validation summary:', round(lens_summary, 4), '\\n')",
        ]
    ) + "\n"


def build_analysis_r_code(*, seed: int, start_date: str, n: int, change_point: int, effect: int, topic: Topic, plot_path: str) -> str:
    return f"""# WebR data analysis note: {topic.key}
set.seed({r_seed_value(seed)})
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
set.seed({r_seed_value(seed)})
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
set.seed({r_seed_value(seed)})
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
set.seed({r_seed_value(seed)})
cohort_labels <- paste("cohort", LETTERS[1:6])
week <- 0:5
start_level <- runif(length(cohort_labels), 0.74, 0.93)
decay <- runif(length(cohort_labels), 0.055, 0.14)
retention <- sapply(
  seq_along(week),
  function(w) pmax(0.04, start_level * exp(-decay * w) + rnorm(length(cohort_labels), 0, 0.018))
)
retention <- matrix(
  pmin(1, as.vector(retention)),
  nrow = length(cohort_labels),
  ncol = length(week),
  dimnames = list(cohort_labels, paste0("week ", week))
)
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
set.seed({r_seed_value(seed)})
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


def build_seasonality_scan_r_code(*, seed: int, start_date: str, n: int, topic: Topic, plot_path: str) -> str:
    return f"""# Seasonality scan: {topic.key}
set.seed({r_seed_value(seed)})
day <- seq.Date(as.Date({r_string(start_date)}), by = "day", length.out = {n})
weekday <- weekdays(day)
weekday_order <- weekdays(as.Date("2026-01-05") + 0:6)
phase <- seq_along(day)
weekday_effect <- setNames(round(stats::runif(7, -8, 12), 1), weekday_order)
value <- round(pmax(1, {topic.base} + {topic.slope} * phase + weekday_effect[weekday] + stats::rnorm(length(day), 0, {topic.noise})))
daily <- data.frame(day = day, weekday = factor(weekday, levels = weekday_order), value = value)
weekday_summary <- aggregate(value ~ weekday, daily, mean)
weekday_summary$value <- round(weekday_summary$value, 1)
recent <- tail(daily, 14)
recent$weekday_baseline <- weekday_summary$value[match(recent$weekday, weekday_summary$weekday)]
recent$gap <- recent$value - recent$weekday_baseline

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(6.0, 4.7, 3.2, 1.2), bg = "white")
bar_mid <- barplot(
  weekday_summary$value,
  names.arg = as.character(weekday_summary$weekday),
  las = 2,
  col = {r_string(topic.color)},
  border = "white",
  ylab = {r_string(topic.metric)},
  main = {r_string(topic.title)}
)
points(bar_mid, weekday_summary$value, pch = 21, bg = {r_string(topic.accent)}, col = "white", cex = 1.15)
abline(h = mean(daily$value), col = "#111827", lwd = 2, lty = 2)
legend("topright", legend = c("weekday baseline", "overall mean"), fill = c({r_string(topic.color)}, NA), border = c("white", NA), lty = c(NA, 2), lwd = c(NA, 2), col = c(NA, "#111827"), bty = "n")
par(op)
grDevices::dev.off()
"""


def build_seasonality_scan_summary_r_code(topic: Topic) -> str:
    return f"""# Seasonality summary
strongest <- weekday_summary$weekday[which.max(weekday_summary$value)]
weakest <- weekday_summary$weekday[which.min(weekday_summary$value)]
largest_recent_gap <- recent[which.max(abs(recent$gap)), ]
cat({r_string(topic.entity)}, "seasonality scan\\n")
cat("strongest weekday baseline:", as.character(strongest), "\\n")
cat("weakest weekday baseline:", as.character(weakest), "\\n")
cat("largest recent baseline gap:", format(largest_recent_gap$day, "%Y-%m-%d"), round(largest_recent_gap$gap, 1), "\\n")
cat("recent mean vs full mean:", round(mean(recent$value), 1), "vs", round(mean(daily$value), 1), "\\n")
"""


def build_distribution_shift_r_code(*, seed: int, start_date: str, n: int, topic: Topic, plot_path: str) -> str:
    return f"""# Distribution shift: {topic.key}
set.seed({r_seed_value(seed)})
day <- seq.Date(as.Date({r_string(start_date)}), by = "day", length.out = {n})
phase <- seq_along(day)
period <- ifelse(phase <= floor(length(phase) / 2), "early", "recent")
shift <- ifelse(period == "recent", stats::runif(1, -4, 10), 0)
spread <- ifelse(period == "recent", {topic.noise} * stats::runif(1, 0.8, 1.35), {topic.noise})
value <- round(pmax(1, {topic.base} + {topic.slope} * phase + shift + {topic.amplitude} * sin(2 * pi * phase / 14) + stats::rnorm(length(day), 0, spread)))
shift_df <- data.frame(day = day, period = factor(period, levels = c("early", "recent")), value = value)
q_early <- stats::quantile(shift_df$value[shift_df$period == "early"], probs = c(0.1, 0.5, 0.9))
q_recent <- stats::quantile(shift_df$value[shift_df$period == "recent"], probs = c(0.1, 0.5, 0.9))
median_shift <- unname(q_recent["50%"] - q_early["50%"])

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(4.5, 4.7, 3.2, 1.2), bg = "white")
boxplot(
  value ~ period,
  data = shift_df,
  col = c("#e5e7eb", {r_string(topic.color)}),
  border = "#111827",
  ylab = {r_string(topic.metric)},
  main = {r_string(topic.title)}
)
stripchart(value ~ period, data = shift_df, vertical = TRUE, method = "jitter", pch = 21, bg = {r_string(topic.accent)}, col = "white", add = TRUE)
legend("topleft", legend = c("daily value", "recent period"), pch = c(21, 15), pt.bg = c({r_string(topic.accent)}, {r_string(topic.color)}), col = c("white", {r_string(topic.color)}), bty = "n")
par(op)
grDevices::dev.off()
"""


def build_distribution_shift_summary_r_code(topic: Topic) -> str:
    return f"""# Distribution shift summary
iqr_early <- stats::IQR(shift_df$value[shift_df$period == "early"])
iqr_recent <- stats::IQR(shift_df$value[shift_df$period == "recent"])
cat({r_string(topic.entity)}, "distribution shift\\n")
cat("early median:", round(unname(q_early["50%"]), 1), "\\n")
cat("recent median:", round(unname(q_recent["50%"]), 1), "\\n")
cat("median shift:", round(median_shift, 1), "\\n")
cat("IQR early vs recent:", round(iqr_early, 1), "vs", round(iqr_recent, 1), "\\n")
"""


def build_threshold_lens_r_code(*, seed: int, topic: Topic, plot_path: str) -> str:
    return f"""# Threshold lens: {topic.key}
set.seed({r_seed_value(seed)})
n <- 220
segment <- sample(c("low", "middle", "high"), n, replace = TRUE, prob = c(0.36, 0.44, 0.20))
score <- pmin(1, pmax(0, stats::rbeta(n, shape1 = 2.2 + (segment == "high"), shape2 = 2.8 - 0.4 * (segment == "high"))))
true_prob <- pmin(0.95, pmax(0.05, 0.18 + 0.62 * score + 0.10 * (segment == "high")))
outcome <- stats::rbinom(n, size = 1, prob = true_prob)
thresholds <- seq(0.15, 0.85, by = 0.05)
metrics <- data.frame(threshold = thresholds, precision = NA_real_, recall = NA_real_, flagged = NA_real_)
for (i in seq_along(thresholds)) {{
  predicted <- score >= thresholds[i]
  tp <- sum(predicted & outcome == 1)
  fp <- sum(predicted & outcome == 0)
  fn <- sum(!predicted & outcome == 1)
  metrics$precision[i] <- ifelse(tp + fp == 0, NA, tp / (tp + fp))
  metrics$recall[i] <- ifelse(tp + fn == 0, NA, tp / (tp + fn))
  metrics$flagged[i] <- mean(predicted)
}}
metrics$f1 <- 2 * metrics$precision * metrics$recall / (metrics$precision + metrics$recall)
best <- metrics[which.max(metrics$f1), ]

grDevices::svg({r_string(plot_path)}, width = 7.2, height = 4.6, bg = "white")
op <- par(mar = c(4.5, 4.7, 3.2, 4.2), bg = "white")
plot(metrics$threshold, metrics$precision, type = "b", pch = 19, col = {r_string(topic.color)}, ylim = c(0, 1), xlab = "threshold", ylab = "precision / recall", main = {r_string(topic.title)})
lines(metrics$threshold, metrics$recall, type = "b", pch = 21, bg = {r_string(topic.accent)}, col = {r_string(topic.accent)})
lines(metrics$threshold, metrics$f1, type = "b", pch = 17, col = "#111827")
abline(v = best$threshold, col = "#111827", lwd = 2, lty = 2)
legend("bottomleft", legend = c("precision", "recall", "F1", "best threshold"), col = c({r_string(topic.color)}, {r_string(topic.accent)}, "#111827", "#111827"), pch = c(19, 21, 17, NA), lty = c(1, 1, 1, 2), bty = "n")
par(op)
grDevices::dev.off()
"""


def build_threshold_lens_summary_r_code(topic: Topic) -> str:
    return f"""# Threshold lens summary
cat({r_string(topic.entity)}, "threshold lens\\n")
cat("best threshold:", round(best$threshold, 2), "\\n")
cat("precision at best:", round(best$precision, 3), "\\n")
cat("recall at best:", round(best$recall, 3), "\\n")
cat("flagged share at best:", paste0(round(100 * best$flagged, 1), "%"), "\\n")
"""


def r_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def r_seed_value(seed: int) -> int:
    return ((int(seed) - 1) % R_SET_SEED_MAX) + 1


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


def validate_style_templates(runner: Path) -> dict[str, Any]:
    spec = build_style_validation_spec()
    runner_result = run_webr_runner(runner, spec)
    expected_ids = [cell["id"] for cell in spec["cells"] if cell["mode"] == "r"]
    actual_ids = [item.get("id") for item in runner_result.get("r_results", [])]
    missing_ids = [cell_id for cell_id in expected_ids if cell_id not in actual_ids]
    if missing_ids:
        raise RuntimeError(f"webR style validation missed R cell result ids: {missing_ids}")
    return {
        "schema": "web-r.notebook.style-validation.v1",
        "ok": True,
        "style_count": len(STYLES),
        "r_cell_count": len(expected_ids),
        "styles": [style.key for style in STYLES],
    }


def build_style_validation_spec() -> dict[str, Any]:
    cells: list[dict[str, Any]] = []
    required_packages: set[str] = set()
    for index, style in enumerate(STYLES):
        topic = TOPICS[index % len(TOPICS)]
        validation_seed = 3_340_467_507 + index * 137
        blueprint = build_diversity_blueprint(topic, style, validation_seed, index)
        required_packages.add(str(blueprint["package_profile"]["package"]))
        style_cells = build_style_cells(
            style=style,
            topic=topic,
            title=f"Template validation: {style.label}",
            seed=validation_seed,
            start_date="2026-01-01",
            n=60,
            change_point=34 + index,
            effect=10 + index,
            blueprint=blueprint,
        )
        for cell in style_cells:
            cloned = dict(cell)
            cloned["id"] = index * 10 + int(cell["id"])
            cloned["validation_style"] = style.key
            cells.append(cloned)
    return {
        "schema": "web-r.notebook.style-validation-spec.v1",
        "series_date": "2026-01-01",
        "title": "Web-R Notebook style template validation",
        "required_packages": sorted(required_packages),
        "cells": cells,
    }


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
    topic_source_context = spec.get("topic", {}).get("source_context") or {}
    package_results = runner_result.get("packages", [])
    installed_packages = {
        str(item.get("package", "")).strip()
        for item in package_results
        if isinstance(item, dict) and str(item.get("package", "")).strip()
    }
    required_packages = {str(value).strip() for value in spec.get("required_packages", []) if str(value).strip()}
    if not required_packages.issubset(installed_packages):
        missing = sorted(required_packages - installed_packages)
        raise RuntimeError(f"Web-R Notebook package execution contract failed for: {', '.join(missing)}")
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
        "topic_source_context": topic_source_context,
        "style": spec["style"]["key"],
        "blueprint": spec["blueprint"],
        "webr_packages": package_results,
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
        "topic_source_context": topic_source_context,
        "style": spec["style"]["key"],
        "blueprint": spec["blueprint"],
        "webr_packages": package_results,
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
        "data_rpackage": json.dumps(package_results, ensure_ascii=False, separators=(",", ":")),
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


def published_notebook_blueprint_fingerprints(env: dict[str, str]) -> set[str]:
    sql = f"""
SELECT ifNull(data_meta, '') AS data_meta
  FROM webr_webr.notebook
 WHERE uuid_user = toUUID('{NOTEBOOK_BOT_UUID}')
 ORDER BY created_at DESC
 LIMIT 100000
 FORMAT JSONEachRow
"""
    fingerprints: set[str] = set()
    for row in clickhouse_json_each_row(env, sql):
        meta = parse_json_maybe(row.get("data_meta"))
        if not isinstance(meta, dict):
            continue
        blueprint = meta.get("blueprint")
        if not isinstance(blueprint, dict):
            continue
        fingerprint = str(blueprint.get("fingerprint", "")).strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", fingerprint):
            fingerprints.add(fingerprint)
    return fingerprints


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
        with urllib.request.urlopen(request, timeout=clickhouse_http_timeout(env)) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read(300).decode("utf-8", errors="replace")
        raise SystemExit(f"ClickHouse query failed: HTTP {exc.code}: {redact_detail(env, detail)}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"ClickHouse query failed: {exc.__class__.__name__}") from exc
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def insert_json_each_row(env: dict[str, str], table: str, row: dict[str, Any]) -> dict[str, Any]:
    sql = (
        f"INSERT INTO {table} SETTINGS insert_deduplicate = 1, insert_distributed_sync = 1 FORMAT JSONEachRow\n"
        + json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        + "\n"
    )
    attempts = max(1, env_int(env, "WEBR_NOTEBOOK_DAILY_INSERT_ATTEMPTS", 6))
    backoff = max(0.0, env_float(env, "WEBR_NOTEBOOK_DAILY_INSERT_BACKOFF_SECONDS", 10.0))
    last_message = ""
    last_retryable = False
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(clickhouse_url(env), data=sql.encode("utf-8"), method="POST")
        request.add_header("Authorization", "Basic " + base64.b64encode(f"{first_env(env, 'CH_USER', 'CLICKHOUSE_USER')}:{first_env(env, 'CH_PASSWORD', 'CLICKHOUSE_PASSWORD')}".encode("utf-8")).decode("ascii"))
        request.add_header("Content-Type", "text/plain; charset=utf-8")
        try:
            with urllib.request.urlopen(request, timeout=clickhouse_http_timeout(env)) as response:
                response.read()
            if attempt > 1:
                print(f"ClickHouse insert retry succeeded attempt={attempt}")
            return {"inserted": True, "insert_deferred": False, "insert_failure": ""}
        except urllib.error.HTTPError as exc:
            detail = exc.read(300).decode("utf-8", errors="replace")
            sanitized = redact_detail(env, detail)
            last_message = f"HTTP {exc.code}: {sanitized}"
            retryable = is_retryable_clickhouse_insert_error(exc.code, sanitized)
        except urllib.error.URLError as exc:
            last_message = exc.__class__.__name__
            retryable = True
        last_retryable = retryable
        if attempt >= attempts or not retryable:
            reason = short_clickhouse_reason(last_message)
            if last_retryable and env_bool(env, "WEBR_NOTEBOOK_DAILY_PUBLISH_TRANSIENT_FAIL_OPEN", True):
                print(f"[notebook] insert_deferred reason={reason}", file=sys.stderr)
                return {"inserted": False, "insert_deferred": True, "insert_failure": reason}
            raise SystemExit(f"ClickHouse insert failed: {last_message}")
        print(f"ClickHouse insert retry attempt={attempt + 1}/{attempts} reason={short_clickhouse_reason(last_message)}")
        if backoff:
            time.sleep(backoff)
    return {"inserted": False, "insert_deferred": True, "insert_failure": short_clickhouse_reason(last_message)}


def clickhouse_url(env: dict[str, str]) -> str:
    try:
        return build_clickhouse_url(
            env,
            default_format="JSONEachRow",
            max_execution_time=env.get("WEBR_NOTEBOOK_DAILY_CH_MAX_EXECUTION_TIME", "30"),
            max_threads=env.get("WEBR_NOTEBOOK_DAILY_CH_MAX_THREADS", "1"),
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc


def clickhouse_http_timeout(env: dict[str, str]) -> float:
    return max(10.0, env_float(env, "WEBR_NOTEBOOK_DAILY_HTTP_TIMEOUT", 40.0))


def env_bool(env: dict[str, str], key: str, default: bool) -> bool:
    raw = str(env.get(key, "") or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def env_int(env: dict[str, str], key: str, default: int) -> int:
    try:
        return int(str(env.get(key, "") or "").strip() or default)
    except ValueError:
        return default


def env_float(env: dict[str, str], key: str, default: float) -> float:
    try:
        return float(str(env.get(key, "") or "").strip() or default)
    except ValueError:
        return default


def is_retryable_clickhouse_insert_error(status_code: int, detail: str) -> bool:
    upper = detail.upper()
    if status_code in {401, 403}:
        return False
    if is_clickhouse_insert_contract_error(detail):
        return False
    return (
        status_code in {408, 429, 500, 502, 503, 504}
        or "TIMEOUT" in upper
        or "TOO_MANY_SIMULTANEOUS" in upper
        or "NOT_INITIALIZED" in upper
        or "NOT INITIALIZED" in upper
        or "TABLE_IS_READ_ONLY" in upper
        or "KEEPER_EXCEPTION" in upper
        or "CONNECTION LOSS" in upper
    )


def short_clickhouse_reason(message: str) -> str:
    upper = message.upper()
    if "NOT_INITIALIZED" in upper or "NOT INITIALIZED" in upper:
        return "clickhouse-not-initialized"
    if "TABLE_IS_READ_ONLY" in upper or "KEEPER_EXCEPTION" in upper or "CONNECTION LOSS" in upper:
        return "clickhouse-replica-unavailable"
    if "CANNOT PARSE" in upper or "CANNOT_PARSE" in upper or "CANNOT CONVERT" in upper or "CANNOT_CONVERT" in upper:
        return "clickhouse-parse-error"
    if is_clickhouse_insert_contract_error(message):
        return "clickhouse-request-error"
    if message.startswith("HTTP 401"):
        return "clickhouse-auth"
    if message.startswith("HTTP 403"):
        return "clickhouse-permission"
    if "TIMEOUT" in upper or "TIMED OUT" in upper:
        return "timeout"
    if "TOO_MANY_SIMULTANEOUS" in upper:
        return "server-busy"
    if message.startswith("HTTP 429"):
        return "rate-limited"
    if message.startswith("HTTP 5"):
        return "server-error"
    return "temporary-error"


def is_clickhouse_insert_contract_error(detail: str) -> bool:
    upper = detail.upper()
    return any(
        token in upper
        for token in (
            "UNKNOWN_IDENTIFIER",
            "UNKNOWN IDENTIFIER",
            "UNKNOWN_TABLE",
            "UNKNOWN TABLE",
            "UNKNOWN_DATABASE",
            "UNKNOWN DATABASE",
            "NO SUCH COLUMN",
            "MISSING COLUMNS",
            "TYPE_MISMATCH",
            "TYPE MISMATCH",
            "SYNTAX_ERROR",
            "SYNTAX ERROR",
            "CANNOT PARSE",
            "CANNOT_PARSE",
            "CANNOT CONVERT",
            "CANNOT_CONVERT",
            "CANNOT_READ_ALL_DATA",
        )
    )


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
