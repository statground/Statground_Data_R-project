# R Community Batch

`R Community`는 R 언어 공식 발행물과 커뮤니티 흐름을 GitHub Actions에서 주기 수집하는 배치입니다. 외부 API 키 없이 공개 RSS/Atom, HTML, no-auth public endpoint만 사용합니다.

## 수집 대상

- R Project / R Foundation: release notes, What's New, The R Blog, The R Journal, conferences
- R mailing list archives: R-announce, R-packages, R-help, R-devel, R-package-devel, selected R-SIG archives
- Reddit R communities: `r/rstats`, `r/RStudio`, `r/Rlanguage`, `r/rprogramming`, `r/rshiny`, `r/quarto`, `r/tidymodels`
- Mastodon / Fediverse: R 관련 태그와 R Foundation, useR!, rOpenSci, CRANberries 계정
- 한국 R 커뮤니티: DCInside R/RStudio 갤러리, Korea R user group, Seoul R Meetup
- Stack Overflow, Posit Community, Bioconductor, rOpenSci, R-bloggers, R Weekly, R-universe, R-Ladies, R user group indexes

## 실행 방식

기본 스케줄은 6시간마다 실행되며 결과는 `data/collected/r/` 아래 JSONL/report로 생성한 뒤 GitHub Actions artifact로 업로드합니다. 수동 실행에서 `commit_output=true`를 선택하면 같은 경로의 결과 파일을 저장소에 커밋할 수 있습니다.

```bash
python scripts/collect_r_sources.py \
  --config config/r_sources.yaml \
  --out-dir data/collected/r \
  --since-days 14
```

## 출력 JSONL

각 줄은 하나의 수집 항목이며 stable `external_id`, source metadata, canonical URL, title, summary, author, published time, collected time, language, tags, raw collector metadata를 포함합니다.

## 운영 DDL

`ddl/`에는 AliSQL SSOT 예시 테이블과 ClickHouse 분석 이벤트 로그 예시가 포함되어 있습니다. GitHub Actions는 기본적으로 DB에 직접 쓰지 않고, 생성된 JSONL을 ingest 단계에서 검증 후 반영하는 구조를 전제로 둡니다.
