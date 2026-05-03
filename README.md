# Statground Data R-project Mastodon

`https://fosstodon.org/@R_Foundation`의 공개 게시물을 GitHub Actions에서 주기적으로 수집해 Kafka `webr.events`로 발행하는 repo입니다.

## 동작 구조

- 수집 대상: Mastodon instance `https://fosstodon.org`, account `R_Foundation`
- 발행 토픽: `webr.events`
- 이벤트 타입: `webr.mastodon.log.v1`, `webr.mastodon.raw.v1`, `webr.mastodon.board.v1`
- 상태 파일: `data/mastodon/r_foundation/state.json`
- 기본 범위: boost/reblog는 제외하고 reply는 포함합니다.
- raw 원문 언어: `language_code = en`
- board 표시 언어: `language_code = ko`
- 증분 수집: 매시 `17,47`분
- 전체 검증/backfill: 매주 일요일 UTC 18:13, 한국시간 월요일 03:13

## GitHub Secrets

필수:

- `KAFKA_BROKERS`: 외부에서 접근 가능한 Kafka bootstrap 주소입니다. `127.0.0.1`, `localhost`, `0.0.0.0`은 거부합니다.
- `KAFKA_USERNAME` 또는 `KAFKA_EXTERNAL_USER`
- `KAFKA_PASSWORD` 또는 `KAFKA_EXTERNAL_PASSWORD`
- 하나 이상의 AI provider secret: `OPENROUTER_API_KEY`, `GROQ_API_KEY`, `CEREBRAS_API_KEY`, `GH_MODELS_API_KEY`

선택:

- `KAFKA_SECURITY_PROTOCOL`: 기본값은 SASL 인증이 있을 때 `SASL_PLAINTEXT`입니다.
- `MASTODON_TOKEN`: 공개 API만으로도 동작하지만 rate limit 여유가 필요하면 지정합니다.

선택 GitHub Variables:

- `MASTODON_R_FOUNDATION_ACCOUNT_ID`: 계정 lookup API 호출을 생략하고 싶을 때 사용합니다.
- `MASTODON_TRANSLATE_ENABLED`: 기본값은 `true`입니다. `false`면 raw/log만 발행합니다.
- `MASTODON_TRANSLATION_MODEL`: 기본값은 `google/gemini-2.0-flash-exp:free`입니다.
- `MASTODON_FAIL_ON_TRANSLATION_ERROR`: 기본값은 `false`입니다.

## Kafka 준비

이미지 첨부를 base64로 포함할 수 있으므로 `webr.events` topic은 producer의 `MAX_KAFKA_EVENT_BYTES`보다 큰 메시지를 받을 수 있어야 합니다.

R-Blogger와 같은 방식으로 `raw`는 원문 보존, `board`는 한국어 번역 게시판 표시, `log`는 수집/번역 파이프라인 관측용 이벤트입니다.
