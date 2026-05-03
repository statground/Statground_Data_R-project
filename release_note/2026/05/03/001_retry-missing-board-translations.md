# Mastodon board 번역 누락 재시도 개선

## 변경 내용

- `webr_mastodon.raw`에는 적재됐지만 `webr_mastodon.board` 번역 row 생성에 실패한 status를 다음 증분 실행에서 다시 번역하도록 `board_payload_hash` 기반 상태 추적을 추가했다.
- AI provider의 429/일시 오류는 provider별로 짧은 backoff 재시도를 수행하도록 보강했다.
- 제목과 본문을 별도 AI 호출로 번역하던 방식을 단일 JSON 응답 호출로 바꿔 backfill 시 provider 요청 수를 줄였다.
- 기본 번역 모델을 더 이상 endpoint가 없던 `google/gemini-2.0-flash-exp:free`에서 `openai/gpt-oss-20b`로 바꾸고, provider별 모델 override 환경 변수를 지원하게 했다.
- 현재 운영 ClickHouse에서 이미 board에 반영된 9개 status는 state에 `published`로 표시하고, 누락된 36개 status는 `failed` / retry pending 상태로 표시했다.

## 의도

최초 backfill 중 번역 provider 오류가 나도 raw 수집 상태와 board 번역 상태를 분리해 관리한다. 이후 hourly incremental 실행이 raw 변경분만 보는 데서 멈추지 않고, board 번역이 미완료된 기존 raw도 다시 시도하게 만든다.
