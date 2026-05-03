# Mastodon board 수동 번역 보강

## 변경 내용

- 운영 ClickHouse `webr_mastodon.raw`에는 active로 남아 있었지만 `webr_mastodon.board` 최신 active row가 없던 36개 R Foundation Mastodon status를 직접 한국어로 번역해 `webr_mastodon.board`에 채웠다.
- 수동 삽입 row에는 `created_log.type = mastodon_board_manual_translation`을 남기고, 원본 `uuid`, `status_created_at`, `status_id`를 유지했다.
- `data/mastodon/r_foundation/state.json`의 board 상태도 `published_manual`로 맞춰 다음 GitHub Actions 증분 실행이 같은 36개를 다시 번역하지 않게 했다.

## 확인

- `raw_active = 45`
- `board_active_latest = 45`
- `missing_active_board = 0`
- `manual_rows_inserted = 36`
