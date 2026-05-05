# Statground Data R Blogger

Go-based R-bloggers crawler for the Web-R runtime Kafka ingestion path.

The GitHub Actions job crawls `r-bloggers.com`, writes source rows to
`webr.events` as `webr.rblogger.raw.v1` / `webr.rblogger.log.v1` events, then
translates each new English source row and emits `webr.rblogger.board.v1`.
It also reads ClickHouse for board translations that are missing or older than
one month, retranslates them, and publishes fresh `webr.rblogger.board.v1`
events back to Kafka. ClickHouse consumes those Kafka events through the
`Statground_SQL` `Data_R_Blogger_*` Kafka Engine queue and materialized views.

This repository intentionally does not keep dated change-log files.

## Required Secrets

For the scheduled crawl path:

- `KAFKA_BROKERS`
- `KAFKA_USERNAME` or `KAFKA_EXTERNAL_USER`
- `KAFKA_PASSWORD` or `KAFKA_EXTERNAL_PASSWORD`
- `CH_HOST` or `CLICKHOUSE_HOST`
- `CH_USER` or `CLICKHOUSE_USER`
- `CH_PASSWORD` or `CLICKHOUSE_PASSWORD`
- one of `OPENROUTER_API_KEY`, `GROQ_API_KEY`, `CEREBRAS_API_KEY`,
  `GH_MODELS_API_KEY`

For manual `rebuild_board=true`, Kafka secrets are not required, but the same
`CH_USER` / `CH_PASSWORD` ClickHouse secrets and one AI provider secret are
required.

## Useful Variables

- `RBLOGGER_MAX_PAGES_FROM_HOME`: listing pages to inspect, default `1`
- `RBLOGGER_MAX_URLS`: crawl cap, default `0` for no cap
- `RBLOGGER_STALE_TRANSLATION_LIMIT`: number of missing/month-old board
  translations to retry per run, default `20`; set `0` to disable
- `RBLOGGER_REBUILD_LIMIT`: manual rebuild cap for `rebuild_board=true`,
  default `0` for a full rebuild; positive values rebuild only that many UUIDs
- `RBLOGGER_REBUILD_BATCH_SIZE`: ClickHouse mutation/insert batch size for
  board rebuild, default `50`
- `RBLOGGER_TRANSLATE_ENABLED`: set `false` to ingest raw/log only
- `RBLOGGER_TRANSLATION_MODEL`: default `google/gemini-2.0-flash-exp:free`
- `CH_PORT` or `CLICKHOUSE_PORT`: default `8123`
- `CH_DATABASE` or `CLICKHOUSE_DATABASE`: default `Data_R_Blogger_Raw`
- `CH_SECURE` or `CLICKHOUSE_SECURE`: set `true` for HTTPS
- `CH_TIMEOUT` or `CLICKHOUSE_TIMEOUT`: ClickHouse read timeout seconds, default `60`
- `RBLOGGER_KAFKA_BATCH_SIZE`: default `50`
- `RBLOGGER_KAFKA_WRITE_CHUNK_SIZE`: default `50`
- `RBLOGGER_KAFKA_MAX_MESSAGE_BYTES`: default `524288`

The scheduled GitHub Actions workflow runs once per hour at minute `15`.

## Identifier Policy

- Kafka `event_uuid` and log UUIDs are generated as UUIDv7 so event streams and
  logs keep useful time locality.
- Raw/board row UUIDs remain deterministic from the canonical article URL. That
  UUID is the dedupe key for a public source article and should not be rewritten
  to a random or time-based value without a separate alias/backfill plan.
- If a future serving table rekeys R-bloggers content to UUIDv7, preserve the
  deterministic URL UUID through the Web-R UUID alias registry so old public
  URLs continue to resolve without redirect.

Translation prompts are written in English and explicitly forbid URLs,
Markdown links, HTML `<a>` tags, and `href` attributes. The Go command also
post-processes translated output to remove hyperlinks before publishing
`board` events.

## Full Board Rebuild

Run the workflow manually with `rebuild_board=true` and `rebuild_limit=0` to
rebuild every active `Data_R_Blogger_Raw.raw` row into `Data_R_Blogger_Service.board`.

That mode translates each raw row again, deactivates existing Korean board rows,
aligns their `created_at` to the raw crawl timestamp, then inserts fresh active
board rows whose `created_at` also matches the raw crawl timestamp. Blank or
whitespace-only translated content falls back to the translated title. The
ClickHouse user used for `CH_USER` must be allowed to run `ON CLUSTER`
mutations and to `ALTER` / `INSERT` on `Data_R_Blogger_Service.board_local` /
`Data_R_Blogger_Service.board`.

When `rebuild_limit` is greater than `0`, the command performs a scoped partial
rebuild and deactivates only the selected raw UUIDs. Existing Korean board rows
are deactivated globally only for the full rebuild path.
