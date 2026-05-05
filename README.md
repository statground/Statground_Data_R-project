# Statground Data R-project

This repository contains scheduled collectors for Statground R-project data.
It now owns the R Foundation Mastodon sync, the R-bloggers collector that used
to live in `Statground_Data_R_Blogger`, and the new R package intelligence
collectors described in `temp/r_project`.

R Foundation Mastodon writes Web-R board/runtime events through `webr.events`
and ClickHouse stores them in the split databases
`Data_R_Project_Mastodon_Raw`, `Data_R_Project_Mastodon_Log`,
`Data_R_Project_Mastodon_Service`, and `Data_R_Project_Mastodon_Mart`.

R-bloggers writes the same Kafka event family and ClickHouse stores it in
`Data_R_Blogger_Raw`, `Data_R_Blogger_Log`, `Data_R_Blogger_Service`, and
`Data_R_Blogger_Mart`.

R package intelligence collectors live under `collectors/rpkg` and publish
normalized events to `rpkg.events` for ClickHouse ingestion through
`Data_R_Package_Log`, then into `Data_R_Package_Raw`,
`Data_R_Package_Service`, and `Data_R_Package_Mart`.

R YouTube intelligence also lives under `collectors/rpkg`. It publishes
`r.youtube.*` events to `r.youtube.events`; ClickHouse stores them in
`Data_R_Youtube_Log`, `Data_R_Youtube_Raw`, `Data_R_Youtube_Service`, and
`Data_R_Youtube_Mart`. Legacy Web-R official YouTube rows from
`webr_board.youtube` are migrated with the `web_r_official_youtube` tag so
existing `/workshop/youtube/` pages keep their article enrichment while new R
ecosystem YouTube collection grows separately.

## Workflows

- `.github/workflows/sync-r-foundation-mastodon-to-kafka.yml`
  keeps the existing R Foundation Mastodon sync running hourly at minute `17`.
- `.github/workflows/rblogger_collect.yml`
  runs the merged R-bloggers crawler hourly at minute `15`.
- `.github/workflows/r-package-cran-metadata.yml`
  runs CRAN package metadata hourly at minute `10`.
- `.github/workflows/r-package-cran-downloads.yml`
  runs CRAN download snapshots hourly at minute `20`.
- `.github/workflows/r-package-cran-reverse-dependencies.yml`
  runs CRAN reverse dependency snapshots hourly at minute `30`.
- `.github/workflows/r-package-cran-checks.yml`
  runs CRAN check snapshots hourly at minute `40`.
- `.github/workflows/r-package-cran-archive.yml`
  runs CRAN archive snapshots hourly at minute `50`.
- `.github/workflows/r-package-r-core-news.yml`
  runs R Core news snapshots hourly at minute `5`.
- `.github/workflows/r-project-websites.yml`
  runs R website directory discovery hourly at minute `35`.
- `.github/workflows/r-youtube-source-seeds.yml`
  publishes the R YouTube seed catalog hourly at minute `45`.
- `.github/workflows/r-youtube-video-metadata.yml`
  publishes YouTube Data API `videos.list` snapshots hourly at minute `55`.
- `.github/workflows/r-youtube-public-transcripts.yml`
  publishes public transcript segments hourly at minute `25`.
- `.github/workflows/r-package-intelligence.yml`
  remains as a manual dispatcher for ad hoc R package collector runs.

Required secrets for `r-package-intelligence.yml`:

- `KAFKA_BROKERS` or `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_USERNAME`
- `KAFKA_PASSWORD`

Optional variables:

- `RPKG_KAFKA_TOPIC`, default `rpkg.events`
- `R_YOUTUBE_KAFKA_TOPIC`, default `r.youtube.events`
- `RPKG_CRAN_METADATA_LIMIT`, default `0` for all rows
- `RPKG_DOWNLOAD_TOP`, default `100`
- `RPKG_DOWNLOAD_PERIOD`, default `last-month`
- `YOUTUBE_API_KEY` secret for `r-youtube-video-metadata.yml`
- `R_YOUTUBE_VIDEO_IDS` variable for explicit video snapshot/transcript IDs
- `R_YOUTUBE_MENTION_PACKAGES` variable for package mention extraction

## DB Setup

The repository-local setup files are in `sql/`:

- `sql/clickhouse/001_data_r_package_core.sql`
- `sql/clickhouse/002_move_data_r_package_queue_to_log.sql`
- `sql/clickhouse/005_data_r_youtube_core.sql`
- `sql/clickhouse/006_data_r_youtube_operational_state.sql`
- `sql/clickhouse/007_remove_data_r_package_tidb_comments.sql`
- `sql/clickhouse/008_remove_data_r_youtube_tidb_comments.sql`
- `sql/clickhouse/Data_R_Project_Mastodon_*`
- `sql/clickhouse/Data_R_Blogger_*`
- `sql/clickhouse/Data_R_Youtube_*`
- `sql/clickhouse/migrations/20260505_split_mastodon_rblogger_databases.sql`
- `sql/clickhouse/migrations/20260505_migrate_webr_board_youtube.sql`
- `sql/clickhouse/migrations/20260505_update_webr_board_v_article_youtube.sql`
- `sql/trino/README.md`

The same DB contract is mirrored into `Statground_SQL` so the live database
shape and the durable SQL repository stay aligned.
