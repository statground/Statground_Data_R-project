# R-project ecosystem intelligence plan

This note folds the planning files in `temp/r_project` into the repository
contract. Collection belongs in `Statground_Data_R-project`; Web-R should read
from the resulting Data_* ClickHouse layers instead of running crawlers inside
the service repo.

## Data domains

- R Foundation Mastodon:
  `Data_R_Project_Mastodon_Log`, `Data_R_Project_Mastodon_Raw`,
  `Data_R_Project_Mastodon_Service`, `Data_R_Project_Mastodon_Mart`.
- R-bloggers:
  `Data_R_Blogger_Log`, `Data_R_Blogger_Raw`, `Data_R_Blogger_Service`,
  `Data_R_Blogger_Mart`.
- R package intelligence:
  `Data_R_Package_Log`, `Data_R_Package_Raw`, `Data_R_Package_Service`,
  `Data_R_Package_Mart`.
- R YouTube intelligence:
  `Data_R_Youtube_Log`, `Data_R_Youtube_Raw`,
  `Data_R_Youtube_Service`, `Data_R_Youtube_Mart`.

Every ClickHouse DDL must be cluster-aware: `ON CLUSTER statground_cluster`,
replicated local tables with `{shard}` and `{replica}`, and public
`Distributed` tables.

## Service surfaces

- `/r/` should become the large R-project ecosystem entry point in Web-R.
- `/r/packages/` should expose package profiles, checks, downloads,
  dependencies, lifecycle signals, package comparison, and enterprise allowlist
  style reports from `Data_R_Package_Mart`.
- `/r/youtube/` should expose R YouTube Radar: new R videos, high-growth
  videos, conference/tutorial content, Korean R content, caption coverage, and
  package mention highlights from `Data_R_Youtube_Mart`.
- `/r/packages/{package_name}/youtube/` should join package profile data with
  `Data_R_Youtube_Mart.package_youtube_presence_daily` and service-level
  mention tables.
- `/r/youtube/search/` should search transcript segments from
  `Data_R_Youtube_Raw.r_youtube_transcript_segment_raw` or a future search
  service projection.

## YouTube collection policy

The YouTube v2 planning document supersedes the earlier "do not collect
transcript text in MVP" stance:

- Collect what is publicly or contractually collectable.
- Store source method, permission path, retention policy, and failure taxonomy
  with the data.
- Do not store video/audio files in MVP.
- Keep comment author identifiers hashed and serve comments only as aggregate
  or anonymized analysis unless a stricter policy is approved.
- Track API quota in `Data_R_Youtube_Log`/`Data_R_Youtube_Service` and keep
  high-cost `search.list` and `captions.list` jobs budgeted.

Implemented MVP collectors:

- `youtube-source-seeds`: loads `fixtures/r_youtube_seed.yml` into
  `r.youtube.source.seed.v1`.
- `youtube-video-snapshots`: uses YouTube Data API `videos.list` for metadata,
  statistics, duration, language, captions flag, and package mentions from
  title/description.
- `youtube-public-transcripts`: uses public transcript access when available,
  stores segment text with `retention_policy_code`, and extracts package
  mentions.

Future collectors from the document remain explicit backlog:

- `channels.list` refresh and uploads playlist discovery.
- `playlistItems.list` crawl.
- Budgeted `search.list` expansion.
- `captions.list` metadata and OAuth-only `captions.download`.
- `yt-dlp` subtitle fallback.
- `commentThreads.list` with privacy-preserving hashes.
- Mart builders for package media presence, R YouTube Radar, Korean content gap,
  learning map, and video-to-package knowledge graph.

Operational state policy:

- TiDB is reserved for user master data only.
- R YouTube API quota accounting is stored in
  `Data_R_Youtube_Log.api_quota_ledger`.
- R YouTube crawl cursor state is stored in
  `Data_R_Youtube_Service.crawl_cursor_current`.
- GitHub Actions collectors may publish cursor/quota events, but they must not
  create or update TiDB collection tables.

## Legacy Web-R official YouTube

The existing `webr_board.youtube` and `webr_board.youtube_local` rows are not
deleted. They are migrated into:

- `Data_R_Youtube_Raw.r_youtube_video_snapshot_raw`
- `Data_R_Youtube_Service.r_youtube_video_current`

Those rows carry:

- `source_tag = 'web_r_official_youtube'`
- `source_category = 'web_r_board_article_video'`
- `source_method = 'legacy_webr_board_youtube'`
- `source_confidence = 'admin_migrated_legacy'`

`webr_board.v_article_youtube` then reads from
`Data_R_Youtube_Service.v_webr_official_youtube`, preserving the current
Web-R article enrichment contract while moving the data ownership to the
R-project Data repository.
