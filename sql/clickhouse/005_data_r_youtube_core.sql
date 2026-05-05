SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE DATABASE IF NOT EXISTS `Data_R_Youtube_Log`
ON CLUSTER statground_cluster
COMMENT 'R YouTube Kafka queues, ingestion errors, and collection logs. ClickHouse operational log layer.';

CREATE DATABASE IF NOT EXISTS `Data_R_Youtube_Raw`
ON CLUSTER statground_cluster
COMMENT 'R YouTube raw collection events, video snapshots, transcript segments, comments, and package mentions. ClickHouse owns collector operational state through Data_R_Youtube_Log and Data_R_Youtube_Service.';

CREATE DATABASE IF NOT EXISTS `Data_R_Youtube_Service`
ON CLUSTER statground_cluster
COMMENT 'R YouTube normalized service tables and Web-R official YouTube serving views.';

CREATE DATABASE IF NOT EXISTS `Data_R_Youtube_Mart`
ON CLUSTER statground_cluster
COMMENT 'R YouTube package media presence, radar, and content-gap marts.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Log`.kafka_r_youtube_events_queue
ON CLUSTER statground_cluster
(
    event_id String COMMENT 'Kafka JSON event_id. UUID v7 string',
    event_type String COMMENT 'Kafka JSON event_type, for example r.youtube.video.snapshot.v1',
    schema_version UInt16 COMMENT 'Kafka JSON schema_version',
    source String COMMENT 'Collector source code',
    source_url String COMMENT 'Source URL that produced this event',
    repository String COMMENT 'Logical repository namespace, normally YouTube or R-YouTube',
    package_name String COMMENT 'R package name when the event is package-scoped',
    package_version String COMMENT 'R package version when applicable',
    observed_at String COMMENT 'Upstream observation timestamp as ISO-8601 text',
    collected_at String COMMENT 'Collector timestamp as ISO-8601 text',
    payload_hash String COMMENT 'SHA-256 hash of canonical payload JSON',
    payload String COMMENT 'Canonical payload JSON string'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-platform:19092',
    kafka_topic_list = 'r.youtube.events',
    kafka_group_name = 'clickhouse_data_r_youtube_events_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream'
COMMENT 'Kafka Engine queue for R YouTube intelligence events. Do not SELECT except for debugging.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Log`.kafka_ingest_error_local
ON CLUSTER statground_cluster
(
    event_id_raw String COMMENT 'Raw event_id string from Kafka message if parsed; may be empty for parser errors',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic where malformed message was consumed',
    kafka_partition UInt32 COMMENT 'Kafka partition of malformed message',
    kafka_offset UInt64 COMMENT 'Kafka offset of malformed message',
    event_type LowCardinality(String) COMMENT 'Event type if parsed; empty when JSON parsing failed',
    raw_message String COMMENT 'Original raw Kafka message for replay/debug',
    error_message String COMMENT 'ClickHouse parser error message',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse error capture timestamp'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Log/kafka_ingest_error_local', '{replica}')
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, kafka_topic, kafka_partition, kafka_offset)
SETTINGS index_granularity = 8192
COMMENT 'R YouTube Kafka ingestion parser error local replicated table.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Log`.kafka_ingest_error
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Log`.kafka_ingest_error_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Log', 'kafka_ingest_error_local', rand())
COMMENT 'Distributed R YouTube Kafka ingestion parser error table.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Log`.collection_log_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Event UUID converted from event_id, or generated for malformed UUIDs',
    event_id String COMMENT 'Collector event_id',
    event_type LowCardinality(String) COMMENT 'R YouTube event type',
    source LowCardinality(String) COMMENT 'Collector source code',
    source_method LowCardinality(String) COMMENT 'Collection method from payload',
    source_url String COMMENT 'Source URL that produced this event',
    repository LowCardinality(String) COMMENT 'Logical repository namespace',
    package_name String COMMENT 'Package name when package-scoped',
    collection_status LowCardinality(String) COMMENT 'Collection status such as collected, skipped, failed, seeded',
    error_code LowCardinality(String) COMMENT 'Failure taxonomy code when present',
    payload_hash String COMMENT 'SHA-256 payload hash',
    payload String COMMENT 'Canonical payload JSON string',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Observed timestamp normalized for queries',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp normalized for queries',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Log/collection_log_local', '{replica}')
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, event_type, source, collection_status, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Operational log for R YouTube collection attempts and failures.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Log`.collection_log
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Log`.collection_log_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Log', 'collection_log_local', cityHash64(event_id))
COMMENT 'Distributed R YouTube collection log table.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_event_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Event UUID converted from collector event_id',
    event_id String COMMENT 'Kafka event_id as published by collector',
    event_type LowCardinality(String) COMMENT 'R YouTube event type discriminator',
    schema_version UInt16 COMMENT 'Event schema version',
    source LowCardinality(String) COMMENT 'Collector source code',
    source_url String COMMENT 'Source URL that produced this event',
    repository LowCardinality(String) COMMENT 'Logical repository namespace',
    package_name String COMMENT 'Package name for package-scoped events',
    package_version String COMMENT 'Package version when applicable',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp normalized for analytic queries',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp normalized for analytic queries',
    payload_hash String COMMENT 'SHA-256 hash of canonical payload JSON',
    payload String COMMENT 'Canonical payload JSON string; raw audit field; SSOT not here',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Raw/r_youtube_event_raw_local', '{replica}')
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, event_type, repository, package_name, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Generic R YouTube collector event raw local table. Kafka-fed OLAP audit data; SSOT not here.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_event_raw
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Raw`.r_youtube_event_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Raw', 'r_youtube_event_raw_local', cityHash64(event_id))
COMMENT 'Distributed generic R YouTube collector event raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Log`.mv_kafka_r_youtube_event_to_raw
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Raw`.r_youtube_event_raw_local
AS
SELECT
    ifNull(toUUIDOrNull(event_id), generateUUIDv4()) AS uuid,
    event_id,
    event_type,
    schema_version,
    source,
    source_url,
    repository,
    package_name,
    package_version,
    ifNull(parseDateTime64BestEffortOrNull(observed_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS observed_at,
    ifNull(parseDateTime64BestEffortOrNull(collected_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS collected_at,
    payload_hash,
    payload,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_R_Youtube_Log`.kafka_r_youtube_events_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Log`.mv_kafka_r_youtube_event_to_collection_log
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Log`.collection_log_local
AS
SELECT
    ifNull(toUUIDOrNull(event_id), generateUUIDv4()) AS uuid,
    event_id,
    event_type,
    source,
    ifNull(nullIf(JSONExtractString(payload, 'source_method'), ''), '') AS source_method,
    source_url,
    repository,
    package_name,
    ifNull(nullIf(JSONExtractString(payload, 'collection_status'), ''), 'collected') AS collection_status,
    ifNull(nullIf(JSONExtractString(payload, 'error_code'), ''), '') AS error_code,
    payload_hash,
    payload,
    ifNull(parseDateTime64BestEffortOrNull(observed_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS observed_at,
    ifNull(parseDateTime64BestEffortOrNull(collected_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS collected_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_R_Youtube_Log`.kafka_r_youtube_events_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Log`.mv_kafka_r_youtube_event_parse_error_to_dlq
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Log`.kafka_ingest_error_local
AS
SELECT
    event_id AS event_id_raw,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    event_type AS event_type,
    _raw_message AS raw_message,
    _error AS error_message,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_R_Youtube_Log`.kafka_r_youtube_events_queue
WHERE length(_error) > 0;

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_source_seed_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    source_code String COMMENT 'Stable seed source code',
    source_type LowCardinality(String) COMMENT 'Seed type: channel, playlist, video, query, channel_or_playlist_family',
    title String COMMENT 'Seed title',
    url String COMMENT 'Seed URL or query URL',
    category LowCardinality(String) COMMENT 'R YouTube source category',
    language_hint LowCardinality(String) COMMENT 'Language hint from seed catalog',
    source_confidence LowCardinality(String) COMMENT 'Confidence: confirmed, candidate, admin_verified, rejected',
    priority LowCardinality(String) COMMENT 'Collection priority such as P0, P1, P2',
    parsed_ref_type LowCardinality(String) COMMENT 'Parsed YouTube URL reference type',
    parsed_video_id String COMMENT 'Parsed video ID when source is video/shorts/live',
    parsed_playlist_id String COMMENT 'Parsed playlist ID when source is playlist',
    parsed_channel_id String COMMENT 'Parsed channel ID when source is channel',
    parsed_handle String COMMENT 'Parsed handle or custom/user route when present',
    notes String COMMENT 'Seed notes from planning document',
    payload_json String COMMENT 'Raw normalized seed payload JSON',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Seed observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Raw/r_youtube_source_seed_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, source_code, source_type, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Raw R YouTube source seed catalog events loaded from fixture and admin curation.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_source_seed_raw
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Raw`.r_youtube_source_seed_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Raw', 'r_youtube_source_seed_raw_local', cityHash64(source_code))
COMMENT 'Distributed R YouTube source seed raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Raw`.mv_r_youtube_event_to_source_seed
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Raw`.r_youtube_source_seed_raw_local
AS
SELECT
    uuid,
    event_id,
    JSONExtractString(payload, 'source_code') AS source_code,
    JSONExtractString(payload, 'source_type') AS source_type,
    JSONExtractString(payload, 'title') AS title,
    JSONExtractString(payload, 'url') AS url,
    JSONExtractString(payload, 'category') AS category,
    JSONExtractString(payload, 'language_hint') AS language_hint,
    JSONExtractString(payload, 'source_confidence') AS source_confidence,
    JSONExtractString(payload, 'priority') AS priority,
    JSONExtractString(payload, 'parsed_ref_type') AS parsed_ref_type,
    JSONExtractString(payload, 'parsed_video_id') AS parsed_video_id,
    JSONExtractString(payload, 'parsed_playlist_id') AS parsed_playlist_id,
    JSONExtractString(payload, 'parsed_channel_id') AS parsed_channel_id,
    JSONExtractString(payload, 'parsed_handle') AS parsed_handle,
    JSONExtractString(payload, 'notes') AS notes,
    payload AS payload_json,
    observed_at,
    collected_at,
    payload_hash,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_event_raw_local
WHERE event_type = 'r.youtube.source.seed.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_video_snapshot_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    youtube_video_id String COMMENT 'YouTube video ID',
    youtube_channel_id String COMMENT 'YouTube channel ID',
    playlist_ids String COMMENT 'JSON array string of playlist IDs when available',
    video_title String COMMENT 'Video title at collection time',
    video_description String COMMENT 'Video description at collection time',
    canonical_url String COMMENT 'Canonical YouTube video URL',
    thumbnail_url String COMMENT 'Representative thumbnail URL',
    published_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Video published timestamp normalized to Asia/Seoul',
    duration_seconds Nullable(UInt32) COMMENT 'Video duration in seconds',
    view_count Nullable(UInt64) COMMENT 'View count snapshot from YouTube API or legacy source',
    like_count Nullable(UInt64) COMMENT 'Like count snapshot from YouTube API or legacy source',
    comment_count Nullable(UInt64) COMMENT 'Comment count snapshot from YouTube API when available',
    favorite_count Nullable(UInt64) COMMENT 'Favorite count snapshot from YouTube API when available',
    caption_available UInt8 COMMENT 'Caption availability flag: 1 available, 0 unavailable/unknown',
    default_audio_language LowCardinality(String) COMMENT 'Default audio language code when available',
    default_language LowCardinality(String) COMMENT 'Default metadata language code when available',
    tags_json String COMMENT 'Raw JSON array of YouTube tags',
    thumbnail_urls_json String COMMENT 'Raw JSON object of thumbnail variants',
    source_method LowCardinality(String) COMMENT 'Collection method: videos.list, search.list, legacy_webr_board_youtube, etc.',
    source_tag LowCardinality(String) COMMENT 'Operational tag such as r_project_ecosystem_youtube or web_r_official_youtube',
    source_category LowCardinality(String) COMMENT 'Source category such as conference, tutorial, Web-R board, or education',
    source_confidence LowCardinality(String) COMMENT 'Confidence: seed, confirmed, admin_migrated_legacy, discovered, candidate',
    language_code LowCardinality(String) COMMENT 'BCP-47 language code of video metadata text',
    uuid_article Nullable(UUID) COMMENT 'Legacy Web-R article UUID when this video is attached to a board article',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    payload_json String COMMENT 'Raw normalized source payload JSON string',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Raw/r_youtube_video_snapshot_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, youtube_video_id, source_tag, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Raw YouTube video metadata/statistics snapshots for R intelligence and migrated Web-R official YouTube rows.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_video_snapshot_raw
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Raw`.r_youtube_video_snapshot_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Raw', 'r_youtube_video_snapshot_raw_local', cityHash64(youtube_video_id))
COMMENT 'Distributed R YouTube video snapshot raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Raw`.mv_r_youtube_event_to_video_snapshot
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Raw`.r_youtube_video_snapshot_raw_local
AS
SELECT
    uuid,
    event_id,
    JSONExtractString(payload, 'youtube_video_id') AS youtube_video_id,
    JSONExtractString(payload, 'youtube_channel_id') AS youtube_channel_id,
    JSONExtractString(payload, 'playlist_ids_json') AS playlist_ids,
    JSONExtractString(payload, 'video_title') AS video_title,
    JSONExtractString(payload, 'video_description') AS video_description,
    JSONExtractString(payload, 'canonical_url') AS canonical_url,
    JSONExtractString(payload, 'thumbnail_url') AS thumbnail_url,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'published_at'), 3, 'Asia/Seoul') AS published_at,
    toUInt32OrNull(JSONExtractString(payload, 'duration_seconds')) AS duration_seconds,
    toUInt64OrNull(JSONExtractString(payload, 'view_count')) AS view_count,
    toUInt64OrNull(JSONExtractString(payload, 'like_count')) AS like_count,
    toUInt64OrNull(JSONExtractString(payload, 'comment_count')) AS comment_count,
    toUInt64OrNull(JSONExtractString(payload, 'favorite_count')) AS favorite_count,
    toUInt8(if(JSONExtractBool(payload, 'caption_available'), 1, toUInt8OrZero(JSONExtractString(payload, 'caption_available')))) AS caption_available,
    JSONExtractString(payload, 'default_audio_language') AS default_audio_language,
    JSONExtractString(payload, 'default_language') AS default_language,
    JSONExtractString(payload, 'tags_json') AS tags_json,
    JSONExtractString(payload, 'thumbnail_urls_json') AS thumbnail_urls_json,
    JSONExtractString(payload, 'source_method') AS source_method,
    ifNull(nullIf(JSONExtractString(payload, 'source_tag'), ''), 'r_project_ecosystem_youtube') AS source_tag,
    JSONExtractString(payload, 'source_category') AS source_category,
    JSONExtractString(payload, 'source_confidence') AS source_confidence,
    ifNull(nullIf(JSONExtractString(payload, 'language_code'), ''), 'und') AS language_code,
    toUUIDOrNull(JSONExtractString(payload, 'uuid_article')) AS uuid_article,
    payload_hash,
    payload AS payload_json,
    observed_at,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_event_raw_local
WHERE event_type = 'r.youtube.video.snapshot.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_transcript_segment_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    youtube_video_id String COMMENT 'YouTube video ID',
    caption_track_key String COMMENT 'Caption track key from API or derived public transcript source',
    language_code LowCardinality(String) COMMENT 'Caption language code such as en, ko, ja, es',
    segment_index UInt32 COMMENT 'Segment index within the caption track',
    start_ms UInt64 COMMENT 'Segment start time in milliseconds from video start',
    end_ms UInt64 COMMENT 'Segment end time in milliseconds from video start',
    duration_ms UInt64 COMMENT 'Segment duration in milliseconds',
    text_raw String COMMENT 'Raw subtitle/transcript text collected from source',
    text_normalized String COMMENT 'Normalized transcript text for search and package mention extraction',
    is_auto_generated UInt8 COMMENT 'Auto-generated caption flag',
    source_method LowCardinality(String) COMMENT 'Collection method: public_transcript_api, ytdlp_subtitle, youtube_captions_download_oauth, etc.',
    collection_status LowCardinality(String) COMMENT 'Collection status or failure state',
    retention_policy_code LowCardinality(String) COMMENT 'Retention policy code for transcript text',
    payload_hash String COMMENT 'SHA-256 hash of normalized segment text and timing',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Raw/r_youtube_transcript_segment_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, youtube_video_id, language_code, segment_index, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Raw YouTube transcript/caption segment text for R intelligence search and package mention extraction.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_transcript_segment_raw
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Raw`.r_youtube_transcript_segment_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Raw', 'r_youtube_transcript_segment_raw_local', cityHash64(youtube_video_id))
COMMENT 'Distributed R YouTube transcript segment raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Raw`.mv_r_youtube_event_to_transcript_segment
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Raw`.r_youtube_transcript_segment_raw_local
AS
SELECT
    uuid,
    event_id,
    JSONExtractString(payload, 'youtube_video_id') AS youtube_video_id,
    JSONExtractString(payload, 'caption_track_key') AS caption_track_key,
    ifNull(nullIf(JSONExtractString(payload, 'language_code'), ''), 'und') AS language_code,
    toUInt32OrZero(JSONExtractString(payload, 'segment_index')) AS segment_index,
    toUInt64OrZero(JSONExtractString(payload, 'start_ms')) AS start_ms,
    toUInt64OrZero(JSONExtractString(payload, 'end_ms')) AS end_ms,
    toUInt64OrZero(JSONExtractString(payload, 'duration_ms')) AS duration_ms,
    JSONExtractString(payload, 'text_raw') AS text_raw,
    JSONExtractString(payload, 'text_normalized') AS text_normalized,
    toUInt8(if(JSONExtractBool(payload, 'is_auto_generated'), 1, toUInt8OrZero(JSONExtractString(payload, 'is_auto_generated')))) AS is_auto_generated,
    JSONExtractString(payload, 'source_method') AS source_method,
    ifNull(nullIf(JSONExtractString(payload, 'collection_status'), ''), 'collected') AS collection_status,
    ifNull(nullIf(JSONExtractString(payload, 'retention_policy_code'), ''), 'retain_public_caption_best_effort') AS retention_policy_code,
    payload_hash,
    observed_at,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_event_raw_local
WHERE event_type = 'r.youtube.transcript.segment.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_comment_thread_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    youtube_video_id String COMMENT 'YouTube video ID',
    comment_thread_id String COMMENT 'YouTube comment thread ID',
    comment_id String COMMENT 'YouTube comment ID',
    parent_comment_id String COMMENT 'Parent comment ID; empty for top-level comments',
    author_channel_id_hash String COMMENT 'Hashed author channel ID for privacy-preserving analysis',
    author_display_name_hash String COMMENT 'Hashed author display name for privacy-preserving analysis',
    text_original String COMMENT 'Original comment text as returned by source, subject to retention policy',
    text_normalized String COMMENT 'Normalized comment text for topic/error/question extraction',
    like_count UInt64 COMMENT 'Comment like count snapshot',
    published_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Comment published timestamp normalized to Asia/Seoul',
    updated_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Comment updated timestamp normalized to Asia/Seoul',
    total_reply_count UInt32 COMMENT 'Reply count for top-level thread when available',
    source_method LowCardinality(String) COMMENT 'Collection method, normally commentThreads.list',
    retention_policy_code LowCardinality(String) COMMENT 'Retention policy for comment text and author hashes',
    payload_hash String COMMENT 'SHA-256 hash of normalized comment payload',
    payload_json String COMMENT 'Raw normalized JSON payload',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Raw/r_youtube_comment_thread_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, youtube_video_id, comment_thread_id, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Raw YouTube comments for R learning and FAQ analysis; public display must aggregate or anonymize.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_comment_thread_raw
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Raw`.r_youtube_comment_thread_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Raw', 'r_youtube_comment_thread_raw_local', cityHash64(youtube_video_id))
COMMENT 'Distributed R YouTube comment thread raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Raw`.mv_r_youtube_event_to_comment_thread
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Raw`.r_youtube_comment_thread_raw_local
AS
SELECT
    uuid,
    event_id,
    JSONExtractString(payload, 'youtube_video_id') AS youtube_video_id,
    JSONExtractString(payload, 'comment_thread_id') AS comment_thread_id,
    JSONExtractString(payload, 'comment_id') AS comment_id,
    JSONExtractString(payload, 'parent_comment_id') AS parent_comment_id,
    JSONExtractString(payload, 'author_channel_id_hash') AS author_channel_id_hash,
    JSONExtractString(payload, 'author_display_name_hash') AS author_display_name_hash,
    JSONExtractString(payload, 'text_original') AS text_original,
    JSONExtractString(payload, 'text_normalized') AS text_normalized,
    toUInt64OrZero(JSONExtractString(payload, 'like_count')) AS like_count,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'published_at'), 3, 'Asia/Seoul') AS published_at,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'updated_at'), 3, 'Asia/Seoul') AS updated_at,
    toUInt32OrZero(JSONExtractString(payload, 'total_reply_count')) AS total_reply_count,
    JSONExtractString(payload, 'source_method') AS source_method,
    ifNull(nullIf(JSONExtractString(payload, 'retention_policy_code'), ''), 'public_comment_aggregate_only') AS retention_policy_code,
    payload_hash,
    payload AS payload_json,
    observed_at,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_event_raw_local
WHERE event_type = 'r.youtube.comment.thread.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_package_mention_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    youtube_video_id String COMMENT 'YouTube video ID',
    package_name String COMMENT 'R package name detected from title/description/tags/transcript/comments',
    repository LowCardinality(String) COMMENT 'Package repository: CRAN, Bioconductor, R-universe, unknown',
    mention_source LowCardinality(String) COMMENT 'Mention source: title, description, tag, transcript, comment, link_url',
    language_code LowCardinality(String) COMMENT 'Language code of source text when known',
    segment_start_ms UInt64 COMMENT 'Transcript segment start time in milliseconds; 0 when not segment-based',
    segment_end_ms UInt64 COMMENT 'Transcript segment end time in milliseconds; 0 when not segment-based',
    match_text String COMMENT 'Short matched text around the package mention',
    confidence LowCardinality(String) COMMENT 'Mention confidence: high, medium, low, rejected',
    confidence_score Float32 COMMENT 'Numeric confidence score from mention extractor',
    extractor_version LowCardinality(String) COMMENT 'Package mention extractor version',
    payload_hash String COMMENT 'SHA-256 hash for deduplication',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Raw/r_youtube_package_mention_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, youtube_video_id, package_name, mention_source, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Raw R package mentions extracted from YouTube metadata, transcript, comments, and links.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Raw`.r_youtube_package_mention_raw
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Raw`.r_youtube_package_mention_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Raw', 'r_youtube_package_mention_raw_local', cityHash64(package_name))
COMMENT 'Distributed R YouTube package mention raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Raw`.mv_r_youtube_event_to_package_mention
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Raw`.r_youtube_package_mention_raw_local
AS
SELECT
    uuid,
    event_id,
    JSONExtractString(payload, 'youtube_video_id') AS youtube_video_id,
    package_name,
    ifNull(nullIf(repository, ''), 'unknown') AS repository,
    JSONExtractString(payload, 'mention_source') AS mention_source,
    ifNull(nullIf(JSONExtractString(payload, 'language_code'), ''), 'und') AS language_code,
    toUInt64OrZero(JSONExtractString(payload, 'segment_start_ms')) AS segment_start_ms,
    toUInt64OrZero(JSONExtractString(payload, 'segment_end_ms')) AS segment_end_ms,
    JSONExtractString(payload, 'match_text') AS match_text,
    ifNull(nullIf(JSONExtractString(payload, 'confidence'), ''), 'medium') AS confidence,
    toFloat32OrZero(JSONExtractString(payload, 'confidence_score')) AS confidence_score,
    ifNull(nullIf(JSONExtractString(payload, 'extractor_version'), ''), 'rpkg-youtube-mention-v1') AS extractor_version,
    payload_hash,
    observed_at,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_event_raw_local
WHERE event_type = 'r.youtube.package.mention.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.r_youtube_source_seed_current_local
ON CLUSTER statground_cluster
(
    source_code String COMMENT 'Stable seed source code',
    source_type LowCardinality(String) COMMENT 'Seed type',
    title String COMMENT 'Seed title',
    url String COMMENT 'Seed URL or query URL',
    category LowCardinality(String) COMMENT 'R YouTube source category',
    language_hint LowCardinality(String) COMMENT 'Language hint',
    source_confidence LowCardinality(String) COMMENT 'Confidence',
    priority LowCardinality(String) COMMENT 'Collection priority',
    parsed_ref_type LowCardinality(String) COMMENT 'Parsed YouTube URL reference type',
    parsed_video_id String COMMENT 'Parsed video ID',
    parsed_playlist_id String COMMENT 'Parsed playlist ID',
    parsed_channel_id String COMMENT 'Parsed channel ID',
    parsed_handle String COMMENT 'Parsed handle',
    notes String COMMENT 'Seed notes',
    active UInt8 DEFAULT 1 COMMENT 'Serving active flag',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Service/r_youtube_source_seed_current_local', '{replica}', collected_at)
ORDER BY (source_code, source_type)
SETTINGS index_granularity = 8192
COMMENT 'Current R YouTube source seed catalog for service use.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.r_youtube_source_seed_current
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Service`.r_youtube_source_seed_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Service', 'r_youtube_source_seed_current_local', cityHash64(source_code))
COMMENT 'Distributed current R YouTube source seed catalog.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Service`.mv_source_seed_raw_to_current
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Service`.r_youtube_source_seed_current_local
AS
SELECT
    source_code,
    source_type,
    title,
    url,
    category,
    language_hint,
    source_confidence,
    priority,
    parsed_ref_type,
    parsed_video_id,
    parsed_playlist_id,
    parsed_channel_id,
    parsed_handle,
    notes,
    if(source_confidence = 'rejected', 0, 1) AS active,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_source_seed_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.r_youtube_video_current_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Latest source event UUID',
    youtube_video_id String COMMENT 'YouTube video ID',
    uuid_article Nullable(UUID) COMMENT 'Legacy Web-R article UUID when attached to a board article',
    canonical_url String COMMENT 'Canonical YouTube video URL',
    video_title String COMMENT 'Current video title',
    video_description String COMMENT 'Current video description',
    thumbnail_url String COMMENT 'Representative thumbnail URL',
    youtube_channel_id String COMMENT 'YouTube channel ID',
    channel_title String COMMENT 'Channel title when known',
    published_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Video published timestamp',
    duration_seconds Nullable(UInt32) COMMENT 'Video duration in seconds',
    view_count UInt64 COMMENT 'Latest view count snapshot',
    like_count UInt64 COMMENT 'Latest like count snapshot',
    comment_count UInt64 COMMENT 'Latest comment count snapshot',
    caption_available UInt8 COMMENT 'Caption availability flag',
    default_audio_language LowCardinality(String) COMMENT 'Default audio language code',
    default_language LowCardinality(String) COMMENT 'Default metadata language code',
    tags_json String COMMENT 'Raw JSON array of tags',
    source_method LowCardinality(String) COMMENT 'Collection method',
    source_tag LowCardinality(String) COMMENT 'Operational source tag',
    source_category LowCardinality(String) COMMENT 'Source category',
    source_confidence LowCardinality(String) COMMENT 'Source confidence',
    language_code LowCardinality(String) COMMENT 'BCP-47 language code',
    active UInt8 DEFAULT 1 COMMENT 'Serving active flag',
    payload_json String COMMENT 'Latest raw normalized payload JSON',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'First known service timestamp',
    updated_at DateTime64(3, 'Asia/Seoul') COMMENT 'Latest update timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Service/r_youtube_video_current_local', '{replica}', collected_at)
ORDER BY (youtube_video_id, source_tag, ifNull(uuid_article, toUUID('00000000-0000-0000-0000-000000000000')))
SETTINGS index_granularity = 8192
COMMENT 'Current normalized R YouTube video metadata for serving and Web-R official YouTube compatibility.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.r_youtube_video_current
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Service`.r_youtube_video_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Service', 'r_youtube_video_current_local', cityHash64(youtube_video_id))
COMMENT 'Distributed current R YouTube video metadata.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Service`.mv_video_snapshot_raw_to_current
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Service`.r_youtube_video_current_local
AS
SELECT
    uuid,
    youtube_video_id,
    uuid_article,
    canonical_url,
    video_title,
    video_description,
    thumbnail_url,
    youtube_channel_id,
    JSONExtractString(payload_json, 'channel_title') AS channel_title,
    published_at,
    duration_seconds,
    ifNull(view_count, 0) AS view_count,
    ifNull(like_count, 0) AS like_count,
    ifNull(comment_count, 0) AS comment_count,
    caption_available,
    default_audio_language,
    default_language,
    tags_json,
    source_method,
    source_tag,
    source_category,
    source_confidence,
    language_code,
    1 AS active,
    payload_json,
    collected_at AS created_at,
    collected_at AS updated_at,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_video_snapshot_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.r_youtube_package_mention_current_local
ON CLUSTER statground_cluster
(
    youtube_video_id String COMMENT 'YouTube video ID',
    package_name String COMMENT 'R package name',
    repository LowCardinality(String) COMMENT 'Package repository',
    mention_source LowCardinality(String) COMMENT 'Mention source',
    language_code LowCardinality(String) COMMENT 'Language code',
    segment_start_ms UInt64 COMMENT 'Transcript segment start in milliseconds',
    segment_end_ms UInt64 COMMENT 'Transcript segment end in milliseconds',
    match_text String COMMENT 'Short matched text',
    confidence LowCardinality(String) COMMENT 'Mention confidence',
    confidence_score Float32 COMMENT 'Numeric confidence score',
    extractor_version LowCardinality(String) COMMENT 'Extractor version',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Service/r_youtube_package_mention_current_local', '{replica}', collected_at)
ORDER BY (package_name, youtube_video_id, mention_source, segment_start_ms)
SETTINGS index_granularity = 8192
COMMENT 'Current high/medium-confidence R package mentions in YouTube metadata, transcripts, and comments.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.r_youtube_package_mention_current
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Service`.r_youtube_package_mention_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Service', 'r_youtube_package_mention_current_local', cityHash64(package_name))
COMMENT 'Distributed current R YouTube package mention table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Service`.mv_package_mention_raw_to_current
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Service`.r_youtube_package_mention_current_local
AS
SELECT
    youtube_video_id,
    package_name,
    repository,
    mention_source,
    language_code,
    segment_start_ms,
    segment_end_ms,
    match_text,
    confidence,
    confidence_score,
    extractor_version,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_package_mention_raw_local
WHERE confidence IN ('high', 'medium');

CREATE OR REPLACE VIEW `Data_R_Youtube_Service`.v_webr_official_youtube
ON CLUSTER statground_cluster
AS
SELECT
    uuid,
    uuid_article,
    canonical_url AS url,
    video_title AS title,
    video_description AS description,
    thumbnail_url AS thumbnail,
    CAST(view_count, 'Nullable(Int64)') AS views,
    CAST(like_count, 'Nullable(Int64)') AS likes,
    CAST(duration_seconds, 'Nullable(Int64)') AS duration,
    published_at AS publish_date,
    created_at,
    updated_at,
    CAST(active, 'Nullable(UInt8)') AS active,
    CAST(NULL, 'Nullable(JSON)') AS created_log,
    CAST(NULL, 'Nullable(JSON)') AS updated_log,
    language_code,
    source_tag,
    source_category
FROM `Data_R_Youtube_Service`.r_youtube_video_current
WHERE source_tag = 'web_r_official_youtube'
  AND active = 1;

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Mart`.package_youtube_presence_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'Report date in Asia/Seoul',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart build timestamp',
    uuid UUID COMMENT 'UUID v7 for this mart row build event',
    package_name String COMMENT 'R package name',
    repository LowCardinality(String) COMMENT 'Package repository',
    video_count_total UInt64 COMMENT 'Total distinct R-relevant videos mentioning this package',
    video_count_30d UInt64 COMMENT 'Distinct videos published in last 30 days mentioning this package',
    trusted_video_count_total UInt64 COMMENT 'Distinct videos from trusted/seed/admin-verified sources mentioning this package',
    transcript_mention_count_total UInt64 COMMENT 'Total transcript segment mentions for this package',
    comment_mention_count_total UInt64 COMMENT 'Total comment mentions for this package',
    total_view_count_snapshot UInt64 COMMENT 'Sum of latest view count snapshots for videos mentioning this package',
    tutorial_video_count UInt64 COMMENT 'Videos classified as tutorial for this package',
    conference_video_count UInt64 COMMENT 'Videos classified as conference talk for this package',
    korean_video_count UInt64 COMMENT 'Korean-language videos mentioning this package',
    latest_video_published_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Latest video publish time mentioning this package',
    youtube_presence_score Float32 COMMENT 'Composite package media presence score',
    content_gap_score_ko Float32 COMMENT 'Korean content gap score'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Mart/package_youtube_presence_daily_local', '{replica}', created_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, package_name, repository, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Daily package-level YouTube presence mart for R packages.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Mart`.package_youtube_presence_daily
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Mart`.package_youtube_presence_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Mart', 'package_youtube_presence_daily_local', cityHash64(package_name))
COMMENT 'Distributed package-level YouTube presence mart.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Mart`.youtube_radar_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'Report date in Asia/Seoul',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart build timestamp',
    uuid UUID COMMENT 'UUID v7 for this radar row',
    youtube_video_id String COMMENT 'YouTube video ID',
    video_title String COMMENT 'Video title',
    canonical_url String COMMENT 'Canonical video URL',
    source_tag LowCardinality(String) COMMENT 'Operational source tag',
    source_category LowCardinality(String) COMMENT 'Source category',
    language_code LowCardinality(String) COMMENT 'Language code',
    published_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Video published timestamp',
    view_count UInt64 COMMENT 'Latest view count snapshot',
    package_mention_count UInt64 COMMENT 'Distinct package mentions',
    transcript_segment_count UInt64 COMMENT 'Transcript segment count',
    radar_score Float32 COMMENT 'Composite radar score'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Mart/youtube_radar_daily_local', '{replica}', created_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, radar_score, youtube_video_id, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Daily R YouTube radar mart for new, trending, captioned, Korean, tutorial, and conference videos.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Mart`.youtube_radar_daily
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Mart`.youtube_radar_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Mart', 'youtube_radar_daily_local', cityHash64(youtube_video_id))
COMMENT 'Distributed R YouTube radar mart.';

CREATE OR REPLACE VIEW `Data_R_Youtube_Mart`.v_package_youtube_presence_latest
ON CLUSTER statground_cluster
AS
SELECT *
FROM `Data_R_Youtube_Mart`.package_youtube_presence_daily
WHERE report_date = (
    SELECT max(report_date)
    FROM `Data_R_Youtube_Mart`.package_youtube_presence_daily
);
