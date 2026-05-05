SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Log`.api_quota_ledger_local
ON CLUSTER statground_cluster
(
    quota_date Date COMMENT 'Quota accounting date in Asia/Seoul',
    uuid UUID COMMENT 'Quota usage event UUID',
    api_key_alias LowCardinality(String) COMMENT 'Internal alias for API key or OAuth client; never store secret',
    method_name LowCardinality(String) COMMENT 'YouTube Data API method name, e.g. videos.list, search.list',
    quota_cost UInt32 COMMENT 'Quota cost per request for the method at collection time',
    request_count UInt32 COMMENT 'Number of requests made for this method/key/date',
    quota_units_used UInt64 COMMENT 'Total quota units used = quota_cost * request_count',
    source LowCardinality(String) COMMENT 'Collector source code',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Log/api_quota_ledger_local', '{replica}', (request_count, quota_units_used))
PARTITION BY toYYYYMM(quota_date)
ORDER BY (quota_date, api_key_alias, method_name, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R YouTube API quota usage ledger. ClickHouse owns collector quota accounting; no secrets are stored.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Log`.api_quota_ledger
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Log`.api_quota_ledger_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Log', 'api_quota_ledger_local', cityHash64(api_key_alias, method_name))
COMMENT 'Distributed R YouTube API quota usage ledger.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Log`.mv_r_youtube_event_to_api_quota_ledger
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Log`.api_quota_ledger_local
AS
SELECT
    toDate(ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'quota_date'), 3, 'Asia/Seoul'), collected_at)) AS quota_date,
    uuid,
    ifNull(nullIf(JSONExtractString(payload, 'api_key_alias'), ''), 'default') AS api_key_alias,
    JSONExtractString(payload, 'method_name') AS method_name,
    toUInt32OrZero(JSONExtractString(payload, 'quota_cost')) AS quota_cost,
    toUInt32OrZero(JSONExtractString(payload, 'request_count')) AS request_count,
    toUInt64OrZero(JSONExtractString(payload, 'quota_units_used')) AS quota_units_used,
    source,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_event_raw_local
WHERE event_type = 'r.youtube.quota.usage.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.crawl_cursor_current_local
ON CLUSTER statground_cluster
(
    cursor_scope LowCardinality(String) COMMENT 'Cursor scope: channel_uploads, playlist_items, search_query, video_stats, comments, transcripts',
    source_key String COMMENT 'External source key: channel ID, playlist ID, query hash, or video ID',
    next_page_token String COMMENT 'YouTube API nextPageToken when applicable',
    last_success_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Last successful crawl timestamp',
    last_failure_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Last failed crawl timestamp',
    failure_count UInt32 COMMENT 'Consecutive failure count',
    last_error_code LowCardinality(String) COMMENT 'Last error code taxonomy',
    active UInt8 DEFAULT 1 COMMENT 'Active flag: 1 active cursor, 0 disabled',
    payload_json String COMMENT 'Latest cursor payload JSON',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Youtube_Service/crawl_cursor_current_local', '{replica}', collected_at)
ORDER BY (cursor_scope, source_key)
SETTINGS index_granularity = 8192
COMMENT 'Current R YouTube collector crawl cursor state. ClickHouse-owned service state.';

CREATE TABLE IF NOT EXISTS `Data_R_Youtube_Service`.crawl_cursor_current
ON CLUSTER statground_cluster
AS `Data_R_Youtube_Service`.crawl_cursor_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Youtube_Service', 'crawl_cursor_current_local', cityHash64(cursor_scope, source_key))
COMMENT 'Distributed current R YouTube collector crawl cursor state.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Service`.mv_r_youtube_event_to_crawl_cursor_current
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Service`.crawl_cursor_current_local
AS
SELECT
    JSONExtractString(payload, 'cursor_scope') AS cursor_scope,
    JSONExtractString(payload, 'source_key') AS source_key,
    JSONExtractString(payload, 'next_page_token') AS next_page_token,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'last_success_at'), 3, 'Asia/Seoul') AS last_success_at,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'last_failure_at'), 3, 'Asia/Seoul') AS last_failure_at,
    toUInt32OrZero(JSONExtractString(payload, 'failure_count')) AS failure_count,
    JSONExtractString(payload, 'last_error_code') AS last_error_code,
    if(JSONExtractString(payload, 'active') = '0', 0, 1) AS active,
    payload AS payload_json,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_event_raw_local
WHERE event_type = 'r.youtube.crawl.cursor.v1';
