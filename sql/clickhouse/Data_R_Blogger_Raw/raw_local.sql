CREATE TABLE IF NOT EXISTS Data_R_Blogger_Raw.raw_local
ON CLUSTER statground_cluster
(
    uuid UUID,
    created_at Nullable(DateTime64(3, 'Asia/Seoul')),
    created_log Nullable(JSON),
    updated_at Nullable(DateTime64(3, 'Asia/Seoul')),
    updated_log Nullable(JSON),
    active Nullable(UInt8),
    github_path Nullable(String),
    title Nullable(String),
    content Nullable(String),
    url Nullable(String),
    url_hash String,
    language_code LowCardinality(String) DEFAULT 'en' COMMENT 'BCP-47 language code of written text; migrated R-bloggers raw rows are en',
    created_at_key DateTime64(3, 'Asia/Seoul') MATERIALIZED coalesce(created_at, updated_at, now64(3, 'Asia/Seoul'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Blogger_Raw/raw_local', '{replica}')
ORDER BY (url_hash, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R-bloggers raw crawled article rows local replicated table; legacy webr_board_raw.rblogger renamed to raw; source language en';
