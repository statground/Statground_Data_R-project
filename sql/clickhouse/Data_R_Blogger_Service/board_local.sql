CREATE TABLE IF NOT EXISTS Data_R_Blogger_Service.board_local
ON CLUSTER statground_cluster
(
    uuid UUID,
    title Nullable(String),
    content Nullable(String),
    active Nullable(UInt8),
    created_at Nullable(DateTime64(3, 'Asia/Seoul')),
    updated_at Nullable(DateTime64(3, 'Asia/Seoul')),
    created_log Nullable(JSON),
    updated_log Nullable(JSON),
    language_code LowCardinality(String) DEFAULT 'ko' COMMENT 'BCP-47 language code of written text; migrated legacy rows are ko',
    version_at DateTime64(3, 'Asia/Seoul') MATERIALIZED coalesce(updated_at, created_at, now64(3, 'Asia/Seoul'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Blogger_Service/board_local', '{replica}')
ORDER BY (uuid, language_code, version_at)
SETTINGS index_granularity = 8192
COMMENT 'R-bloggers curated board/version rows local replicated table; source legacy webr_board.rblogger';
