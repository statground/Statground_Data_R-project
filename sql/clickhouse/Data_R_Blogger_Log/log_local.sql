CREATE TABLE IF NOT EXISTS Data_R_Blogger_Log.log_local
ON CLUSTER statground_cluster
(
    uuid UUID,
    created_at Nullable(DateTime64(3, 'Asia/Seoul')),
    created_log Nullable(JSON),
    language_code LowCardinality(String) DEFAULT 'en' COMMENT 'BCP-47 language code associated with the crawled source; migrated R-bloggers log rows are en',
    created_at_key DateTime64(3, 'Asia/Seoul') MATERIALIZED coalesce(created_at, now64(3, 'Asia/Seoul'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Blogger_Log/log_local', '{replica}')
ORDER BY (created_at_key, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R-bloggers crawl log local replicated table; source legacy webr_board_raw.rblogger_log; source language en';
