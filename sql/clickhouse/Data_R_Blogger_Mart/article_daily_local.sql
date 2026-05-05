CREATE TABLE IF NOT EXISTS `Data_R_Blogger_Mart`.article_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'R-bloggers article report date',
    raw_active_count UInt64 COMMENT 'Active raw article count observed for the day',
    board_active_count UInt64 COMMENT 'Active Korean board row count observed for the day',
    stale_translation_count UInt64 COMMENT 'Rows requiring translation refresh when computed',
    computed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart computation timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Blogger_Mart/article_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY report_date
SETTINGS index_granularity = 8192
COMMENT 'Daily R-bloggers article mart local replicated table.';
