CREATE TABLE IF NOT EXISTS `Data_R_Project_Mastodon_Mart`.status_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'Mastodon status report date',
    instance_host LowCardinality(String) COMMENT 'Mastodon instance host',
    account_acct LowCardinality(String) COMMENT 'Mastodon account acct',
    raw_active_count UInt64 COMMENT 'Active raw status count observed for the day',
    board_active_count UInt64 COMMENT 'Active Korean board row count observed for the day',
    image_status_count UInt64 COMMENT 'Active raw statuses with at least one embedded image',
    computed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart computation timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Project_Mastodon_Mart/status_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, instance_host, account_acct)
SETTINGS index_granularity = 8192
COMMENT 'Daily R Project Mastodon status mart local replicated table.';
