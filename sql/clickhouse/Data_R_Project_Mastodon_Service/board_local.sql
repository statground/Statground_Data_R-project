CREATE TABLE IF NOT EXISTS Data_R_Project_Mastodon_Service.board_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Stable Mastodon status UUID shared with raw.uuid',
    title Nullable(String) COMMENT 'Korean board title translated from source Mastodon status text',
    content Nullable(String) COMMENT 'Korean compact HTML board content translated from source Mastodon status text',
    active Nullable(UInt8) COMMENT '1=visible latest board translation, 0=tombstone/deactivated row',
    created_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Original Mastodon status created_at aligned to raw row',
    updated_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Translation refresh timestamp; NULL for first translation',
    created_log Nullable(JSON) COMMENT 'Translation creation metadata',
    updated_log Nullable(JSON) COMMENT 'Translation refresh metadata',
    language_code LowCardinality(String) DEFAULT 'ko' COMMENT 'BCP-47 language code of board text; translated Mastodon board rows are ko',
    version_at DateTime64(3, 'Asia/Seoul') MATERIALIZED coalesce(updated_at, created_at, now64(3, 'Asia/Seoul'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Project_Mastodon_Service/board_local', '{replica}')
ORDER BY (uuid, language_code, version_at)
SETTINGS index_granularity = 8192
COMMENT 'Mastodon curated Korean board/version rows local replicated table';
