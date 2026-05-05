CREATE TABLE IF NOT EXISTS Data_R_Project_Mastodon_Log.kafka_ingest_error_local
ON CLUSTER statground_cluster
(
    event_uuid_raw String COMMENT 'Raw event_uuid string from Kafka message if parsed; may be empty for parser errors',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic where malformed or invalid message was consumed',
    kafka_partition UInt32 COMMENT 'Kafka partition of malformed or invalid message',
    kafka_offset UInt64 COMMENT 'Kafka offset of malformed or invalid message',
    event_type LowCardinality(String) COMMENT 'Event type if parsed; empty when JSON parsing failed',
    raw_message String COMMENT 'Original raw Kafka message for parser errors or payload JSON for semantic errors; replay/debug only',
    error_message String COMMENT 'ClickHouse parser error or semantic validation error message',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse error capture timestamp in Asia/Seoul'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Project_Mastodon_Log/kafka_ingest_error_local', '{replica}')
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, kafka_topic, kafka_partition, kafka_offset)
SETTINGS index_granularity = 8192
COMMENT 'Mastodon Kafka ingestion parser/semantic error local replicated table; OLAP 로그 전용; SSOT 아님';
